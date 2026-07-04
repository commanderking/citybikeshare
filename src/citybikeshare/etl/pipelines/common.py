import os
import json
import re
import polars as pl
from citybikeshare.etl.constants import (
    DEFAULT_FINAL_COLUMNS,
    BICYCLE_TRANSIT_SYSTEMS_RENAMED_STATION_COLUMNS,
)

from citybikeshare.context import PipelineContext


def rename_columns_for_keys(renamed_columns_dict):
    def inner(df):
        headers = df.collect_schema().names()
        relevant_columns = {
            key: renamed_columns_dict[key]
            for key in headers
            if key in renamed_columns_dict
        }
        renamed_columns = df.rename(relevant_columns)
        return renamed_columns

    return inner


def _parse_date_columns(df, columns, date_formats):
    """Lazily parse string date columns. Each original value is stashed as `<col>_pre_clean`
    (so a genuinely-empty value can later be told apart from one that matched no format),
    then coalesced over the formats — first match wins, null if none match."""
    return df.with_columns(
        [pl.col(c).alias(f"{c}_pre_clean") for c in columns]
    ).with_columns(
        [
            pl.coalesce(
                [
                    pl.col(c)
                    .str.replace(r"\.\d+", "")  # strip fractional seconds
                    .str.strptime(pl.Datetime, fmt, strict=False)
                    for fmt in date_formats
                ]
            ).alias(c)
            for c in columns
        ]
    )


def _assert_column_parsed(frame, column, guidance, pre_clean=None):
    """Raise if a value was present in the source but failed to parse — the parsed
    `column` is null while its `<column>_pre_clean` original was present and non-blank.
    Genuinely empty/missing values pass through as null.

    """
    pre_clean = pre_clean or f"{column}_pre_clean"
    null_count = frame[column].is_null().sum()
    if not null_count:
        return

    bad = frame.filter(
        pl.col(column).is_null()
        & pl.col(pre_clean).is_not_null()
        & (pl.col(pre_clean).str.strip_chars() != "")
    )
    if len(bad):
        examples = bad[pre_clean].unique().head(5).to_list()
        raise ValueError(
            f"{len(bad)} value(s) in column '{column}' could not be parsed. "
            f"Examples: {examples}. {guidance}"
        )

    print(f"ℹ️  {column}: {null_count} row(s) had no value (left null).")


def _assert_all_dates_parsed(frame, columns, date_formats):
    """Raise if a column has a value that was present in the source but matched none of
    date_formats — a format the city's YAML doesn't account for. Genuinely empty/missing
    values are allowed through as null."""
    guidance = (
        f"They matched none of date_formats={date_formats}. Add the matching "
        f"format(s) to the city's date_formats in its YAML."
    )
    for column in columns:
        _assert_column_parsed(frame, column, guidance)


def convert_columns_to_datetime(date_column_names, date_formats, time_unit: str = "ms"):
    """
    Convert one or more columns to datetime and raise error if there are date formts unaccounted for

    Parameters
    ----------
    date_column_names : list[str]
        Names of columns to convert.
    date_formats : list[str]
        Possible date string formats (Polars-compatible strptime formats).
    time_unit : str, default 'ms'
        Target datetime precision ('us', 'ms', or 'ns').
    """

    def inner(df):
        schema = df.collect_schema()
        columns_to_parse = [
            c for c in date_column_names if schema.get(c) not in (pl.Datetime, pl.Date)
        ]

        if columns_to_parse:
            df = _parse_date_columns(df, columns_to_parse, date_formats)
        else:
            print("✅ All datetime columns already parsed")

        # Normalize precision for every date column (parsed or already a datetime).
        df = df.with_columns(
            [pl.col(c).cast(pl.Datetime(time_unit)).alias(c) for c in date_column_names]
        )

        # No string columns needed parsing -> nothing to validate, stay fully lazy/streaming.
        if not columns_to_parse:
            return df

        # Materialize ONCE (the single CSV scan+parse). Validate on the in-memory frame,
        # then hand downstream a memory-backed lazy frame so sink_parquet won't re-parse.
        frame = df.collect(engine="streaming")
        _assert_all_dates_parsed(frame, columns_to_parse, date_formats)
        return frame.drop([f"{c}_pre_clean" for c in columns_to_parse]).lazy()

    return inner


def select_final_columns(df, final_columns):
    ### Ensure all the final_columns exist
    current_headers = df.collect_schema().names()

    missing_headers = [
        column for column in final_columns if column not in current_headers
    ]

    print(current_headers)

    add_headers = [pl.lit(None).alias(col) for col in missing_headers]

    if add_headers:
        print(
            f"⚠️ file does not have headers {missing_headers}. Adding with default to null"
        )
        df = df.with_columns(add_headers)
    return df.select(final_columns)


# Know this applies to Philadelphia, Mexico City, and Vancouver
def offset_two_digit_years(df):
    return df.with_columns(
        [
            pl.when(pl.col("start_time").dt.year() < 100)
            .then(pl.col("start_time").dt.offset_by("2000y"))
            .otherwise(pl.col("start_time"))
            .alias("start_time"),
            pl.when(pl.col("end_time").dt.year() < 100)
            .then(pl.col("end_time").dt.offset_by("2000y"))
            .otherwise(pl.col("end_time"))
            .alias("end_time"),
        ]
    )


def calculate_end_time(df, context):
    """
    Parse Austin start_time using all available date_formats,
    ensure duration_minutes is numeric, and compute end_time.
    """
    date_formats = context.get("date_formats", ["%m/%d/%Y %I:%M:%S %p"])

    # Have consistent start time
    df = df.with_columns(
        [
            pl.coalesce(
                [
                    pl.col("start_time")
                    .str.replace(r"\.\d+", "")  # strip decimals
                    .str.strptime(pl.Datetime, fmt, strict=False)
                    for fmt in date_formats
                ]
            ).alias("start_time"),
            ## Austin has some numbers that have commas (1,027)
            pl.col("duration_minutes").str.replace_all(",", "").cast(pl.Int32),
        ]
    )

    # Compute end_time
    df = df.with_columns(
        (pl.col("start_time") + pl.duration(minutes=pl.col("duration_minutes"))).alias(
            "end_time"
        )
    )

    return df


def convert_milliseconds_to_datetime(df):
    headers = df.collect_schema().names()
    ### most recent Montreal data notes start time and end time in ms whereas previous versions used a date.
    if "start_ms" in headers:
        df = df.with_columns(
            # start_ms auto converts to string instead of integer - cast before converting to datetime
            [pl.col("start_ms").cast(pl.Int64), pl.col("end_ms").cast(pl.Int64)]
        ).with_columns(
            [
                pl.from_epoch("start_ms", time_unit="ms").alias("start_time"),
                pl.from_epoch("end_ms", time_unit="ms").alias("end_time"),
            ]
        )
    return df


def filter_null_rows(df):
    return df.filter(~pl.all_horizontal(pl.all().is_null()))


def get_stations_df(context: PipelineContext):
    metadata_path = context.metadata_directory

    station_info_json = metadata_path / "station_information.json"
    stations = []
    with open(station_info_json, "r") as file:
        data = json.load(file)
        stations = data["data"]["stations"]
    df = pl.DataFrame(stations).select(["station_id", "name"])
    return df.lazy()


def handle_oslo_legacy_stations(df, config, context: PipelineContext):
    stations_df = get_stations_df(context)
    metadata_path = context.metadata_directory

    stations_df = stations_df.select(["station_id", "name"]).with_columns(
        [pl.col("station_id").cast(pl.Int64)]
    )

    headers = df.collect_schema().names()
    ### Older data does not contain duration column
    if "duration" not in headers:
        station_mapping_df = pl.scan_csv(
            metadata_path / "legacy_new_station_id_mapping.csv"
        ).with_columns(pl.col("legacy_id").cast(pl.String))
        df = (
            df.rename(
                {
                    "start_station_id": "start_station_legacy_id",
                    "end_station_id": "end_station_legacy_id",
                }
            )
            .join(
                station_mapping_df,
                left_on="start_station_legacy_id",
                right_on="legacy_id",
            )
            .rename({"new_id": "start_station_id"})
            .join(
                station_mapping_df,
                left_on="end_station_legacy_id",
                right_on="legacy_id",
            )
            .rename({"new_id": "end_station_id"})
            .join(stations_df, left_on="start_station_id", right_on="station_id")
            .rename({"name": "start_station_name"})
            .join(stations_df, left_on="end_station_id", right_on="station_id")
            .rename({"name": "end_station_name"})
        )
    return df


def get_guadalajara_stations_df(context: PipelineContext):
    """
    Load the stations DataFrame from a file starting with 'nomenclatura'.
    """
    download_path = context.download_directory

    files = list(download_path.glob("nomenclatura*.csv"))
    if not files:
        raise FileNotFoundError(
            "No file starting with 'nomenclatura' found in the directory."
        )
    station_info_csv = files[0]
    return (
        pl.scan_csv(station_info_csv, encoding="utf8-lossy")
        .select(["id", "name"])
        .with_columns(pl.col("id").cast(pl.String))
    )


def handle_guadalajara_stations(df, config, context):
    stations_df = get_guadalajara_stations_df(context)

    df = (
        df.join(stations_df, left_on="start_station_id", right_on="id")
        .rename({"name": "start_station_name"})
        .join(stations_df, left_on="end_station_id", right_on="id")
        .rename({"name": "end_station_name"})
    )
    return df


def get_mexico_city_stations_lf(context: PipelineContext):
    metadata_path = context.metadata_directory
    station_file = metadata_path / "station_information.json"

    stations = []
    with open(station_file) as f:
        results = json.load(f)
        stations = results["data"]["stations"]
    stations_lf = pl.LazyFrame(stations).select(["station_id", "name"])
    return stations_lf


def join_mexico_city_station_names(df, config, context):
    stations_lf = get_mexico_city_stations_lf(context)
    return (
        df.join(
            stations_lf,
            left_on="start_station_id",
            right_on="station_id",
            how="left",
        )
        .rename({"name": "start_station_name"})
        .join(
            stations_lf,
            left_on="end_station_id",
            right_on="station_id",
            how="left",
        )
        .rename({"name": "end_station_name"})
    )


def clean_datetimes(df):
    # Mexico City Specific For now
    date_formats = ["%m-%d-%Y", "%d/%m/%Y", "%Y-%m-%d"]

    time_formats = ["%H:%M:%S", "%H:%M:%S %p"]

    return df.with_columns(
        [
            # Parse start/end dates
            pl.coalesce(
                [
                    pl.col("start_date").str.strptime(pl.Datetime, fmt, strict=False)
                    for fmt in date_formats
                ]
            ).alias("start_date"),
            pl.coalesce(
                [
                    pl.col("end_date").str.strptime(pl.Datetime, fmt, strict=False)
                    for fmt in date_formats
                ]
            ).alias("end_date"),
            # Parse times (handling fractional seconds + padding)
            pl.coalesce(
                [
                    pl.col("starting_time")
                    .str.replace(r"\.\d+", "")
                    .str.zfill(8)
                    .str.strptime(pl.Time, fmt, strict=False)
                    for fmt in time_formats
                ]
            ).alias("starting_time"),
            pl.coalesce(
                [
                    pl.col("ending_time")
                    .str.replace(r"\.\d+", "")
                    .str.zfill(8)
                    .str.strptime(pl.Time, fmt, strict=False)
                    for fmt in time_formats
                ]
            ).alias("ending_time"),
        ]
    )


def combine_datetimes(df):
    return df.with_columns(
        [
            pl.col("start_date")
            .dt.combine(pl.col("starting_time"))
            .alias("start_time"),
            pl.col("end_date").dt.combine(pl.col("ending_time")).alias("end_time"),
        ]
    )


def stations_csv_to_df(context):
    city = context.city
    download_path = context.download_directory
    # LA has station names with special characters: CicLAvia South LA � Exposition Hub
    df = pl.scan_csv(os.path.join(download_path, "stations.csv"), encoding="utf8-lossy")
    return df.pipe(
        rename_columns_for_keys(BICYCLE_TRANSIT_SYSTEMS_RENAMED_STATION_COLUMNS)
    ).with_columns(
        [
            pl.col("station_id").cast(pl.String),
        ]
    )


def append_station_names(trips_df, stations_df):
    joined_df = (
        trips_df.join(
            stations_df.select(["station_id", "station_name"]),
            left_on="start_station_id",
            right_on="station_id",
            how="left",
        )
        .with_columns(pl.col("station_name").alias("start_station_name"))
        .join(
            stations_df.select(["station_id", "station_name"]),
            left_on="end_station_id",
            right_on="station_id",
            how="left",
        )
        .with_columns(pl.col("station_name").alias("end_station_name"))
        .drop(["station_name", "station_name_right"])
    )

    return joined_df


def process_bicycle_transit_system(df, context):
    stations_df = stations_csv_to_df(context)
    df = append_station_names(df, stations_df).drop(
        "start_station_id", "end_station_id"
    )
    return df


def _assert_durations_parsed(frame):
    """Raise if a non-empty duration value failed to parse to seconds — i.e. it
    didn't match HH:MM:SS. Genuinely empty/missing values are allowed through as
    null. Delegates to _assert_column_parsed so the loud-error behavior matches dates.
    """
    _assert_column_parsed(
        frame,
        "duration",
        "They did not match HH:MM:SS. The source format may have changed, or these "
        "rows are corrupt. Fix the source, or add the value(s) under "
        "'invalid_values: duration' in the city's YAML to drop those rows.",
    )


def remove_invalid_rows(df, invalid_values):
    """Drop rows whose value in a column matches a curated list of known-invalid
    values, logging what's removed so it stays visible.

    `invalid_values` maps column -> exact raw values to drop. This handles KNOWN
    garbage explicitly; anything *not* listed that still fails to parse trips the loud
    validators downstream (the safety net). Runs after the data is materialized
    (e.g. by convert_to_datetime) so the audit collect hits the in-memory frame
    rather than re-scanning the CSV.
    """
    for column, bad_values in invalid_values.items():
        if not bad_values:
            continue
        bad_mask = pl.col(column).is_in(list(bad_values))
        dropped = df.filter(bad_mask).select(column).collect()  # tiny: only matches
        if dropped.height:
            breakdown = dropped[column].value_counts().to_dicts()
            print(
                f"🧹 remove_invalid_rows: dropped {dropped.height} row(s) on "
                f"'{column}': {breakdown}"
            )
        df = df.filter(~bad_mask)
    return df


def handle_odd_hour_duration(df):
    ### HH:MM:SS - but hours can go over 24 for Taipei. Known-corrupt values are
    ### removed upstream by remove_invalid_rows; anything left that isn't HH:MM:SS
    ### raises in _assert_durations_parsed rather than being silently nulled.

    # strict=False is intentional: a malformed component becomes null *here* so
    # _assert_durations_parsed can flag it with a descriptive error (and tell an
    # empty-but-valid value apart from a corrupt one). With strict=True the cast
    # would throw a cryptic InvalidOperationError before validation runs. This is
    # the same parse-leniently-then-assert pattern the date parsing uses.
    parts = pl.col("duration").str.split_exact(":", 3)
    df = df.with_columns(
        pl.col("duration").alias("duration_pre_clean"),
        (
            # hour to seconds
            parts.struct.field("field_0").cast(pl.Int64, strict=False) * 3600
            # minutes to seconds
            + parts.struct.field("field_1").cast(pl.Int64, strict=False) * 60
            + parts.struct.field("field_2").cast(pl.Int64, strict=False)
        ).alias("duration"),
    )

    # Materialize once to validate, then hand downstream a memory-backed lazy frame.
    frame = df.collect(engine="streaming")
    _assert_durations_parsed(frame)
    return frame.drop("duration_pre_clean").lazy()


def clean_header_quotes(df: pl.DataFrame) -> pl.DataFrame:
    """
    Strip only leading/trailing single or double quotes from column names.

    Examples:
    ---------
    '"자전거번호"'  →  '자전거번호'
    "'대여일시"    →  '대여일시'
    "Bob's Station" → "Bob's Station" (unchanged)
    """
    cleaned = {}
    for col in df.columns:
        # Remove a single pair of quotes at the beginning/end if both exist
        new_col = re.sub(r"^['\"](.*)['\"]$", r"\1", col.strip())
        cleaned[col] = new_col
    return df.rename(cleaned)


## In Lyft bikeshares, gender and birthyears are often fully null in later years
## To avoid polars reading these as a column of only nulls, cast to Utf-8
def cast_optional_columns(df: pl.LazyFrame) -> pl.LazyFrame:
    headers = df.collect_schema().names()
    exprs = []

    if "bike_id" in headers:
        exprs.append(pl.col("bike_id").cast(pl.Utf8))

    if "gender" in headers:
        exprs.append(pl.col("gender").cast(pl.Utf8))

    if "birth_year" in headers:
        exprs.append(pl.col("birth_year").cast(pl.Utf8))

    if "bike_type" in headers:
        exprs.append(pl.col("bike_type").cast(pl.Utf8))

    if "membership_type" in headers:
        exprs.append(pl.col("membership_type").cast(pl.Utf8))

    if exprs:
        df = df.with_columns(exprs)

    return df


def _resolve_montreal_station_file_path(raw_dir, fname):
    """Locate a station file under raw/, tolerating the gzip suffix (raw is stored .csv.gz)."""
    path = raw_dir / fname
    if path.exists():
        return path
    gz = raw_dir / f"{fname}.gz"
    if gz.exists():
        return gz
    raise ValueError(
        f"Montreal station file '{fname}' (or .gz) not found in {raw_dir}. Check "
        f"`station_files` in montreal.yaml."
    )


def _read_montreal_station_file(raw_dir, fname):
    """Read one Montreal station file into a [code, name] frame. The id header varies by
    file — code (most), Code (Stations_2019), pk (2021_stations) — so take whichever is
    present. (polars reads the .csv.gz transparently.)"""
    path = _resolve_montreal_station_file_path(raw_dir, fname)
    df = pl.read_csv(path, infer_schema_length=0, encoding="utf8-lossy")
    id_col = next((c for c in ("code", "Code", "pk") if c in df.columns), None)
    if id_col is None:
        raise ValueError(
            f"Montreal station file {path.name} has no code/Code/pk column; has {df.columns}"
        )
    return (
        df.select(
            pl.col(id_col).cast(pl.String).str.strip_chars().alias("code"),
            pl.col("name").cast(pl.String).str.strip_chars().alias("name"),
        )
        .filter(
            pl.col("code").is_not_null()
            & (pl.col("code") != "")
            & pl.col("name").is_not_null()
            & (pl.col("name") != "")
        )
        .unique(subset="code", keep="last")
    )


def _build_montreal_code_maps(station_files, raw_dir):
    """Build the two lookup frames for the era-A numeric-code join (see montreal.yaml eras):

    * year_map [year, code, name] — the per-year station files, so a code that was reused
      for a different physical station in a later year resolves to the RIGHT name for the
      trip's own year (the reason this can't be a latest-wins union).
    * last_known_name_map [code, name] — the most recent name any year ever gave each code.
      Consulted ONLY when a code is absent from its own trip year's file. As of 2026-07 this
      rescues exactly two codes, both referenced by 2019 trips but missing from
      Stations_2019.csv: 6708 (→ "de Bleury / de Maisonneuve", its 2020 name) and 6034
      (→ "St-Urbain / René-Lévesque", last listed 2018). ~25k endpoints.
    """
    per_year = {
        int(year): _read_montreal_station_file(raw_dir, fname)
        for year, fname in station_files.get("code_years", {}).items()
    }
    year_map = pl.concat(
        [
            frame.with_columns(pl.lit(year, dtype=pl.Int64).alias("year"))
            for year, frame in per_year.items()
        ],
        how="vertical_relaxed",
    ).select("year", "code", "name")

    # Latest name any year gave each code (files concatenated in ascending year → keep last).
    last_known_name_map = pl.concat(
        [per_year[y] for y in sorted(per_year)], how="vertical_relaxed"
    ).unique(subset="code", keep="last")

    return year_map.lazy(), last_known_name_map.lazy()


def _resolve_code_endpoints(df, year_map, last_known_name_map):
    """Attach start/end station names by joining each endpoint's numeric code on the trip's
    own year (exact station file), then filling any miss from last_known_name_map — the
    latest name any year gave that code (only two 2019 codes need it; see
    _build_montreal_code_maps)."""
    df = df.with_columns(pl.col("start_time").dt.year().cast(pl.Int64).alias("_year"))
    for prefix in ("start", "end"):
        key = f"{prefix}_station_code"
        df = (
            df.join(
                year_map, left_on=["_year", key], right_on=["year", "code"], how="left"
            )
            .rename({"name": "_name_exact_year"})
            .join(last_known_name_map, left_on=key, right_on="code", how="left")
            .rename({"name": "_name_last_known"})
            .with_columns(
                pl.coalesce(["_name_exact_year", "_name_last_known"]).alias(
                    f"{prefix}_station_name"
                )
            )
            .drop("_name_exact_year", "_name_last_known")
        )
    return df.drop("_year")


def _resolve_pk_endpoints(df, pk_map):
    """Attach start/end station names for era B, whose stations are keyed by emplacement_pk
    in their own namespace (a single station file, no per-year reuse issue)."""
    for prefix, key in (
        ("start", "emplacement_pk_start"),
        ("end", "emplacement_pk_end"),
    ):
        df = df.join(pk_map, left_on=key, right_on="code", how="left").rename(
            {"name": f"{prefix}_station_name"}
        )
    return df


def _assert_montreal_names_resolved(df, pairs):
    """Raise if a station code/pk was present in the source but no station
    file mapped it to a name (it would otherwise pass through as a silent null). `pairs` is a
    list of (key_column, name_column). This is what fires when a new year's station file
    wasn't added"""
    audits = [
        df.filter(
            pl.col(key).is_not_null()
            & (pl.col(key).str.strip_chars() != "")
            & pl.col(name).is_null()
        )
        .select(pl.col(key).alias("key"))
        .unique()
        .head(25)
        for key, name in pairs
    ]
    unresolved = sorted(set(pl.concat(pl.collect_all(audits))["key"].to_list()))
    if unresolved:
        raise ValueError(
            f"Montreal: {len(unresolved)} station code/pk value(s) had no station-file match "
            f"and would be left nameless. Examples: {unresolved[:25]}. Add/fix the matching "
            f"station file under `station_files` in montreal.yaml."
        )


def resolve_montreal_station_names(df, config, context: PipelineContext):
    """Turn Montreal's numeric station keys into real names using the per-year station files.

    Era A/B trips carry only a numeric station key (era A code / era B emplacement_pk); the
    human name lives in the station files. Era C already carries the name inline and passes
    straight through. Runs after the timestamps are parsed so the code join can key on each
    trip's own year. See the eras block in montreal.yaml for what A/B/C mean."""
    headers = df.collect_schema().names()
    station_files = config.get("station_files", {})
    raw_dir = context.raw_directory

    # era A: numeric station code — join on the trip's own year.
    if "start_station_code" in headers:
        year_map, last_known_name_map = _build_montreal_code_maps(
            station_files, raw_dir
        )
        df = _resolve_code_endpoints(df, year_map, last_known_name_map)
        _assert_montreal_names_resolved(
            df,
            [
                ("start_station_code", "start_station_name"),
                ("end_station_code", "end_station_name"),
            ],
        )
        return df

    # era B: emplacement_pk — single station file.
    if "emplacement_pk_start" in headers:
        pk_map = _read_montreal_station_file(raw_dir, station_files["pk_file"]).lazy()
        df = _resolve_pk_endpoints(df, pk_map)
        _assert_montreal_names_resolved(
            df,
            [
                ("emplacement_pk_start", "start_station_name"),
                ("emplacement_pk_end", "end_station_name"),
            ],
        )
        return df

    # era C: trips already carry real station names — nothing to do.
    return df


PROCESSING_FUNCTIONS = {
    "rename_columns": lambda df, config, context: df.pipe(
        rename_columns_for_keys(config["renamed_columns"])
    ),
    "clean_header_quotes": lambda df, config, context: clean_header_quotes(df),
    "convert_to_datetime": lambda df, config, context: df.pipe(
        convert_columns_to_datetime(["start_time", "end_time"], config["date_formats"])
    ),
    "select_final_columns": lambda df, config, context: select_final_columns(
        df, config.get("final_columns", DEFAULT_FINAL_COLUMNS)
    ),
    "offset_two_digit_years": lambda df, config, context: offset_two_digit_years(df),
    "austin_calculate_end_time": lambda df, config, context: calculate_end_time(
        df, config
    ),
    "convert_milliseconds_to_datetime": lambda df,
    config,
    context: convert_milliseconds_to_datetime(df),
    ### Montreal
    "resolve_montreal_station_names": lambda df,
    config,
    context: resolve_montreal_station_names(df, config, context),
    "filter_null_rows": lambda df, config, context: filter_null_rows(df),
    # City-centric functions
    ### Oslo
    "handle_oslo_legacy_stations": lambda df,
    config,
    context: handle_oslo_legacy_stations(df, config, context),
    ### Philadelphia and Los Angeles
    "process_bicycle_transit_stations": lambda df,
    config,
    context: process_bicycle_transit_system(df, context),
    ### Guadalajara
    "handle_guadalajara_stations": lambda df,
    config,
    context: handle_guadalajara_stations(df, config, context),
    ### Taipei
    "remove_invalid_rows": lambda df, config, context: remove_invalid_rows(
        df, config.get("invalid_values", {})
    ),
    "handle_odd_hour_duration": lambda df, config, context: handle_odd_hour_duration(
        df
    ),
    ### Mexico City
    "join_mexico_city_station_names": lambda df,
    config,
    context: join_mexico_city_station_names(df, config, context),
    "clean_datetimes": lambda df, config, context: clean_datetimes(df),
    "combine_datetimes": lambda df, config, context: combine_datetimes(df),
    "cast_optional_columns": lambda df, config, context: cast_optional_columns(df),
}
