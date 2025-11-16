import os
import json
import re
import polars as pl
from src.citybikeshare.etl.constants import (
    DEFAULT_FINAL_COLUMNS,
    BICYCLE_TRANSIT_SYSTEMS_RENAMED_STATION_COLUMNS,
)

from src.citybikeshare.context import PipelineContext


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


def convert_columns_to_datetime(date_column_names, date_formats, time_unit: str = "ms"):
    """
    Convert one or more columns to datetime

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

        # identify which columns still need parsing
        columns_to_parse = [
            c for c in date_column_names if schema.get(c) not in (pl.Datetime, pl.Date)
        ]

        df.with_columns(
            [pl.col(column).alias(f"{column}_pre_clean") for column in columns_to_parse]
        )
        if columns_to_parse:
            df = df.with_columns(
                ## Keep original date columns to enable logging original date columns
                [
                    pl.coalesce(
                        [
                            pl.col(column)
                            .str.replace(r"\.\d+", "")  # strip fractional seconds
                            .str.strptime(pl.Datetime, fmt, strict=False)
                            for fmt in date_formats
                        ]
                    ).alias(column)
                    for column in columns_to_parse
                ],
            )
        else:
            print("✅ All datetime columns already parsed")

        ## Log all the null columns
        for column in columns_to_parse:
            null_expr = pl.col(column).is_null().sum().alias("n_nulls")
            null_count = df.select(null_expr).collect().item()

            if null_count > 0:
                print(f"⚠️  {null_count} null values found in '{column}' after parsing.")
                # Collect only a few bad rows (with raw + parsed)
                bad_rows = df.filter(pl.col(column).is_null()).head(5).collect()
                print(bad_rows)

        # make sure date times are the same time unit (default ms)
        return df.with_columns(
            [
                pl.col(column).cast(pl.Datetime(time_unit)).alias(column)
                for column in date_column_names
            ]
        )

    return inner


def select_final_columns(df, final_columns):
    ### Ensure all the final_columns exist
    current_headers = df.collect_schema().names()

    missing_headers = [
        column for column in final_columns if column not in current_headers
    ]

    add_headers = [pl.lit(None).alias(col) for col in missing_headers]

    if add_headers:
        print(f"⚠️ file does not headers {missing_headers}. Adding with default to null")
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


def handle_odd_hour_duration(df):
    ### HH:MM:SS - but hours can go over 24 for Taipei
    parts = pl.col("duration").str.split_exact(":", 3)
    return df.with_columns(
        (
            # hour to seconds
            parts.struct.field("field_0").cast(pl.Int64) * 3600
            # minutes to seconds
            + parts.struct.field("field_1").cast(pl.Int64) * 60
            + parts.struct.field("field_2").cast(pl.Int64)
        ).alias("duration")
    )


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
def cast_gender_birthyear(df: pl.LazyFrame) -> pl.LazyFrame:
    headers = df.collect_schema().names()
    exprs = []

    if "gender" in headers:
        exprs.append(pl.col("gender").cast(pl.Utf8))

    if "birth_year" in headers:
        exprs.append(pl.col("birth_year").cast(pl.Utf8))

    if exprs:
        df = df.with_columns(exprs)

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
    "filter_null_rows": lambda df, config, context: filter_null_rows(df),
    # City-centric functions
    ### Oslo
    "handle_oslo_legacy_stations": lambda df,
    config,
    context: handle_oslo_legacy_stations(df, context.city, context),
    ### Philadelphia and Los Angeles
    "process_bicycle_transit_stations": lambda df,
    config,
    context: process_bicycle_transit_system(df, context),
    ### Guadalajara
    "handle_guadalajara_stations": lambda df,
    config,
    context: handle_guadalajara_stations(df, config, context),
    ### Taipei
    "handle_odd_hour_duration": lambda df, config, context: handle_odd_hour_duration(
        df
    ),
    ### Mexico City
    "join_mexico_city_station_names": lambda df,
    config,
    context: join_mexico_city_station_names(df, config, context),
    "clean_datetimes": lambda df, config, context: clean_datetimes(df),
    "combine_datetimes": lambda df, config, context: combine_datetimes(df),
    "cast_gender_birthyear": lambda df, config, context: cast_gender_birthyear(df),
}
