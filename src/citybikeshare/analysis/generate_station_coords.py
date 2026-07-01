import csv
import gzip
import io
import json
from pathlib import Path
from typing import Optional

import polars as pl

from citybikeshare.context import PipelineContext
from citybikeshare.config.loader import load_city_config
from citybikeshare.etl.transform import filter_filenames


def _list_raw_files(raw_directory: Path, config: dict):
    """Every raw input, gzipped or not, that the city's ``file_matcher`` accepts and
    ``excluded_filenames`` rejects — the SAME filtering the transform stage applies via
    ``filter_filenames``. Reusing it keeps the analysis stage reading exactly what the
    pipeline reads: it skips a city's excluded duplicate/cumulative archives (e.g. NYC's
    2013/2018 dupes, which would otherwise inflate n_obs) and non-trip files like a
    separate station-list CSV that happens to carry lat/lng. Globbing only ``*.csv``
    silently skips the gzipped inputs (a real bug class in this repo), so glob both."""
    files = [
        str(p)
        for p in (*raw_directory.glob("*.csv.gz"), *raw_directory.glob("*.csv"))
    ]
    return sorted(filter_filenames(files, config))


def _get_header(path: Path) -> list[str]:
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", errors="replace") as f:
        line = f.readline()
    return next(csv.reader(io.StringIO(line)))


def _resolve_column(header: set[str], candidates) -> Optional[str]:
    """First configured column name that actually exists in this file's header.
    Candidates may be a single name or a list (to span a city's schema eras)."""
    if candidates is None:
        return None
    if isinstance(candidates, str):
        candidates = [candidates]
    for c in candidates:
        if c in header:
            return c
    return None


def _select_station_coords(
    path: Path, header: set[str], columns: dict, date_cands
) -> Optional[pl.LazyFrame]:
    """Project one endpoint's (start or end) columns in one file into the normalized
    station-observation schema [station, id, lat, lng, dt_raw]. ``columns`` maps each
    role (name/id/lat/lng) to its candidate source headers. Returns None when this file
    lacks the coordinate columns for that endpoint (schema drift across eras is expected)."""
    name_c = _resolve_column(header, columns.get("name"))
    lat_c = _resolve_column(header, columns.get("lat"))
    lng_c = _resolve_column(header, columns.get("lng"))
    if not (name_c and lat_c and lng_c):
        return None
    id_c = _resolve_column(header, columns.get("id"))
    date_c = _resolve_column(header, date_cands)

    lf = pl.scan_csv(path, infer_schema_length=0)  # all-Utf8: robust to messy raw
    return lf.select(
        pl.col(name_c).str.strip_chars().alias("station"),
        (pl.col(id_c) if id_c else pl.lit(None)).cast(pl.String).alias("id"),
        pl.col(lat_c).cast(pl.Float64, strict=False).alias("lat"),
        pl.col(lng_c).cast(pl.Float64, strict=False).alias("lng"),
        (pl.col(date_c) if date_c else pl.lit(None)).cast(pl.String).alias("dt_raw"),
    )


def _collect_coordinate_frames(
    coords_cfg: dict, raw_directory: Path, date_cands, config: dict
) -> list[pl.LazyFrame]:
    """Project every raw file's start/end coordinate columns into the normalized
    station-observation schema. Files lacking coords for an endpoint are skipped
    (schema drift across eras is expected)."""
    frames: list[pl.LazyFrame] = []
    for path in _list_raw_files(raw_directory, config):
        header = set(_get_header(path))
        for start_or_end in ("start", "end"):
            columns = coords_cfg.get(start_or_end)
            if not columns:
                continue
            f = _select_station_coords(path, header, columns, date_cands)
            if f is not None:
                frames.append(f)
    return frames


def _format_station_records(agg: pl.DataFrame) -> dict[str, dict]:
    """One JSON-ready record per station; dates as ISO strings (or null)."""
    out: dict[str, dict] = {}
    for r in agg.iter_rows(named=True):
        out[r["station"]] = {
            "lat": r["lat"],
            "lng": r["lng"],
            "id": r["id"],
            "n_obs": r["n_obs"],
            "first_seen": r["first_seen"].isoformat() if r["first_seen"] else None,
            "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
        }
    return out


def generate_station_coords(context: PipelineContext):
    """Stage 1 — as-observed station coordinates, keyed by exact source name.

    Coordinates live per-trip in the raw CSVs but are dropped at ``final_columns``,
    so this reads ``raw/`` directly (not the transformed parquet). Output is lossless:
    every name string the source ever used, with its trip-count-weighted mean point
    and active date range. Deduping happens later in ``canonicalize_station_coords``.
    """
    city = context.city
    config = load_city_config(city)

    # Resolve config, bailing harmlessly (skip, not error) for cities this step can't
    # serve: those with no `coordinates` block, or a non-inline strategy (Tier B reads a
    # separate station file; external has no local coords). Inline is all we handle here.
    coords_cfg = config.get("coordinates")
    if not coords_cfg:
        print(f"⏭️  {city}: no `coordinates` config; skipping station coords")
        return

    strategy = coords_cfg.get("strategy")
    if strategy != "inline":
        # station_file / external strategies are handled elsewhere / not yet wired.
        reason = coords_cfg.get("reason", f"strategy '{strategy}' not yet supported")
        print(f"⏭️  {city}: coordinate strategy '{strategy}' — {reason}")
        return

    print(f"Generating station coords for: {city}")
    date_cands = coords_cfg.get("date_column")
    frames = _collect_coordinate_frames(
        coords_cfg, context.raw_directory, date_cands, config
    )
    if not frames:
        raise ValueError(
            f"{city}: `coordinates.strategy` is 'inline' but no raw file carried the "
            f"configured coordinate columns. Check the start/end column candidates."
        )

    # Parse dates with the city's configured formats (coalesce = try each in turn), not
    # polars' single-format inference. Inference picks ONE format from the data and
    # silently nulls every row in another era's format — Boston mixes "%Y-%m-%d %H:%M:%S"
    # with a fractional-seconds variant, and inference dropped ~80 stations' dates. A
    # no-match parses to null here only as an intermediate step; it's validated below
    # (parse-then-assert) so a genuinely new format fails loud rather than nulling silently.
    date_formats = config.get("date_formats") or []
    parse_dt = (
        pl.coalesce(
            [pl.col("dt_raw").str.to_datetime(f, strict=False) for f in date_formats]
        )
        if date_formats
        else pl.col("dt_raw").str.to_datetime(strict=False)
    )

    # Stack every file/endpoint frame into one long table of station observations.
    # `vertical_relaxed` tolerates the minor schema differences across eras (e.g. id is a
    # string in newer files, absent in older ones) instead of erroring on the mismatch.
    raw = pl.concat(frames, how="vertical_relaxed").with_columns(
        parse_dt.dt.date().alias("dt")
    )

    # named = observations that name a station — the universe we expect to geolocate, and
    # the coverage denominator. Nameless rows are dockless endpoints (a bike parked away
    # from any dock): they often carry a (privacy-rounded) coordinate but no station, so
    # they can't become a station record or alias and are excluded from the station set.
    # Keeping them out of *both* the numerator and denominator stops them from distorting
    # coverage; defining the denominator as "named" (rather than "has a coordinate") means
    # a source change that nulls coordinates for real stations still drops coverage loudly.
    has_name = pl.col("station").is_not_null() & (pl.col("station").str.len_chars() > 0)
    has_coord = pl.col("lat").is_not_null() & pl.col("lng").is_not_null()
    named = raw.filter(has_name)
    nameless_with_coord = raw.filter(has_coord & ~has_name)

    # clean = named observations on a usable point inside the plausibility box (kept);
    # rejected = named points that fell outside the box. We surface rejected to a sidecar
    # so a dropped point is always auditable, never silently discarded.
    located = named.filter(has_coord)
    clean = located
    rejected = located.filter(
        pl.lit(False)
    )  # nothing rejected when no bounding_box is configured
    bounding_box = coords_cfg.get("bounding_box")
    if bounding_box:
        lat_min, lat_max, lng_min, lng_max = bounding_box
        inside = pl.col("lat").is_between(lat_min, lat_max) & pl.col("lng").is_between(
            lng_min, lng_max
        )
        clean = located.filter(inside)
        rejected = located.filter(~inside)

    # Collapse all observations of a station name into one row. The point is the mean of
    # every coordinate seen for that name — trip-count-weighted, since each trip is a row,
    # so busy stations' points are dominated by their many sightings. n_obs records how
    # many observations back the point (a confidence signal); first/last_seen give the
    # active date range; pl.first("id") keeps a representative id (ids vary across eras).
    # Note this keys by exact name — merging near-identical names is canonicalize's job.
    station_agg = (
        clean.group_by("station")
        .agg(
            pl.col("lat").mean().round(6).alias("lat"),
            pl.col("lng").mean().round(6).alias("lng"),
            pl.first("id").alias("id"),
            pl.len().alias("n_obs"),
            pl.col("dt").min().alias("first_seen"),
            pl.col("dt").max().alias("last_seen"),
        )
        .sort("n_obs", descending=True)
    )

    # Out-of-box rows, grouped to the distinct bad (name, point) with a count and date
    # range — compact even when many rows share one garbage coordinate (e.g. 0,0).
    rejected_agg = (
        rejected.group_by("station", "lat", "lng")
        .agg(
            pl.len().alias("n_obs"),
            pl.col("dt").min().alias("first_seen"),
            pl.col("dt").max().alias("last_seen"),
        )
        .sort("n_obs", descending=True)
    )

    # Date validation (parse-then-assert, mirroring _assert_all_dates_parsed): a row whose
    # dt_raw was present and non-blank but parsed to null matched none of date_formats — a
    # format the YAML doesn't account for. Collect just the distinct offending strings
    # (cheap) so we can fail loud with examples instead of silently nulling the date.
    bad_dates = (
        clean.filter(
            pl.col("dt").is_null()
            & pl.col("dt_raw").is_not_null()
            & (pl.col("dt_raw").str.strip_chars() != "")
        )
        .select("dt_raw")
        .unique()
        .head(5)
    )

    # One scan over the raw rows: collect the aggregate, coverage denominator (named
    # observations), dockless count, rejected rows, and bad-date examples together so
    # polars shares it
    agg, total_df, dockless_df, rejected_df, bad_dates_df = pl.collect_all(
        [
            station_agg,
            named.select(pl.len()),
            nameless_with_coord.select(pl.len()),
            rejected_agg,
            bad_dates,
        ],
        engine="streaming",
    )
    if len(bad_dates_df):
        examples = bad_dates_df["dt_raw"].to_list()
        raise ValueError(
            f"{city}: date value(s) matched none of date_formats={date_formats}: "
            f"{examples}. Add the matching format(s) to the city's date_formats in its YAML."
        )

    # Coverage guard: a wholesale source change would null the coordinate columns and
    # quietly drop every station — fail loud rather than emit an empty/garbage set.
    # Denominator is named observations; numerator is those that landed on a usable in-box
    # point. Dockless (nameless) rows are excluded from both, so they can't mask a real
    # coordinate regression.
    total = total_df.item()
    dockless = dockless_df.item()
    kept = int(agg["n_obs"].sum())
    coverage = (kept / total) if total else 0.0
    min_coverage = coords_cfg.get("min_coverage", 0.0)
    if coverage < min_coverage:
        raise ValueError(
            f"{city}: coordinate coverage {coverage:.1%} < required {min_coverage:.1%} "
            f"({kept:,}/{total:,} named station-observations on a usable in-box point). "
            f"Source format may have changed."
        )

    out = _format_station_records(agg)
    # Dates serialize as ISO strings (or null); to_dicts would otherwise emit date objects.
    rejected_records = rejected_df.with_columns(
        pl.col("first_seen").cast(pl.String), pl.col("last_seen").cast(pl.String)
    ).to_dicts()

    context.analysis_directory.mkdir(parents=True, exist_ok=True)
    output_file = context.analysis_directory / "station_coords.json"
    with open(output_file, "w") as f:
        json.dump(out, f, indent=2, ensure_ascii=False, sort_keys=True)

    # Always written (an empty list when nothing was rejected) so the audit file is a
    # predictable place to look, not something that appears only on failure.
    rejected_file = context.analysis_directory / "station_coords_rejected.json"
    with open(rejected_file, "w") as f:
        json.dump(rejected_records, f, indent=2, ensure_ascii=False)

    rejected_rows = sum(r["n_obs"] for r in rejected_records)
    print(
        f"✅ {city}: {len(out)} stations, {coverage:.1%} coverage "
        f"({kept:,}/{total:,} named station-observations) → {output_file}\n"
        f"   {rejected_rows:,} out-of-box rows ({len(rejected_records)} distinct points) "
        f"→ {rejected_file}\n"
        f"   {dockless:,} dockless (nameless) rows excluded from the station set"
    )
