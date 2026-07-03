import csv
import gzip
import io
import json
from enum import StrEnum
from pathlib import Path
from typing import NamedTuple, Optional

import polars as pl

from citybikeshare.context import PipelineContext
from citybikeshare.config.loader import load_city_config
from citybikeshare.etl.transform import filter_filenames


class StationKey(StrEnum):
    NAME = "name"
    ID = "id"


def _list_raw_files(raw_directory: Path, config: dict):
    """Only use files that are used in the actual transform. Stations.csv file are excluded. Certain cities like NYC also have duplicated data in their s3 bucket that need to be filtered out"""
    files = [
        str(p) for p in (*raw_directory.glob("*.csv.gz"), *raw_directory.glob("*.csv"))
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


class _EndpointColumns(NamedTuple):
    key: str
    id: Optional[str]
    lat: str
    lng: str
    date: Optional[str]


def _resolve_endpoint_columns(
    header: set[str], columns: dict, key_role: StationKey, date_cands
) -> Optional[_EndpointColumns]:
    """Map one endpoint's role → actual header name for this file, selecting the station-key
    column by ``key_role``. Returns None when the required key/lat/lng aren't all present —
    schema drift across a city's eras is expected, and that file/endpoint is simply skipped."""
    key_c = _resolve_column(header, columns.get(key_role))
    lat_c = _resolve_column(header, columns.get("lat"))
    lng_c = _resolve_column(header, columns.get("lng"))
    if not (key_c and lat_c and lng_c):
        return None
    return _EndpointColumns(
        key=key_c,
        id=_resolve_column(header, columns.get("id")),
        lat=lat_c,
        lng=lng_c,
        date=_resolve_column(header, date_cands),
    )


def _select_station_coords(path: Path, cols: _EndpointColumns) -> pl.LazyFrame:
    """Project a file's resolved columns into the normalized station-observation schema
    [station, id, lat, lng, dt_raw]. All-Utf8 scan is robust to messy raw values."""
    lf = pl.scan_csv(path, infer_schema_length=0)
    return lf.select(
        pl.col(cols.key).str.strip_chars().alias("station"),
        (pl.col(cols.id) if cols.id else pl.lit(None)).cast(pl.String).alias("id"),
        pl.col(cols.lat).cast(pl.Float64, strict=False).alias("lat"),
        pl.col(cols.lng).cast(pl.Float64, strict=False).alias("lng"),
        (pl.col(cols.date) if cols.date else pl.lit(None))
        .cast(pl.String)
        .alias("dt_raw"),
    )


def _collect_coordinate_frames(
    coords_cfg: dict,
    raw_directory: Path,
    date_cands,
    config: dict,
    key_role: StationKey,
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
            cols = _resolve_endpoint_columns(header, columns, key_role, date_cands)
            if cols is not None:
                frames.append(_select_station_coords(path, cols))
    return frames


def _load_station_names(raw_directory: Path, names_cfg: dict) -> dict[str, str]:
    """id → human name from a source station-list file (e.g. Bicycle Transit Systems'
    ``stations.csv``), used to label id-keyed station records. Read straight from ``raw/``
    (gzip-transparent) so it tracks the same source of truth as the trip files. The id/name
    header candidates span a city's schema (Philadelphia's Station_ID / LA's "Kiosk ID")."""
    fname = names_cfg["file"]
    path = raw_directory / fname
    if not path.exists() and (raw_directory / f"{fname}.gz").exists():
        path = raw_directory / f"{fname}.gz"
    if not path.exists():
        raise ValueError(
            f"coordinates.station_names.file '{fname}' (or .gz) not found in {raw_directory}"
        )
    df = pl.read_csv(path, infer_schema_length=0, encoding="utf8-lossy")
    header = set(df.columns)
    id_c = _resolve_column(header, names_cfg.get("id"))
    name_c = _resolve_column(header, names_cfg.get("name"))
    if not (id_c and name_c):
        raise ValueError(
            f"station-names file {path.name} lacks the configured id/name columns "
            f"(id={names_cfg.get('id')}, name={names_cfg.get('name')}); has {sorted(header)}"
        )
    rows = (
        df.select(
            pl.col(id_c).cast(pl.String).str.strip_chars().alias("id"),
            pl.col(name_c).cast(pl.String).str.strip_chars().alias("name"),
        )
        .drop_nulls()
        .unique(subset="id", keep="last")
    )
    return dict(zip(rows["id"].to_list(), rows["name"].to_list()))


def _format_station_records(
    agg: pl.DataFrame, names: Optional[dict[str, str]] = None
) -> dict[str, dict]:
    """One JSON-ready record per station; dates as ISO strings (or null). When ``names`` is
    provided (id-keyed cities), each record also carries the human ``name`` looked up by its
    id key — null if the station-names file has no entry for that id."""
    out: dict[str, dict] = {}
    for r in agg.iter_rows(named=True):
        rec = {
            "lat": r["lat"],
            "lng": r["lng"],
            "id": r["id"],
            "n_obs": r["n_obs"],
            "first_seen": r["first_seen"].isoformat() if r["first_seen"] else None,
            "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
        }
        if names is not None:
            rec["name"] = names.get(r["station"])
        out[r["station"]] = rec
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
    # Which role identifies a station (see StationKey). Constructing from the config value
    # validates it — an unknown `key:` raises here rather than silently mis-keying.
    key_role = StationKey(coords_cfg.get("key", StationKey.NAME))
    names_cfg = coords_cfg.get("station_names")
    names = _load_station_names(context.raw_directory, names_cfg) if names_cfg else None
    date_cands = coords_cfg.get("date_column")
    frames = _collect_coordinate_frames(
        coords_cfg, context.raw_directory, date_cands, config, key_role
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
    #
    # `coordinates.date_formats` overrides the top-level list for THIS stage only. Needed when
    # a city's main pipeline parses a 2-digit year as year 00YY and fixes it in a later step
    # (philadelphia's offset_two_digit_years) — that step doesn't run here, so we parse the
    # 2-digit year correctly with an explicit "%m/%d/%y" instead of inheriting the 00YY wart.
    date_formats = coords_cfg.get("date_formats") or config.get("date_formats") or []
    parse_dt = (
        pl.coalesce(
            [pl.col("dt_raw").str.to_datetime(f, strict=False) for f in date_formats]
        )
        if date_formats
        else pl.col("dt_raw").str.to_datetime(strict=False)
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
    bounding_box = coords_cfg.get("bounding_box")
    if bounding_box:
        lat_min, lat_max, lng_min, lng_max = bounding_box
        inside = pl.col("lat").is_between(lat_min, lat_max) & pl.col("lng").is_between(
            lng_min, lng_max
        )
    else:
        inside = pl.lit(True)  # no box configured → nothing is out-of-box

    # Aggregate one file/endpoint at a time and merge the (tiny) partials, rather than
    # concatenating every row into one frame. Peak memory is then bounded by a single
    # file's size, not the whole city's row count — the one-shot streaming collect OOMs on
    # the largest cities (NYC ~600M endpoint-observations). Partials carry SUMS (sum_lat,
    # sum_lng, cnt) so the final mean = Σlat/Σcnt is identical to a single-pass mean.
    station_parts: list[
        pl.DataFrame
    ] = []  # station, sum_lat, sum_lng, cnt, id, dt_min, dt_max
    rejected_parts: list[pl.DataFrame] = []  # station, lat, lng, cnt, dt_min, dt_max
    named_total = 0  # coverage denominator
    dockless_total = 0  # nameless-but-coordinate rows, excluded + reported
    bad_dates: set[str] = set()  # distinct unparseable date strings (fail loud below)

    for lf in frames:
        base = lf.with_columns(parse_dt.dt.date().alias("dt"))
        named_lf = base.filter(has_name)
        located_lf = named_lf.filter(has_coord)
        clean_lf = located_lf.filter(inside)
        rejected_lf = located_lf.filter(~inside)

        # Per-station partial over kept rows: sums (not means) so partials are mergeable;
        # first non-null id is a representative (ids vary across eras).
        station_part = clean_lf.group_by("station").agg(
            pl.col("lat").sum().alias("sum_lat"),
            pl.col("lng").sum().alias("sum_lng"),
            pl.len().alias("cnt"),
            pl.col("id").drop_nulls().first().alias("id"),
            pl.col("dt").min().alias("dt_min"),
            pl.col("dt").max().alias("dt_max"),
        )
        # Out-of-box rows grouped to the distinct bad (name, point) — compact even when
        # many rows share one garbage coordinate (e.g. 0,0).
        rejected_part = rejected_lf.group_by("station", "lat", "lng").agg(
            pl.len().alias("cnt"),
            pl.col("dt").min().alias("dt_min"),
            pl.col("dt").max().alias("dt_max"),
        )
        # Parse-then-assert: a row whose dt_raw was present but parsed to null matched none
        # of date_formats. Collect the distinct offenders (capped) to fail loud below.
        bad_part = (
            clean_lf.filter(
                pl.col("dt").is_null()
                & pl.col("dt_raw").is_not_null()
                & (pl.col("dt_raw").str.strip_chars() != "")
            )
            .select("dt_raw")
            .unique()
            .head(5)
        )
        sp, rp, named_df, dockless_df, bad_df = pl.collect_all(
            [
                station_part,
                rejected_part,
                named_lf.select(pl.len()),
                base.filter(has_coord & ~has_name).select(pl.len()),
                bad_part,
            ],
            engine="streaming",
        )
        if sp.height:
            station_parts.append(sp)
        if rp.height:
            rejected_parts.append(rp)
        named_total += named_df.item()
        dockless_total += dockless_df.item()
        for v in bad_df["dt_raw"].to_list():
            if len(bad_dates) < 5:
                bad_dates.add(v)

    if bad_dates:
        raise ValueError(
            f"{city}: date value(s) matched none of date_formats={date_formats}: "
            f"{sorted(bad_dates)}. Add the matching format(s) to the city's date_formats "
            f"in its YAML."
        )

    # Merge the per-file partials: sum the sums/counts per station, then the point is the
    # trip-count-weighted mean Σlat/Σcnt (busy stations dominated by their many sightings).
    if station_parts:
        merged = (
            pl.concat(station_parts, how="vertical_relaxed")
            .group_by("station")
            .agg(
                pl.col("sum_lat").sum(),
                pl.col("sum_lng").sum(),
                pl.col("cnt").sum(),
                pl.col("id").drop_nulls().first().alias("id"),
                pl.col("dt_min").min(),
                pl.col("dt_max").max(),
            )
        )
        agg = merged.select(
            pl.col("station"),
            (pl.col("sum_lat") / pl.col("cnt")).round(6).alias("lat"),
            (pl.col("sum_lng") / pl.col("cnt")).round(6).alias("lng"),
            pl.col("id"),
            pl.col("cnt").alias("n_obs"),
            pl.col("dt_min").alias("first_seen"),
            pl.col("dt_max").alias("last_seen"),
        ).sort("n_obs", descending=True)
    else:
        agg = pl.DataFrame(
            schema={
                "station": pl.String,
                "lat": pl.Float64,
                "lng": pl.Float64,
                "id": pl.String,
                "n_obs": pl.UInt32,
                "first_seen": pl.Date,
                "last_seen": pl.Date,
            }
        )

    if rejected_parts:
        rejected_df = (
            pl.concat(rejected_parts, how="vertical_relaxed")
            .group_by("station", "lat", "lng")
            .agg(
                pl.col("cnt").sum().alias("n_obs"),
                pl.col("dt_min").min().alias("first_seen"),
                pl.col("dt_max").max().alias("last_seen"),
            )
            .sort("n_obs", descending=True)
        )
    else:
        rejected_df = pl.DataFrame(
            schema={
                "station": pl.String,
                "lat": pl.Float64,
                "lng": pl.Float64,
                "n_obs": pl.UInt32,
                "first_seen": pl.Date,
                "last_seen": pl.Date,
            }
        )

    # Coverage guard: a wholesale source change would null the coordinate columns and
    # quietly drop every station — fail loud rather than emit an empty/garbage set.
    # Denominator is named observations; numerator is those that landed on a usable in-box
    # point. Dockless (nameless) rows are excluded from both, so they can't mask a real
    # coordinate regression.
    total = named_total
    dockless = dockless_total
    kept = int(agg["n_obs"].sum()) if agg.height else 0
    coverage = (kept / total) if total else 0.0
    min_coverage = coords_cfg.get("min_coverage", 0.0)
    if coverage < min_coverage:
        raise ValueError(
            f"{city}: coordinate coverage {coverage:.1%} < required {min_coverage:.1%} "
            f"({kept:,}/{total:,} {key_role}-keyed station-observations on a usable in-box point). "
            f"Source format may have changed."
        )

    out = _format_station_records(agg, names)
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
        f"({kept:,}/{total:,} {key_role}-keyed station-observations) → {output_file}\n"
        f"   {rejected_rows:,} out-of-box rows ({len(rejected_records)} distinct points) "
        f"→ {rejected_file}\n"
        f"   {dockless:,} dockless (no {key_role}) rows excluded from the station set"
    )
