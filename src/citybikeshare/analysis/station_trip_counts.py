import json
from pathlib import Path

import polars as pl

from citybikeshare.context import PipelineContext
from citybikeshare.analysis.canonicalize_station_coords import CanonicalStation
from citybikeshare.utils.io import write_json


def _build_canonical_name_map(
    canonical_stations: list[CanonicalStation], id_keyed: bool
) -> dict[str, str]:
    """Map each raw trip station value → its canonical display name, so name variants of one
    physical station are counted together. Name-keyed
    trips map via each canonical record's name + aliases; id-keyed trips (a parquet still keyed
    by station id) map via its ``ids``."""
    mapping: dict[str, str] = {}
    for station in canonical_stations:
        keys = (
            station.get("ids", [])
            if id_keyed
            else [station["name"], *station.get("aliases", [])]
        )
        for key in keys:
            mapping[key] = station["name"]
    return mapping


def _resolve_station_columns(
    column_names: set[str], city: str
) -> tuple[str, str, bool]:
    """The (start, end) station columns the trips are keyed by, plus whether that key is a
    station id. Trips are keyed by name for every city (LA/Philadelphia get names appended in
    transform); fall back to id for any still-id-keyed parquet (e.g. montreal/guadalajara)."""
    if {"start_station_name", "end_station_name"} <= column_names:
        return "start_station_name", "end_station_name", False
    if {"start_station_id", "end_station_id"} <= column_names:
        return "start_station_id", "end_station_id", True
    raise ValueError(
        f"{city}: transformed data has no start/end station name or id columns"
    )


def _count_trips_per_station_year(
    trips_lf: pl.LazyFrame, start_station_col: str, end_station_col: str
) -> pl.DataFrame:
    """One row per (station, year) with trips leaving (``trips_from``) and arriving
    (``trips_to``); a station absent from one direction in a year gets 0 there."""
    departures = (
        trips_lf.group_by(start_station_col, "year")
        .agg(pl.len().alias("trips_from"))
        .rename({start_station_col: "station"})
    )
    arrivals = (
        trips_lf.group_by(end_station_col, "year")
        .agg(pl.len().alias("trips_to"))
        .rename({end_station_col: "station"})
    )
    return (
        departures.join(arrivals, on=["station", "year"], how="full", coalesce=True)
        .with_columns(
            pl.col("trips_from").fill_null(0), pl.col("trips_to").fill_null(0)
        )
        .collect()
    )


def _roll_up_name_variants(
    counts: pl.DataFrame, canonical_stations_path: Path, id_keyed: bool
) -> tuple[pl.DataFrame, int]:
    """Sum each station's name variants into its canonical station via
    ``station_coords_canonical.json``. Returns the rolled-up counts and the number of station
    names with no canonical match (kept as-is). No canonical file → counts unchanged, 0."""
    if not canonical_stations_path.exists():
        return counts, 0

    name_map = _build_canonical_name_map(
        json.loads(canonical_stations_path.read_text()), id_keyed
    )
    map_df = pl.DataFrame(
        {"station": list(name_map.keys()), "canonical": list(name_map.values())},
        schema={"station": pl.String, "canonical": pl.String},
    )
    counts = counts.join(map_df, on="station", how="left")
    unmatched = counts.filter(pl.col("canonical").is_null())["station"].n_unique()
    rolled = (
        counts.with_columns(
            pl.col("canonical").fill_null(pl.col("station")).alias("station")
        )
        .drop("canonical")
        .group_by("station", "year")
        .agg(pl.col("trips_from").sum(), pl.col("trips_to").sum())
    )
    return rolled, unmatched


def count_station_trips(context: PipelineContext):
    """Per-station, per-year counts of trips leaving and arriving
    from the transformed trip parquet. Both directions are bucketed by the
    trip's ``year`` (start-time based). Name variants are rolled up to one canonical station
    via ``station_coords_canonical.json`` when present;
    """
    city = context.city
    print(f"Counting station trips for: {city}")

    lf = pl.scan_parquet(context.transformed_directory / "**/*.parquet")
    column_names = set(lf.collect_schema().names())
    start_station_col, end_station_col, id_keyed = _resolve_station_columns(
        column_names, city
    )

    counts = _count_trips_per_station_year(lf, start_station_col, end_station_col)

    # Report dockless / null-station endpoints: excluded from the per-station set
    dockless_rows = counts.filter(pl.col("station").is_null())
    dockless_trips = int(
        dockless_rows["trips_from"].sum() + dockless_rows["trips_to"].sum()
    )
    counts = counts.filter(pl.col("station").is_not_null())

    canonical_stations_path = (
        context.analysis_directory / "station_coords_canonical.json"
    )
    counts, unmatched = _roll_up_name_variants(
        counts, canonical_stations_path, id_keyed
    )

    records = (
        counts.sort("station", "year")
        .select("station", "year", "trips_from", "trips_to")
        .to_dicts()
    )

    output_file = context.analysis_directory / "station_trip_counts.json"
    write_json(output_file, records, minified=True)

    print(
        f"✅ {city}: {counts['station'].n_unique():,} stations, {len(records):,} "
        f"station-year rows → {output_file}"
    )
    if unmatched:
        print(f"   {unmatched} station name(s) not in canonical set — kept un-rolled")
    if dockless_trips:
        print(f"   {dockless_trips:,} dockless (null-station) trip-endpoints excluded")
