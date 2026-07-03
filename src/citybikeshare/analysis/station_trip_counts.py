import json

import polars as pl

from citybikeshare.context import PipelineContext
from citybikeshare.utils.io import write_json


def _build_canonical_name_map(
    canonical_stations: list[dict], id_keyed: bool
) -> dict[str, str]:
    """Map each raw trip station value → its canonical display name, so name variants of one
    physical station (the aliases ``canonicalize`` merged) are counted together. Name-keyed
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


def count_station_trips(context: PipelineContext):
    """Per-station, per-year counts of trips leaving (``trips_from``) and arriving
    (``trips_to``), from the transformed trip parquet. Both directions are bucketed by the
    trip's ``year`` (start-time based). Name variants are rolled up to one canonical station
    via ``station_coords_canonical.json`` when present;
    """
    city = context.city
    print(f"Counting station trips for: {city}")

    lf = pl.scan_parquet(context.transformed_directory / "**/*.parquet")
    schema = set(lf.collect_schema().names())
    # Trips are keyed by station name for every city (LA/Philadelphia get names appended in
    # transform); fall back to id for any still-id-keyed parquet (e.g. montreal/guadalajara).
    if {"start_station_name", "end_station_name"} <= schema:
        start_col, end_col, id_keyed = "start_station_name", "end_station_name", False
    elif {"start_station_id", "end_station_id"} <= schema:
        start_col, end_col, id_keyed = "start_station_id", "end_station_id", True
    else:
        raise ValueError(
            f"{city}: transformed data has no start/end station name or id columns"
        )

    departures = (
        lf.group_by(start_col, "year")
        .agg(pl.len().alias("trips_from"))
        .rename({start_col: "station"})
    )
    arrivals = (
        lf.group_by(end_col, "year")
        .agg(pl.len().alias("trips_to"))
        .rename({end_col: "station"})
    )
    counts = (
        departures.join(arrivals, on=["station", "year"], how="full", coalesce=True)
        .with_columns(
            pl.col("trips_from").fill_null(0), pl.col("trips_to").fill_null(0)
        )
        .collect()
    )

    # Dockless / null-station endpoints: excluded from the per-station set, reported.
    dockless = counts.filter(pl.col("station").is_null())
    dockless_trips = int(dockless["trips_from"].sum() + dockless["trips_to"].sum())
    counts = counts.filter(pl.col("station").is_not_null())

    # Roll name variants up to their canonical station name when the canonical file exists.
    unmatched = 0
    canon_path = context.analysis_directory / "station_coords_canonical.json"
    if canon_path.exists():
        name_map = _build_canonical_name_map(
            json.loads(canon_path.read_text()), id_keyed
        )
        map_df = pl.DataFrame(
            {"station": list(name_map.keys()), "canonical": list(name_map.values())},
            schema={"station": pl.String, "canonical": pl.String},
        )
        counts = counts.join(map_df, on="station", how="left")
        unmatched = counts.filter(pl.col("canonical").is_null())["station"].n_unique()
        counts = (
            counts.with_columns(
                pl.col("canonical").fill_null(pl.col("station")).alias("station")
            )
            .drop("canonical")
            .group_by("station", "year")
            .agg(pl.col("trips_from").sum(), pl.col("trips_to").sum())
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
