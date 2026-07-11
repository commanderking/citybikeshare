"""Committed, cumulative station coordinates for GBFS-sourced cities.

A GBFS feed lists only *currently active* stations, so replacing a stored copy on each
refresh would silently drop retired stations — and their historical trips would lose the
name + coordinate the feed used to provide. To keep them, we harvest each fetched GBFS
``station_information.json`` into a **committed**, cumulative table at
``config/station_coordinates/<city>.csv`` (id, name, lat, lng): newest values win per id, ids
are never dropped.

Refresh is by MERGE, never replace — that is the whole point. Values are stored as strings so
the on-disk round-trip is byte-stable and a no-op refresh doesn't churn git. The table does
NOT track when a station was active — the trip data is the authoritative source for that, so
the file changes only when a station's name or coordinate actually changes.
"""

import json
from pathlib import Path

import polars as pl

from citybikeshare.context import PipelineContext

STATION_COORDINATES_DIR = (
    Path(__file__).resolve().parents[1] / "config" / "station_coordinates"
)

_COORDINATE_COLUMNS = ["id", "name", "lat", "lng"]


def station_coordinates_path(city: str) -> Path:
    """Path to a city's committed, cumulative station coordinates."""
    return STATION_COORDINATES_DIR / f"{city}.csv"


def load_station_coordinates(city: str) -> pl.LazyFrame:
    """id (String) → name + point from the city's committed station coordinates.

    All-String scan so ids join cleanly against the trip ids (also cast to String); lat/lng
    are re-cast to Float for coordinate use.
    """
    path = station_coordinates_path(city)
    if not path.exists():
        raise FileNotFoundError(
            f"No committed station coordinates at {path}. Build them with "
            f"`citybikeshare build-station-coordinates {city}`."
        )
    return pl.scan_csv(path, infer_schema_length=0).select(
        pl.col("id").str.strip_chars().alias("id"),
        pl.col("name"),
        pl.col("lat").cast(pl.Float64, strict=False),
        pl.col("lng").cast(pl.Float64, strict=False),
    )


def _read_gbfs_stations(path: Path) -> list[dict]:
    """The station list from a GBFS ``station_information.json`` (under ``data.stations``)."""
    return json.loads(path.read_text())["data"]["stations"]


def _load_committed_coordinates(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    df = pl.read_csv(path, infer_schema_length=0)
    return {
        r["id"]: {c: r[c] for c in _COORDINATE_COLUMNS} for r in df.iter_rows(named=True)
    }


def update_gbfs_station_coordinates(
    context: PipelineContext, required: bool = True
) -> None:
    """Merge the fetched GBFS ``station_information.json`` into the committed coordinates.

    City-agnostic: every GBFS ``station_information`` shares the same shape (id, name, lat,
    lon under ``data.stations``), so the committed table is the only per-city input. Reads as a
    short sequence — merge (``_merge_stations``), then report (``_report_coordinate_changes``).

    ``required=False`` (the pre-transform hook) turns a missing GBFS file into a skip: the
    committed table is the durable fallback, so a clone without a fresh fetch still works.
    """
    city = context.city
    path = station_coordinates_path(city)
    gbfs_path = context.metadata_directory / "station_information.json"

    if not gbfs_path.exists():
        if not required:
            n = len(_load_committed_coordinates(path))
            print(
                f"ℹ️  {city}: no GBFS station_information.json on disk; keeping committed "
                f"coordinates as-is ({n} stations)."
            )
            return
        raise FileNotFoundError(
            f"No {gbfs_path} to build {city}'s station coordinates. Download it first."
        )

    previous = _load_committed_coordinates(path)
    merged = _merge_stations(previous, _read_gbfs_stations(gbfs_path))

    if merged == previous:
        print(
            f"✅ {city}: station coordinates already up to date "
            f"({len(merged)} stations) — no changes"
        )
        return

    rows = [merged[i] for i in sorted(merged, key=_id_sort_key)]
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema={c: pl.String for c in _COORDINATE_COLUMNS}).write_csv(path)

    _report_coordinate_changes(city, path, previous, merged)


def _merge_stations(previous: dict[str, dict], stations: list[dict]) -> dict[str, dict]:
    """Merge a GBFS station list onto the committed coordinates: newest wins per id, ids never
    dropped, coords mirrored verbatim (empty when the feed omits a point)."""
    merged = {sid: dict(rec) for sid, rec in previous.items()}
    for st in stations:
        # Store the GBFS station_id verbatim — it's the key trips join on. Do NOT strip
        # leading zeros: some systems zero-pad on both the feed and the trip side (Vancouver
        # "0001"), so stripping would break the match. Mexico's ids are already unpadded, and
        # its trip-side padding is normalized in the transform join, not here.
        sid = str(st.get("station_id", "")).strip()
        if not sid:
            continue
        lat, lng = st.get("lat"), st.get("lon")
        merged[sid] = {
            "id": sid,
            "name": (st.get("name") or "").strip(),
            "lat": f"{round(float(lat), 6)}" if lat is not None else "",
            "lng": f"{round(float(lng), 6)}" if lng is not None else "",
        }
    return merged


def _report_coordinate_changes(
    city: str, path: Path, previous: dict[str, dict], merged: dict[str, dict]
) -> None:
    """Print the coordinate diff for review: total written, new ids, renames, and coord changes.
    Coords are server-side stable between fetches, so any change is a real edit worth a look."""
    new_ids = sorted(set(merged) - set(previous), key=_id_sort_key)
    renamed = [
        (i, previous[i]["name"], merged[i]["name"])
        for i in previous
        if i in merged and merged[i]["name"] != previous[i]["name"]
    ]
    moved = [
        (
            i,
            f"{previous[i]['lat']},{previous[i]['lng']}",
            f"{merged[i]['lat']},{merged[i]['lng']}",
        )
        for i in set(previous) & set(merged)
        if (previous[i]["lat"], previous[i]["lng"])
        != (merged[i]["lat"], merged[i]["lng"])
    ]

    print(f"✅ {city}: wrote {len(merged)} stations → {path}")
    if new_ids:
        print(f"   ➕ {len(new_ids)} new id(s): {new_ids}")
    if renamed:
        print(f"   ✏️  {len(renamed)} renamed id(s) — review before committing:")
        for sid, old, new in renamed:
            print(f"      {sid}: {old!r} → {new!r}")
    if moved:
        print(f"   📍 {len(moved)} id(s) with changed coords — review:")
        for sid, old, new in moved:
            print(f"      {sid}: {old} → {new}")


def _id_sort_key(i: str) -> tuple[int, object]:
    """Numeric ids ascend by value; non-numeric (hyphen pairs) sort after, lexicographically."""
    return (0, int(i)) if i.isdigit() else (1, i)


# GBFS coordinate cities, for the CLI's `build-station-coordinates`. Adding a city is one line
# here plus the `refresh_station_coordinates` pre-transform step + a `station_coordinates`
# coord source in its YAML — no new harvest code (the harvester is city-agnostic).
STATION_COORDINATES_BUILDERS = {
    "mexico_city": update_gbfs_station_coordinates,
    "vancouver": update_gbfs_station_coordinates,
}

# Pre-transform steps (see station_maps.PRE_TRANSFORM_FUNCTIONS); best-effort (required=False)
# so the committed coordinates are the durable fallback when no fresh GBFS is on disk. One
# generic step for every GBFS city — the city is taken from the context.
PRE_TRANSFORM_FUNCTIONS = {
    "refresh_station_coordinates": lambda context, config: (
        update_gbfs_station_coordinates(context, required=False)
    ),
}
