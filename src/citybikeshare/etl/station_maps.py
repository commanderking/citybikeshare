"""Committed, cumulative id→name station maps.

Some cities identify stations in their trip files by numeric id only (Guadalajara's
``Origen_Id`` / ``Destino_Id``); the human-readable names live in a separate
"nomenclatura" station file that the source *rewrites every month*. No single month's
file covers the full history — old ids get dropped, new ids appear, and the same id can
be renamed — so joining trips against just the latest file silently drops every trip
whose station that file happens to omit.

To decouple from whatever the source currently serves, we keep a **committed**,
cumulative map per city at ``config/station_maps/<city>.csv``. ``update_*`` gets
every nomenclatura file on disk into it (newest name wins per id, ids are never
dropped), and the transform join reads only the committed map and asserts full coverage
"""

import re
from pathlib import Path

import polars as pl

from citybikeshare.context import PipelineContext

STATION_MAP_DIR = Path(__file__).resolve().parents[1] / "config" / "station_maps"

_NOMENCLATURA_RE = re.compile(r"nomenclatura_(\d{4})_(\d{2})", re.IGNORECASE)


def station_map_path(city: str) -> Path:
    """Path to a city's committed, cumulative id→name station map."""
    return STATION_MAP_DIR / f"{city}.csv"


def load_station_map(city: str) -> pl.LazyFrame:
    """id (String) → name from the city's committed station map.

    Read all-String (``infer_schema_length=0``) so ids join cleanly against the trip
    ids (also cast to String) regardless of how many are numeric.
    """
    path = station_map_path(city)
    if not path.exists():
        raise FileNotFoundError(
            f"No committed station map at {path}. Build it with "
            f"`citybikeshare build-station-map {city}`."
        )
    return pl.scan_csv(path, infer_schema_length=0).select(
        pl.col("id").str.strip_chars().alias("id"),
        pl.col("name").alias("name"),
    )


def _find_nomenclatura_files(context: PipelineContext) -> list[Path]:
    """Every nomenclatura file on disk, oldest→newest by the ``YYYY_MM`` in its name.

    Looks in both ``download/`` (plain ``.csv``) and ``raw/`` (gzipped ``.csv.gz``),
    deduping to one file per month — download wins over raw, and a plain ``.csv`` wins
    over ``.csv.gz`` (identical content). Ascending order lets the newest file's name
    win for any shared id when the caller merges in sequence.
    """
    found: dict[str, Path] = {}
    for directory in (context.download_directory, context.raw_directory):
        if not directory.exists():
            continue
        for path in (
            *directory.glob("nomenclatura*.csv"),
            *directory.glob("nomenclatura*.csv.gz"),
        ):
            match = _NOMENCLATURA_RE.search(path.name)
            key = f"{match.group(1)}_{match.group(2)}" if match else path.name
            found.setdefault(key, path)
    return [found[key] for key in sorted(found)]


def _read_nomenclatura(path: Path) -> list[tuple[str, str]]:
    """(id, name) pairs from one nomenclatura file. ``utf8-lossy`` tolerates the older
    files' non-UTF-8 bytes; newer files supersede those ids with clean names."""
    rows = (
        pl.read_csv(path, infer_schema_length=0, encoding="utf8-lossy")
        .select(
            pl.col("id").str.strip_chars().alias("id"),
            pl.col("name").str.strip_chars().alias("name"),
        )
        .drop_nulls("id")
        .filter(pl.col("id") != "")
    )
    return list(zip(rows["id"].to_list(), rows["name"].to_list()))


def update_guadalajara_station_map(
    context: PipelineContext, required: bool = True
) -> None:
    """Harvest every nomenclatura file on disk into the committed station map.

    The existing committed map is the lowest-priority base, so ids whose source file the
    source no longer serves survive even when absent from every file on disk. On-disk
    files are then applied oldest→newest, so the newest name wins per id (better encoding
    + current naming). Prints new and renamed ids so genuine relocations can be reviewed
    before committing; leaves the file untouched (no git churn) when nothing changed.

    ``required=False`` makes a missing nomenclatura a skip rather than an error — used by
    the transform hook, where the committed map is the durable fallback and the refresh
    is only a best-effort improvement when fresh station files happen to be on disk.
    """
    city = context.city
    path = station_map_path(city)

    previous: dict[str, str] = {}
    if path.exists():
        prev = pl.read_csv(path, infer_schema_length=0)
        previous = dict(zip(prev["id"].to_list(), prev["name"].to_list()))

    files = _find_nomenclatura_files(context)
    if not files:
        if not required:
            print(
                f"ℹ️  {city}: no nomenclatura files on disk; keeping committed "
                f"station map as-is ({len(previous)} stations)."
            )
            return
        raise FileNotFoundError(
            f"No nomenclatura_* files found in {context.download_directory} or "
            f"{context.raw_directory} to build {city}'s station map."
        )

    merged = dict(previous)
    for file in files:
        for station_id, name in _read_nomenclatura(file):
            merged[station_id] = name

    new_ids = sorted(set(merged) - set(previous), key=int)
    renamed = sorted(
        (
            (sid, previous[sid], merged[sid])
            for sid in previous
            if merged[sid] != previous[sid]
        ),
        key=lambda row: int(row[0]),
    )

    if not new_ids and not renamed:
        print(
            f"✅ {city}: station map already up to date "
            f"({len(merged)} stations) — no changes"
        )
        return

    out = (
        pl.DataFrame({"id": list(merged.keys()), "name": list(merged.values())})
        .with_columns(pl.col("id").cast(pl.Int64))
        .sort("id")
        .with_columns(pl.col("id").cast(pl.String))
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    out.write_csv(path)

    print(f"✅ {city}: wrote {len(merged)} stations → {path}")
    print(f"   harvested {len(files)} file(s): {[f.name for f in files]}")
    if new_ids:
        print(f"   ➕ {len(new_ids)} new id(s): {new_ids}")
    if renamed:
        print(f"   ✏️  {len(renamed)} renamed id(s) — review before committing:")
        for sid, old, new in renamed:
            print(f"      {sid}: {old!r} → {new!r}")


# City → builder for its committed station map, used by the CLI's `build-station-map`.
STATION_MAP_BUILDERS = {
    "guadalajara": update_guadalajara_station_map,
}

# Step name → callable(context, config), for a city's `pre_transform_pipeline` (run once
# before transform's file loop; see transform.run_pre_transform_steps). Mirrors
# PROCESSING_FUNCTIONS, but these are run-scoped side effects rather than per-file df
# transforms. The station-map refresh is best-effort (required=False): the committed map
# is the durable fallback, so a clone that hasn't synced station files still transforms.
PRE_TRANSFORM_FUNCTIONS = {
    "refresh_guadalajara_station_map": lambda context, config: (
        update_guadalajara_station_map(context, required=False)
    ),
}
