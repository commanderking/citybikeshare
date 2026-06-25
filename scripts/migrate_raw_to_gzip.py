#!/usr/bin/env python3
"""One-time migration: gzip a city's ``raw/*.csv`` in place and rewrite the
extract / clean / transform state sidecars so those stages **skip** on the next
run instead of rebuilding from scratch.

Why the state must be rewritten, not just the filenames: gzipping changes a
file's size *and* mtime, and the stages detect "unchanged" via a size+mtime
signature (see ``etl/state.py``). So a rename alone would still trip every
incremental check. This script refreshes the recorded signatures from the new
``.csv.gz`` files, and the decoded content is identical, so the existing parquet
stays valid.

What each state file needs:
  * transform.state.json — keyed on the *input* filename. Remap key
    ``foo.csv`` -> ``foo.csv.gz`` and refresh its signature. ``outputs`` (the
    parquet name) are unchanged because ``_parquet_name`` strips ``.csv.gz`` to
    the same ``foo.parquet``.
  * extract.state.json — keyed on the *download archive* (unchanged). Only the
    ``outputs`` list (raw filenames) is remapped ``foo.csv`` -> ``foo.csv.gz``
    so the skip's ``_all_outputs_exist`` check passes.
  * clean.state.json — keyed on the raw filename; remap key + refresh signature.
    NOTE: ``clean.py`` currently globs ``raw/*.csv`` only, so clean-pipeline
    cities also need a one-line code change to read ``.csv.gz`` before this
    helps them. Non-clean cities (no clean.state.json) are unaffected.

Safe & idempotent: a re-run finds no ``*.csv`` and no-ops. raw/ stays
recoverable (``gunzip``). The original is removed only after the ``.csv.gz`` is
written and integrity-verified.

Usage:
    poetry run python scripts/migrate_raw_to_gzip.py <city> [<city> ...]
"""

import gzip
import json
import os
import shutil
import sys
from pathlib import Path

CHUNK = 1024 * 1024


def gzip_in_place(csv_path: Path) -> Path:
    """Compress ``foo.csv`` -> ``foo.csv.gz`` (streaming), verify, then remove
    the original. Returns the new path."""
    gz_path = Path(str(csv_path) + ".gz")
    with open(csv_path, "rb") as fin, gzip.open(gz_path, "wb", compresslevel=6) as fout:
        shutil.copyfileobj(fin, fout, length=CHUNK)

    # Verify the archive is intact and non-empty before deleting the source.
    if gz_path.stat().st_size == 0:
        raise RuntimeError(f"gzip produced an empty file: {gz_path}")
    with gzip.open(gz_path, "rb") as f:
        while f.read(CHUNK):
            pass

    csv_path.unlink()
    return gz_path


def _signature(path: Path) -> dict:
    st = os.stat(path)
    return {"bytes": st.st_size, "mtime": st.st_mtime}


def _load(path: Path):
    return json.load(open(path)) if path.exists() else None


def _dump(path: Path, data: dict) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def _remap_keyed_state(path: Path, raw_dir: Path, renamed: dict, label: str) -> None:
    """For state keyed on the input filename (transform, clean): rename the key
    and refresh its size+mtime signature from the new .gz file."""
    state = _load(path)
    if state is None:
        return
    new_state = {}
    remapped = 0
    for name, entry in state.items():
        if name in renamed:
            gz_name = renamed[name]
            new_state[gz_name] = {**entry, **_signature(raw_dir / gz_name)}
            remapped += 1
        else:
            new_state[name] = entry
    _dump(path, new_state)
    print(f"  {label}: remapped {remapped}/{len(state)} keys (+ refreshed signatures)")


def migrate_city(city: str) -> None:
    city_dir = Path("data") / city
    raw_dir = city_dir / "raw"
    if not raw_dir.exists():
        print(f"⚠️  {city}: no raw/ directory, skipping")
        return

    csvs = sorted(raw_dir.glob("*.csv"))
    if not csvs:
        print(f"🟡 {city}: no raw/*.csv to migrate (already gzipped?)")
        return

    print(f"📦 {city}: gzipping {len(csvs)} raw .csv files in place")
    renamed: dict[str, str] = {}  # old .csv name -> new .csv.gz name
    for csv_path in csvs:
        gz_path = gzip_in_place(csv_path)
        renamed[csv_path.name] = gz_path.name
    print(f"  ✅ gzipped {len(renamed)} files")

    # clean.state is keyed on the raw filename -> remap key & refresh signature.
    _remap_keyed_state(city_dir / "clean.state.json", raw_dir, renamed, "clean.state")

    # transform.state is also keyed on its INPUT filename — but for clean-pipeline
    # cities that input is the cleaned/ copy (which this migration never touches),
    # not raw/. Only remap when transform actually reads raw/ (mirrors
    # PipelineContext.transform_input_directory), otherwise we'd rename keys that
    # still legitimately point at unchanged cleaned files and force a full rebuild.
    cleaned = city_dir / "cleaned"
    reads_cleaned = cleaned.exists() and (
        any(cleaned.glob("*.csv")) or any(cleaned.glob("*.csv.gz"))
    )
    if reads_cleaned:
        print("  transform.state: left as-is (transform reads cleaned/, not raw/)")
    else:
        _remap_keyed_state(
            city_dir / "transform.state.json", raw_dir, renamed, "transform.state"
        )

    # extract is keyed on the download archive -> only the outputs list changes.
    extract_path = city_dir / "extract.state.json"
    extract_state = _load(extract_path)
    if extract_state is not None:
        touched = 0
        for entry in extract_state.values():
            outputs = entry.get("outputs", [])
            new_outputs = [renamed.get(o, o) for o in outputs]
            if new_outputs != outputs:
                touched += 1
            entry["outputs"] = new_outputs
        _dump(extract_path, extract_state)
        print(f"  extract.state: remapped outputs in {touched} entries")

    print(f"🎉 {city}: migration complete\n")


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 1
    if not Path("pyproject.toml").exists():
        print("Error: run from the project root (no pyproject.toml here).")
        return 1
    for city in argv:
        migrate_city(city)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
