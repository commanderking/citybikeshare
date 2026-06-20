"""
Extract stage for the bikeshare ETL pipeline.

Responsible for:
- Finding downloaded ZIP or CSV files for a city
- Extracting ZIP archives (if needed)
- Collecting all CSV file paths for the next transform step

Incremental: each top-level download is recorded in the extract state file by its
size+mtime signature plus the raw files it produced. On a re-run an archive whose
signature is unchanged (and whose outputs still exist) is skipped.
"""

import os
import shutil
import zipfile
from pathlib import Path
from typing import List
import tempfile
import polars as pl
from citybikeshare.context import PipelineContext
from citybikeshare.config.loader import load_city_config
from citybikeshare.etl.state import (
    file_signature,
    is_unchanged,
    load_state,
    write_state,
)


def _extract_archive(zip_path, raw_dir: Path) -> List[Path]:
    """Extract one archive (recursing into nested zips) into raw_dir.

    Returns the list of CSV paths produced from this archive.
    """
    produced: List[Path] = []
    to_process = [zip_path]

    while to_process:
        current = to_process.pop()
        print(f"📂 Extracting: {current}")
        try:
            with zipfile.ZipFile(current, "r") as archive:
                with tempfile.TemporaryDirectory() as temp_dir:
                    archive.extractall(temp_dir)

                    for root, _, files in os.walk(temp_dir):
                        for file in files:
                            # AppleDouble metadata (._foo.csv, __MACOSX/) — never a real CSV
                            if file.startswith("._"):
                                continue
                            full_path = os.path.join(root, file)

                            if zipfile.is_zipfile(full_path):
                                # Copy nested zips to a safe path before temp deletion
                                copied_path = os.path.join(tempfile.gettempdir(), file)
                                shutil.copy(full_path, copied_path)
                                to_process.append(copied_path)
                                print(f"📦 Found nested zip (copied): {copied_path}")

                            elif file.lower().endswith(".csv"):
                                target_path = raw_dir / file
                                shutil.move(full_path, target_path)
                                produced.append(target_path)
                                print(f"✅ Extracted CSV: {target_path}")

                            elif file.lower().endswith(".txt"):
                                txt_path = Path(full_path)
                                csv_path = raw_dir / (txt_path.stem + ".csv")
                                try:
                                    print(f"📝 Converting TXT → CSV: {txt_path.name}")
                                    lf = pl.scan_csv(txt_path, encoding="utf8-lossy")
                                    lf = lf.collect()
                                    lf.write_csv(csv_path)
                                    produced.append(csv_path)
                                    print(f"✅ Converted and saved as: {csv_path.name}")
                                except Exception as e:
                                    print(
                                        f"⚠️  Failed to convert {txt_path.name} → CSV ({e})"
                                    )
        except zipfile.BadZipFile:
            print(f"⚠️  Skipping invalid ZIP file: {current}")

    return produced


def _all_outputs_exist(raw_dir: Path, outputs: List[str]) -> bool:
    return all((raw_dir / name).exists() for name in outputs)


def extract_city_data(context: PipelineContext, overwrite: bool = False) -> List[Path]:
    """
    Extract all downloaded archives for a city into its raw folder.

    Supports nested ZIPs, .txt-to-.csv conversion, and removes AppleDouble (._) metadata files.
    Skips downloads whose size+mtime signature is unchanged since the last run.
    """
    config = load_city_config(context.city)
    city_name = config["name"]
    download_directory = context.download_directory
    raw_dir = context.raw_directory
    raw_dir.mkdir(parents=True, exist_ok=True)

    if overwrite:
        print(f"🧹 Clearing existing extracted data for {city_name}")
        shutil.rmtree(raw_dir, ignore_errors=True)
        raw_dir.mkdir(parents=True, exist_ok=True)
        state = {}
    else:
        state = load_state(context.extract_state_path)

    print(f"📦 Extracting data for {city_name}")

    # Clean any legacy AppleDouble (._) files left in raw/ before the idempotency
    # check, so recorded outputs are compared against a clean directory.
    for root, _, files in os.walk(raw_dir):
        for file in files:
            if file.startswith("._"):
                os.remove(os.path.join(root, file))
                print(f"🧹 Removed AppleDouble file: {file}")

    new_state: dict = {}
    csv_files: List[Path] = []

    for entry in sorted(Path(download_directory).iterdir()):
        source_name = entry.name
        recorded = state.get(source_name)

        is_zip = zipfile.is_zipfile(entry)
        is_csv = entry.suffix.lower() == ".csv"
        if not (is_zip or is_csv):
            continue

        # Skip when the source is unchanged and its outputs are still present.
        if (
            recorded
            and is_unchanged(entry, recorded)
            and _all_outputs_exist(raw_dir, recorded.get("outputs", []))
        ):
            print(f"🟡 Skipping extract - {source_name} unchanged")
            new_state[source_name] = recorded
            csv_files.extend(raw_dir / name for name in recorded["outputs"])
            continue

        if is_zip:
            produced = _extract_archive(entry, raw_dir)
        else:
            dest = raw_dir / source_name
            shutil.copy(entry, dest)
            produced = [dest]
            print(f"✅ Copied CSV: {dest.name}")

        csv_files.extend(produced)
        new_state[source_name] = {
            **file_signature(entry),
            "outputs": [p.name for p in produced],
        }

    write_state(context.extract_state_path, new_state)

    if not csv_files:
        print(
            f"⚠️  No CSV files found or converted for {city_name}. Did download succeed?"
        )
    else:
        print(f"📂 Extracted and converted {len(csv_files)} CSV files for {city_name}")

    return csv_files
