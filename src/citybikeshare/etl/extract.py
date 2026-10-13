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

import gzip
import os
import shutil
import zipfile
from pathlib import Path
from typing import List, Optional
import tempfile
from citybikeshare.context import PipelineContext
from citybikeshare.config.loader import load_city_config
from citybikeshare.etl.state import (
    file_signature,
    is_unchanged,
    load_state,
    write_state,
)


_GZIP_LEVEL = 6
_COPY_CHUNK = 1024 * 1024


def _gzip_file(src_path, dst_path: Path) -> None:
    """Stream-compress src_path -> dst_path (gzip). Bounded memory, no full read."""
    with (
        open(src_path, "rb") as fin,
        gzip.open(dst_path, "wb", compresslevel=_GZIP_LEVEL) as fout,
    ):
        shutil.copyfileobj(fin, fout, length=_COPY_CHUNK)


def _gzip_uncompressed_size(gz_path: Path) -> int:
    """Uncompressed size of a gzip file from its ISIZE trailer (the last 4 bytes,
    little-endian). Cheap — no full decompress. Valid for members < 4 GiB, which
    covers monthly bikeshare CSVs; this is only used for the cumulative-archive
    "unchanged member" check, so a rare >4 GiB false-miss just rewrites the file."""
    with open(gz_path, "rb") as f:
        f.seek(-4, os.SEEK_END)
        return int.from_bytes(f.read(4), "little")


def _map_raw_files_to_downloads(
    previous_run_metadata: dict, present_downloads: set
) -> dict:
    """raw name -> the download that produced it, for downloads still present this run.

    A download that has left download/ (a renamed cumulative archive, e.g. Daejeon) is
    dropped, so its successor may rewrite the files it produced.
    """
    download_by_raw_file = {}
    for source, entry in previous_run_metadata.items():
        if source not in present_downloads:
            continue
        for output in entry.get("outputs", []):
            download_by_raw_file[output] = source
    return download_by_raw_file


def _assert_not_produced_by_other_download(
    target_path: Path, source_name: str, download_by_raw_file: dict
) -> None:
    """Raise before overwriting a raw file that another present download produced.

    Capital Bikeshare's April 2024 zip and Divvy's January 2026 zip each name their CSV after
    the same month in an earlier year, so the later extract silently replaced that month.
    """
    producer = download_by_raw_file.get(target_path.name)
    if target_path.exists() and producer not in (None, source_name):
        raise ValueError(
            f"{source_name} would overwrite raw/{target_path.name}, which {producer} "
            f"produced, with different content. If {source_name}'s member is misnamed, "
            f"map it to its correct name under `renamed_archive_members` in the city's YAML."
        )


def _deposit_member_as_gzip(
    full_path: str | Path,
    file: str,
    raw_dir: Path,
    source_name: str,
    download_by_raw_file: dict,
) -> Path:
    """Gzip a source member verbatim into raw/<file>.gz and return its path.

    Skips the rewrite when an unchanged copy already exists (cumulative archives
    re-bundle the same members), matching the size trailer as elsewhere. Used for both
    `.csv` members and, when a city keeps them, `.json` members — raw/ is the source of
    truth, so the bytes are stored as-is; interpreting JSON is the clean stage's job.
    """
    target_path = raw_dir / (file + ".gz")
    src_size = os.path.getsize(full_path)
    if target_path.exists() and _gzip_uncompressed_size(target_path) == src_size:
        print(f"🟡 Unchanged member, keeping {target_path.name}")
    else:
        _assert_not_produced_by_other_download(
            target_path, source_name, download_by_raw_file
        )
        _gzip_file(full_path, target_path)
        kind = Path(file).suffix.lstrip(".").upper()
        print(f"✅ Extracted {kind} (gzip): {target_path.name}")
    download_by_raw_file[target_path.name] = source_name
    # Drop any stale uncompressed sibling from a pre-gzip run.
    (raw_dir / file).unlink(missing_ok=True)
    return target_path


def _raw_name_for_member(file: str) -> Optional[str]:
    """Name a zip member is stored under in raw/ (before `.gz`), or None to skip it."""
    lower = file.lower()
    if lower.endswith((".csv", ".json")):
        return file
    # Chicago bundles a README.txt with its trip archives.
    if lower.endswith(".txt") and not lower.startswith("readme"):
        # Rosario ships its 2021 trips as a `.txt` (tab-delimited, Latin-1). Store the bytes
        # as-is under a `.csv` name; making it a well-formed CSV is the clean stage's job.
        return Path(file).stem + ".csv"
    return None


def _extract_archive(
    zip_path, raw_dir: Path, source_name: str, download_by_raw_file: dict, renames: dict
) -> List[Path]:
    """Extract one archive (recursing into nested zips) into raw_dir.

    Returns the list of raw paths produced from this archive. `.json` members are kept in
    raw/ alongside CSVs (e.g. Madrid's JSON-era trips) rather than dropped; clean converts
    them, and raises on any non-CSV file no converter step claims. ``renames`` maps a
    misnamed member to the name it should be stored under (`renamed_archive_members`).
    """
    raw_files: List[Path] = []
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
                            file = renames.get(file, file)

                            if zipfile.is_zipfile(full_path):
                                # Copy nested zips to a safe path before temp deletion
                                copied_path = os.path.join(tempfile.gettempdir(), file)
                                shutil.copy(full_path, copied_path)
                                to_process.append(copied_path)
                                print(f"📦 Found nested zip (copied): {copied_path}")
                                continue

                            raw_name = _raw_name_for_member(file)
                            if raw_name:
                                raw_files.append(
                                    _deposit_member_as_gzip(
                                        full_path,
                                        raw_name,
                                        raw_dir,
                                        source_name,
                                        download_by_raw_file,
                                    )
                                )
        except zipfile.BadZipFile:
            print(f"⚠️  Skipping invalid ZIP file: {current}")

    return raw_files


def _all_outputs_exist(raw_dir: Path, outputs: List[str]) -> bool:
    return all((raw_dir / name).exists() for name in outputs)


def _orphan_raw_files(
    raw_dir: Path, previous_run_metadata: dict, current_run_metadata: dict
) -> dict:
    """Detect raw files left behind by archives no longer present in download/.

    Returns {vanished_archive: [orphaned output names]}. When a source archive
    disappears (e.g. Montreal renames its cumulative zip and the old one is
    pruned from download/), the CSVs it extracted would otherwise linger in raw/
    and be re-counted downstream.

    A previously-recorded output counts as an orphan only when it still exists on
    disk AND no archive present this run produces a file of the same name — this
    protects cumulative archives whose member names are stable across re-bundles
    (e.g. Daejeon) and never flags untracked legacy files (anything not recorded
    in the prior state).
    """
    current_outputs = {
        name
        for entry in current_run_metadata.values()
        for name in entry.get("outputs", [])
    }
    vanished = set(previous_run_metadata) - set(current_run_metadata)
    orphans: dict = {}
    for source_name in sorted(vanished):
        names = [
            output
            for output in previous_run_metadata[source_name].get("outputs", [])
            if output not in current_outputs and (raw_dir / output).exists()
        ]
        if names:
            orphans[source_name] = names
    return orphans


def _apply_orphan_cleanup(raw_dir: Path, orphans: dict, prune: bool) -> List[str]:
    """Delete orphaned raw files when prune is enabled; otherwise warn and keep.

    Deletion is opt-in per city (`prune_renamed_archives: true`) because raw/ is
    the local source of truth after download — a wrong delete may be
    unrecoverable if the source no longer serves the file. Detection/warning is
    global so a newly cumulative city can't silently duplicate data for long.
    """
    removed: List[str] = []
    for source_name, names in orphans.items():
        if prune:
            for output in names:
                (raw_dir / output).unlink()
                removed.append(output)
                print(
                    f"🗑️  Removed orphaned raw file (source {source_name} gone): {output}"
                )
        else:
            print(
                f"⚠️  Archive {source_name} is gone from download/; its raw outputs "
                f"may now be duplicates: {names}. "
                f"Set 'prune_renamed_archives: true' for this city to auto-clean."
            )
    return removed


def extract_city_data(context: PipelineContext, overwrite: bool = False) -> List[Path]:
    """
    Extract all downloaded archives for a city into its raw folder.

    Supports nested ZIPs, .txt-to-.csv conversion, and skips AppleDouble (._) metadata files.
    Skips downloads whose size+mtime signature is unchanged since the last run. Raw files left
    behind by archives no longer present in download/ are warned about, and deleted when the
    city sets `prune_renamed_archives: true`.
    """
    config = load_city_config(context.city)
    city_name = config["name"]
    download_directory = context.download_directory
    raw_dir = context.raw_directory

    if overwrite:
        print(f"🧹 Clearing existing extracted data for {city_name}")
        shutil.rmtree(raw_dir, ignore_errors=True)
        previous_run_metadata = {}
    else:
        previous_run_metadata = load_state(context.extract_state_path)
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"📦 Extracting data for {city_name}")

    current_run_metadata: dict = {}
    raw_files_for_city: List[Path] = []
    downloads = sorted(Path(download_directory).iterdir())
    download_by_raw_file = _map_raw_files_to_downloads(
        previous_run_metadata, {entry.name for entry in downloads}
    )
    renamed_members = config.get("renamed_archive_members", {})

    for entry in downloads:
        source_name = entry.name
        recorded = previous_run_metadata.get(source_name)

        # An xlsx is itself a zip, so test for it before is_zipfile — otherwise it would be
        # "extracted" (no .csv members) and silently dropped.
        is_xlsx = entry.suffix.lower() == ".xlsx"
        is_zip = not is_xlsx and zipfile.is_zipfile(entry)
        is_csv = entry.suffix.lower() == ".csv"
        if not (is_zip or is_csv or is_xlsx):
            continue

        # Skip when the source is unchanged and its outputs are still present.
        if (
            recorded
            and is_unchanged(entry, recorded)
            and _all_outputs_exist(raw_dir, recorded.get("outputs", []))
        ):
            print(f"🟡 Skipping extract - {source_name} unchanged")
            current_run_metadata[source_name] = recorded
            raw_files_for_city.extend(raw_dir / name for name in recorded["outputs"])
            continue

        if is_zip:
            raw_files = _extract_archive(
                entry,
                raw_dir,
                source_name,
                download_by_raw_file,
                renamed_members.get(source_name, {}),
            )
        else:
            # Top-level CSV or XLSX: store the bytes verbatim, gzipped. Converting an xlsx to
            # CSV is the clean stage's job (see CONVERT_FUNCTIONS).
            raw_files = [
                _deposit_member_as_gzip(
                    entry, source_name, raw_dir, source_name, download_by_raw_file
                )
            ]

        raw_files_for_city.extend(raw_files)
        current_run_metadata[source_name] = {
            **file_signature(entry),
            "outputs": [p.name for p in raw_files],
        }

    orphans = _orphan_raw_files(raw_dir, previous_run_metadata, current_run_metadata)
    _apply_orphan_cleanup(
        raw_dir, orphans, prune=config.get("prune_renamed_archives", False)
    )

    write_state(context.extract_state_path, current_run_metadata)

    if not raw_files_for_city:
        print(
            f"⚠️  No CSV files found or converted for {city_name}. Did download succeed?"
        )
    else:
        print(
            f"📂 Extracted and converted {len(raw_files_for_city)} raw files for {city_name}"
        )

    return raw_files_for_city
