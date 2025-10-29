"""
Extract stage for the bikeshare ETL pipeline.

Responsible for:
- Finding downloaded ZIP or CSV files for a city
- Extracting ZIP archives (if needed)
- Collecting all CSV file paths for the next transform step
"""

import os
import shutil
import zipfile
from pathlib import Path
from typing import List
import tempfile

from src.citybikeshare.config.loader import load_city_config

from src.citybikeshare.utils.paths import get_zip_directory, get_raw_files_directory


def extract_city_data(city: str, overwrite: bool = False) -> List[Path]:
    """
    Extract all downloaded archives for a city into its raw folder.

    Supports nested ZIPs and removes AppleDouble (._) metadata files.

    Parameters
    ----------
    city : str
        City name (matches YAML config `name`)
    overwrite : bool
        Whether to clear out old extracted files before extraction.

    Returns
    -------
    List[Path]
        List of extracted CSV file paths ready for transform stage.
    """
    config = load_city_config(city)
    city_name = config["name"]

    zip_dir = get_zip_directory(city_name)
    raw_dir = get_raw_files_directory(city_name)
    raw_dir.mkdir(parents=True, exist_ok=True)

    if overwrite:
        print(f"🧹 Clearing existing extracted data for {city_name}")
        shutil.rmtree(raw_dir, ignore_errors=True)
        raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"📦 Extracting data for {city_name}")

    # Initialize queue of ZIPs to process (supports nested zips)
    to_process = [
        os.path.join(zip_dir, f)
        for f in os.listdir(zip_dir)
        if zipfile.is_zipfile(os.path.join(zip_dir, f))
    ]
    csv_files: List[Path] = []

    while to_process:
        zip_path = to_process.pop()
        print(f"📂 Extracting: {zip_path}")

        try:
            with zipfile.ZipFile(zip_path, "r") as archive:
                with tempfile.TemporaryDirectory() as temp_dir:
                    archive.extractall(temp_dir)

                    for root, _, files in os.walk(temp_dir):
                        for file in files:
                            full_path = os.path.join(root, file)

                            if zipfile.is_zipfile(full_path):
                                # 🚨 Copy nested zips to a safe path before temp deletion
                                copied_path = os.path.join(tempfile.gettempdir(), file)
                                shutil.copy(full_path, copied_path)
                                to_process.append(copied_path)
                                print(f"📦 Found nested zip (copied): {copied_path}")

                            elif file.lower().endswith(".csv"):
                                target_path = os.path.join(raw_dir, file)
                                shutil.move(full_path, target_path)
                                csv_files.append(Path(target_path))
                                print(f"✅ Extracted CSV: {target_path}")

        except zipfile.BadZipFile:
            print(f"⚠️  Skipping invalid ZIP file: {zip_path}")

    # Copy any standalone CSVs (non-zipped) into raw dir
    for file in Path(zip_dir).iterdir():
        if file.suffix.lower() == ".csv":
            dest = raw_dir / file.name
            shutil.copy(file, dest)
            csv_files.append(dest)
            print(f"✅ Copied CSV: {dest.name}")

    # Remove AppleDouble (._) files
    for root, _, files in os.walk(raw_dir):
        for file in files:
            if file.startswith("._"):
                os.remove(os.path.join(root, file))
                print(f"🧹 Removed AppleDouble file: {file}")

    if not csv_files:
        print(f"⚠️  No CSV files found for {city_name}. Did download succeed?")
    else:
        print(f"📂 Extracted {len(csv_files)} CSV files for {city_name}")

    return csv_files
