"""
Generic downloader that delegates to city-specific scripts when needed.
"""

import importlib
import subprocess
from pathlib import Path
from typing import Optional
from src.citybikeshare.config.loader import load_city_config
from src.citybikeshare.context import PipelineContext


def download_city_data(
    context: PipelineContext, output_dir: Optional[Path] = None
) -> Path:
    """
    Downloads raw data for the given city based on its config.

    If a custom downloader exists under scripts/download_{city}.py,
    it will be used automatically.
    """
    config = load_city_config(context.city)
    name = config["name"]
    aws_sync = config.get("aws_sync")
    s3_bucket = config.get("s3_bucket")
    s3_sync_options = config.get("s3_sync_options", "")

    output_dir = context.download_directory

    # --------------------------------------------------
    # 1️⃣ Try to import a custom script if it exists
    # --------------------------------------------------
    try:
        custom_module = importlib.import_module(
            f"src.citybikeshare.etl.custom_downloaders.{name}"
        )
        if hasattr(custom_module, "download"):
            custom_module.download(config, context)
            return output_dir
    except ModuleNotFoundError:
        pass  # No custom script — continue

    # --------------------------------------------------
    # 2️⃣ Otherwise, it's AWS Sync
    # --------------------------------------------------
    if aws_sync:
        if s3_bucket is None:
            raise ValueError("aws_sync is true, but no s3_bucket defined in yaml")
        else:
            print("🚀 Syncing from S3")
            cmd = f"aws s3 sync {s3_bucket} {output_dir} {s3_sync_options}"
            subprocess.run(cmd, shell=True, check=True)
            return output_dir

    raise ValueError(f"No download method found for {name}")
