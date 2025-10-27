import hashlib
import os
from pathlib import Path
import shutil


def compute_file_hash(filepath, chunk_size=8192):
    """Compute SHA256 hash for file integrity checking."""
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_file_metadata(filepath, city_config):
    """Get file size and last modified timestamp."""
    stat = os.stat(filepath)
    size = stat.st_size
    modified_at = stat.st_mtime
    name = os.path.basename(filepath)
    file_hash = compute_file_hash(filepath)

    return {
        "size": size,
        "modified_at": modified_at,
        "name": name,
        "file_hash": file_hash,
        "system_name": city_config["system_name"],
    }


def delete_folder(folder_path):
    """
    Delete a folder and all its contents (files + subdirectories).
    """
    path = Path(folder_path)
    if not path.exists():
        print(f"⚠️ Folder not found: {path}")
        return

    shutil.rmtree(path)
    print("🗑️  Clearing folder to write completely new parquets")
