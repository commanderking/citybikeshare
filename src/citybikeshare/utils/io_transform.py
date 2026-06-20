from pathlib import Path
import shutil


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
