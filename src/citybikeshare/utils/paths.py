import os
import definitions


def get_analysis_directory():
    path = definitions.ANALYSIS_DIR
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_csv_files(directory):
    trip_files = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d != "__MACOSX"]
        for file in files:
            if file.endswith(".csv") and not file.startswith("__MACOSX/"):
                csv_path = os.path.join(root, file)
                trip_files.append(csv_path)
    return trip_files
