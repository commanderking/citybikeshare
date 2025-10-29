import os
import definitions


def get_city_directory(city):
    city_raw_data_path = definitions.DATA_DIR / city
    city_raw_data_path.mkdir(parents=True, exist_ok=True)
    return city_raw_data_path


def get_zip_directory(city):
    path = definitions.DATA_DIR / city / "zip"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_sync_output_directory(city):
    ### London already stores files as csvs in s3 rather than zips
    ### Exception for now - move to config later if this becomes more common for s3_syncs
    if city == "london":
        return get_raw_files_directory(city)
    return get_zip_directory(city)


def get_raw_files_directory(city):
    path = definitions.DATA_DIR / city / "raw"
    path.mkdir(parents=True, exist_ok=True)
    return path


## Right now applies to Seoul because files are combination of zip and encoding, so need multiple processing steps
def get_download_directory(city):
    path = definitions.DATA_DIR / city / "download"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_parquet_directory(city):
    path = definitions.DATA_DIR / city / "parquet"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_metadata_directory(city):
    path = definitions.DATA_DIR / city / "metadata"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_output_format(is_csv):
    return "csv" if is_csv else "parquet"


def get_output_directory():
    path = definitions.OUTPUT_DIR
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_city_output_directory(city):
    path = definitions.OUTPUT_DIR / city
    path.mkdir(parents=True, exist_ok=True)
    return path


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
