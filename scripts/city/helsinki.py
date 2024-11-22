import os
import zipfile
import datetime
import requests
import polars as pl
import scripts.utils as utils

ZIP_PATH = utils.get_zip_directory("helsinki")
CSV_PATH = utils.get_raw_files_directory("helsinki")


### Download code based on https://github.com/Geometrein/helsinki-city-bikes/blob/main/scraper.py
def generate_link(year):
    url = f"http://dev.hsl.fi/citybikes/od-trips-{year}/od-trips-{year}.zip"
    return url


def download_year(year, download_path, chunk_size=128):
    url = generate_link(year)
    path = os.path.join(download_path, f"{year}.zip")
    try:
        r = requests.get(url, stream=True)
        with open(path, "wb") as fd:
            print(f"Downloading {url}")
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)
        print("Download Complete")
    except:
        print("Experienced a problem with downloading the files")


def unzip_files_in_directory(zip_dir, output_dir):
    for zip_file in os.listdir(zip_dir):
        zip_path = os.path.join(zip_dir, zip_file)

        if zipfile.is_zipfile(zip_path):
            print(f"Unzipping: {zip_file}")
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(output_dir)

            print(f"Contents of {zip_file} moved to {output_dir}")
        else:
            print(f"Skipping non-zip file: {zip_file}")


def download_and_unzip():
    for year in range(2016, datetime.datetime.now().year + 1):
        download_year(year, ZIP_PATH)
    unzip_files_in_directory(ZIP_PATH, CSV_PATH)


renamed_columns = {
    "Departure": "start_time",
    "Return": "end_time",
    "Departure station name": "start_station_name",
    "Return station name": "end_station_name",
}


# Departure,Return,Departure station id,Departure station name,Return station id,Return station name,Covered distance (m),Duration (sec.)

# 2024-04-30T23:59:23,2024-05-01T00:31:48,018,Porthania,103,Korppaantie,6115,1941

date_columns = ["start_time", "end_time"]
date_formats = ["%Y-%m-%dT%H:%M:%S"]

final_columns = ["start_station_name", "end_station_name", "start_time", "end_time"]


def create_all_trips_df():
    files = utils.get_csv_files(CSV_PATH)
    all_dfs = []
    for file in files:
        print(f"reading {file}")

        df = pl.read_csv(file, infer_schema_length=0)

        df = (
            df.rename(renamed_columns)
            .select(final_columns)
            .pipe(utils.convert_columns_to_datetime(date_columns, date_formats))
        )

        all_dfs.append(df)

    all_trips = pl.concat(all_dfs, how="diagonal")
    return all_trips


def build_trips(args):
    df = create_all_trips_df()
    utils.log_final_results(df, args)
    utils.create_all_trips_file(df, args)
    utils.create_recent_year_file(df, args)
    return df


if __name__ == "__main__":
    download_and_unzip()
