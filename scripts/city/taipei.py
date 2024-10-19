import os
from zipfile import ZipFile
from io import BytesIO
import requests
import polars as pl
import utils
import definitions

renamed_columns = {
    "rent_time": "start_time",
    "rent_station": "start_station_name",
    "return_station": "end_station_name",
    "return_time": "end_time",
    "rent": "duration_seconds",
    "infodate": "info_date",
}

RAW_TAIPEI_COLUMNS = [
    "rent_time",
    "rent_station",
    "return_time",
    "return_station",
    "rent",
    "infodate",
]

# Specify the path where the Parquet file should be saved

PARQUET_OUTPUT_PATH = definitions.DATA_DIR / "taipei_all_trips.parquet"

TAIPEI_CSVS_PATH = utils.get_raw_files_directory("taipei")


def read_csv_file(file_path, has_header=True, columns=None):
    if has_header:
        df = pl.read_csv(file_path, encoding="utf8-lossy")
    else:
        df = pl.read_csv(
            file_path,
            has_header=False,
            new_columns=columns,
            infer_schema_length=10000,
            encoding="utf8-lossy",
        )
    return df


def determine_has_header(file_path, expected_columns):
    # Not all files have headers
    with open(file_path, "r", encoding="utf-8", errors="replace") as file:
        first_line = file.readline().strip().split(",")
        return all(item in expected_columns for item in first_line)


def toSeconds(rent_series):
    rows_in_seconds = []
    for rent_time in rent_series:
        time_array = rent_time.split(":")

        seconds = (
            int(time_array[0]) * 3600 + int(time_array[1]) * 60 + int(time_array[2])
        )
        rows_in_seconds.append(seconds)
    return pl.Series(rows_in_seconds)


def create_df_with_all_trips(folder_path, raw_columns):
    csv_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".csv")
    ]
    dataframes = []

    for file_path in csv_files:
        print(file_path)
        has_header = determine_has_header(file_path, raw_columns)
        df = read_csv_file(file_path, has_header, raw_columns)

        df = (
            df.rename(renamed_columns)
            .with_columns(
                [
                    pl.col("start_time").str.to_datetime("%Y-%m-%d %H:%M:%S"),
                    pl.col("end_time").str.to_datetime("%Y-%m-%d %H:%M:%S"),
                    ### map_batches is not ideal, but using to_time is not an option because hours can go over 24.
                    pl.col("duration_seconds").map_batches(
                        toSeconds, return_dtype=pl.Int64
                    ),
                ]
            )
            .drop("info_date")
        )
        dataframes.append(df)

    combined_df = pl.concat(dataframes)
    return combined_df


### CSV files often have non-ASCII characters (i.e. 202403_YouBike2.0≤º√“®Í•d∏ÍÆ∆.csv)
def clean_filename(filename):
    return f"{filename[:6]}.csv"


##
# Get main csv, which lists monthly csv data in zip form
# For all monthly zips, unzip all csv files to folder
# Read all csvs and bundle into one large parquet file
def extract_all_csvs():
    print(TAIPEI_CSVS_PATH)
    df = pl.read_csv(
        "https://tcgbusfs.blob.core.windows.net/dotapp/youbike_second_ticket_opendata/YouBikeHis.csv"
    )

    file_urls = df["fileURL"].to_list()

    for file_url in file_urls:
        print(file_url)

        print(file_url)
        # Make an HTTP GET request to fetch the content of the zip file
        response = requests.get(file_url, timeout=10000)

        if response.status_code == 200:
            with ZipFile(BytesIO(response.content)) as zip_file:
                zip_contents = zip_file.namelist()

                for file in zip_contents:
                    clean_file = clean_filename(file)

                    source = zip_file.open(file)
                    target_path = os.path.join(TAIPEI_CSVS_PATH, clean_file)

                    with open(target_path, "wb") as target_file:
                        target_file.write(source.read())
                        print(f"Extracted and cleaned file to {target_path}")
        else:
            print(f"Failed to download file from {file_url}")


def export_to_parquet(df, output_path):
    # Save the DataFrame to a Parquet file
    print("writing parquet")
    df.write_parquet(output_path)


def create_all_trips_parquet(args):
    if not args.skip_unzip:
        extract_all_csvs()

    all_trips_df = create_df_with_all_trips(TAIPEI_CSVS_PATH, RAW_TAIPEI_COLUMNS)
    utils.log_final_results(all_trips_df, args)
    utils.create_all_trips_file(all_trips_df, args)
    utils.create_recent_year_file(all_trips_df, args)
