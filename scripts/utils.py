import os
import json
import zipfile
from datetime import timedelta
import polars as pl
import definitions
import tempfile
import shutil
from dateutil.parser import parse


def get_city_directory(city):
    city_raw_data_path = definitions.DATA_DIR / city
    city_raw_data_path.mkdir(parents=True, exist_ok=True)
    return city_raw_data_path


def get_zip_directory(city):
    path = definitions.DATA_DIR / city / "zip"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_raw_files_directory(city):
    path = definitions.DATA_DIR / city / "raw"
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


def get_all_trips_path(args):
    file_format = get_output_format(args.csv)
    path = get_output_directory() / "historical_trips"
    path.mkdir(parents=True, exist_ok=True)

    return path / f"{args.city}_all_trips.{file_format}"


def get_recent_year_path(args):
    file_format = get_output_format(args.csv)
    path = get_output_directory() / "current_year"
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{args.city}_current_year.{file_format}"


def get_csv_files(directory):
    trip_files = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d != "__MACOSX"]
        for file in files:
            if file.endswith(".csv") and not file.startswith("__MACOSX/"):
                csv_path = os.path.join(root, file)
                trip_files.append(csv_path)
    return trip_files


def does_file_exist(file_name, file_size, folder):
    # Construct the full path of the file in the folder
    target_file_path = os.path.join(folder, file_name)

    # Check if the file exists in the folder
    if os.path.isfile(target_file_path):
        # Compare sizes
        target_file_size = os.path.getsize(target_file_path)
        return file_size == target_file_size
    return False


def write_to_parquet(df, file_path, **kwargs):
    """
    Writes the DataFrame or LazyFrame to a Parquet file.

    Parameters:
    - df: polars.DataFrame or polars.LazyFrame
    - file_path: str, Path to write the Parquet file.
    - kwargs: Additional keyword arguments for write_parquet or sink_parquet.
    """
    if isinstance(df, pl.LazyFrame):
        # Use sink_parquet for LazyFrame
        df.sink_parquet(file_path, **kwargs)
    elif isinstance(df, pl.DataFrame):
        # Use write_parquet for DataFrame
        df.write_parquet(file_path, **kwargs)
    else:
        raise TypeError("Input must be a polars.DataFrame or polars.LazyFrame")


def match_all_city_files(file_path, city):
    return True


def unzip_city_zips(city, city_matcher=match_all_city_files):
    city_zip_directory = get_zip_directory(city)
    raw_output_dir = get_raw_files_directory(city)

    to_process = [
        os.path.join(city_zip_directory, f)
        for f in os.listdir(city_zip_directory)
        if zipfile.is_zipfile(os.path.join(city_zip_directory, f))
        and city_matcher(f, city)
    ]

    while to_process:
        zip_path = to_process.pop()
        print(f"📂 Extracting: {zip_path}")

        with zipfile.ZipFile(zip_path, "r") as archive:
            with tempfile.TemporaryDirectory() as temp_dir:
                archive.extractall(temp_dir)

                for root, _, files in os.walk(temp_dir):
                    for file in files:
                        full_path = os.path.join(root, file)

                        if zipfile.is_zipfile(full_path):
                            # 🚨 Copy to a safe path before the temp dir is deleted
                            copied_path = os.path.join(tempfile.gettempdir(), file)
                            shutil.copy(full_path, copied_path)
                            to_process.append(copied_path)
                            print(f"📦 Found nested zip (copied): {copied_path}")

                        elif file.lower().endswith(".csv"):
                            target_path = os.path.join(raw_output_dir, file)
                            shutil.move(full_path, target_path)
                            print(f"✅ Extracted CSV: {target_path}")

    for root, _, files in os.walk(raw_output_dir):
        for file in files:
            if file.startswith("._"):
                os.remove(os.path.join(root, file))
                print(f"🧹 Removed AppleDouble file: {file}")


def rename_columns_for_keys(renamed_columns_dict):
    def inner(df):
        headers = df.collect_schema().names()
        relevant_columns = {
            key: renamed_columns_dict[key]
            for key in headers
            if key in renamed_columns_dict
        }
        return df.rename(relevant_columns)

    return inner


def get_recent_year_df(date_column):
    """Returns all rows one year from the last date"""

    def inner(df):
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        max_date = df.select(pl.max(date_column)).to_series()[0]
        one_year_ago = max_date - timedelta(days=365)

        # Filter the DataFrame for the last year of data
        last_year_df = df.filter(pl.col(date_column) >= one_year_ago)

        return last_year_df

    return inner


def convert_columns_to_datetime(date_column_names, date_formats):
    def inner(df):
        df = df.with_columns(
            [
                pl.coalesce(
                    [
                        pl.col(date_column)
                        # Some data has milliseconds, and some even have a mix of milliseconds and microseconds. Just remove these to reduce the trouble of formatting different datetimes
                        .str.replace(r"\.\d+", "")
                        .str.strptime(pl.Datetime, format, strict=False)
                        for format in date_formats
                    ]
                ).alias(date_column)
                for date_column in date_column_names
            ]
        )

        return df

    return inner


# Know this applies to Philadelphia, Mexico City, and Vancouver
def offset_two_digit_years(df):
    return df.with_columns(
        [
            pl.when(pl.col("start_time").dt.year() < 100)
            .then(pl.col("start_time").dt.offset_by("2000y"))
            .otherwise(pl.col("start_time"))
            .alias("start_time"),
            pl.when(pl.col("end_time").dt.year() < 100)
            .then(pl.col("end_time").dt.offset_by("2000y"))
            .otherwise(pl.col("end_time"))
            .alias("end_time"),
        ]
    )


def get_stations_df(city):
    METADATA_PATH = get_metadata_directory(city)

    station_info_json = METADATA_PATH / "station_information.json"
    stations = []
    with open(station_info_json, "r") as file:
        data = json.load(file)
        stations = data["data"]["stations"]
    df = pl.DataFrame(stations).select(["station_id", "name"])
    return df.lazy()


def handle_oslo_legacy_stations(df, args):
    stations_df = get_stations_df(args.city)
    METADATA_PATH = get_metadata_directory(args.city)

    stations_df = stations_df.select(["station_id", "name"]).with_columns(
        [pl.col("station_id").cast(pl.Int64)]
    )

    headers = df.collect_schema().names()
    ### Older data does not contain duration column
    if "duration" not in headers:
        station_mapping_df = pl.scan_csv(
            METADATA_PATH / "legacy_new_station_id_mapping.csv"
        ).with_columns(pl.col("legacy_id").cast(pl.String))
        df = (
            df.rename(
                {
                    "start_station_id": "start_station_legacy_id",
                    "end_station_id": "end_station_legacy_id",
                }
            )
            .join(
                station_mapping_df,
                left_on="start_station_legacy_id",
                right_on="legacy_id",
            )
            .rename({"new_id": "start_station_id"})
            .join(
                station_mapping_df,
                left_on="end_station_legacy_id",
                right_on="legacy_id",
            )
            .rename({"new_id": "end_station_id"})
            .join(stations_df, left_on="start_station_id", right_on="station_id")
            .rename({"name": "start_station_name"})
            .join(stations_df, left_on="end_station_id", right_on="station_id")
            .rename({"name": "end_station_name"})
        )
    return df


def create_all_trips_file(df, args):
    all_trips_path = get_all_trips_path(args)
    if args.csv:
        print("generating csv...this will take a bit...")
        df.write_csv(all_trips_path)
        print("csv files created")
    else:
        ### https://stackoverflow.com/questions/50604133/convert-csv-to-parquet-file-using-python
        print("generating all trips parquet... this will take a bit...")
        write_to_parquet(df, all_trips_path)
        print("parquet file for all trips created")


def create_recent_year_file(df, args, date_column="start_time"):
    df = get_recent_year_df(date_column)(df)
    recent_year_path = get_recent_year_path(args)
    if args.csv:
        print("generating recent year csv...this will take a bit...")
        df.write_csv(recent_year_path)
        print("csv files created")
    else:
        ### https://stackoverflow.com/questions/50604133/convert-csv-to-parquet-file-using-python
        print("generating recent year parquet... this will take a bit...")
        write_to_parquet(df, recent_year_path)
        print("parquet file for recent year created")


def get_bookend_dates(df):
    # Use min and max to find the earliest start_time and latest end_time
    result = df.select(
        pl.col("start_time").min().alias("earliest_start_time"),
        pl.col("end_time").max().alias("latest_end_time"),
    )

    print(result)

    # Extract values from the resulting DataFrame
    earliest_start_time = result["earliest_start_time"][0].isoformat()
    latest_end_time = result["latest_end_time"][0].isoformat()
    return earliest_start_time, latest_end_time


def fill_missing_years(data, start_year, end_year):
    # Convert the list to a dictionary for quick lookup by year
    year_dict = {entry["year"]: entry["has_null"] for entry in data}

    # Generate the full range of years and fill in missing ones with has_null: 0
    filled_data = [
        {"year": year, "has_null": year_dict.get(year, 0)}
        for year in range(start_year, end_year + 1)
    ]

    return filled_data


def get_null_rows_by_year(df, **kwargs):
    headers = kwargs.get("null_headers", df.columns)
    start_time, end_time = get_bookend_dates(df)

    start_year = parse(start_time).year
    end_year = parse(end_time).year
    lazy_df = df.lazy()
    null_df = (
        lazy_df.select(headers)
        .filter(
            pl.any_horizontal(pl.all().is_null())  # Keep rows with any NULL values
        )
        .with_columns(
            pl.col("start_time").dt.year().alias("year"),
            pl.any_horizontal(pl.all().is_null()).alias("has_null"),
        )
        .groupby("year")
        .agg(pl.col("has_null").sum())
        .sort("year")
    )

    collected_null_df = null_df.collect()

    print(collected_null_df)

    total_null_rows = collected_null_df["has_null"].sum()

    print(total_null_rows)
    nulls_by_year = fill_missing_years(
        collected_null_df.to_dicts(), start_year, end_year
    )

    return nulls_by_year, total_null_rows


def log_final_results(df, args, **kwargs):
    """Print all rows that have NULL in at least one column"""

    city = args.city
    city_json = {}
    json_data = {}

    output_directory = get_output_directory()
    summary_path = output_directory / "system_statistics.json"

    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    try:
        with open(summary_path, "r") as f:
            json_data = json.load(f)
    except Exception as e:
        print(f"No logging file found, will create new one. Error: {e}")
    null_headers = kwargs.get("null_headers", df.columns)

    null_rows_by_year, total_null_rows = get_null_rows_by_year(
        df, null_headers=null_headers
    )
    for header in null_headers:
        null_count = df.select(pl.col(header).is_null().sum()).item()
        city_json[f"null_{header}"] = null_count
    city_json["null_by_year"] = null_rows_by_year

    first_trip, last_trip = get_bookend_dates(df)
    city_json = city_json | {
        "total_rows": df.height,
        "null_rows": total_null_rows,
        "percent_complete": 100 - round(((total_null_rows / df.height) * 100), 2),
        "first_trip": first_trip,
        "last_trip": last_trip,
    }

    json_data[city] = city_json
    with open(summary_path, "w") as f:
        json.dump(json_data, f, indent=4)

    return df


def print_null_data(df):
    df_null_rows = df.filter(pl.any_horizontal(pl.all().is_null()))
    print(df_null_rows)
    return df


def assess_null_data(df):
    headers = df.columns
    for header in headers:
        null_count = df.select(pl.col(header).is_null().sum()).item()
        if null_count != 0:
            print(f"{header} has {null_count} rows with null values")
    return df


def create_final_files_and_logs(df, args):
    create_all_trips_file(df, args)
    create_recent_year_file(df, args)
    df = log_final_results(df, args)

    return df
