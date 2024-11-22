import polars as pl
import scripts.utils as utils

CITY = "london"
CSV_PATH = utils.get_raw_files_directory(CITY)

version_one_columns = {
    "Start date": "start_time",
    "End date": "end_time",
    "Start station": "start_station_name",
    "End station": "end_station_name",
}

legacy_columns = {
    "Start Date": "start_time",
    "End Date": "end_time",
    "EndStation Name": "end_station_name",
    "StartStation Name": "start_station_name",
}

# 81 has this
legacy_columns_2 = {
    "Start Date": "start_time",
    "End Date": "end_time",
    "End Station Name": "end_station_name",
    "Start Station Name": "start_station_name",
}

date_columns = ["start_time", "end_time"]
date_formats = ["%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M"]

final_columns = ["start_station_name", "end_station_name", "start_time", "end_time"]

config = {
    "name": "vancouver",
    "column_mappings": [
        {"header_matcher": "Start station", "mapping": version_one_columns},
        {"header_matcher": "StartStation Name", "mapping": legacy_columns},
        {"header_matcher": "Start Station Name", "mapping": legacy_columns_2},
    ],
}


def create_all_trips_df(args):
    files = utils.get_csv_files(CSV_PATH)
    all_dfs = []
    for file in files:
        print(f"reading {file}")

        df = pl.read_csv(file, infer_schema_length=0)

        df = (
            df.pipe(utils.rename_columns(config["column_mappings"], final_columns))
            .select(final_columns)
            .pipe(utils.convert_columns_to_datetime(date_columns, date_formats))
        )

        all_dfs.append(df)

    all_trips = pl.concat(all_dfs, how="diagonal")
    return all_trips


def build_trips(args):
    df = create_all_trips_df(args)
    utils.log_final_results(df, args)
    utils.create_all_trips_file(df, args)
    utils.create_recent_year_file(df, args)
    return df
