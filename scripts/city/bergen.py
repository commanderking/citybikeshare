import polars as pl
import scripts.utils as utils
import scripts.city.utils.norway_cities as norway_cities

CITY = "bergen"

ZIP_PATH = utils.get_zip_directory(CITY)
OPEN_DATA_URL = "https://bergenbysykkel.no/en/open-data/historical"
CSV_PATH = utils.get_raw_files_directory(CITY)
METADATA_PATH = utils.get_metadata_directory(CITY)
date_formats = ["%Y-%m-%d %H:%M:%S.%f%:z", "%Y-%m-%d %H:%M:%S%:z"]

renamed_columns = {
    "started_at": "start_time",
    "ended_at": "end_time",
    "start_station_name": "start_station_name",
    "end_station_name": "end_station_name",
}
final_columns = ["start_station_name", "end_station_name", "start_time", "end_time"]


def create_all_trips_df(args):
    files = utils.get_csv_files(CSV_PATH)
    all_dfs = []
    for file in files:
        print(f"reading {file}")
        df = (
            pl.read_csv(file)
            .rename(renamed_columns)
            .select(final_columns)
            .with_columns(
                [
                    pl.coalesce(
                        [
                            pl.col("start_time").str.strptime(
                                pl.Datetime, format, strict=False
                            )
                            for format in date_formats
                        ]
                    ),
                    pl.coalesce(
                        [
                            pl.col("end_time").str.strptime(
                                pl.Datetime, format, strict=False
                            )
                            for format in date_formats
                        ]
                    ),
                ]
            )
        )
        all_dfs.append(df)

    all_trips = pl.concat(all_dfs)
    return all_trips


def get_exports(url):
    norway_cities.get_exports(url, CITY)


def build_trips(args):
    df = create_all_trips_df(args)
    utils.log_final_results(df, args)
    utils.create_all_trips_file(df, args)
    utils.create_recent_year_file(df, args)
    return df


if __name__ == "__main__":
    get_exports(OPEN_DATA_URL)
