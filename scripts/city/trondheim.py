import polars as pl
import scripts.utils as utils
from scripts.city.utils.norway_cities import (
    final_columns,
    date_columns,
    date_formats,
    norway_renamed_columns,
    get_exports,
)

CITY = "trondheim"

ZIP_PATH = utils.get_zip_directory(CITY)
OPEN_DATA_URL = "https://trondheimbysykkel.no/en/open-data/historical"
CSV_PATH = utils.get_raw_files_directory(CITY)
METADATA_PATH = utils.get_metadata_directory(CITY)


def create_all_trips_df():
    files = utils.get_csv_files(CSV_PATH)
    all_dfs = []
    for file in files:
        print(f"reading {file}")
        df = (
            pl.read_csv(file)
            .rename(norway_renamed_columns)
            .select(final_columns)
            .pipe(utils.convert_columns_to_datetime(date_columns, date_formats))
        )
        all_dfs.append(df)

    all_trips = pl.concat(all_dfs)
    return all_trips


def build_trips(args):
    df = create_all_trips_df()
    utils.log_final_results(df, args)
    utils.create_all_trips_file(df, args)
    utils.create_recent_year_file(df, args)
    return df


if __name__ == "__main__":
    get_exports(OPEN_DATA_URL, CITY)
