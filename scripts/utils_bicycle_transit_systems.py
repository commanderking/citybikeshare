import os
import polars as pl

import scripts.utils as utils

RENAMED_STATION_COLUMNS = {
    ## Philadelphia
    "Station_ID": "station_id",
    "Station_Name": "station_name",
    "Day of Go_live_date": "go_live_date",
    ## Los Angeles
    "Kiosk ID": "station_id",
    "Kiosk Name": "station_name",
    "Go Live Date": "go_live_date",
    ## Both
    "Status": "status",
}


def stations_csv_to_df(args):
    city = args.city
    CSV_PATH = utils.get_raw_files_directory(city)
    # LA has station names with special characters: CicLAvia South LA � Exposition Hub
    df = pl.scan_csv(os.path.join(CSV_PATH, "stations.csv"), encoding="utf8-lossy")
    return df.pipe(utils.rename_columns_for_keys(RENAMED_STATION_COLUMNS)).with_columns(
        [
            pl.col("station_id").cast(pl.String),
        ]
    )


def append_station_names(trips_df, stations_df):
    joined_df = (
        trips_df.join(
            stations_df.select(["station_id", "station_name"]),
            left_on="start_station_id",
            right_on="station_id",
            how="left",
        )
        .with_columns(pl.col("station_name").alias("start_station_name"))
        .join(
            stations_df.select(["station_id", "station_name"]),
            left_on="end_station_id",
            right_on="station_id",
            how="left",
        )
        .with_columns(pl.col("station_name").alias("end_station_name"))
        .drop(["station_name", "station_name_right"])
    )

    return joined_df
