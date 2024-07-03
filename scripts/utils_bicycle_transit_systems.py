import os
import sys
import polars as pl

project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)
import scripts.utils as utils

RENAMED_STATION_COLUMNS = {
    "Station_ID": "station_id",
    "Station_Name": "station_name",
    "Day of Go_live_date": "go_live_date",
    "Status": "status"
}


def stations_csv_to_df(args):
    city = args.city
    CSV_PATH = utils.get_raw_files_directory(city)
    # LA has station names with special characters: CicLAvia South LA � Exposition Hub
    df = pl.read_csv(os.path.join(CSV_PATH, "stations.csv"), encoding="utf8-lossy")
    return df.rename(RENAMED_STATION_COLUMNS).with_columns([
        pl.col("station_id").cast(pl.String),
    ])
def append_station_names(trips_df, stations_df):
    joined_df = trips_df.join(
        stations_df.select(['station_id', 'station_name']),
        left_on='start_station_id',
        right_on='station_id',
        how='left'
    ).with_columns(
        pl.col('station_name').alias('start_station_name')
    ).join(
        stations_df.select(['station_id', 'station_name']),
        left_on='end_station_id',
        right_on='station_id',
        how='left'   
    ).with_columns(
        pl.col('station_name').alias('end_station_name')
    ).drop(["station_name", "station_name_right"])
    
    return joined_df
