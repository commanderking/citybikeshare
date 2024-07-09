import sys
import os

import polars as pl
project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)
import scripts.constants


def get_trips_per_year(city):
    print("reading parquet")
    parquet_file = f'./output/{city}_all_trips.parquet'
    
    print("done reading")
    # Read the Parquet file into a Polars DataFrame
    df = pl.read_parquet(parquet_file).with_columns([
        (pl.col('end_time') - pl.col('start_time')).alias('duration_seconds'),
        pl.col('start_time').dt.year().alias('year')
    ])
    
    df = df.select("*").group_by("year").agg([
        pl.count('start_time').alias('count'),
        pl.mean('duration_seconds').alias('mean'),
        pl.median('duration_seconds').alias('median'),
    ]).with_columns(pl.lit(city).alias("system"))

    print(df)
    
    return df

def get_all_cities_trip_per_year():
    all_cities_df = None
    for city in scripts.constants.US_CITIES:
        df = get_trips_per_year(city)
        if all_cities_df is None:
            all_cities_df = df
        else:
            all_cities_df = pl.concat([all_cities_df, df])
            del df
    return all_cities_df

if __name__ == "__main__":
    result = get_all_cities_trip_per_year()
