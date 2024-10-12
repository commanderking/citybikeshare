import sys
import os
import json
import polars as pl
project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)
import scripts.constants
import scripts.utils as utils

def handle_duration(df):
    headers = df.columns
    if "duration_seconds" not in headers:
        df = df.with_columns([
            (pl.col('end_time') - pl.col('start_time')).dt.total_seconds().alias('duration_seconds'),
        ])
    return df.with_columns([pl.col('start_time').dt.year().alias('year')])


def get_trips_per_year(city):
    print(f'reading {city}')
    parquet_file = f'./output/{city}_all_trips.parquet'
    
    query = (
        pl.scan_parquet(parquet_file)
        .drop_nulls(subset=['end_time', 'start_time'])
        .pipe(handle_duration)
        .select("*").group_by("year").agg([
            pl.count('start_time').alias('trip_count'),
            pl.mean('duration_seconds').alias('mean_duration'),
            pl.col('duration_seconds').quantile(0.25).alias('first_quantile_duration'),
            pl.median('duration_seconds').alias('median_duration'),
            pl.col('duration_seconds').quantile(0.75).alias('third_quantile_duration')
        ]).with_columns(pl.lit(city).alias("system"))
    )
    df = query.collect()
    
    
    return df

def get_all_cities_trip_per_year(cities):
    print("grouping trips taken by year")
    city_dfs = [get_trips_per_year(city) for city in cities]
    all_cities_df = (
        pl.concat(city_dfs)
        .sort("system", "year")
    )
    
    # Write the result to a JSON file
    json_string = all_cities_df.write_json(row_oriented=True)
    json_data = json.loads(json_string)

    output_path = utils.get_analysis_directory() / "trips_per_year.json"
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(json_data, file, indent=4)  

    return all_cities_df

def output_recent_dates(cities):
    print("determining most recent trip taken")
    most_recent_dates = []
    for city in cities:
        print(f'reading {city}')
        parquet_file = f'./output/{city}_all_trips.parquet'
        
        lazy_frame = pl.scan_parquet(parquet_file)
        lazy_frame = lazy_frame.select(pl.max("end_time").dt.strftime('%Y-%m-%d'))
        
        most_recent_dates.append({
            "system": city,
            "latest_trip": lazy_frame.collect().item()
        })
        
    output_path = utils.get_analysis_directory() / "latest_trips.json"    
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(most_recent_dates, file, indent=4)  # `indent=4` is optional, but it makes the JSON pretty-printed

if __name__ == "__main__":
    output_recent_dates(scripts.constants.ALL_CITIES)
    df = get_all_cities_trip_per_year(scripts.constants.ALL_CITIES)
