import os
import sys
from datetime import timedelta, datetime
import polars as pl

project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)
import definitions


def get_city_directory(city):
    city_raw_data_path = definitions.DATA_DIR / city
    city_raw_data_path.mkdir(parents=True, exist_ok=True)
    return city_raw_data_path

def get_zip_directory(city):
    path = definitions.DATA_DIR / city / 'zip'
    path.mkdir(parents=True, exist_ok=True)   
    return path

def get_raw_files_directory(city):
    path = definitions.DATA_DIR / city / 'raw'  
    path.mkdir(parents=True, exist_ok=True)   
    return path

def get_output_format(is_csv):
    return "csv" if is_csv else "parquet"


def get_all_trips_path(args):
    file_format = get_output_format(args.csv)
    path = definitions.DATA_DIR / f'{args.city}_all_trips.{file_format}'
    return path

def get_recent_year_path(args):
    file_format = get_output_format(args.csv)
    path = definitions.DATA_DIR / f'{args.city}_recent_year.{file_format}'
    return path

def get_csv_files(directory):
    trip_files = []
    for file in os.listdir(directory):
        if (file.endswith(".csv")):
            csv_path = os.path.join(directory, file)
            trip_files.append(csv_path)

    return trip_files

def get_recent_year_df(df):
    # Get the maximum date
    max_date = df.select(pl.max("start_time")).to_series()[0]
    
    

    print(max_date)
    # Calculate the date one year ago
    one_year_ago = max_date - timedelta(days=365)

    print(one_year_ago)
    # Filter the DataFrame for the last year of data
    last_year_df = df.filter(pl.col("start_time") >= one_year_ago)
    
    return last_year_df

def create_files(df, args):
    all_trips_path = get_all_trips_path(args)
    if args.csv:
        print ("generating csv...this will take a bit...")
        df.write_csv(all_trips_path)
        print("csv files created")
    else: 
        ### https://stackoverflow.com/questions/50604133/convert-csv-to-parquet-file-using-python
        print ("generating parquet... this will take a bit...")
        df.write_parquet(all_trips_path)
        print('parquet files created')

def create_recent_year_file(df, args):
    df = get_recent_year_df(df)
    recent_year_path = get_recent_year_path(args)
    if args.csv:
        print ("generating recent year csv...this will take a bit...")
        df.write_csv(recent_year_path)
        print("csv files created")
    else: 
        ### https://stackoverflow.com/questions/50604133/convert-csv-to-parquet-file-using-python
        print ("generating recent year parquet... this will take a bit...")
        df.write_parquet(recent_year_path)
        print('parquet files created')