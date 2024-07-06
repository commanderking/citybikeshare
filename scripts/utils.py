import os
import sys
import json
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

def get_output_directory():
    path = definitions.OUTPUT_DIR
    path.mkdir(parents=True, exist_ok=True)
    return path

def get_all_trips_path(args):
    file_format = get_output_format(args.csv)
    path = get_output_directory() / f'{args.city}_all_trips.{file_format}'
    return path

def get_recent_year_path(args):
    file_format = get_output_format(args.csv)
    path = get_output_directory() / f'{args.city}_recent_year.{file_format}' 
    return path

def get_csv_files(directory):
    trip_files = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d != '__MACOSX']
        for file in files:
            if (file.endswith(".csv") and not file.startswith("__MACOSX/")):
                csv_path = os.path.join(root, file)
                trip_files.append(csv_path)

    return trip_files

def get_recent_year_df(df):
    """Returns all rows one year from the last date"""
    max_date = df.select(pl.max("start_time")).to_series()[0]
    one_year_ago = max_date - timedelta(days=365)

    # Filter the DataFrame for the last year of data
    last_year_df = df.filter(pl.col("start_time") >= one_year_ago)
    
    return last_year_df

def create_all_trips_file(df, args):
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

def log_final_results(df, args):
    '''Print all rows that have NULL in at least one column'''

    city = args.city
    city_json = { "final_data": {} }
    json_data = {}

    output_directory = get_output_directory()
    logged_path = output_directory / "logged.json"
    try:
        with open(logged_path, 'r') as f:
            json_data = json.load(f)
    except:
        print("No logging file found, will create new one.")


    headers = df.columns
    for header in headers:
        null_count = df.select(pl.col(header).is_null().sum()).item()
        print(f'Column {header} has {null_count} rows with null values')
        city_json["final_data"][header] = null_count

    df_null_rows = (
        df
        .filter(
            pl.any_horizontal(pl.all().is_null())
        )
    )
    
    current_time =  datetime.now() 
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    city_json["final_data"] = {
        "total_rows": df.height,
        "null_rows": df_null_rows.height,
        "percent_null": round(((df_null_rows.height / df.height) * 100), 2),
        "updated_at": formatted_time
    }
    
    print(f'{df_null_rows.height} rows have at least one column with a null value')
    print(f'There are {df.height} total rows')
    print(f'{round(((df_null_rows.height / df.height) * 100), 2)}% of trips have a null value)')
    

    json_data[city] = city_json
    with open(logged_path, 'w') as f:
        json.dump(json_data, f, indent=4)
    
def assess_null_data(df):
    headers = df.columns
    for header in headers:
        null_count = df.select(pl.col(header).is_null().sum()).item()
        if (null_count != 0):
            print(f'{header} has {null_count} rows with null values')