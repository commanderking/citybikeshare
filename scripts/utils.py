import os
import sys
import json
from datetime import timedelta, datetime
import polars as pl
import zipfile
import constants


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

def get_analysis_directory():
    path = get_output_directory() / 'analysis'
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

def match_all_city_files(file_path, city):
    return True

def unzip_city_zips(city, city_matcher=match_all_city_files):
    city_zip_directory = get_zip_directory(city)
    for file in os.listdir(city_zip_directory):
        file_path = os.path.join(city_zip_directory, file)
        if (zipfile.is_zipfile(file_path) and city_matcher(file_path, city)):
            with zipfile.ZipFile(file_path, mode="r") as archive:
                print(file_path)
                archive.extractall(get_raw_files_directory(city))

def get_applicable_columns_mapping(df, rename_dict):
    # Filter the rename dictionary to include only columns that exist in the DataFrame
    existing_columns = df.columns
    filtered_rename_dict = {old: new for old, new in rename_dict.items() if old in existing_columns}

    return filtered_rename_dict


def rename_columns(args, mappings, final_column_headers=constants.final_columns):
    def inner(df):
        headers = df.columns
        applicable_renamed_columns = []
        
        # TODO: This should be more robust - theoretically, multiple column mappings could match and the 
        # last match would be the mapping used
        for mapping in mappings:
            if (mapping["header_matcher"] in headers):
                applicable_renamed_columns = get_applicable_columns_mapping(df, mapping["mapping"])
                final_columns = mapping.get("final_columns", final_column_headers)
                renamed_df = df.rename(applicable_renamed_columns).select(final_columns)
                return renamed_df
        raise ValueError(f'We could not rename the columns because no valid column mappings for {args.city} match the data! The headers we found are: {df.columns}')
    
    return inner

def get_recent_year_df(date_column):
    """Returns all rows one year from the last date"""
    def inner (df):
        max_date = df.select(pl.max(date_column)).to_series()[0]
        one_year_ago = max_date - timedelta(days=365)

        # Filter the DataFrame for the last year of data
        last_year_df = df.filter(pl.col(date_column) >= one_year_ago)
        
        return last_year_df
    return inner

def convert_date_columns_to_datetime(date_column_names, date_formats):
    def inner(df):
        df = df.with_columns([
            pl.coalesce([
                # Replace . and everything that follows with empty string. Some Boston dates have milliseconds
                df[date_column].str.replace(r"\.\d+", "").str.strptime(pl.Datetime, format, strict=False)
                for format in date_formats
            ]).alias(date_column) for date_column in date_column_names
        ])
        return df
    return inner
    

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

def create_recent_year_file(df, args, date_column="start_time"):
    df = get_recent_year_df(date_column)(df)
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
    return df