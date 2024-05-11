# import definitions

import argparse
import os
import sys
import zipfile
import city.boston as boston
from pathlib import Path

project_root = os.getenv('PROJECT_ROOT')

sys.path.insert(0, project_root)

import definitions

def get_city_directory(city):
    # Define the path for Boston specific raw data
    city_raw_data_path = definitions.DATA_DIR / city
    
    # Ensure the directory exists, create if it does not
    city_raw_data_path.mkdir(parents=True, exist_ok=True)
    
    return city_raw_data_path

def get_zip_directory(city):
    path = definitions.DATA_DIR / city / 'zip'
    
    # Ensure the directory exists, create if it does not
    path.mkdir(parents=True, exist_ok=True)   
    
    return path

def get_raw_files_directory(city):
    path = definitions.DATA_DIR / city / 'raw'  
    path.mkdir(parents=True, exist_ok=True)   
 
    return path

def get_city_processed_data_directory(city):
    path = definitions.RAW_DATA_DIR / city
    
    path.mkdir(parents=True, exist_ok=True)   
 
    return path

def get_output_path(city, format):
    path = definitions.DATA_DIR / f'{city}_all_trips.{format}'
    return path
    

def setup_argparse():
    parser = argparse.ArgumentParser(description='Merging all bikeshare trip data into One CSV or parquet file')

    parser.add_argument(
        '--csv',
        help='Output merged bike trip data into csv file',
        action='store_true'
    )

    parser.add_argument(
        '--parquet',
        help='Output merged bike trip data into parquet file',
        action='store_true'
    )

    parser.add_argument(
        '--skip_unzip',
        help='Skips unzipping of files if files have already been unzipped',
        action='store_true'
    )

    parser.add_argument('city', choices={"Boston", "DC" })

    args = parser.parse_args()
    return args

def extract_zip_files(city):
    print(f'unzipping {city} trip files')
    city_file_matcher = {
        "boston": "-tripdata.zip",
        "NYC": "citibike-tripdata",
        "DC": "capitalbikeshare-tripdata.zip",
        "Chicago": "divvy-tripdata"
    }

    city_zip_directory = get_zip_directory(city)

    for file in os.listdir(city_zip_directory):
        file_path = os.path.join(city_zip_directory, file)
        if (zipfile.is_zipfile(file_path) and city_file_matcher[city] in file):
            with zipfile.ZipFile(file_path, mode="r") as archive:
                archive.extractall(get_raw_files_directory(city))


def export_data(args):
    city = args.city.lower()
    output_format = "csv" if args.csv else "parquet"

    source_directory = get_raw_files_directory(city)
    build_path = get_output_path(city, output_format)
    
    if city == "boston": 
        boston.build_all_trips(source_directory, build_path)

    # if city == "DC":
    #     dc.build_all_trips(source_directory, build_path)

def build_all_trips_file():
    args = setup_argparse()
    
    print(get_city_directory(args.city))

    if args.skip_unzip is False:
        extract_zip_files(args.city)
    else:
        print("skipping unzipping files")

    export_data(args)

if __name__ == "__main__":
    build_all_trips_file()
