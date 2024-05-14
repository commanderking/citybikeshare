import argparse
import os
import sys
import zipfile
import city.usa_cities as usa_cities
import city.taipei as taipei
import utils

project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)

def setup_argparse():
    parser = argparse.ArgumentParser(description='Merging all bikeshare trip data into One CSV or parquet file')

    parser.add_argument(
        '--csv',
        help='Output merged bike trip data into csv file. Default output is parquet file',
        action='store_true'
    )

    parser.add_argument(
        '--skip_unzip',
        help='Skips unzipping of files if files have already been unzipped',
        action='store_true'
    )

    parser.add_argument('city', choices={"Boston", "DC", "Taipei" })

    args = parser.parse_args()
    return args

def build_all_trips_file():
    args = setup_argparse()

    city = args.city.lower()
    print(city)
    output_format = "csv" if args.csv else "parquet"
    
    source_directory = utils.get_raw_files_directory(city)
    build_path = utils.get_output_path(city, output_format)
    
    if city == "boston": 
        usa_cities.build_all_trips(args, source_directory, build_path, usa_cities.rename_boston_columns)

    if city == "dc":
        usa_cities.build_all_trips(args, source_directory, build_path, usa_cities.rename_dc_columns)
    
    if city == "taipei":
        taipei.create_all_trips_parquet(args, build_path)
        

if __name__ == "__main__":
    build_all_trips_file()
