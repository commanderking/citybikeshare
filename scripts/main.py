import argparse
import os
import sys
import zipfile
import city.boston as boston
import city.dc as dc
import city.taipei as taipei
import utils

project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)

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

    parser.add_argument('city', choices={"Boston", "DC", "Taipei" })

    args = parser.parse_args()
    return args

def extract_zip_files(city):
    print(f'unzipping {city} trip files')
    city_file_matcher = {
        "boston": "-tripdata.zip",
        "NYC": "citibike-tripdata",
        "dc": "capitalbikeshare-tripdata.zip",
        "Chicago": "divvy-tripdata"
    }

    city_zip_directory = utils.get_zip_directory(city)

    for file in os.listdir(city_zip_directory):
        file_path = os.path.join(city_zip_directory, file)
        if (zipfile.is_zipfile(file_path) and city_file_matcher[city] in file):
            with zipfile.ZipFile(file_path, mode="r") as archive:
                archive.extractall(utils.get_raw_files_directory(city))


def build_all_trips_file():
    args = setup_argparse()

    city = args.city.lower()
    output_format = "csv" if args.csv else "parquet"
    
    if args.skip_unzip is False:
        extract_zip_files(city)
    else:
        print("skipping unzipping files")
    
    source_directory = utils.get_raw_files_directory(city)
    build_path = utils.get_output_path(city, output_format)
    
    if city == "boston": 
        boston.build_all_trips(source_directory, build_path)

    if city == "dc":
        dc.build_all_trips(source_directory, build_path)
    
    if city == "taipei":
        taipei.create_all_trips_parquet()
        

if __name__ == "__main__":
    build_all_trips_file()
