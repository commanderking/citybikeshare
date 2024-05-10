import argparse
import os
import zipfile
import boston
import dc 
from pathlib import Path

CURRENT_PATH = os.path.dirname(__file__)
def get_absolute_path(filename):
    return os.path.abspath(os.path.join(CURRENT_PATH, filename)) 

def get_city_zip_directory(city):
    return get_absolute_path(f'../data/{city.lower()}_zip')

def get_unzipped_csv_directory(city):
    return get_absolute_path(f"../data/{city.lower()}_csvs")

def get_build_path(city, fileFormat):
    return get_absolute_path(f"../../build/{city}/all_trips.{fileFormat}")

def create_folders(city):
    if not os.path.exists(get_absolute_path(f'../../build/{city.lower()}')):
        os.makedirs(get_absolute_path(f'../../build/{city.lower()}'))
        print(f'created build directory - files will be output in build/${city} folder')
    else:
        print("build directory found")

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

    ### create folder if needed
    outputPath = get_unzipped_csv_directory(city)
    if not os.path.exists(get_absolute_path(outputPath)):
        os.makedirs(outputPath)

    city_file_matcher = {
        "Boston": "-tripdata.zip",
        "NYC": "citibike-tripdata",
        "DC": "capitalbikeshare-tripdata.zip",
        "Chicago": "divvy-tripdata"
    }

    city_zip_directory = get_city_zip_directory(city)

    for file in os.listdir(city_zip_directory):
        file_path = os.path.join(city_zip_directory, file)
        if (zipfile.is_zipfile(file_path) and city_file_matcher[city] in file):
            with zipfile.ZipFile(file_path, mode="r") as archive:
                archive.extractall(outputPath)


def export_data(args):
    city = args.city
    output_format = "csv" if args.csv else "parquet"

    source_directory = get_unzipped_csv_directory(city)
    build_path = get_build_path(city, output_format)

    if city == "Boston": 
        boston.build_all_trips(source_directory, build_path)

    if city == "DC":
        dc.build_all_trips(source_directory, build_path)

def merge_data():
    args = setup_argparse()
    create_folders(args.city)

    if args.skip_unzip is False:
        extract_zip_files(args.city)
    else:
        print("skipping unzipping files")

    export_data(args)

if __name__ == "__main__":
    merge_data()
