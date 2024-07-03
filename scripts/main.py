import argparse
import os
import sys
import city.usa_cities as usa_utils
import city.taipei as taipei
import city.toronto as toronto
import constants
project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)

us_cities = list(constants.config.keys())
other_cities = ["taipei", "toronto"]
all_cities = us_cities + other_cities

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

    parser.add_argument('city', choices=set(all_cities))

    args = parser.parse_args()
    return args

def build_all_trips_file():
    args = setup_argparse()
    
    city = args.city
        
    if city in us_cities:
        usa_utils.build_all_trips(args)

    if city == "taipei":
        taipei.create_all_trips_parquet(args)
    
    if city == "toronto":
        toronto.build_trips(args)


if __name__ == "__main__":
    build_all_trips_file()
