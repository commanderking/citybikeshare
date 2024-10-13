import argparse
import os
import sys
import city.usa_cities as usa_utils
import city.taipei as taipei
import city.toronto as toronto
import city.mexico_city as mexico_city
import city.montreal as montreal
import city.vancouver as vancouver
import city.oslo as oslo
import constants
project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)

other_cities = constants.GLOBAL_CITIES
all_cities = constants.ALL_CITIES

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
    
    if city == "vancouver":
        vancouver.build_trips(args)
    if city == "oslo":
        oslo.build_trips(args)
        
    if city in constants.US_CITIES:
        usa_utils.build_all_trips(args)

    if city == "taipei":
        taipei.create_all_trips_parquet(args)
    
    if city == "toronto":
        toronto.build_trips(args)
    
    if city == "montreal":
        montreal.build_trips(args)
    
    if city == "mexico_city":
        mexico_city.build_trips(args)
if __name__ == "__main__":
    build_all_trips_file()
