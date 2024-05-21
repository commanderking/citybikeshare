import argparse
import os
import sys
import city.usa_cities as usa_cities
import city.taipei as taipei

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

    parser.add_argument('city', choices={"Boston", "DC", "Taipei", "Chicago", "NYC", "SF"})

    args = parser.parse_args()
    return args

def build_all_trips_file():
    args = setup_argparse()
    
    args.city = args.city.lower()
    city = args.city
        
    if city == "boston": 
        usa_cities.build_all_trips(args)

    if city == "dc":
        usa_cities.build_all_trips(args)
        
    if city == "chicago":
        usa_cities.build_all_trips(args)

    if city == "nyc":
        usa_cities.build_all_trips(args)
    
    if city == "sf":
        usa_cities.build_all_trips(args)

    if city == "taipei":
        taipei.create_all_trips_parquet(args)

if __name__ == "__main__":
    build_all_trips_file()
