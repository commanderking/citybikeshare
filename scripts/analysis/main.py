import argparse
import os
import sys

project_root = os.getenv("PROJECT_ROOT")
sys.path.insert(0, project_root)
import scripts.constants as constants
import scripts.analysis.analyzer as analyzer
import scripts.utils as utils

other_cities = constants.GLOBAL_CITIES
all_cities = constants.ALL_CITIES


def setup_argparse():
    parser = argparse.ArgumentParser(description="Analyze parquet files")
    parser.add_argument("city", choices=set([*all_cities, "all"]))

    args = parser.parse_args()
    return args


def run_analysis():
    args = setup_argparse()
    city = args.city

    if city == "all":
        analyzer.analyze_all_cities()
    else:
        analyzer.analyze_city(city)


if __name__ == "__main__":
    run_analysis()
