import argparse
import city.usa_cities as usa_utils
import city.taipei as taipei
import city.toronto as toronto
import city.mexico_city as mexico_city
import city.montreal as montreal
import city.vancouver as vancouver
import city.oslo as oslo
import city.bergen as bergen
import city.trondheim as trondheim
import constants

other_cities = constants.GLOBAL_CITIES
all_cities = constants.ALL_CITIES

city_builders = {
    "vancouver": vancouver.build_trips,
    "oslo": oslo.build_trips,
    "bergen": bergen.build_trips,
    "trondheim": trondheim.build_trips,
    "taipei": taipei.create_all_trips_parquet,
    "toronto": toronto.build_trips,
    "montreal": montreal.build_trips,
    "mexico_city": mexico_city.build_trips,
}


def setup_argparse():
    parser = argparse.ArgumentParser(
        description="Merging all bikeshare trip data into One CSV or parquet file"
    )

    parser.add_argument(
        "--csv",
        help="Output merged bike trip data into csv file. Default output is parquet file",
        action="store_true",
    )

    parser.add_argument(
        "--skip_unzip",
        help="Skips unzipping of files if files have already been unzipped",
        action="store_true",
    )

    parser.add_argument("city", choices=set([*all_cities, "all"]))

    args = parser.parse_args()
    return args


def build_city(args):
    city = args.city
    if city in constants.US_CITIES:
        usa_utils.build_all_trips(args)
    else:
        city_builders[city](args)


if __name__ == "__main__":
    args = setup_argparse()
    city = args.city

    if city == "all":
        for city in all_cities:
            setattr(args, "city", city)
            build_city(args)
    build_city(args)
