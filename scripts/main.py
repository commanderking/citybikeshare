import argparse
import city.usa_cities as usa_utils
import city.taipei as taipei
import city.mexico_city as mexico_city
import city.montreal as montreal
import city.vancouver as vancouver
import city.oslo as oslo
import city.bergen as bergen
import city.trondheim as trondheim
import city.london as london
import city.helsinki as helsinki
import city.guadalajara as guadalajara
import constants

other_cities = constants.GLOBAL_CITIES
all_cities = constants.ALL_CITIES

city_builders = {
    "vancouver": vancouver.build_trips,
    "oslo": oslo.build_trips,
    "london": london.build_trips,
    "bergen": bergen.build_trips,
    "trondheim": trondheim.build_trips,
    "taipei": taipei.create_all_trips_parquet,
    "mexico_city": mexico_city.build_trips,
    "helsinki": helsinki.build_trips,
    "guadalajara": guadalajara.build_trips,
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
        "--parquet", help="Generates a parquet file with all trips", action="store_true"
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
