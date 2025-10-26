import argparse
import city.usa_cities as usa_utils
import city.mexico_city as mexico_city
import constants

other_cities = constants.GLOBAL_CITIES
all_cities = constants.ALL_CITIES

city_builders = {
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

    print(city)

    if city == "all":
        print(f"Attempting to build all cities - {constants.CONFIG_CITIES} ")

        for name in constants.CONFIG_CITIES:
            print(f"🚀 Running sync for {name}")
            try:
                args.city = name
                usa_utils.build_all_trips(args)

                print(f"✅ Successfully built {name}\n")
            except Exception as e:
                print(f"❌ Error building {name}: {e}\n")
    else:
        if city in constants.CONFIG_CITIES:
            usa_utils.build_all_trips(args)
        else:
            city_builders[city](args)


if __name__ == "__main__":
    args = setup_argparse()
    build_city(args)
