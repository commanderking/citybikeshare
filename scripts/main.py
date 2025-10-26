import argparse
import city.city_builder as city_builder
import constants

all_cities = constants.ALL_CITIES


def setup_argparse():
    parser = argparse.ArgumentParser(
        description="Merging all bikeshare trip data into parquet files"
    )

    parser.add_argument(
        "--skip_unzip",
        help="Skips unzipping of files if files have already been unzipped",
        action="store_true",
    )

    parser.add_argument("city", choices=set([*all_cities, "all"]))

    return parser.parse_args()


def build_city(args):
    city = args.city

    print(city)

    if city == "all":
        print(f"Attempting to build all cities - {constants.CONFIG_CITIES} ")

        for name in constants.CONFIG_CITIES:
            print(f"🚀 Running sync for {name}")
            try:
                args.city = name
                city_builder.build_all_trips(args)

                print(f"✅ Successfully built {name}\n")
            except Exception as e:
                print(f"❌ Error building {name}: {e}\n")
    else:
        city_builder.build_all_trips(args)


if __name__ == "__main__":
    args = setup_argparse()
    build_city(args)
