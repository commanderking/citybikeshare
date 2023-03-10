import argparse
import os
import sqlite3
import zipfile
import csvformat

CURRENT_PATH = os.path.dirname(__file__)
def get_absolute_path(filename):
    return os.path.abspath(os.path.join(CURRENT_PATH, filename)) 

RAW_BLUEBIKE_ZIP_DIRECTORY = get_absolute_path("../data/blue_bike_data")
CSV_DIRECTORY = get_absolute_path("../data/monthlyTripCsvs") 

SQLITE_DB = get_absolute_path("../../build/bluebikes.db")
ALL_TRIPS = get_absolute_path("../../build/all_trips.csv")

def setup_argparse():
    parser = argparse.ArgumentParser(description='Merging all Bike Trip Data into One File')
    parser.add_argument(
        '--csv',
        help='Output merged bike trip data into csv file only',
        action='store_true'
    )
    parser.add_argument(
        '--sqlite',
        help='Output merged bike trip data into sqlite file only',
        action='store_true'
    )
    parser.add_argument(
        '--skip_unzip',
        help='Skips unzipping of files if files have already been unzipped',
        action='store_true'
    )

    args = parser.parse_args()
    return args

def extract_zip_files():
    print('unzipping bluebike trip files')
    for file in os.listdir(RAW_BLUEBIKE_ZIP_DIRECTORY):
        file_path = os.path.join(RAW_BLUEBIKE_ZIP_DIRECTORY, file)
        if (zipfile.is_zipfile(file_path) and "bluebikes-tripdata" in file):
            with zipfile.ZipFile(file_path, mode="r") as archive:
                archive.extractall(CSV_DIRECTORY)


def export_data(args):
    output_csv = args.csv
    output_sqlite = args.sqlite

    no_output_args = output_csv is False and output_sqlite is False

    if no_output_args:
        output_csv = True
        output_sqlite = True

    trip_files = csvformat.get_csv_files(CSV_DIRECTORY)

    df = csvformat.create_formatted_df(trip_files)

    if output_sqlite:
        print("generating sqlite db... this will take a bit...")
        connection = sqlite3.connect(SQLITE_DB)
        with connection:
            df.to_sql(name="bike_trip", con=connection, if_exists="replace")

    if output_csv:
        print ("generating csv...this will take a bit...")
        df.to_csv(ALL_TRIPS, index=True, header=True)

def merge_data():
    args = setup_argparse()
    if args.skip_unzip is False:
        extract_zip_files()
    export_data(args)

if __name__ == "__main__":
    merge_data()
