import argparse
import os
import zipfile
import boston
import dc 
CURRENT_PATH = os.path.dirname(__file__)
def get_absolute_path(filename):
    return os.path.abspath(os.path.join(CURRENT_PATH, filename)) 

def getCityZipDirectory(city):
    return get_absolute_path(f'../data/{city.lower()}_zip')

def getCsvOutputDir(city):
    return get_absolute_path(f"../data/{city.lower()}_csvs")

def getOutputPath(city, fileFormat):
    return get_absolute_path(f"../../build/{city}/all_trips.{fileFormat}")


CSV_FILE = get_absolute_path("../../build/all_trips.csv")
PARQUET_FILE = get_absolute_path("../../build/all_trips.parquet")

def createFolders(city):
    if not os.path.exists(get_absolute_path(f'../../build/{city.lower()}')):
        os.makedirs(get_absolute_path(f'../../build/{city.lower()}'))
        print(f'created build directory - files will be output in build/${city} folder')
    else:
        print("build directory found")


def setup_argparse():
    parser = argparse.ArgumentParser(description='Merging all Bike Trip Data into One File')

    parser.add_argument('city', choices={"Boston", "DC" })

    parser.add_argument(
        '--csv',
        help='Output merged bike trip data into csv file only',
        action='store_true'
    )

    parser.add_argument(
        '--parquet',
        help='Output merged bike trip data into parquet file only',
        action='store_true'
    )

    parser.add_argument(
        '--skip_unzip',
        help='Skips unzipping of files if files have already been unzipped',
        action='store_true'
    )

    args = parser.parse_args()
    return args

def extract_zip_files(city):
    print(f'unzipping {city} trip files')

    ### create folder if needed
    outputPath = getCsvOutputDir(city)
    if not os.path.exists(get_absolute_path(outputPath)):
        os.makedirs(outputPath)

    city_file_matcher = {
        "Boston": "bluebikes-tripdata",
        "NYC": "citibike-tripdata",
        "DC": "capitalbikeshare-tripdata",
        "Chicago": "divvy-tripdata"
    }

    for file in os.listdir(getCityZipDirectory(city)):
        file_path = os.path.join(getCityZipDirectory(city), file)
        if (zipfile.is_zipfile(file_path) and city_file_matcher[city] in file):
            with zipfile.ZipFile(file_path, mode="r") as archive:
                archive.extractall(outputPath)


def export_data(args):
    city = args.city
    output_csv = args.csv
    output_parquet = args.parquet

    no_output_args = output_csv is False and output_parquet is False

    if no_output_args:
        output_csv = True
        output_parquet = True

    df = None

    if city == "Boston": 
        trip_files = boston.get_csv_files(getCsvOutputDir(city))
        df = boston.create_formatted_df(trip_files)

    if city == "DC":
        trip_files = dc.get_csv_files(getCsvOutputDir(city))
        df = dc.create_formatted_df(trip_files)

    if output_csv:
        print ("generating csv...this will take a bit...")
        df.to_csv(getOutputPath(city, 'csv'), index=True, header=True)
        print("csv file created")
    
    if output_parquet:
        ### Helpful - if need to do this on s3 at future date
        ### https://stackoverflow.com/questions/50604133/convert-csv-to-parquet-file-using-python
        print ("generating parquet... this will take a bit...")
        df.to_parquet(getOutputPath(city, 'parquet'))
        print("parquet file created")

def merge_data():
    args = setup_argparse()
    createFolders(args.city)
    if args.skip_unzip is False:
        extract_zip_files(args.city)
    export_data(args)

if __name__ == "__main__":
    merge_data()
