import pandas 
import requests 
import os
from zipfile import ZipFile
from io import BytesIO

from bikeshare.definitions import TAIPEI_CSVS_PATH
import polars as pl
import os

CURRENT_PATH = os.path.dirname(__file__)
def get_absolute_path(filename):
    return os.path.abspath(os.path.join(CURRENT_PATH, filename)) 


def read_csv_file(file_path, has_header=True, columns=None):
    if has_header:
        df = pl.read_csv(file_path)
    else:
        df = pl.read_csv(file_path, has_header=False, new_columns=columns, infer_schema_length=40000)
    return df

def determine_has_header(file_path, expected_columns):
    # Not all files have headers 
    with open(file_path, 'r', encoding="utf-8", errors="replace") as file:
        first_line = file.readline().strip().split(',')
        return all(item in expected_columns for item in first_line)

def toSeconds(rentColumn):
    rowsInSeconds = []
    for row in rentColumn:
        timeArray = row.split(":")
        
        seconds = int(timeArray[0]) * 3600 + int(timeArray[1]) * 60 + int(timeArray[2])
        rowsInSeconds.append(seconds)
    return pl.Series(rowsInSeconds)

def convert_rent_time_to_seconds(df, column_name):
    return df.with_columns(
        (pl.col(column_name)
         .map_batches(lambda timeString: toSeconds(timeString))
        )
    )


def create_df_with_all_trips(folder_path, expected_columns):
    csv_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.csv')]
    dataframes = []

    for file_path in csv_files:
        print(file_path)
        has_header = determine_has_header(file_path, expected_columns)

        # Read and clean the file
        with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
            data = file.read().replace('�', '')  # Replace or remove the replacement character
            
        # Write the cleaned data back to the original file
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(data)
        df = read_csv_file(file_path, has_header, expected_columns)
        dataframes.append(convert_rent_time_to_seconds(df, 'rent'))

    combined_df = pl.concat(dataframes)
    return combined_df



def clean_filename(filename):
    # Replace non-ASCII characters
    return f'{filename[:6]}.csv'


##
# Get main csv, which lists monthly csv data in zip form
# For all monthly zips, unzip all csv files to folder
# Read all csvs and bundle into one large parquet file
def extract_all_csvs():
    print(TAIPEI_CSVS_PATH)
    df = pandas.read_csv("https://tcgbusfs.blob.core.windows.net/dotapp/youbike_second_ticket_opendata/YouBikeHis.csv")
    
    for index, row in df.iterrows():
        file_url = row['fileURL']  # Assume the column containing the URLs is named 'fileURL'
        
        print(file_url)
        # Make an HTTP GET request to fetch the content of the zip file
        response = requests.get(file_url)
                
        if response.status_code == 200:
            with ZipFile(BytesIO(response.content)) as zip_file:
                zip_contents = zip_file.namelist()
                print(f"Contents of the zip file from {file_url}: {zip_contents}")
                
                for file in zip_contents:
                    clean_file = clean_filename(file)
                    
                    print(clean_file)
                    source = zip_file.open(file)
                    target_path = os.path.join(TAIPEI_CSVS_PATH, clean_file)

                    with open(target_path, 'wb') as target_file:
                        target_file.write(source.read())
                        print(f"Extracted and cleaned file to {target_path}")
        else:
            print(f"Failed to download file from {file_url}")
            
def export_to_parquet(df, output_path):
    # Save the DataFrame to a Parquet file
    print("writing parquet")
    df.write_parquet(output_path)

if __name__ == "__main__":
    # extract_all_csvs()
    expected_columns = ["rent_time","rent_station","return_time","return_station","rent","infodate"]

    combined_df = create_df_with_all_trips(TAIPEI_CSVS_PATH, expected_columns)
    
    # Specify the path where the Parquet file should be saved
    output_parquet_path = get_absolute_path("../../../build/taipei.parquet")
    export_to_parquet(combined_df, output_parquet_path)