import os
from zipfile import ZipFile
from io import BytesIO
import requests
import polars as pl


### CSV files often have non-ASCII characters (i.e. 202403_YouBike2.0≤º√“®Í•d∏ÍÆ∆.csv)
def clean_filename(filename):
    return f"{filename[:6]}.csv"


##
# Get main csv, which lists monthly csv data in zip form
# For all monthly zips, unzip all csv files to folder
# Read all csvs and bundle into one large parquet file
def download(config, context):
    download_path = context.download_directory
    df = pl.read_csv(
        "https://tcgbusfs.blob.core.windows.net/dotapp/youbike_second_ticket_opendata/YouBikeHis.csv"
    )

    file_urls = df["fileURL"].to_list()

    for file_url in file_urls:
        # Make an HTTP GET request to fetch the content of the zip file
        response = requests.get(file_url, timeout=10000)

        if response.status_code == 200:
            with ZipFile(BytesIO(response.content)) as zip_file:
                zip_contents = zip_file.namelist()

                for file in zip_contents:
                    clean_file = clean_filename(file)

                    source = zip_file.open(file)
                    target_path = os.path.join(download_path, clean_file)

                    with open(target_path, "wb") as target_file:
                        target_file.write(source.read())
                        print(f"Extracted and cleaned file to {target_path}")
        else:
            print(f"Failed to download file from {file_url}")
