import os
import zipfile
import datetime
import requests
import scripts.utils as utils

ZIP_PATH = utils.get_zip_directory("helsinki")


### Referenced https://github.com/Geometrein/helsinki-city-bikes/blob/main/scraper.py
def generate_link(year):
    url = f"http://dev.hsl.fi/citybikes/od-trips-{year}/od-trips-{year}.zip"
    return url


def download_year(year, download_path, chunk_size=128):
    url = generate_link(year)
    path = os.path.join(download_path, f"{year}.zip")
    try:
        r = requests.get(url, stream=True)
        with open(path, "wb") as fd:
            print(f"Downloading {url}")
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)
        print("Download Complete")
    except:
        print("Experienced a problem with downloading the files")


def unzip_files_in_directory(zip_dir, output_dir):
    for zip_file in os.listdir(zip_dir):
        zip_path = os.path.join(zip_dir, zip_file)

        if zipfile.is_zipfile(zip_path):
            print(f"Unzipping: {zip_file}")
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(output_dir)

            print(f"Contents of {zip_file} moved to {output_dir}")
        else:
            print(f"Skipping non-zip file: {zip_file}")


def download(config):
    city = config.get("name")
    zip_path = utils.get_zip_directory(city)
    for year in range(2016, datetime.datetime.now().year + 1):
        download_year(year, zip_path)
