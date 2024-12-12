import os
import requests
import polars as pl
from pathlib import Path
from playwright.sync_api import sync_playwright
import scripts.utils as utils

CSV_PATH = utils.get_raw_files_directory("guadalajara")
OPEN_DATA_ROOT = "https://www.mibici.net"
OPEN_DATA_URL = "https://www.mibici.net/es/datos-abiertos"

# Constants for file processing
RENAMED_COLUMNS = {
    "Viaje_Id": "trip_id",
    "Usuario_Id": "user_id",
    "Genero": "gender",
    "Inicio_del_viaje": "start_time",
    "Fin_del_viaje": "end_time",
    "Origen_Id": "start_station_id",
    "Destino_Id": "end_station_id",
}
DATE_FORMATS = ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M"]


def get_csv_links(playwright, url):
    """
    Fetch CSV file links from the given URL using Playwright.
    """
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)
    links = [
        f"{OPEN_DATA_ROOT}{link.get_attribute('href')}"
        for link in page.query_selector_all("a")
        if link.get_attribute("href") and ".csv" in link.get_attribute("href")
    ]

    browser.close()
    return links


def download_csv(link, csv_path, headers):
    """
    Download a single CSV file from the given link.
    """
    try:
        response = requests.get(link, headers=headers)
        response.raise_for_status()
        filename = os.path.basename(link)
        file_path = os.path.join(csv_path, filename)
        with open(file_path, "wb") as file:
            file.write(response.content)
        print(f"Downloaded: {filename}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {link}: {e}")


def download_all_csvs(links, csv_path):
    """
    Download all CSV files from a list of links.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/csv,application/csv,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;q=0.9,*/*;q=0.8",
        "Referer": "https://www.mibici.net/",
    }
    for link in links:
        download_csv(link, csv_path, headers)


def get_exports(url, csv_path):
    """
    Fetch CSV links and download them to the specified path.
    """
    with sync_playwright() as playwright:
        csv_links = get_csv_links(playwright, url)
    print(f"Found links: {csv_links}")
    download_all_csvs(csv_links, csv_path)


def get_stations_df():
    """
    Load the stations DataFrame from a file starting with 'nomenclatura'.
    """
    files = list(CSV_PATH.glob("nomenclatura*.csv"))
    if not files:
        raise FileNotFoundError(
            "No file starting with 'nomenclatura' found in the directory."
        )
    station_info_csv = files[0]
    return pl.read_csv(station_info_csv, encoding="utf8-lossy").select(["id", "name"])


def create_df_with_all_trips(folder_path):
    """
    Create a combined DataFrame from all trip CSV files.
    """
    csv_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".csv") and "nomenclatura" not in f
    ]
    dataframes = []
    stations_df = get_stations_df()

    for file_path in csv_files:
        print(f"Processing: {file_path}")
        df = pl.read_csv(file_path, encoding="utf8-lossy", null_values="NA")
        df = (
            df.rename(RENAMED_COLUMNS)
            .join(stations_df, left_on="start_station_id", right_on="id")
            .rename({"name": "start_station_name"})
            .join(stations_df, left_on="end_station_id", right_on="id")
            .rename({"name": "end_station_name"})
            .pipe(utils.assess_null_data)
            .pipe(
                utils.convert_columns_to_datetime(
                    ["start_time", "end_time"], DATE_FORMATS
                )
            )
            .select(
                ["start_station_name", "end_station_name", "start_time", "end_time"]
            )
        )
        dataframes.append(df)

    return pl.concat(dataframes)


def build_trips(args):
    """
    Orchestrates the process of downloading data, processing trips, and saving results.
    """
    if not args.skip_unzip:
        get_exports(OPEN_DATA_URL, CSV_PATH)
    df = create_df_with_all_trips(CSV_PATH)
    utils.create_all_trips_file(df, args)
    utils.log_final_results(df, args)
    utils.create_recent_year_file(df, args)


if __name__ == "__main__":
    # Example usage
    get_exports(OPEN_DATA_URL, CSV_PATH)
