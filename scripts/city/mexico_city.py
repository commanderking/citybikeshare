import os
import requests
import json
import polars as pl
from playwright.sync_api import sync_playwright
import scripts.utils as utils

CSV_PATH = utils.get_raw_files_directory("mexico_city")
MEXICO_CITY_OPEN_DATA_URL = "https://ecobici.cdmx.gob.mx/en/open-data/"
MEXICO_CSVS_PATH = utils.get_raw_files_directory("mexico_city")
STATION_INFORMATION_FILE = (
    utils.get_metadata_directory("mexico_city") / "station_information.json"
)


def run_get_exports(playwright, url, csv_path):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(url)

    most_recent_year = 2024
    initial_year = 2010

    for year in range(initial_year, most_recent_year):
        print(year)
        page.get_by_role("link", name=f"{year}", exact=True).click()

    links = page.query_selector_all("a")

    for link in links:
        href = link.get_attribute("href")
        if href and ".csv" in href:
            print(f"Clicking on link with href: {href}")
            link.click()
            with page.expect_download(timeout=120000) as download_info:
                link.click()
            download = download_info.value
            download.save_as(os.path.join(csv_path, download.suggested_filename))

    browser.close()


def get_stations_info():
    url = "https://gbfs.mex.lyftbikes.com/gbfs/es/station_information.json"

    try:
        # Make a GET request to fetch the data
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the JSON content
        data = response.json()

        # Save the JSON data to a file
        with open(STATION_INFORMATION_FILE, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

        print(
            f"Data successfully downloaded and saved to '{STATION_INFORMATION_FILE}'."
        )
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching the data: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def get_exports(url, csv_path):
    # Mexico City data only includes station ids, not names
    # get_stations_info()
    with sync_playwright() as playwright:
        run_get_exports(playwright, url, csv_path)


mexico_city_renamed_columns = {
    "Genero_Usuario": "gender",
    "Edad_Usuario": "age",
    "Bici": "bike_id",
    "Ciclo_Estacion_Retiro": "start_station_id",
    "Fecha_Retiro": "start_date",
    "Hora_Retiro": "starting_time",
    "Ciclo_Estacion_Arribo": "end_station_id",
    "Fecha_Arribo": "end_date",
    "Hora_Arribo": "ending_time",
}

# Problem CSV - 2022-09.csv
mexico_city_misaligned_renamed_columns = {
    "Genero_usuario": "gender",
    "Edad_usuario": "age",
    "Bici": "bike_id",
    "CE_retiro": "start_station_id",
    "Fecha_retiro": "start_date",
    "Hora_retiro": "starting_time",
    "CE_arribo": "end_station_id",
    "Fecha_arribo": "end_date",
    "Hora_arribo": "ending_time",
}


if __name__ == "__main__":
    get_exports(MEXICO_CITY_OPEN_DATA_URL, CSV_PATH)
