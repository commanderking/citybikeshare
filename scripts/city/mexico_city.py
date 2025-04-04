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

date_formats = ["%m-%d-%Y", "%d/%m/%Y", "%Y-%m-%d"]

time_formats = ["%H:%M:%S", "%H:%M:%S %p"]


def get_stations_df():
    stations = []
    with open(STATION_INFORMATION_FILE) as f:
        results = json.load(f)
        stations = results["data"]["stations"]
    stations_df = pl.DataFrame(stations).select(["station_id", "name"])
    return stations_df


def create_df_with_all_trips(folder_path):
    csv_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".csv")
    ]
    dataframes = []

    stations_df = get_stations_df()
    for file_path in csv_files:
        print(file_path)
        df = pl.read_csv(
            file_path,
            schema_overrides={
                # 2018-12.csv - `C037` as dtype `i64` at column 'Bici'
                "Bici": pl.String,
                # 2024-05.csv - `273-274` as dtype `i64` at column 'Ciclo_Estacion_Retiro'
                "Ciclo_Estacion_Retiro": pl.String,
                # 2019-03.csv - `32:11.8` as dtype `i64` at column 'Ciclo_Estacion_Arribo'
                "Ciclo_Estacion_Arribo": pl.String,
                # 2022-08.csv - Typo error in column header - `390-391` as dtype `i64` at column 'Ciclo_EstacionArribo'
                "Ciclo_EstacionArribo": pl.String,
                # # Problem csv - 2022-09.csv - `390-391` as dtype `i64` at column 'CE_retiro'
                "CE_retiro": pl.String,
                "CE_arribo": pl.String,
            },
            # 2022_10.csv - `NULL` as dtype `i64` at column 'Edad_Usuario'
            null_values="NULL",
        )
        headers = df.columns

        ### Single header errors in csvs
        if "Ciclo_EstacionArribo" in headers:
            df = df.rename({"Ciclo_EstacionArribo": "Ciclo_Estacion_Arribo"})
        if "Fecha Arribo" in headers:
            df = df.rename({"Fecha Arribo": "Fecha_Arribo"})
        if "Hora_Arribo\r" in headers:
            df = df.rename({"Hora_Arribo\r": "Hora_Arribo"})
        if "Hora Arribo" in headers:
            df = df.rename({"Hora Arribo": "Hora_Arribo"})
        # In one file 2021-06, there are two headers for Hora Retiro. The duplicated one shoudl be Hora_Retiro
        if "Hora_Retiro_duplicated_0" in headers:
            df = df.rename({"Hora_Retiro_duplicated_0": "Hora_Arribo"})

        renamed_columns = mexico_city_renamed_columns
        if "Genero_usuario" in headers:
            renamed_columns = mexico_city_misaligned_renamed_columns

        df = (
            df.rename(renamed_columns)
            .pipe(utils.assess_null_data)
            .join(
                stations_df,
                left_on="start_station_id",
                right_on="station_id",
                how="left",
            )
            .rename({"name": "start_station_name"})
            .join(
                stations_df, left_on="end_station_id", right_on="station_id", how="left"
            )
            .rename({"name": "end_station_name"})
            .with_columns(
                [
                    pl.coalesce(
                        [
                            pl.col("start_date").str.strptime(
                                pl.Datetime, format, strict=False
                            )
                            for format in date_formats
                        ]
                    ),
                    pl.coalesce(
                        [
                            pl.col("end_date").str.strptime(
                                pl.Datetime, format, strict=False
                            )
                            for format in date_formats
                        ]
                    ),
                    pl.coalesce(
                        [
                            pl.col("starting_time")
                            .str.replace(r"\.\d+", "")
                            .str.zfill(8)
                            .str.strptime(pl.Time, format, strict=False)
                            for format in time_formats
                        ]
                    ),
                    pl.coalesce(
                        [
                            pl.col("ending_time")
                            .str.replace(r"\.\d+", "")
                            .str.zfill(8)
                            .str.strptime(pl.Time, format, strict=False)
                            for format in time_formats
                        ]
                    ),
                ]
            )
            .with_columns(
                [
                    pl.col("start_date")
                    .dt.combine(pl.col("starting_time"))
                    .alias("start_time"),
                    pl.col("end_date")
                    .dt.combine(pl.col("ending_time"))
                    .alias("end_time"),
                ]
            )
            .pipe(utils.offset_two_digit_years)
            .pipe(utils.assess_null_data)
        )

        df = df.select(
            [
                "start_time",
                "end_time",
                "start_station_name",
                "start_station_id",
                "end_station_name",
                "end_station_id",
            ]
        ).pipe(utils.print_null_data)
        dataframes.append(df)

    combined_df = pl.concat(dataframes)
    return combined_df


def build_trips(args):
    null_headers = ["start_station_id", "end_station_id", "end_time", "start_time"]
    get_stations_df()
    df = create_df_with_all_trips(MEXICO_CSVS_PATH)
    utils.create_all_trips_file(df, args)
    utils.log_final_results(df, args, null_headers=null_headers)
    utils.create_recent_year_file(df, args)


if __name__ == "__main__":
    get_exports(MEXICO_CITY_OPEN_DATA_URL, CSV_PATH)
