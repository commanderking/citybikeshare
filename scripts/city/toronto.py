from zipfile import ZipFile
from io import BytesIO
import os
import requests
import polars as pl

import scripts.utils as utils
import definitions

PARQUET_OUTPUT_PATH = definitions.DATA_DIR / "toronto_all_trips.parquet"

TORONTO_CSV_PATH = utils.get_raw_files_directory("toronto")


def extract_csvs():
    # To hit our API, you'll be making requests to:
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

    # Datasets are called "packages". Each package can contain many "resources"
    # To retrieve the metadata for this package and its resources, use the package name in this page's URL:
    url = base_url + "/api/3/action/package_show"
    params = {"id": "bike-share-toronto-ridership-data"}
    package = requests.get(url, params=params).json()

    # To get resource data:
    for idx, resource in enumerate(package["result"]["resources"]):
        # To get metadata for non datastore_active resources:
        if not resource["datastore_active"]:
            url = base_url + "/api/3/action/resource_show?id=" + resource["id"]
            resource_metadata = requests.get(url).json()
            response = requests.get(resource_metadata["result"]["url"], timeout=10000)
            if response.status_code == 200:
                with ZipFile(BytesIO(response.content)) as zip_file:
                    zip_contents = zip_file.namelist()
                    for file in zip_contents:
                        # Setup paths and names
                        file_name = os.path.basename(file)
                        file_path = os.path.join(TORONTO_CSV_PATH, file_name)
                        target_path = file_path.lower().replace(" ", "_")

                        # Skip if already downloaded
                        if os.path.exists(target_path):
                            print(
                                f"🟡 Skipping Download - {os.path.basename(target_path)} exists"
                            )

                        # Download and Extract
                        else:
                            if file.endswith(".csv"):
                                source = zip_file.open(file)
                                print(file)
                                with open(target_path, "wb") as target_file:
                                    target_file.write(source.read())
                                    print(
                                        f"✅ Extracted and cleaned file to {target_path}"
                                    )
            else:
                print(
                    f"Failed to download file from {resource_metadata['result']['url']}"
                )


### Keeping for reference
# def create_all_trips_df():
#     csv_files = [
#         os.path.join(TORONTO_CSV_PATH, f)
#         for f in os.listdir(TORONTO_CSV_PATH)
#         if f.endswith(".csv")
#     ]

#     dfs = []
#     for file in csv_files:
#         print(file)
#         # TODO: utf8-lossy needed because there are some special characters in csv
#         # Example: Gailbraith Rd / KingG��s College Cr. (U of T)
#         df = pl.read_csv(file, infer_schema_length=0, encoding="utf8-lossy")

#         df = map_columns(df)
#         date_formats = ["%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S", "%d/%m/%Y %H:%M"]
#         df = (
#             df.with_columns(
#                 [
#                     pl.coalesce(
#                         [
#                             pl.col("start_time").str.strptime(
#                                 pl.Datetime, format, strict=False
#                             )
#                             for format in date_formats
#                         ]
#                     ),
#                     pl.coalesce(
#                         [
#                             pl.col("end_time").str.strptime(
#                                 pl.Datetime, format, strict=False
#                             )
#                             for format in date_formats
#                         ]
#                     ),
#                     pl.col("duration").cast(pl.Int32),
#                 ]
#             )
#             # Toronto has a fe  w data points in 2017 that have year as 17
#             .pipe(utils.offset_two_digit_years)
#             # 2020-10.csv has 249 rows where start and end date are null
#             .filter(
#                 pl.col("start_time").is_not_null() & pl.col("end_time").is_not_null()
#             )
#         )
#         dfs.append(df)
#     return pl.concat(dfs)


if __name__ == "__main__":
    extract_csvs()
