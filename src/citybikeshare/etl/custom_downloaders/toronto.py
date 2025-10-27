from zipfile import ZipFile
from io import BytesIO
import os
import requests

import scripts.utils as utils


def download(config):
    city = config.get("name")
    csv_path = utils.get_raw_files_directory(city)
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
                        file_path = os.path.join(csv_path, file_name)
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
