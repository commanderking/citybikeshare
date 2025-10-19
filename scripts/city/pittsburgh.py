import os
import json
import csv
import urllib.request
from playwright.sync_api import sync_playwright
import scripts.utils as utils

CSV_PATH = utils.get_raw_files_directory("pittsburgh")


def run(playwright):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()
    page.goto("https://data.wprdc.org/dataset/pogoh-trip-data")

    # Get resource ids

    resources = page.query_selector_all(".resource-item")

    resource_ids = []
    for resource in resources:
        data_id = resource.get_attribute("data-id")
        resource_ids.append(data_id)

    page.goto("https://data.wprdc.org/dataset/healthyride-trip-data")
    healthy_ride_resources = page.query_selector_all(".resource-item")

    for resource in healthy_ride_resources:
        data_id = resource.get_attribute("data-id")
        resource_ids.append(data_id)
    browser.close()

    # There's an extra resource that isn't trip data
    trip_data_dictionary_id = "fe985614-775c-42e3-9647-59099806329d"
    resource_ids = [item for item in resource_ids if item != trip_data_dictionary_id]

    return resource_ids


def get_monthly_resource_ids():
    with sync_playwright() as playwright:
        resource_ids = run(playwright)
        return resource_ids


def query_data(resource_ids):
    for resource_id in resource_ids:
        url = f"https://data.wprdc.org/api/3/action/datastore_search?resource_id={resource_id}&limit=10000000"
        csv_file = f"{resource_id}.csv"
        csv_file_path = os.path.join(CSV_PATH, csv_file)
        if os.path.exists(csv_file_path):
            print(f"🟡 Skipping Download for {csv_file_path} - file already exists")
        else:
            try:
                fileobj = urllib.request.urlopen(url)
                data = json.load(fileobj)
                trips = data["result"]["records"]

                with open(csv_file_path, "w", newline="") as csvfile:
                    # Create a CSV writer object
                    writer = csv.writer(csvfile)

                    # Write header
                    writer.writerow(trips[0].keys())

                    # Write rows
                    for row in trips:
                        writer.writerow(row.values())
                    print("✅ CSV written")
            except Exception as e:
                print(f"{resource_id} returned with an error: {e}")


if __name__ == "__main__":
    resource_ids = get_monthly_resource_ids()
    query_data(resource_ids)
