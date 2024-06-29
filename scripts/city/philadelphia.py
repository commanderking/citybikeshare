import os
import sys
from playwright.sync_api import sync_playwright

project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)
import definitions
import scripts.utils as utils
import scripts.city.usa_cities as usa_cities

def run(playwright):
    # Create a download directory if it doesn't exist
    download_path = utils.get_zip_directory("philadelphia")
    os.makedirs(download_path, exist_ok=True)

    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()
    page.goto('https://www.rideindego.com/about/data/')

    # Find all .zip file links and download them
    zip_links = page.query_selector_all('a[href$=".zip"]')
    for link in zip_links:
        with page.expect_download() as download_info:
            link.click()
        download = download_info.value
        download.save_as(os.path.join(download_path, download.suggested_filename))
        print(f'Downloaded { download.suggested_filename }')

    browser.close()

def get_zip_files():
    with sync_playwright() as playwright:
        run(playwright)
    
def build_trips(args):
    if not args.skip_unzip:
        usa_cities.extract_zip_files("philadelphia")
    # all_trips_df = create_all_trips_df()
    
    # utils.print_null_rows(all_trips_df)
    
    # utils.create_all_trips_file(all_trips_df, args)
    # utils.create_recent_year_file(all_trips_df, args)