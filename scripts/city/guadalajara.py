import os
import requests
from playwright.sync_api import sync_playwright
import scripts.utils as utils

CSV_EXTENSION = ".csv"
CSV_PATH = utils.get_raw_files_directory("guadalajara")
OPEN_DATA_ROOT = "https://www.mibici.net"
OPEN_DATA_URL = "https://www.mibici.net/es/datos-abiertos"

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/csv,application/csv,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;q=0.9,*/*;q=0.8",
    "Referer": "https://www.mibici.net/",
}


def get_csv_links(playwright, url):
    """
    Fetch CSV file links from the given URL using Playwright.
    """
    browser = playwright.chromium.launch(headless=True)
    try:
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto(url)

        links = [
            f"{OPEN_DATA_ROOT}{link.get_attribute('href')}"
            for link in page.query_selector_all("a")
            if link.get_attribute("href")
            and CSV_EXTENSION in link.get_attribute("href")
        ]

        if not links:
            print(f"No CSV links found at {url}")
        return links

    except Exception as e:
        print(f"Error fetching CSV links: {e}")
        return []

    finally:
        browser.close()


def download_csv(link, csv_path, headers):
    """
    Download a single CSV file from the given link.
    """
    filename = os.path.basename(link)
    file_path = os.path.join(csv_path, filename)

    if os.path.exists(file_path):
        print(
            f"🟡 Skipping Download for {os.path.exists(file_path)} - file already exists"
        )
        return

    try:
        response = requests.get(link, headers=headers)
        response.raise_for_status()
        with open(file_path, "wb") as file:
            file.write(response.content)
        print(f"Downloaded: {filename}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {link}: {e}")


def download_all_csvs(links, csv_path):
    for link in links:
        download_csv(link, csv_path, REQUEST_HEADERS)


def get_exports(url, csv_path):
    """
    Fetch CSV links and download them to the specified path.
    """
    with sync_playwright() as playwright:
        csv_links = get_csv_links(playwright, url)
    download_all_csvs(csv_links, csv_path)


if __name__ == "__main__":
    # Example usage
    get_exports(OPEN_DATA_URL, CSV_PATH)
