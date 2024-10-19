import scripts.utils_playwright as utils_playwright

PHILADELPHIA_BIKESHARE_URL = "https://www.rideindego.com/about/data/"


def get_zip_files():
    utils_playwright.get_bicycle_transit_systems_zips(
        PHILADELPHIA_BIKESHARE_URL, "philadelphia"
    )


if __name__ == "__main__":
    get_zip_files()
