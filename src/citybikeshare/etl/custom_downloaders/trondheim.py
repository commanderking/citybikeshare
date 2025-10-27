from src.citybikeshare.etl.custom_downloaders.utils.norway_cities import (
    download_files,
)


def download(config):
    source_url = config.get("source_url", "")
    city = config.get("name", "")
    download_files(source_url, city)
