from src.citybikeshare.etl.custom_downloaders.utils.bicycle_transit_systems import (
    download_files,
)


def download(config):
    download_files(config)
