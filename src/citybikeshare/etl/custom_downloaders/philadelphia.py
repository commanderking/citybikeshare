from src.citybikeshare.etl.custom_downloaders.utils.bicycle_transit_systems import (
    download_files,
)


def download(config, context):
    download_files(config, context)
