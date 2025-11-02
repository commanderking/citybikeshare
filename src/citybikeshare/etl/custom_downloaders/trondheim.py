from src.citybikeshare.etl.custom_downloaders.utils.norway_cities import (
    download_files,
)


def download(config, context):
    source_url = config.get("source_url", "")
    download_files(source_url, context)
