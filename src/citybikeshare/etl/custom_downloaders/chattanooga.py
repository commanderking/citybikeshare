from src.citybikeshare.etl.custom_downloaders.utils.get_single_csv_export import (
    get_exports,
)


def download(config, context):
    source_url = config.get("source_url", "")
    get_exports(source_url, context)
