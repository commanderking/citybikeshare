from pathlib import Path
from src.citybikeshare.utils.io_clean import (
    CLEAN_FUNCTIONS,
)
from src.citybikeshare.config.loader import load_city_config
from src.citybikeshare.utils.paths import get_zip_directory, get_raw_files_directory


def convert_csvs_to_parquet(files, args):
    config = load_city_config(args.city)
    for file in files:
        print(f"Processing {file}")
        clean_pipeline = config.get("clean_pipeline", [])
        for step in clean_pipeline:
            CLEAN_FUNCTIONS[step](file)


def clean(args):
    city = args.city
    path = get_raw_files_directory(city)
    config = load_city_config(city)

    clean_pipeline = config.get("cleaning_pipeline", [])

    if len(clean_pipeline) > 0:
        for step in clean_pipeline:
            CLEAN_FUNCTIONS[step](path, config)

        # 3. Strip header quotes
        # (can also be done lazily in transform if you prefer)
        print(f"✅ Cleaned raw CSVs for {city}")
    else:
        print(
            f"No cleaning needed for {city}. If cleaning expected, please update cleaning_pipeline in the city's yaml file"
        )
