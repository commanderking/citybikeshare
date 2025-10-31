from pathlib import Path
from src.citybikeshare.utils.io_clean import (
    CLEAN_FUNCTIONS,
)
from src.citybikeshare.config.loader import load_city_config
from src.citybikeshare.utils.paths import get_raw_files_directory


def convert_csvs_to_parquet(files, args):
    config = load_city_config(args.city)
    for file in files:
        print(f"Processing {file}")
        clean_pipeline = config.get("clean_pipeline", [])
        for step in clean_pipeline:
            CLEAN_FUNCTIONS[step](file)


def clean_city_data(args):
    city = args.city
    path = get_raw_files_directory(city)
    config = load_city_config(city)
    clean_pipeline = config.get("clean_pipeline", [])

    csv_files = list(Path(path).glob("*.csv"))
    if not csv_files:
        print(f"⚠️ No CSV files found for {city}")
        return

    print(f"🧽 Cleaning {len(csv_files)} CSV files for {city}...")

    for csv_file in csv_files:
        print(f"\n📄 Cleaning {csv_file.name}")
        for step in clean_pipeline:
            fn = CLEAN_FUNCTIONS.get(step)
            if fn:
                fn(csv_file, config)
            else:
                print(f"⚠️ Unknown clean step: {step}")

    print(f"✅ Finished cleaning all CSVs for {city}")
