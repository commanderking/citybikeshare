import json
from citybikeshare.context import PipelineContext
import polars as pl
from collections import Counter
from citybikeshare.etl.transform import filter_filenames
from citybikeshare.config.loader import load_city_config
from citybikeshare.utils.paths import get_csv_files


def analyze_headers(context: PipelineContext):
    file_headers = {}
    header_counts = Counter()
    config = load_city_config(context.city)
    read_csv_opts = config.get("read_csv_options", {})
    csv_files = get_csv_files(context.raw_directory)
    filtered_files = filter_filenames(csv_files, config)

    for file in filtered_files:
        print(file)
        df = pl.read_csv(file, n_rows=1, ignore_errors=True, **read_csv_opts)
        file_headers[str(file)] = df.columns
        for col in df.columns:
            header_counts[col] += 1
    print(header_counts)

    output_path = context.metadata_directory / "headers.json"
    with open(output_path, "w") as file:
        json.dump(header_counts, file, indent=2)
        print(f"✅ Created {file.name} with json headers")
