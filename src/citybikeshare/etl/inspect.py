import json
from src.citybikeshare.context import PipelineContext
import polars as pl
from collections import Counter


def analyze_headers(context: PipelineContext):
    file_headers = {}
    header_counts = Counter()
    for file in context.raw_directory.rglob("*.csv"):
        print(file)
        df = pl.read_csv(file, n_rows=1, ignore_errors=True)
        file_headers[str(file)] = df.columns
        for col in df.columns:
            header_counts[col] += 1
    print(header_counts)

    output_path = context.metadata_directory / "headers.json"
    with open(output_path, "w") as file:
        json.dump(header_counts, file, indent=2)
        print(f"✅ Created {file} with json headers")
