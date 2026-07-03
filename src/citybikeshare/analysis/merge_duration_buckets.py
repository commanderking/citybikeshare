import json
from pathlib import Path

from citybikeshare.utils.io import write_json


def merge_duration_buckets(analysis_folder):
    all_records = []

    analysis_folder = Path(analysis_folder)
    for city_dir in analysis_folder.iterdir():
        if not city_dir.is_dir():
            continue
        summary_file = city_dir / "duration_buckets.json"
        if summary_file.exists():
            with open(summary_file, "r") as f:
                city_data = json.load(f)

                for record in city_data:
                    record["city"] = city_dir.name
                    all_records.append(record)

    output_path = analysis_folder / "duration_buckets_all_cities.json"
    write_json(output_path, all_records)

    print(f"✅ Merged {len(all_records)} records into {output_path}")
