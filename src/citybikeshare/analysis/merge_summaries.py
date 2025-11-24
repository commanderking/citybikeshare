import json
from pathlib import Path


def merge_city_summaries(analysis_folder):
    all_records = []

    analysis_folder = Path("analysis")
    for city_dir in analysis_folder.iterdir():
        if not city_dir.is_dir():
            continue
        summary_file = city_dir / "summary.json"
        if summary_file.exists():
            with open(summary_file, "r") as f:
                city_data = json.load(f)

                ## Polars will generate "null" as year for data without an end time
                ## TODO: Considering checking the end_time and bucket then.
                filtered_data = [d for d in city_data if d.get("year") is not None]
                for record in filtered_data:
                    record["city"] = city_dir.name
                    all_records.append(record)

    output_path = analysis_folder / "summary_all_cities.json"
    with open(output_path, "w") as f:
        # Need to ensure_ascii false because polars will read in weird characters
        # and need to map the characters to proper header
        json.dump(all_records, f, indent=2, ensure_ascii=False)

    print(f"✅ Merged {len(all_records)} records into {output_path}")
