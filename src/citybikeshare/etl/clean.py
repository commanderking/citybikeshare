from pathlib import Path
from citybikeshare.utils.io_clean import (
    CLEAN_FUNCTIONS,
    JSON_CONVERT_FUNCTIONS,
    materialize_cleaned_source,
    stream_clean_to_gzip,
)
from citybikeshare.config.loader import load_city_config
from citybikeshare.context import PipelineContext
from citybikeshare.etl.state import (
    file_signature,
    is_unchanged,
    load_state,
    write_state,
)


def clean_city_data(context: PipelineContext):
    city = context.city
    raw_dir = context.raw_directory
    cleaned_dir = context.cleaned_directory
    config = load_city_config(city)
    clean_pipeline = config.get("clean_pipeline", [])

    if not clean_pipeline:
        print(
            "No cleaning necessary! If this is a mistake, make sure the city's yaml file as a clean_pipeline configuration."
        )
        return

    # Raw inputs may be plain `.csv` or gzipped `.csv.gz` (after the raw-gzip migration).
    csv_files = sorted([*Path(raw_dir).glob("*.csv"), *Path(raw_dir).glob("*.csv.gz")])
    # Trip-as-JSON cities also keep `.json`/`.json.gz` in raw/; a JSON converter step in the
    # clean_pipeline turns those into cleaned CSVs (see JSON_CONVERT_FUNCTIONS).
    json_files = sorted([*Path(raw_dir).glob("*.json"), *Path(raw_dir).glob("*.json.gz")])
    json_converters = [s for s in clean_pipeline if s in JSON_CONVERT_FUNCTIONS]
    if json_files and not json_converters:
        raise ValueError(
            f"{city}: raw/ contains {len(json_files)} JSON files but clean_pipeline has no "
            f"JSON converter (one of {sorted(JSON_CONVERT_FUNCTIONS)})."
        )
    if not csv_files and not json_files:
        print(f"⚠️ No CSV or JSON files found for {city}")
        return

    # Large cities can opt into a streaming, gzip-compressed cleaned copy instead of an
    # uncompressed full duplicate (e.g. Seoul: ~40G raw). The output is `<name>.csv.gz`.
    compress = config.get("compress_cleaned", False)

    print(
        f"🧽 Cleaning {len(csv_files)} CSV + {len(json_files)} JSON files for {city}..."
    )
    cleaned_dir.mkdir(parents=True, exist_ok=True)

    state = load_state(context.clean_state_path)
    new_state: dict = {}

    for raw_file in csv_files:
        # Derive the cleaned name from the `.csv` base, independent of whether raw is
        # gzipped — so a `.csv.gz` raw never becomes `.csv.gz.gz`, and cleaned names
        # stay stable across the migration (keeping transform's state keys valid).
        base_name = (
            raw_file.name[:-3] if raw_file.name.endswith(".gz") else raw_file.name
        )
        cleaned_name = base_name + ".gz" if compress else base_name
        cleaned_file = cleaned_dir / cleaned_name
        recorded = state.get(raw_file.name)

        # Skip when the raw input is unchanged and its cleaned output still exists.
        if recorded and is_unchanged(raw_file, recorded) and cleaned_file.exists():
            print(f"🟡 Skipping clean - {raw_file.name} unchanged")
            new_state[raw_file.name] = recorded
            continue

        if compress:
            # Single streaming pass: raw -> gzipped cleaned, bounded memory, no copy.
            print(f"\n📄 Cleaning (stream+gzip) {raw_file.name}")
            stream_clean_to_gzip(raw_file, cleaned_file, clean_pipeline, config)
        else:
            # Materialize a plain-text COPY (decompressing if raw is gzipped) and
            # mutate the copy, leaving raw/ immutable.
            print(f"\n📄 Cleaning {raw_file.name}")
            materialize_cleaned_source(raw_file, cleaned_file)
            for step in clean_pipeline:
                fn = CLEAN_FUNCTIONS.get(step)
                if fn:
                    fn(cleaned_file, config)
                else:
                    print(f"⚠️ Unknown clean step: {step}")

        new_state[raw_file.name] = {
            **file_signature(raw_file),
            "outputs": [cleaned_file.name],
        }

    # JSON trip files → cleaned CSVs. The converter decides per file whether to emit output
    # (station snapshots and CSV-covered months are skipped), so a skipped file records an
    # empty output list and still short-circuits on the next unchanged run.
    for raw_file in json_files:
        base_name = (
            raw_file.name[:-3] if raw_file.name.endswith(".gz") else raw_file.name
        )
        # <name>.json[.gz] -> <name>.csv[.gz]
        stem = base_name[: -len(".json")] if base_name.endswith(".json") else base_name
        cleaned_name = stem + ".csv" + (".gz" if compress else "")
        cleaned_file = cleaned_dir / cleaned_name
        recorded = state.get(raw_file.name)

        if (
            recorded
            and is_unchanged(raw_file, recorded)
            and all((cleaned_dir / o).exists() for o in recorded.get("outputs", []))
        ):
            print(f"🟡 Skipping clean - {raw_file.name} unchanged")
            new_state[raw_file.name] = recorded
            continue

        print(f"\n📄 Converting JSON {raw_file.name}")
        produced = None
        for step in json_converters:
            produced = JSON_CONVERT_FUNCTIONS[step](
                raw_file, cleaned_file, config, csv_files
            )

        new_state[raw_file.name] = {
            **file_signature(raw_file),
            "outputs": [produced.name] if produced else [],
        }

    write_state(context.clean_state_path, new_state)
    print(f"✅ Finished cleaning all files for {city}")
