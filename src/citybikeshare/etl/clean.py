import shutil
from pathlib import Path
from citybikeshare.utils.io_clean import (
    CLEAN_FUNCTIONS,
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

    csv_files = list(Path(raw_dir).glob("*.csv"))
    if not csv_files:
        print(f"⚠️ No CSV files found for {city}")
        return

    print(f"🧽 Cleaning {len(csv_files)} CSV files for {city}...")
    cleaned_dir.mkdir(parents=True, exist_ok=True)

    state = load_state(context.clean_state_path)
    new_state: dict = {}

    for raw_file in csv_files:
        cleaned_file = cleaned_dir / raw_file.name
        recorded = state.get(raw_file.name)

        # Skip when the raw input is unchanged and its cleaned output still exists.
        if recorded and is_unchanged(raw_file, recorded) and cleaned_file.exists():
            print(f"🟡 Skipping clean - {raw_file.name} unchanged")
            new_state[raw_file.name] = recorded
            continue

        # Copy raw -> cleaned and mutate the COPY, leaving raw/ immutable.
        print(f"\n📄 Cleaning {raw_file.name}")
        shutil.copy2(raw_file, cleaned_file)
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

    write_state(context.clean_state_path, new_state)
    print(f"✅ Finished cleaning all CSVs for {city}")
