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


def _cleaned_csv_name(raw_name: str, compress: bool) -> str:
    """Cleaned filename for a CSV raw input. Derived from the `.csv` base independent of
    whether raw is gzipped — so a `.csv.gz` raw never becomes `.csv.gz.gz`, and cleaned names
    stay stable across the raw-gzip migration (keeping transform's state keys valid)."""
    base = raw_name[:-3] if raw_name.endswith(".gz") else raw_name
    return base + ".gz" if compress else base


def _cleaned_json_name(raw_name: str, compress: bool) -> str:
    """Cleaned filename for a JSON raw input: `<name>.json[.gz]` → `<name>.csv[.gz]` (the
    converter emits CSV)."""
    base = raw_name[:-3] if raw_name.endswith(".gz") else raw_name
    stem = base[: -len(".json")] if base.endswith(".json") else base
    return stem + ".csv" + (".gz" if compress else "")


def _assert_json_converter_configured(json_files, json_converters, city: str) -> None:
    """Fail loud when raw/ holds JSON but no clean_pipeline step can convert it — otherwise
    those files would be silently ignored (transform reads only CSVs)."""
    if json_files and not json_converters:
        raise ValueError(
            f"{city}: raw/ contains {len(json_files)} JSON files but clean_pipeline has no "
            f"JSON converter (one of {sorted(JSON_CONVERT_FUNCTIONS)})."
        )


def _clean_output_is_current(raw_file: Path, recorded, cleaned_dir: Path) -> bool:
    """True when the raw input is unchanged since the last run and every cleaned output it
    recorded still exists — so it can be skipped. A deliberately-skipped file (e.g. a station
    JSON) records no outputs, so an empty list short-circuits here too (nothing to re-check)."""
    return (
        bool(recorded)
        and is_unchanged(raw_file, recorded)
        and all((cleaned_dir / o).exists() for o in recorded.get("outputs", []))
    )


def _apply_clean_functions(cleaned_file: Path, clean_pipeline, config) -> None:
    """Apply each configured CLEAN_FUNCTIONS step in order, mutating cleaned_file in place.
    Steps handled elsewhere (streaming line steps, JSON converters) aren't in CLEAN_FUNCTIONS
    and are reported as unknown here — the materialize path only knows in-place CSV fixes."""
    for step in clean_pipeline:
        fn = CLEAN_FUNCTIONS.get(step)
        if fn:
            fn(cleaned_file, config)
        else:
            print(f"⚠️ Unknown clean step: {step}")


def _clean_one_csv(
    raw_file: Path, cleaned_dir: Path, clean_pipeline, config, compress: bool, recorded
) -> dict:
    """Clean one CSV raw file into cleaned_dir and return its clean-state entry (or the prior
    entry unchanged when it can be skipped). Large cities stream straight to gzip; others
    materialize a working copy and mutate it via the configured CLEAN_FUNCTIONS."""
    cleaned_file = cleaned_dir / _cleaned_csv_name(raw_file.name, compress)
    if _clean_output_is_current(raw_file, recorded, cleaned_dir):
        print(f"🟡 Skipping clean - {raw_file.name} unchanged")
        return recorded

    if compress:
        # Single streaming pass: raw -> gzipped cleaned, bounded memory, no copy.
        print(f"\n📄 Cleaning (stream+gzip) {raw_file.name}")
        stream_clean_to_gzip(raw_file, cleaned_file, clean_pipeline, config)
    else:
        # Materialize a plain-text COPY (decompressing if raw is gzipped) and mutate the
        # copy, leaving raw/ immutable.
        print(f"\n📄 Cleaning {raw_file.name}")
        materialize_cleaned_source(raw_file, cleaned_file)
        _apply_clean_functions(cleaned_file, clean_pipeline, config)

    return {**file_signature(raw_file), "outputs": [cleaned_file.name]}


def _convert_one_json(
    raw_file: Path,
    cleaned_dir: Path,
    json_converters,
    config,
    compress: bool,
    csv_files,
    recorded,
) -> dict:
    """Convert one JSON raw file to a cleaned CSV and return its clean-state entry (or the prior
    entry unchanged when it can be skipped). The converter may decline to emit output (station
    snapshot, or a month a CSV already covers), in which case the entry records no outputs."""
    cleaned_file = cleaned_dir / _cleaned_json_name(raw_file.name, compress)
    if _clean_output_is_current(raw_file, recorded, cleaned_dir):
        print(f"🟡 Skipping clean - {raw_file.name} unchanged")
        return recorded

    print(f"\n📄 Converting JSON {raw_file.name}")
    produced = None
    for step in json_converters:
        produced = JSON_CONVERT_FUNCTIONS[step](raw_file, cleaned_file, config, csv_files)

    return {**file_signature(raw_file), "outputs": [produced.name] if produced else []}


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

    # Raw inputs may be plain `.csv`/`.csv.gz`; trip-as-JSON cities also keep `.json`/`.json.gz`
    # that a JSON converter step turns into cleaned CSVs (see JSON_CONVERT_FUNCTIONS).
    csv_files = sorted([*Path(raw_dir).glob("*.csv"), *Path(raw_dir).glob("*.csv.gz")])
    json_files = sorted([*Path(raw_dir).glob("*.json"), *Path(raw_dir).glob("*.json.gz")])
    json_converters = [s for s in clean_pipeline if s in JSON_CONVERT_FUNCTIONS]
    _assert_json_converter_configured(json_files, json_converters, city)
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
        new_state[raw_file.name] = _clean_one_csv(
            raw_file, cleaned_dir, clean_pipeline, config, compress, state.get(raw_file.name)
        )

    for raw_file in json_files:
        new_state[raw_file.name] = _convert_one_json(
            raw_file,
            cleaned_dir,
            json_converters,
            config,
            compress,
            csv_files,
            state.get(raw_file.name),
        )

    write_state(context.clean_state_path, new_state)
    print(f"✅ Finished cleaning all files for {city}")
