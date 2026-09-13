import gzip
import shutil
from pathlib import Path
from citybikeshare.utils.io_clean import (
    CLEAN_FUNCTIONS,
    HEADER_PREPEND_FUNCTIONS,
    JSON_CONVERT_FUNCTIONS,
    LINE_CLEAN_FUNCTIONS,
    XLSX_CONVERT_FUNCTIONS,
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


# Non-CSV raw inputs a converter step turns into cleaned CSVs (see JSON_CONVERT_FUNCTIONS and
# XLSX_CONVERT_FUNCTIONS).
_CONVERTED_SUFFIXES = (".json", ".xlsx")
CONVERT_FUNCTIONS = {**JSON_CONVERT_FUNCTIONS, **XLSX_CONVERT_FUNCTIONS}


def _cleaned_converted_name(raw_name: str, compress: bool) -> str:
    """Cleaned filename for a converted raw input: `<name>.json[.gz]` / `<name>.xlsx[.gz]` →
    `<name>.csv[.gz]` (converters emit CSV; a multi-sheet xlsx derives per-sheet names from
    this base)."""
    base = raw_name[:-3] if raw_name.endswith(".gz") else raw_name
    stem = next(
        (base[: -len(sfx)] for sfx in _CONVERTED_SUFFIXES if base.endswith(sfx)), base
    )
    return stem + ".csv" + (".gz" if compress else "")


KNOWN_CLEAN_STEPS = (
    CLEAN_FUNCTIONS.keys()
    | LINE_CLEAN_FUNCTIONS.keys()
    | HEADER_PREPEND_FUNCTIONS.keys()
    | CONVERT_FUNCTIONS.keys()
)


def _normalize_clean_steps(clean_pipeline) -> list[dict]:
    """Read `clean_pipeline` entries into `{"step": name, "files": [...] | None, ...}`. A bare
    step name applies to every file; the mapping form `{step: name, files: [...]}` scopes a
    step to the listed raw filenames, so a one-file patch doesn't rewrite the whole city. Any
    further keys on the mapping are that step's options (converters receive the entry)."""
    steps = []
    for entry in clean_pipeline:
        if isinstance(entry, str):
            steps.append({"step": entry, "files": None})
        elif isinstance(entry, dict) and "step" in entry:
            steps.append({**entry, "files": entry.get("files")})
        else:
            raise ValueError(
                f"clean_pipeline entry must be a step name or {{step: ..., files: [...]}}, "
                f"got {entry!r}"
            )
    return steps


def _assert_clean_steps_valid(steps, raw_files, city: str) -> None:
    """Fail loud on an unknown step name or a `files` scope naming no raw file — a typo in
    either would otherwise leave the targeted file uncleaned while the stage reports success."""
    raw_names = {f.name for f in raw_files}
    for step in steps:
        if step["step"] not in KNOWN_CLEAN_STEPS:
            raise ValueError(
                f"{city}: unknown clean step {step['step']!r} "
                f"(known: {sorted(KNOWN_CLEAN_STEPS)})"
            )
        missing = [f for f in (step["files"] or []) if f not in raw_names]
        if missing:
            raise ValueError(
                f"{city}: clean step {step['step']!r} targets files not in raw/: {missing}"
            )


def _entries_for_file(steps, raw_name: str) -> list[dict]:
    """The step entries that apply to this raw file, in pipeline order."""
    return [s for s in steps if s["files"] is None or raw_name in s["files"]]


def _steps_for_file(steps, raw_name: str) -> list[str]:
    """Names of the steps that apply to this raw file, in pipeline order."""
    return [s["step"] for s in _entries_for_file(steps, raw_name)]


def _passthrough_csv(raw_file: Path, cleaned_file: Path, compress: bool) -> None:
    """Place a raw file in cleaned/ with no cleaning applied (no step targets it). Bytes are
    preserved — only the gzip layer is added/removed to match the cleaned layout. Never link:
    an in-place write to a hardlink would clobber raw/."""
    raw_is_gz = raw_file.name.endswith(".gz")
    if raw_is_gz == compress:
        shutil.copyfile(raw_file, cleaned_file)
    elif compress:
        with open(raw_file, "rb") as fin, gzip.open(cleaned_file, "wb") as fout:
            shutil.copyfileobj(fin, fout)
    else:
        materialize_cleaned_source(raw_file, cleaned_file)


def _converter_entries_for_file(steps, raw_name: str) -> list[dict]:
    return [
        s for s in _entries_for_file(steps, raw_name) if s["step"] in CONVERT_FUNCTIONS
    ]


def _assert_non_csv_inputs_claimed(convert_files, steps, ignored, city: str) -> None:
    """Every non-CSV raw file must be targeted by a converter step or listed under
    `ignored_raw_files` — otherwise it would be silently left out (transform reads only CSVs)."""
    unclaimed = [
        f.name
        for f in convert_files
        if f.name not in ignored and not _converter_entries_for_file(steps, f.name)
    ]
    if unclaimed:
        raise ValueError(
            f"{city}: raw/ holds non-CSV files no converter step targets: {unclaimed}. Add "
            f"them to a converter step's `files:` (one of {sorted(CONVERT_FUNCTIONS)}) or, if "
            f"they aren't trip data, list them under `ignored_raw_files`."
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
    raw_file: Path, cleaned_dir: Path, steps, config, compress: bool, recorded
) -> dict:
    """Clean one CSV raw file into cleaned_dir and return its clean-state entry (or the prior
    entry unchanged when it can be skipped). Large cities stream straight to gzip; others
    materialize a working copy and mutate it via the configured CLEAN_FUNCTIONS. A file no
    step targets is passed through untouched."""
    cleaned_file = cleaned_dir / _cleaned_csv_name(raw_file.name, compress)
    if _clean_output_is_current(raw_file, recorded, cleaned_dir):
        print(f"🟡 Skipping clean - {raw_file.name} unchanged")
        return recorded

    clean_pipeline = _steps_for_file(steps, raw_file.name)
    if not clean_pipeline:
        print(f"\n📄 Passing through {raw_file.name} (no clean step targets it)")
        _passthrough_csv(raw_file, cleaned_file, compress)
    elif compress:
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


def _run_converter(entry, raw_file, cleaned_file, config, csv_files) -> list[Path]:
    """Dispatch one converter entry and normalize its result to a list of produced paths. JSON
    converters return one Path or None (a deliberate skip); XLSX converters return one Path per
    sheet and take their entry as `options`."""
    step = entry["step"]
    if step in XLSX_CONVERT_FUNCTIONS:
        return XLSX_CONVERT_FUNCTIONS[step](
            raw_file, cleaned_file, config, csv_files, options=entry
        )
    produced = JSON_CONVERT_FUNCTIONS[step](raw_file, cleaned_file, config, csv_files)
    return [produced] if produced else []


def _convert_one_source(
    raw_file: Path,
    cleaned_dir: Path,
    steps,
    config,
    compress: bool,
    csv_files,
    ignored,
    recorded,
) -> dict:
    """Convert one non-CSV raw file (JSON, XLSX) to cleaned CSV(s) and return its clean-state
    entry (or the prior entry unchanged when it can be skipped). A file under
    `ignored_raw_files`, or one the converter declines (station snapshot, a month a CSV already
    covers), records no outputs."""
    if raw_file.name in ignored:
        print(f"⏭️  Ignoring {raw_file.name} (listed in ignored_raw_files)")
        return {**file_signature(raw_file), "outputs": []}

    cleaned_file = cleaned_dir / _cleaned_converted_name(raw_file.name, compress)
    if _clean_output_is_current(raw_file, recorded, cleaned_dir):
        print(f"🟡 Skipping clean - {raw_file.name} unchanged")
        return recorded

    print(f"\n📄 Converting {raw_file.name}")
    produced: list[Path] = []
    for entry in _converter_entries_for_file(steps, raw_file.name):
        produced = _run_converter(entry, raw_file, cleaned_file, config, csv_files)

    return {**file_signature(raw_file), "outputs": [p.name for p in produced]}


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

    # Raw inputs may be plain `.csv`/`.csv.gz`; some sources also ship trips as JSON or XLSX,
    # kept in raw/ as received and turned into cleaned CSVs by a converter step.
    csv_files = sorted([*Path(raw_dir).glob("*.csv"), *Path(raw_dir).glob("*.csv.gz")])
    convert_files = sorted(
        p
        for sfx in _CONVERTED_SUFFIXES
        for p in [*Path(raw_dir).glob(f"*{sfx}"), *Path(raw_dir).glob(f"*{sfx}.gz")]
    )
    ignored = set(config.get("ignored_raw_files", []))
    steps = _normalize_clean_steps(clean_pipeline)
    _assert_clean_steps_valid(steps, csv_files + convert_files, city)
    _assert_non_csv_inputs_claimed(convert_files, steps, ignored, city)
    if not csv_files and not convert_files:
        print(f"⚠️ No CSV, JSON or XLSX files found for {city}")
        return

    # Large cities can opt into a streaming, gzip-compressed cleaned copy instead of an
    # uncompressed full duplicate (e.g. Seoul: ~40G raw). The output is `<name>.csv.gz`.
    compress = config.get("compress_cleaned", False)

    print(
        f"🧽 Cleaning {len(csv_files)} CSV + {len(convert_files)} JSON/XLSX files for {city}..."
    )
    cleaned_dir.mkdir(parents=True, exist_ok=True)

    state = load_state(context.clean_state_path)
    new_state: dict = {}

    for raw_file in csv_files:
        new_state[raw_file.name] = _clean_one_csv(
            raw_file, cleaned_dir, steps, config, compress, state.get(raw_file.name)
        )

    for raw_file in convert_files:
        new_state[raw_file.name] = _convert_one_source(
            raw_file,
            cleaned_dir,
            steps,
            config,
            compress,
            csv_files,
            ignored,
            state.get(raw_file.name),
        )

    write_state(context.clean_state_path, new_state)
    print(f"✅ Finished cleaning all files for {city}")
