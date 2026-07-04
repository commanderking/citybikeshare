# Guidance for working in this repo

## Data-quality: fail loud on unexpected data

The ETL pipeline must **surface unexpected or malformed source data as a loud, explicit
error** — never silently null it, drop it, or let it through. A silent fix hides the
dangerous case: a wholesale source format change would quietly null an entire column
while reporting success, and you'd only notice much later in analysis.

The established pattern (in `src/citybikeshare/etl/pipelines/common.py`):

1. **Parse leniently, then assert.** Parse with `strict=False` so a bad value becomes
   null *as an intermediate step*, then validate. `_assert_column_parsed` raises when a
   value was present in the source but failed to parse, listing examples. Genuinely
   empty/missing values are allowed through as null. See `convert_columns_to_datetime`
   (dates) and `handle_odd_hour_duration` + `_assert_durations_parsed` (Taipei durations).
   Do **not** replace these with a plain `strict=True` cast — you lose the descriptive
   error and the empty-vs-malformed distinction.

2. **Handle known-bad data explicitly, not silently.** Curated, known-invalid values are
   dropped by `remove_invalid_rows` via the per-city `invalid_values: {column: [...]}`
   config, and it logs what it removed. This keeps the parsing steps focused on
   well-formed data; anything *not* listed that still fails to parse trips the validators
   above (the safety net).

So the loop for new corruption is: a value fails to parse → the validator raises with the
offending value → confirm it's garbage → add it under `invalid_values` (or fix the
source) → re-run. Each tolerated exception costs one documented, version-controlled line.

When the per-value exclusions for a column start to multiply, prefer a *semantic
plausibility* filter (e.g. drop trips longer than N hours) over curating more exact
strings — it catches the class of corruption rather than each instance.

## Cumulative / renamed sources duplicate data silently

Some sources rename a cumulative archive as they append data (Montreal:
`…010203.zip` → `…01020304.zip`) or re-bundle months into a re-named archive
(Daejeon-style). Stale files left behind by the old name **duplicate trips**, and the
symptom is **inflated counts in analysis, not an error** — nothing crashes.

Defenses already in place:

- Downloaders **prune** archives the source no longer offers (so `download/` doesn't
  accumulate stale renames).
- `extract` removes orphaned `raw/` files for cities that set `prune_renamed_archives:
  true` (`_remove_orphan_raw_files`); other cities get a warning instead of a deletion.
- `transform` auto-removes orphan parquets whose source CSV is gone.

Whenever a source's filenames encode a growing date range, assume this trap exists and
check for it.

## `raw/` is the local source of truth — verify before deleting

Deleting or overwriting `raw/` data may be **unrecoverable** if the source later stops
serving it. Always verify what a file actually contains (e.g. its date coverage) before
removing it — filenames lie (two opaquely-named Montreal files turned out to be the only
copies of 2023 and 2024).

Derived artifacts (`parquet/`, `output/`) are safe to delete — `transform` regenerates
them. This asymmetry is why raw-file deletion is opt-in (`prune_renamed_archives`) while
parquet orphan-cleanup runs automatically.

## `raw/` is stored gzipped (`.csv.gz`)

Raw inputs are gzip-compressed on disk (~5× smaller). The parse path reads `.csv.gz`
transparently (`scan_csv`, plus the gzip-aware helpers in `io_clean.py`), but **any code
that enumerates or opens raw files must handle both `.csv` and `.csv.gz`** — glob `*.csv`
*and* `*.csv.gz`, not just `*.csv`. Globbing only `*.csv` silently skips every input (a
real bug we hit in the clean stage). `extract` writes `.csv.gz`; its cumulative-archive
"unchanged member" check compares the source's uncompressed size against the stored file's
gzip ISIZE trailer rather than a raw byte size.

## Custom downloaders should be thin

A downloader's job is to **fetch bytes and skip cheaply** — derive the skip key from the
URL or listing and skip *before* downloading, rather than pulling a large archive just to
compare it. Let the shared `extract` stage do the unzipping; don't unzip inside the
downloader. See `custom_downloaders/` and the helpers in
`custom_downloaders/utils/download_helpers.py`.

## Behavior is config-driven per city

A city's handling lives in its YAML (`config/cities/<city>.yaml`): the
`processing_pipeline` list of step names, `file_matcher`, `date_formats`,
`invalid_values`, `prune_renamed_archives`, etc. Adding handling means adding a step +
config keys and reusing shared functions in `etl/pipelines/common.py` (registered in
`PROCESSING_FUNCTIONS`) — not city-specific branching in the core ETL code.

## Naming: functions are verbs, data is nouns

Name functions for the **action they perform**, using an imperative verb phrase —
`generate_station_coords`, `summarize_city`, `canonicalize_station_coords`,
`count_station_trips`, `_build_canonical_name_map`. 
