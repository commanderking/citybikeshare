# Plan: detect duplicated trip data loudly

## Problem

Duplicated trips never crash anything. A renamed archive, a re-bundled month, or a source
that publishes monthly *and* annual files (Toronto 2024) all flow through every stage and
surface only as an inflated count in `analysis/`. The current defenses (download prune,
extract orphan warning, transform orphan-parquet cleanup) all key on a file *disappearing*;
none of them detect a file *reappearing under another name*, or two files carrying the same
trips. The state files already record every known file — nothing asks them "is this new
file actually new?"

## Design: an early tripwire and a late gate

| | where | keys on | catches | cost |
|---|---|---|---|---|
| **A. Content fingerprint** | start of `extract` | CRC32 + size (free from zip central directory / gzip trailer) | byte-identical files under different names: case renames, spacing, re-bundles | milliseconds, no decompress |
| **B. Duplicate-trip assertion** | end of `transform`, before partition | the rows | everything: A's cases plus annual-vs-monthly, cumulative supersets, code bugs | one streaming pass over the city's parquets |
| **C. Prune orphan cleaned files** | `clean` | raw name | a raw file removed but its `cleaned/` twin still feeding transform | trivial |

A fires early and names both files precisely; B is the gate nothing gets past. C closes a gap
found while mapping the flow (transform prunes orphan parquets; clean never prunes cleaned).

---

## Part A — content fingerprint at the start of extract

**Files:** `etl/extract.py`, `tests/test_extract.py`

### What's actually on disk (all cities, 2026-09)

| location | kinds | notes |
|---|---|---|
| `raw/` | 2,385 `.csv.gz`, 3 `.xlsx.gz` | uniform; every file has a gzip trailer. Code must still accept a plain `.csv` (CLAUDE.md) |
| `download/` | 818 `.zip` | 14 cities; many multi-member, some nested (nyc 42→413, helsinki 11→69, rosario 1→32) |
| | 1,103 `.csv`, 4 `.xlsx`, 1 `.json` | ~47 GB total (taipei 21 GB, london 14 GB, mexico_city 8 GB). **Not** re-readable per run |
| | 6 `index.html` | S3-bucket listing pages (boston, chicago, columbus, nyc, sf, dc) — not data |
| | 4 `.csv.part` | aborted partial downloads in mexico_city — junk; ignore and delete |

Locally absent (no `raw/`): chattanooga, daejeon, guadalajara, los_angeles, madrid, seoul,
vancouver. mexico_city has downloads but an empty `raw/` (extract never run here).

### Where each fingerprint comes from

| content | fingerprint source | cost |
|---|---|---|
| existing `raw/*.gz` | gzip trailer: CRC32 + ISIZE (last 8 bytes) | one seek per file |
| existing plain `raw/*.csv` (rare) | `zlib.crc32` streamed once, cached in extract state | once |
| incoming zip member | `ZipInfo.CRC` / `file_size` from the central directory; recurse nested zips | free, before extraction |
| incoming plain `.csv`/`.xlsx`/`.json` | computed *during* the gzip stream that already reads it once; unchanged files are skipped by state and never opened | zero extra |
| `.html`, `.part`, anything else | ignored | — |

### Steps

1. `_gzip_trailer(path) -> (crc32, isize)` — generalizes `_gzip_uncompressed_size`.
2. `_archive_member_fingerprints(zip_path) -> {(crc, size): member_name}` — central directory,
   nested zips opened in memory, same member filters as extraction (`__MACOSX`, `._*`, data
   suffixes only).
3. `_raw_fingerprints(raw_dir) -> {(crc, size): raw_name}` — trailers.
4. `_assert_no_duplicate_content(known, incoming)` — merge into `{fingerprint: set(names)}`;
   a name seen on both sides counts once (the unchanged/re-bundled case). Raise if any
   fingerprint has ≥2 distinct names, listing them.
5. Two call points in `extract_city_data`:
   - **archives:** before the per-archive loop, over every zip's listing → nothing lands in
     `raw/` if it fires;
   - **plain files:** gzip to a temp name while computing the CRC, check against `known`,
     then rename into place — same guarantee, no second read.
6. Legitimately identical files: ignore fingerprints below a size floor (~1 KB; header-only
   months), and a per-city `allow_identical_raw_files: [[a, b], ...]` escape hatch.
7. Tests: same bytes under two member names → raise; same name in two archives → ok; raw
   trailer matches a new member under another name → raise; nested zip members included;
   plain-file path raises before the rename; size floor and allow-list honoured; `.html` /
   `.part` ignored.

**Verification:** copy a Toronto raw file under a new name → `extract toronto` raises naming
both; remove it → passes. Also run against a nested-zip city (nyc or helsinki) to confirm the
listing recursion matches what extraction produces.

---

## Part B — duplicate-trip assertion before partitioning

**Files:** `etl/transform.py`, city YAMLs (tolerance overrides only where needed),
`tests/test_transform_duplicates.py`

1. Scan with file attribution: `pl.scan_parquet(..., include_file_paths="source_file")` in
   `partition_parquet` (the one place every parquet of a city is already read together).
2. Trip key = the intersection of `[start_time, end_time, start_station_name,
   end_station_name, bike_id]` with the actual schema. Cities without `bike_id` or station
   names get a weaker key — see calibration below.
3. **Key the assertion on cross-file duplicates, not the duplicate rate.** Measured on
   Toronto (38.6M rows): 161,847 groups of identical rows (0.45%) — almost all group rides in
   the 2016–2018 files, which have no `bike_id`, so the key is minute-granularity times +
   stations and seven friends riding together are indistinguishable; a few hundred are
   zero-duration re-dock glitches. **Only 2 groups span more than one file.** Ingestion
   duplication (monthly + annual, old + renamed) puts every copy in a *different* file, so
   this axis separates the two cleanly where a rate threshold can't.
4. `_assert_no_cross_file_duplicates(lf, key, tolerance)`:
   - streaming `group_by(key).agg(len, source_file.n_unique(), source_file.min(),
     source_file.max())`;
   - **raise** if rows in groups with `n_unique(source_file) > 1` exceed `tolerance` of total
     (default `0.0001`, 0.01% — tight, because genuine data sits near zero on this axis),
     listing the top `(file_a, file_b, count)` pairs — the fix is a config change against
     filenames, so the message must name them;
   - **print** the within-file duplicate count/rate for visibility; never raise on it (that's
     the source's data, not an ingestion error).
5. **Calibration pass before enabling the raise:** run transform for every city in
   report-only mode and confirm the cross-file count is ~0 everywhere. Seven cities have no
   local `raw/` (see Part A inventory) — calibrate those wherever the data lives, or on first
   sync. Cities that split one month across files by size (`_1.csv`, `_2.csv`) should still be
   ~0 (a trip lands in one part); if a city is legitimately non-zero, set
   `duplicate_trip_tolerance` explicitly with a comment rather than loosening the default.
6. Placement: runs on every transform, incremental or not — a new file can only be judged
   against everything already present. It runs *before* `delete_folder(output_path)` so a
   failing run leaves the previous `output/` intact.
7. Tests: two parquets with identical rows → raise, message names both files; duplicates
   below tolerance → pass with printed rate; key falls back correctly when columns are absent.

**Verification:** duplicate a Toronto parquet under a new name → `transform toronto` raises;
git-stash the 2024 `excluded_filenames` guard is no longer testable (source withdrew the
monthlies), so the synthetic case is the regression test.

---

## Part C — prune orphan cleaned files

**Files:** `etl/clean.py`, `tests/test_clean_gzip.py`

1. After the file loop in `clean_city_data`, delete any `cleaned/*` not in the union of
   `outputs` recorded in the new state (mirror of `_remove_orphan_parquets`, including the
   `samefile` case-variant guard).
2. `cleaned/` is derived, so this is unconditional (unlike raw pruning, which is opt-in).
3. Test: a stale cleaned file with no raw source is removed; current outputs untouched.

---

## Order and scope

1. **A** first — smallest, zero data cost, immediately protects against the rename class.
2. **C** — five lines, closes a known gap.
3. **B** — the real gate; needs the calibration pass across all cities before the raise is
   turned on, so it's the only part with a multi-city run in it.

Each part is independently mergeable. Nothing changes `raw/`; A and B only ever raise.

## Out of scope

- Range-request fingerprinting before download (avoids one download per rename; needs a
  new dependency and couples downloaders to raw state — the thin-downloader rule).
- Quarantine-instead-of-halt on duplicates. Revisit when the pipeline is automated.
- A curated manifest of expected filenames: can't tell a new month from a renamed one.

## CLAUDE.md follow-up

Extend "Cumulative / renamed sources duplicate data silently" with A and B as the defenses
that cover *reappearing* files, alongside the existing ones that cover *disappearing* files.
