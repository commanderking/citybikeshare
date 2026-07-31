import csv
import gzip
import itertools
import json
import re
import shutil
from pathlib import Path
from typing import Optional
import tempfile
import chardet

_CHUNK = 64 * 1024 * 1024


def _is_gzip(path) -> bool:
    return str(path).endswith(".gz")


def materialize_cleaned_source(raw_file: Path, dest: Path) -> None:
    """Place a plain-text working copy of ``raw_file`` at ``dest``, decompressing
    when raw is gzipped so the in-place CLEAN_FUNCTIONS can read/write it as text.
    For uncompressed raw this is a byte-identical copy (preserving prior behavior)."""
    if _is_gzip(raw_file):
        with gzip.open(raw_file, "rb") as fin, open(dest, "wb") as fout:
            shutil.copyfileobj(fin, fout, length=_CHUNK)
    else:
        shutil.copy2(raw_file, dest)


def detect_file_encoding(file_path: Path, sample_size: int = 100_000) -> str:
    """Detect probable encoding of a file using chardet. Reads the decompressed
    bytes when the file is gzipped, so detection sees real content, not gzip framing."""
    try:
        opener = gzip.open if _is_gzip(file_path) else open
        with opener(file_path, "rb") as f:
            raw = f.read(sample_size)
        result = chardet.detect(raw)
        return (result["encoding"] or "unknown").lower()
    except Exception:
        return "unknown"


### Seoul is encoded in Korean characters, not utf-8
def convert_file_encoding(csv_file: Path, config):
    cleaning_opts = config.get("cleaning_options", {})
    src_encoding = cleaning_opts.get("source_encoding", "utf-8")
    dst_encoding = cleaning_opts.get("target_encoding", "utf-8")

    detected = detect_file_encoding(csv_file)
    if detected.startswith("utf"):
        print(f"⏭️ Skipping {csv_file.name} (already {detected})")
        return

    tmp_path = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
    with (
        open(csv_file, "r", encoding=src_encoding, errors="replace") as src,
        open(tmp_path, "w", encoding=dst_encoding) as dst,
    ):
        shutil.copyfileobj(src, dst, length=64 * 1024 * 1024)
    Path(csv_file).unlink(missing_ok=True)
    Path(tmp_path).rename(csv_file)
    print(f"✅ Converted {csv_file.name} ({detected} → {dst_encoding})")


### Older Rosario files contain ; and \t in header and content rows
def normalize_delimiters(csv_file: Path, config):
    text = csv_file.read_text(encoding="utf-8", errors="ignore")
    text_clean = text.replace("\t", "").replace(";", ",").replace('"', "")
    csv_file.write_text(text_clean, encoding="utf-8")
    print(f"🧹 Normalized delimiters in {csv_file.name}")


### Vancouver data currently has hidden \r in files (probably from Google Doc or Windows save)
def normalize_newlines(csv_file: Path, config):
    text = csv_file.read_text(encoding="utf-8", errors="ignore")
    text_clean = text.replace("\r\n", "\n").replace("\r", "\n")
    csv_file.write_text(text_clean, encoding="utf-8")
    print(f"🧹 Normalized newlines in {csv_file.name}")


def clean_seoul_files(csv_file: Path, config):
    file_name = str(csv_file)

    if "2306" in file_name:
        text = csv_file.read_text(encoding="utf-8", errors="ignore")
        text_clean = text.replace("2323-06-23", "2023-06-23")
        csv_file.write_text(text_clean, encoding="utf-8")
        print(f"Replaced 2323-06-23 with 2023-06-23 in {csv_file.name}")

    if "2020" in file_name:
        text = csv_file.read_text(encoding="utf-8", errors="ignore")
        text_clean = (
            text.replace("?瘦?,", '", "').replace('??,"', '", "').replace('?,"', '", "')
        )
        csv_file.write_text(text_clean, encoding="utf-8")
        print(f"Cleaned up poor encoding in {csv_file.name}")
    if "2021" in file_name:
        text = csv_file.read_text(encoding="utf-8", errors="ignore")
        text_clean = (
            text.replace("?湯?,", '", "').replace("??,", '", ').replace('?,"', '", "')
        )
        csv_file.write_text(text_clean, encoding="utf-8")
        print(f"Cleaned up poor encoding in {csv_file.name}")


# Rosario has a 2021 file that unzips into a txt file with inconsistent tab separators
# The tab separators is also different for parts of the file
def clean_rosario_files(csv_file: Path, config):
    if "2021" in str(csv_file):
        text = csv_file.read_text(encoding="latin1", errors="ignore")

        text_clean = (
            text.replace('""\t""\t', ",").replace('\t""\t', ",").replace("\t", ",")
        )
        csv_file.write_text(text_clean, encoding="utf-8")
        print(f"🧹 Cleaned quotes, tabs, and normalized CSV format in {csv_file.name}")


CLEAN_FUNCTIONS = {
    "normalize_newlines": normalize_newlines,
    "normalize_delimiters": normalize_delimiters,
    "encode_utf8": convert_file_encoding,
    "clean_seoul_files": clean_seoul_files,
    "clean_rosario_files": clean_rosario_files,
}


# --------------------------------------------------------------------------------------
# JSON → CSV conversion (clean stage). Some sources ship trips as JSON rather than CSV;
# turning that into a well-formed CSV document is a clean-stage concern (not transform,
# which maps already-headed CSVs to the canonical schema). A JSON converter takes
# (raw_file, cleaned_file, config, sibling_csv_files) and returns the cleaned Path it
# produced, or None when it deliberately skips the file. Keyed by clean_pipeline step name.
# --------------------------------------------------------------------------------------


# BiciMAD movement records carry more than we canonicalize; we keep the trip-relevant keys
# (dropping Mongo's `_id` and 2019's fat `track` GPS array) and emit them as columns. The
# JSON era's demographic fields (user_type, ageRange, zip_code) have no CSV-era equivalent —
# transform maps what it can and leaves the rest null.
_BICIMAD_MOVEMENT_COLUMNS = [
    "unplug_hourTime",
    "travel_time",
    "idunplug_station",
    "idplug_station",
    "idunplug_base",
    "idplug_base",
    "user_type",
    "ageRange",
    "user_day_code",
    "zip_code",
]


def _bicimad_scalar(value):
    """Unwrap a MongoDB extended-JSON scalar wrapper to its inner value.

    The 2017–2019H1 `_Usage_Bicimad` exports store fields as `{"$date": "..."}` /
    `{"$oid": "..."}` etc.; writing that dict's repr into a CSV cell would be malformed.
    Single-key wrappers unwrap to their value; anything else passes through unchanged (an
    unexpected multi-key object then fails loud downstream rather than being guessed at).
    """
    if isinstance(value, dict) and len(value) == 1:
        return next(iter(value.values()))
    return value


def _bicimad_is_trip_json(name: str) -> bool:
    """True for a BiciMAD trip JSON. Trip exports are named `*_movements` (2019H2–2021H1)
    or `*_Usage_Bicimad` (2017–2019H1); every other JSON in raw/ is a station snapshot the
    converter skips. If the source ever ships a trip file under a new name it'll be skipped
    silently — add the pattern here when that happens rather than enumerating hypotheticals now.
    """
    stem = name.lower()
    return "_movements" in stem or "_usage_bicimad" in stem


def _bicimad_json_year_month(name: str) -> Optional[tuple[int, int]]:
    """(year, month) from a BiciMAD trip JSON name (leading YYYYMM), else None."""
    match = re.match(r"(\d{4})(\d{2})", name)
    return (int(match.group(1)), int(match.group(2))) if match else None


def _bicimad_csv_year_month(name: str) -> Optional[tuple[int, int]]:
    """(year, month) from a BiciMAD trip CSV name (`trips_YY_MM_Month...`), else None."""
    match = re.match(r"trips_(\d{2})_(\d{2})_", name)
    return (2000 + int(match.group(1)), int(match.group(2))) if match else None


def _bicimad_month_covered_by_csv(name: str, csv_files) -> bool:
    """True when a CSV file already covers this movements JSON's year-month. Some months
    (e.g. 2021-06) ship in both formats; ingesting both would silently double-count, and the
    CSV is the more complete, richer copy (it carries station names + coordinates)."""
    year_month = _bicimad_json_year_month(name)
    csv_months = {
        ym for f in csv_files if (ym := _bicimad_csv_year_month(f.name)) is not None
    }
    return year_month in csv_months


def _write_bicimad_movements_csv(raw_file: Path, cleaned_file: Path) -> int:
    """Stream one movements ndjson into a `;`-delimited CSV of _BICIMAD_MOVEMENT_COLUMNS,
    unwrapping Mongo scalar wrappers. Returns the number of rows written. Fails loud on a
    record without `travel_time` — a station snapshot mis-named as movements, or a schema
    change — rather than writing empty trip rows."""
    read_opener = gzip.open if _is_gzip(raw_file) else open
    write_gzip = _is_gzip(cleaned_file)
    row_count = 0
    with read_opener(raw_file, "rt", encoding="utf-8", errors="replace") as src:
        dst_ctx = (
            gzip.open(cleaned_file, "wt", encoding="utf-8", newline="")
            if write_gzip
            else open(cleaned_file, "w", encoding="utf-8", newline="")
        )
        with dst_ctx as dst:
            writer = csv.writer(dst, delimiter=";")
            writer.writerow(_BICIMAD_MOVEMENT_COLUMNS)
            for line in src:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)  # malformed JSON fails loud, as intended
                if "travel_time" not in record:
                    raise ValueError(
                        f"{raw_file.name}: JSON record has no 'travel_time' (keys: "
                        f"{list(record)[:8]}) — misclassified as a movements file?"
                    )
                writer.writerow(
                    [_bicimad_scalar(record.get(col, "")) for col in _BICIMAD_MOVEMENT_COLUMNS]
                )
                row_count += 1
    return row_count


def convert_bicimad_movements_json(
    raw_file: Path, cleaned_file: Path, config, csv_files
) -> Optional[Path]:
    """Convert one BiciMAD movements JSON (ndjson) into a `;`-delimited cleaned CSV, or skip it
    (returns None) when it's a station snapshot or a month a CSV file already covers.
    """
    name = raw_file.name
    if not _bicimad_is_trip_json(name):
        print(f"⏭️  Skipping non-trip JSON (station snapshot): {name}")
        return None
    if _bicimad_month_covered_by_csv(name, csv_files):
        print(
            f"⏭️  Skipping {name}: {_bicimad_json_year_month(name)} already covered by a "
            f"CSV file (avoiding double-count)"
        )
        return None

    row_count = _write_bicimad_movements_csv(raw_file, cleaned_file)
    print(f"✅ Converted {name} → {cleaned_file.name} ({row_count} rows)")
    return cleaned_file


# A JSON converter takes (raw_file, cleaned_file, config, sibling_csv_files) and returns the
# cleaned Path produced (or None if skipped). Keyed by clean_pipeline step name.
JSON_CONVERT_FUNCTIONS = {
    "convert_bicimad_movements_json": convert_bicimad_movements_json,
}


# --------------------------------------------------------------------------------------
# Streaming clean (for large cities like Seoul). Same fixes as the in-place functions
# above, but applied per line so the whole file never sits in memory, and written
# straight to a gzip-compressed cleaned copy instead of an uncompressed duplicate.
# Each line transform is `(line, raw_file_name, config) -> line` and must be line-local.
# --------------------------------------------------------------------------------------


def clean_seoul_line(line, file_name, config):
    """Line-local version of clean_seoul_files (same replacements, applied per line)."""
    if "2306" in file_name:
        line = line.replace("2323-06-23", "2023-06-23")
    if "2020" in file_name:
        line = (
            line.replace("?瘦?,", '", "').replace('??,"', '", "').replace('?,"', '", "')
        )
    if "2021" in file_name:
        line = (
            line.replace("?湯?,", '", "').replace("??,", '", ').replace('?,"', '", "')
        )
    return line


def drop_unbalanced_quote_lines(line, file_name, config):
    """Drop rows with an odd number of double-quotes.

    A few Seoul source rows carry a stray quote — a station-name field is followed by
    `, ""<n>"` instead of `,"<n>"`, leaving the row's quotes unbalanced, which otherwise
    derails polars' parallel quoted-CSV parser for the entire file. Real examples
    (`...","교", ""0",...` is the malformed part):

        "SPB-40968",...,"01955","디지털입구 교", ""0","2021-03-08 14:32:51",...   # 2021.03
        "SPB-55970",...,"00704","남부법원검찰청 교", ""0","2021-06-12 00:49:24",... # 2021.06
        "SPB-37454",...,"00631","답십리역 1번", ""0","53","0.00"                    # 2020.07~08

    Returns None to signal "drop this line".
    """
    if line.count('"') % 2 != 0:
        return None
    return line


# A line transform may return None to drop the line.
LINE_CLEAN_FUNCTIONS = {
    "clean_seoul_files": clean_seoul_line,
    "drop_unbalanced_quote_lines": drop_unbalanced_quote_lines,
}


# Some Seoul monthly files ship without a header row. Prepend the matching header so the
# rest of the pipeline (rename_columns → select_final_columns → …) treats them like any
# other file. The 3 known headerless files share this 11-column schema.
#
# English equivalents (these map to the target names in seoul.yaml's renamed_columns):
#   자전거번호=bike_id, 대여일시=start_time, 대여 대여소번호=start_station_number,
#   대여 대여소명=start_station_name, 대여거치대=start_dock_number, 반납일시=end_time,
#   반납대여소번호=end_station_number, 반납대여소명=end_station_name,
#   반납거치대=end_dock_number, 이용시간=duration_minutes, 이용거리=distance_meters
_SEOUL_11COL_HEADER = (
    "자전거번호,대여일시,대여 대여소번호,대여 대여소명,대여거치대,"
    "반납일시,반납대여소번호,반납대여소명,반납거치대,이용시간,이용거리\n"
)


def seoul_headerless_header(first_line, file_name, config):
    # Seoul's 3 headerless files are known by name, so the first line isn't needed here.
    headerless = ("대여정보_201812", "대여정보_201904", "대여정보_201905")
    if any(p in file_name for p in headerless):
        return _SEOUL_11COL_HEADER
    return None


# Taipei dropped its header row mid-2023, so most files are headerless; a 7th column (bike_type)
# was added to the headerless layout in 2024-11. Restore the header the source omitted so transform
# reads every file like the headed (2020–2023) ones — header restoration is a well-formedness fix,
# hence it lives in the clean stage rather than transform.
_TAIPEI_HEADERS = {
    6: "rent_time,rent_station,return_time,return_station,rent,infodate\n",
    7: "rent_time,rent_station,return_time,return_station,rent,bike_type,infodate\n",
}


def taipei_prepend_header(first_line, file_name, config):
    fields = first_line.rstrip("\r\n").split(",")
    if not fields or fields[0] == "rent_time":
        return None  # already has a header row
    count = len(fields)
    if count not in _TAIPEI_HEADERS:
        raise ValueError(
            f"{file_name}: headerless file with {count} columns has no known Taipei header "
            f"(known: {sorted(_TAIPEI_HEADERS)}). Check for a source schema change."
        )
    return _TAIPEI_HEADERS[count]


# A header-prepend function takes (first_line, raw filename, config) and returns a header line to
# write first (or None if the file already has one). Keyed by clean_pipeline step name.
HEADER_PREPEND_FUNCTIONS = {
    "seoul_prepend_header": seoul_headerless_header,
    "taipei_prepend_header": taipei_prepend_header,
}


def stream_clean_to_gzip(raw_file: Path, cleaned_file: Path, clean_pipeline, config):
    """Stream raw -> gzipped cleaned in a single pass.

    Reads with the source encoding (encoding steps like `encode_utf8` are handled here
    by the reader, not as a separate rewrite), applies the pipeline's line-local clean
    steps, and writes UTF-8 gzip. Bounded memory, no full copy, no temp file — the
    cleaned output is the only thing written, and compressed.
    """
    src_cfg = config.get("cleaning_options", {}).get("source_encoding", "utf-8")
    detected = detect_file_encoding(raw_file)
    src_encoding = "utf-8" if detected.startswith("utf") else src_cfg

    # gzip's default (level 9) is ~4-5x slower than level 6 for only ~6% smaller output
    # on this data; default to 6 and let a city override via `compress_level`.
    compress_level = config.get("compress_level", 6)

    line_steps = [
        LINE_CLEAN_FUNCTIONS[step]
        for step in clean_pipeline
        if step in LINE_CLEAN_FUNCTIONS
    ]
    header_steps = [
        HEADER_PREPEND_FUNCTIONS[step]
        for step in clean_pipeline
        if step in HEADER_PREPEND_FUNCTIONS
    ]
    name = raw_file.name

    # Read transparently whether raw is plain or gzipped (`.csv` or `.csv.gz`).
    read_opener = gzip.open if _is_gzip(raw_file) else open

    with (
        read_opener(
            raw_file, "rt", encoding=src_encoding, errors="replace", newline=""
        ) as src,
        gzip.open(
            cleaned_file, "wt", encoding="utf-8", newline="", compresslevel=compress_level
        ) as dst,
    ):
        # Peek the first line so header-prepend steps can detect header-vs-data and column
        # count, then feed it back into the line loop so no data is consumed.
        first_line = src.readline()
        for header_fn in header_steps:
            header = header_fn(first_line, name, config)
            if header:
                dst.write(header)

        lines = itertools.chain([first_line], src) if first_line else iter(())
        for line in lines:
            dropped = False
            for fn in line_steps:
                line = fn(line, name, config)
                if line is None:  # a transform signalled "drop this line"
                    dropped = True
                    break
            if not dropped:
                dst.write(line)
