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
import polars as pl

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
# XLSX → CSV conversion (clean stage). One cleaned CSV per worksheet. Every cell is read as
# text — never let the reader infer types: polars' default read_excel silently nulled every
# text cell of a mixed number/text date column (Toronto 2016 Q4). Date columns are then
# normalized to one ISO format so transform sees a uniform document. The per-sheet handling
# is declared on the clean_pipeline entry (`sheets:`), not inferred from the sheet's contents.
# --------------------------------------------------------------------------------------

_ISO_DATETIME = "%Y-%m-%d %H:%M:%S"


def _read_xlsx_workbook(raw_file: Path):
    import fastexcel  # optional dependency; only xlsx-source cities need it

    data = gzip.open(raw_file, "rb").read() if _is_gzip(raw_file) else raw_file.read_bytes()
    return fastexcel.read_excel(data)


def _sheet_csv_path(cleaned_file: Path, sheet_name: str) -> Path:
    """`<cleaned stem>__<sheet slug>.csv[.gz]` — sheet names can carry spaces (even trailing
    ones, as Toronto's do), so they're slugged rather than used verbatim."""
    slug = re.sub(r"[^A-Za-z0-9_-]+", "_", sheet_name.strip())
    name = cleaned_file.name
    base, ext = (
        (name[:-7], ".csv.gz") if name.endswith(".csv.gz") else (name[:-4], ".csv")
    )
    return cleaned_file.with_name(f"{base}__{slug}{ext}")


def _assert_sheets_match_config(workbook_sheets, configured, label: str) -> None:
    """Every worksheet must be declared and every declared sheet must exist — an undeclared
    sheet would be silently dropped, a misspelled one silently never converted."""
    missing = sorted(set(configured) - set(workbook_sheets))
    undeclared = sorted(set(workbook_sheets) - set(configured))
    if missing or undeclared:
        raise ValueError(
            f"{label}: sheets in config but not in workbook: {missing}; "
            f"sheets in workbook but not in config: {undeclared}"
        )


def _assert_sheet_dates_parsed(df, column: str, label: str) -> None:
    """Raise when a date cell was present but matched neither the serial-derived ISO form nor
    the sheet's declared text format."""
    bad = df.filter(
        pl.col(column).is_null()
        & pl.col(f"{column}_pre_clean").is_not_null()
        & (pl.col(f"{column}_pre_clean").str.strip_chars() != "")
    )
    if bad.height:
        examples = bad[f"{column}_pre_clean"].head(5).to_list()
        raise ValueError(
            f"{label}: {bad.height} value(s) in {column!r} could not be parsed. "
            f"Examples: {examples}. Declare their format as text_date_format on the sheet."
        )


def _assert_months_expected(df, column: str, expected_months, label: str) -> None:
    """Plausibility check on the recovered dates: a sheet covering a known quarter must only
    contain those months. This is what catches a wrong `serial_dates_day_month_swapped`
    assumption — un-swapping correct dates would scatter a quarter across all twelve months."""
    seen = sorted(df[column].dt.month().drop_nulls().unique().to_list())
    unexpected = [m for m in seen if m not in expected_months]
    if unexpected:
        raise ValueError(
            f"{label}: {column!r} has months {unexpected} outside expected {expected_months}"
        )


def _normalize_sheet_date_column(df, column: str, sheet_cfg: dict, label: str):
    """Coalesce a text date column into a Datetime.

    Cells Excel stored as date serials arrive as ISO text (the reader renders them); cells
    Excel left as text arrive verbatim and must match `text_date_format`. With
    `serial_dates_day_month_swapped`, the serial-derived values had their day and month
    read the wrong way round at the source (a `d/m` file parsed as `m/d`), so those — and
    only those — are swapped back.
    """
    serial = pl.col(column).str.strptime(pl.Datetime, _ISO_DATETIME, strict=False)
    if sheet_cfg.get("serial_dates_day_month_swapped"):
        # A swap only makes sense if the stored day fits in a month slot.
        too_big = df.select((serial.dt.day() > 12).sum()).item()
        if too_big:
            raise ValueError(
                f"{label}: {too_big} serial-derived {column!r} value(s) have day > 12, so "
                f"they cannot be day/month swapped; the swap assumption doesn't hold."
            )
        serial = pl.datetime(
            serial.dt.year(),
            serial.dt.day(),
            serial.dt.month(),
            serial.dt.hour(),
            serial.dt.minute(),
            serial.dt.second(),
        )
    parts = [serial]
    text_fmt = sheet_cfg.get("text_date_format")
    if text_fmt:
        parts.append(pl.col(column).str.strptime(pl.Datetime, text_fmt, strict=False))

    df = df.with_columns(pl.col(column).alias(f"{column}_pre_clean")).with_columns(
        pl.coalesce(parts).alias(column)
    )
    _assert_sheet_dates_parsed(df, column, label)
    if sheet_cfg.get("expected_months"):
        _assert_months_expected(df, column, sheet_cfg["expected_months"], label)
    return df.drop(f"{column}_pre_clean")


def _write_sheet_csv(df, cleaned_path: Path) -> None:
    if _is_gzip(cleaned_path):
        with gzip.open(cleaned_path, "wb") as f:
            df.write_csv(f, datetime_format=_ISO_DATETIME)
    else:
        df.write_csv(cleaned_path, datetime_format=_ISO_DATETIME)


def convert_xlsx_sheets(
    raw_file: Path, cleaned_file: Path, config, csv_files, options
) -> list[Path]:
    """Convert each declared worksheet of an xlsx into its own cleaned CSV. `options` is the
    clean_pipeline entry; its `sheets: {name: {date_columns, text_date_format,
    serial_dates_day_month_swapped, expected_months}}` declares how each sheet is handled."""
    sheets_cfg = options.get("sheets")
    if not sheets_cfg:
        raise ValueError(
            f"{raw_file.name}: convert_xlsx_sheets needs a `sheets:` mapping on its "
            f"clean_pipeline entry"
        )
    workbook = _read_xlsx_workbook(raw_file)
    _assert_sheets_match_config(workbook.sheet_names, sheets_cfg, raw_file.name)

    produced = []
    for sheet_name, sheet_cfg in sheets_cfg.items():
        label = f"{raw_file.name}[{sheet_name!r}]"
        df = workbook.load_sheet(sheet_name, dtypes="string").to_polars()
        for column in sheet_cfg.get("date_columns", []):
            df = _normalize_sheet_date_column(df, column, sheet_cfg, label)
        out = _sheet_csv_path(cleaned_file, sheet_name)
        _write_sheet_csv(df, out)
        print(f"✅ Converted {label} → {out.name} ({df.height} rows)")
        produced.append(out)
    return produced


# An XLSX converter takes (raw_file, cleaned_file, config, sibling_csv_files, options) and
# returns the list of cleaned Paths produced (one per sheet). Keyed by clean_pipeline step.
XLSX_CONVERT_FUNCTIONS = {
    "convert_xlsx_sheets": convert_xlsx_sheets,
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


# Toronto's 2020-10 file has 249 rows where the comma between `Trip Id` and `Trip  Duration` is
# missing, fusing them into one field (`10000084625,7120,...` = trip 10000084, duration 625s).
# The corruption starts exactly when trip ids crossed 10,000,000 — every fused id is 8 digits —
# so splitting the first field after 8 characters restores the row. Verified against the
# neighbouring ids (…083 / …085) and against end−start for the recovered durations. A 9-field
# row with any other shape is unknown corruption and must raise, not be guessed at.
_TORONTO_2020_10_FIELD_COUNT = 10
_TORONTO_FUSED_ID_DIGITS = 8


def toronto_split_fused_trip_id(line, file_name, config):
    fields = line.rstrip("\r\n").split(",")
    if len(fields) == _TORONTO_2020_10_FIELD_COUNT:
        return line
    first = fields[0]
    fused = (
        len(fields) == _TORONTO_2020_10_FIELD_COUNT - 1
        and first.isdigit()
        and len(first) > _TORONTO_FUSED_ID_DIGITS
    )
    if not fused:
        raise ValueError(
            f"{file_name}: row has {len(fields)} fields and is not a fused trip-id/duration "
            f"row; unknown corruption: {line.rstrip()!r}"
        )
    trip_id, duration = first[:_TORONTO_FUSED_ID_DIGITS], first[_TORONTO_FUSED_ID_DIGITS:]
    return line.replace(first, f"{trip_id},{duration}", 1)


# A line transform may return None to drop the line.
LINE_CLEAN_FUNCTIONS = {
    "clean_seoul_files": clean_seoul_line,
    "drop_unbalanced_quote_lines": drop_unbalanced_quote_lines,
    "toronto_split_fused_trip_id": toronto_split_fused_trip_id,
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
