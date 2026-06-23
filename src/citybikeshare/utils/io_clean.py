import gzip
import shutil
from pathlib import Path
import tempfile
import chardet


def detect_file_encoding(file_path: Path, sample_size: int = 100_000) -> str:
    """Detect probable encoding of a file using chardet."""
    try:
        with open(file_path, "rb") as f:
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
_SEOUL_11COL_HEADER = (
    "자전거번호,대여일시,대여 대여소번호,대여 대여소명,대여거치대,"
    "반납일시,반납대여소번호,반납대여소명,반납거치대,이용시간,이용거리\n"
)


def seoul_headerless_header(file_name):
    headerless = ("대여정보_201812", "대여정보_201904", "대여정보_201905")
    if any(p in file_name for p in headerless):
        return _SEOUL_11COL_HEADER
    return None


# A header-prepend function takes the raw filename and returns a header line to write
# first (or None). Keyed by clean_pipeline step name.
HEADER_PREPEND_FUNCTIONS = {
    "seoul_prepend_header": seoul_headerless_header,
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

    with (
        open(raw_file, "r", encoding=src_encoding, errors="replace", newline="") as src,
        gzip.open(
            cleaned_file, "wt", encoding="utf-8", newline="", compresslevel=compress_level
        ) as dst,
    ):
        for header_fn in header_steps:
            header = header_fn(name)
            if header:
                dst.write(header)

        for line in src:
            dropped = False
            for fn in line_steps:
                line = fn(line, name, config)
                if line is None:  # a transform signalled "drop this line"
                    dropped = True
                    break
            if not dropped:
                dst.write(line)
