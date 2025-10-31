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
def normalize_newlines(csv_file: Path):
    text = csv_file.read_text(encoding="utf-8", errors="ignore")
    text_clean = text.replace("\r\n", "\n").replace("\r", "\n")
    csv_file.write_text(text_clean, encoding="utf-8")
    print(f"🧹 Normalized newlines in {csv_file.name}")


def clean_seoul_files(csv_file: Path, config):
    file_name = str(csv_file)
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
