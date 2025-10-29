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
def convert_folder_encoding(input_dir: str, config) -> None:
    """
    Convert all CSV files in a directory from one encoding to another (default: UTF-8).
    Skips files that are already UTF-8 or already in the target encoding.

    Args:
        input_dir: Directory containing CSV files.
        config: Configuration dict that may contain:
            cleaning_options.encoding: Source encoding (e.g., 'euc-kr', 'shift_jis').
            cleaning_options.encoding_target: Target encoding (default = 'utf-8').
    """
    cleaning_opts = config.get("cleaning_options", {})
    src_encoding = cleaning_opts.get("source_encoding", "utf-8")
    final_encoding = "utf-8"

    input_path = Path(input_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    print(f"Encoding all CSV files from {src_encoding} to utf-8")

    converted = skipped = 0
    for csv_file in input_path.glob("*.csv"):
        detected = detect_file_encoding(csv_file)

        # Skip if already target encoding (e.g., utf-8, utf8, utf-8-sig)
        if detected.startswith("utf"):
            print(f"⏭️  Skipping {csv_file.name} (already {detected})")
            skipped += 1
            continue

        tmp_path = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        try:
            with (
                open(csv_file, "r", encoding=src_encoding, errors="replace") as src,
                open(tmp_path, "w", encoding="utf-8") as dst,
            ):
                shutil.copyfileobj(src, dst, length=64 * 1024 * 1024)  # 64 MB buffer
            Path(csv_file).unlink(missing_ok=True)
            Path(tmp_path).rename(csv_file)
            print(f"✅ {csv_file.name} converted ({detected} → {final_encoding})")
            converted += 1
        except Exception as e:
            print(f"⚠️ Failed to convert {csv_file.name}: {e}")

    print(
        f"🔤 Converted {converted}, skipped {skipped} CSV files (target={final_encoding})"
    )


### Vancouver data currently has hidden \r in files (probably from Google Doc or Windows save)
def normalize_newlines(input_dir: str) -> None:
    """
    Normalize line endings in all CSV files within a directory:
    - Converts Windows (\r\n) and stray carriage returns (\r) to Unix (\n)

    Parameters
    ----------
    input_dir : str
        Directory containing CSV files to clean.
    """
    input_path = Path(input_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    csv_files = list(input_path.glob("*.csv"))
    if not csv_files:
        print(f"⚠️ No CSV files found in {input_dir}")
        return

    cleaned = 0
    for csv_file in csv_files:
        try:
            text = csv_file.read_text(encoding="utf-8", errors="ignore")
            text_clean = text.replace("\r\n", "\n").replace("\r", "\n")
            csv_file.write_text(text_clean, encoding="utf-8")
            cleaned += 1
        except Exception as e:
            print(f"⚠️ Failed to normalize {csv_file.name}: {e}")

    print(f"✅ Normalized newlines in {cleaned} CSV files under {input_dir}")


CLEAN_FUNCTIONS = {
    "normalize_newlines": normalize_newlines,
    "encode_utf8": convert_folder_encoding,
}
