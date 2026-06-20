"""Lightweight per-stage state files for incremental ETL.

Each pipeline stage records, per input file, a cheap size+mtime signature plus the
outputs it produced. On a re-run the stage compares the current signature against
the recorded one and skips any input that hasn't changed (and whose outputs still
exist). State files are plain JSON sidecars under ``data/<city>/`` and are safe to
delete — doing so just forces a full rebuild on the next run.
"""

import json
import os
from pathlib import Path
from typing import Optional


def file_signature(path) -> dict:
    """Return a cheap change-detection signature for a file: size + mtime."""
    stat = os.stat(path)
    return {"bytes": stat.st_size, "mtime": stat.st_mtime}


def load_state(path) -> dict:
    """Load a state file, returning an empty dict if it doesn't exist yet."""
    path = Path(path)
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return json.load(f)


def write_state(path, data: dict) -> None:
    """Write a state file as JSON (sorted keys for stable diffs)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def is_unchanged(path, recorded: Optional[dict]) -> bool:
    """True if the file's current size+mtime matches the recorded signature.

    ``recorded`` may carry extra keys (e.g. "outputs"); only "bytes" and "mtime"
    are compared. A missing record or missing file is treated as changed.
    """
    if not recorded:
        return False
    if not Path(path).exists():
        return False
    current = file_signature(path)
    return (
        recorded.get("bytes") == current["bytes"]
        and recorded.get("mtime") == current["mtime"]
    )
