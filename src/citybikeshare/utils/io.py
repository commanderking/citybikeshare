import json
from pathlib import Path
from typing import Any


def write_json(
    path: Path, data: Any, *, minified: bool = False, sort_keys: bool = False
) -> None:
    """Write ``data`` as JSON to ``path``, creating parent directories as needed.

    ``ensure_ascii`` is always False and the file is
    opened as UTF-8, so non-ASCII station names (CJK, accented Latin, …) are written as
    readable raw UTF-8 rather than ``\\uXXXX`` escapes — and correctly regardless of the
    host's locale (a plain ``open(path, "w")`` would use the platform default encoding and
    mangle those names on a non-UTF-8 system).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    dump_kwargs = {"separators": (",", ":")} if minified else {"indent": 2}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, sort_keys=sort_keys, **dump_kwargs)
