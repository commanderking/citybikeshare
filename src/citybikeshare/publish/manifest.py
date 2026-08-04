"""Release manifest and the diff of a rebuild against what is already published.

The manifest is what makes a data release inspectable: it records, per output and per city,
how much is covered and by which months. The diff is the reason it exists — appending new
months is the expected case, but sources also *restate* history and a parsing fix can move
counts for years you weren't touching. Both show up as a changed row, never as an error, so
publish compares against the previous release and says so out loud.
"""

import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from citybikeshare.publish.builders import Records


def _format_month(record: dict[str, Any]) -> str | None:
    """``YYYY-MM`` when the record sits on a monthly timeline, ``YYYY`` when only yearly."""
    year = record.get("year")
    if year is None:
        return None
    month = record.get("month")
    return f"{year:04d}-{month:02d}" if month is not None else f"{year:04d}"


def summarize_city_coverage(records: Records) -> dict[str, Any]:
    """Row count, trip total and month span for one city's slice of an output."""
    coverage: dict[str, Any] = {"rows": len(records)}

    trips = [r["trips"] for r in records if isinstance(r.get("trips"), int)]
    if trips:
        coverage["trips"] = sum(trips)

    months = sorted(m for m in (_format_month(r) for r in records) if m is not None)
    if months:
        coverage["first_month"] = months[0]
        coverage["last_month"] = months[-1]

    return coverage


def build_output_entry(
    file_name: str, records: Records, payload: str
) -> dict[str, Any]:
    by_city: dict[str, Records] = {}
    for record in records:
        by_city.setdefault(record["city"], []).append(record)

    return {
        "file": file_name,
        "rows": len(records),
        "bytes": len(payload.encode("utf-8")),
        "sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        "cities": {
            city: summarize_city_coverage(rows)
            for city, rows in sorted(by_city.items())
        },
    }


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _without_timestamp(manifest: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in manifest.items() if k != "generated_at"}


def build_manifest(
    schema_version: int, outputs: dict[str, Any], previous_path: Path
) -> dict[str, Any]:
    """Assemble the manifest, carrying the previous timestamp forward when nothing else moved.

    That makes ``generated_at`` mean "when this data last changed" rather than "when publish
    last ran", so re-running publish on unchanged analysis doesn't dirty the file in git.
    """
    content = {"schema_version": schema_version, "outputs": outputs}

    generated_at = _utc_now()
    if previous_path.exists():
        previous = json.loads(previous_path.read_text(encoding="utf-8"))
        if _without_timestamp(previous) == content:
            generated_at = previous.get("generated_at", generated_at)

    return {
        "schema_version": schema_version,
        "generated_at": generated_at,
        "outputs": outputs,
    }


def _index_by_key(records: Records, key: list[str]) -> dict[tuple, dict[str, Any]]:
    return {tuple(r[field] for field in key): r for r in records}


def diff_against_published(
    published_path: Path, records: Records, key: list[str]
) -> dict[str, Any] | None:
    """Compare a rebuilt output against the copy already in ``api/``.

    Returns None when there is nothing to compare against (a first publish). ``changed``
    holds rows whose key already existed but whose values moved — restated history.
    """
    if not published_path.exists():
        return None

    previous = json.loads(published_path.read_text(encoding="utf-8"))
    old = _index_by_key(previous, key)
    new = _index_by_key(records, key)

    changed = [
        {"key": dict(zip(key, identity)), "from": old[identity], "to": new[identity]}
        for identity in old.keys() & new.keys()
        if old[identity] != new[identity]
    ]
    changed.sort(key=lambda c: tuple(str(v) for v in c["key"].values()))

    return {
        "added": sorted(
            new.keys() - old.keys(), key=lambda k: tuple(str(v) for v in k)
        ),
        "removed": sorted(
            old.keys() - new.keys(), key=lambda k: tuple(str(v) for v in k)
        ),
        "changed": changed,
    }


def _describe_key(key: dict[str, Any]) -> str:
    """``boston 2025-11`` — collapse year/month into one readable token."""
    month = _format_month(key)
    rest = [str(v) for f, v in key.items() if f not in ("year", "month")]
    return " ".join(rest + ([month] if month else []))


def _describe_movement(change: dict[str, Any]) -> str:
    """``trips: 100 → 148`` for every field of a changed row that actually moved."""
    parts = []
    for field, new_value in change["to"].items():
        old_value = change["from"].get(field)
        if old_value == new_value:
            continue
        if isinstance(old_value, int) and isinstance(new_value, int):
            parts.append(f"{field}: {old_value:,} → {new_value:,}")
        else:
            parts.append(f"{field}: {old_value} → {new_value}")
    return ", ".join(parts)


def _describe_cities(keys: list[tuple], key_fields: list[str]) -> str:
    """``boston (2), chicago (1)`` — which cities a set of row keys belongs to."""
    if "city" not in key_fields:
        return ""
    index = key_fields.index("city")
    counts = Counter(identity[index] for identity in keys)
    return ", ".join(f"{city} ({n})" for city, n in sorted(counts.items()))


def format_diff(diff: dict[str, Any] | None, key: list[str]) -> list[str]:
    """Human-readable release notes for one output, as lines to print."""
    if diff is None:
        return ["   first publish — no previous release to compare against"]

    lines = []
    for sign, label, keys in (
        ("+", "new rows", diff["added"]),
        ("-", "rows removed", diff["removed"]),
    ):
        if not keys:
            continue
        cities = _describe_cities(keys, key)
        lines.append(
            f"   {sign} {len(keys)} {label}" + (f": {cities}" if cities else "")
        )
    if diff["changed"]:
        count = len(diff["changed"])
        lines.append(
            f"   ⚠️  {count} previously published row{'' if count == 1 else 's'} CHANGED "
            "(history was restated — confirm this is a fix, not duplication):"
        )
        for change in diff["changed"][:10]:
            lines.append(
                f"       {_describe_key(change['key'])}  {_describe_movement(change)}"
            )
        if len(diff["changed"]) > 10:
            lines.append(f"       … and {len(diff['changed']) - 10} more")
    if not lines:
        lines.append("   no change since the previous release")
    return lines
