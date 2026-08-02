"""Release manifest and the diff of a rebuild against what is already published.

The manifest is what makes a data release inspectable: it records, per output and per city,
how much is covered and by which months. The diff is the reason it exists — appending new
months is the expected case, but sources also *restate* history and a parsing fix can move
counts for years you weren't touching. Both show up as a changed row, never as an error, so
publish compares against the previous release and says so out loud.
"""

import hashlib
import json
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
    file_name: str, records: Records, payload: str, missing_cities: list[str]
) -> dict[str, Any]:
    by_city: dict[str, Records] = {}
    for record in records:
        by_city.setdefault(record["city"], []).append(record)

    entry: dict[str, Any] = {
        "file": file_name,
        "rows": len(records),
        "bytes": len(payload.encode("utf-8")),
        "sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        "cities": {
            city: summarize_city_coverage(rows)
            for city, rows in sorted(by_city.items())
        },
    }
    if missing_cities:
        entry["missing_cities"] = missing_cities
    return entry


def build_manifest(schema_version: int, outputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "generated_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
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


def _describe_span(keys: list[tuple], key_fields: list[str]) -> str:
    """``boston 2026-01..2026-06, chicago 2026-01`` for a set of row keys."""
    city_index = key_fields.index("city") if "city" in key_fields else None
    by_city: dict[str, list[tuple]] = {}
    for identity in keys:
        city = identity[city_index] if city_index is not None else "—"
        by_city.setdefault(city, []).append(identity)

    parts = []
    for city, rows in sorted(by_city.items()):
        months = sorted(
            m
            for m in (_format_month(dict(zip(key_fields, r))) for r in rows)
            if m is not None
        )
        if months:
            span = (
                months[0] if months[0] == months[-1] else f"{months[0]}..{months[-1]}"
            )
            parts.append(f"{city} {span} ({len(rows)})")
        else:
            parts.append(f"{city} ({len(rows)})")
    return ", ".join(parts)


def format_diff(diff: dict[str, Any] | None, key: list[str]) -> list[str]:
    """Human-readable release notes for one output, as lines to print."""
    if diff is None:
        return ["   first publish — no previous release to compare against"]

    lines = []
    if diff["added"]:
        lines.append(
            f"   + {len(diff['added'])} new rows: {_describe_span(diff['added'], key)}"
        )
    if diff["removed"]:
        lines.append(
            f"   - {len(diff['removed'])} rows removed: {_describe_span(diff['removed'], key)}"
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
