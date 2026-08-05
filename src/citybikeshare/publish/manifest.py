"""The release manifest: what each published file contains, and when it last changed.

It records per-output size and checksum plus per-city coverage, so a release can be
inspected without parsing the payloads. Reviewing what *changed* between releases is
`git diff api/` — the payloads are pretty-printed for exactly that reason.
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
