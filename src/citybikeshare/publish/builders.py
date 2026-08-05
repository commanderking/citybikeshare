"""Builders that turn per-city analysis JSON into a single published payload.

Each `shape` in `config/api.yaml` maps to one builder here. A builder returns the list of
records that will be written to `api/v<schema_version>/<name>.json`.
"""

import json
from pathlib import Path
from typing import Any, Callable

Records = list[dict[str, Any]]


def discover_cities(analysis_folder: Path) -> list[str]:
    """Every city directory under ``analysis/``, in a stable order."""
    return sorted(d.name for d in analysis_folder.iterdir() if d.is_dir())


def _read_city_section(
    analysis_folder: Path, city: str, source: str, section: str
) -> Records | None:
    """Return ``section`` from ``analysis/<city>/<source>.json``; None if either is absent."""
    path = analysis_folder / city / f"{source}.json"
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(
            f"{path} is a {type(payload).__name__}, not an object of sections — "
            f"cannot take section '{section}' from it"
        )
    return payload.get(section)


def _assert_sections_found(
    missing: list[str], spec: dict[str, Any], cities: list[str]
) -> None:
    """A city missing a published section would vanish from the client with no other symptom."""
    if not missing:
        return
    raise ValueError(
        f"Output '{spec['name']}': {len(missing)} of {len(cities)} cities have no "
        f"'{spec['section']}' section in {spec['source']}.json: {', '.join(missing)}. "
        "Re-run `analyze` for them."
    )


def _assert_uniform_record_keys(records: Records, spec: dict[str, Any]) -> None:
    """One city emitting different fields would make the merged file heterogeneous and break
    the client on whichever rows happen to differ, without anything failing here."""
    shapes: dict[frozenset[str], str] = {}
    for record in records:
        shapes.setdefault(frozenset(record), record.get("city", "?"))
    if len(shapes) > 1:
        described = "; ".join(
            f"{city} has {sorted(keys)}" for keys, city in list(shapes.items())[:4]
        )
        raise ValueError(
            f"Output '{spec['name']}': cities disagree on record fields — {described}"
        )


def _assert_unique_keys(records: Records, key: list[str], spec: dict[str, Any]) -> None:
    """Duplicate keys mean a city was merged twice — the same silent count inflation the
    cumulative-archive renames cause upstream."""
    seen: set[tuple] = set()
    duplicates: list[tuple] = []
    for record in records:
        identity = tuple(record[field] for field in key)
        if identity in seen:
            duplicates.append(identity)
        seen.add(identity)
    if duplicates:
        examples = ", ".join(str(d) for d in duplicates[:5])
        raise ValueError(
            f"Output '{spec['name']}': {len(duplicates)} duplicate keys {key}: {examples}"
        )


def _assert_key_fields_present(
    records: Records, key: list[str], spec: dict[str, Any]
) -> None:
    if not records:
        return
    missing = [field for field in key if field not in records[0]]
    if missing:
        raise ValueError(
            f"Output '{spec['name']}': `key` names {missing}, which the "
            f"'{spec['section']}' records don't have (fields: {sorted(records[0])})"
        )


def build_merged_section(analysis_folder: Path, spec: dict[str, Any]) -> Records:
    """Concatenate one section across every city, tagging each record with its city."""
    cities = discover_cities(analysis_folder)
    records: Records = []
    missing: list[str] = []

    for city in cities:
        section = _read_city_section(
            analysis_folder, city, spec["source"], spec["section"]
        )
        if not section:
            missing.append(city)
            continue
        for record in section:
            records.append({"city": city, **record})

    _assert_sections_found(missing, spec, cities)

    key = spec["key"]
    _assert_key_fields_present(records, key, spec)
    _assert_uniform_record_keys(records, spec)
    _assert_unique_keys(records, key, spec)

    return records


# `shape` dispatches through this registry so an unknown value fails loud at load time
# rather than silently matching nothing. Per-city outputs (`stations` is ~20 MB merged and
# has to be split) are the next entry.
PUBLISH_BUILDERS: dict[str, Callable[[Path, dict[str, Any]], Records]] = {
    "merge_cities": build_merged_section,
}
