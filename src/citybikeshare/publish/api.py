"""The publish stage: per-city analysis JSON -> the `api/` tree served over jsDelivr.

Publish never reads parquet or recomputes anything; it only reshapes what `analyze` already
wrote. That keeps the public payload a pure function of `analysis/`, so a release can be
rebuilt byte-for-byte from a checkout.
"""

import json
from pathlib import Path
from typing import Any

from citybikeshare.config.loader import load_api_config
from citybikeshare.publish.builders import PUBLISH_BUILDERS
from citybikeshare.publish.manifest import (
    build_manifest,
    build_output_entry,
    diff_against_published,
    format_diff,
)
from citybikeshare.utils.io import write_json

REQUIRED_OUTPUT_FIELDS = ("name", "shape", "key")


def _assert_config_valid(config: dict[str, Any]) -> None:
    """Validate the whole config before writing anything, so a typo can't leave `api/`
    half-rebuilt with some outputs from this release and some from the last."""
    if not isinstance(config.get("schema_version"), int):
        raise ValueError("api.yaml: `schema_version` must be an integer")

    outputs = config.get("outputs")
    if not outputs:
        raise ValueError("api.yaml: `outputs` is empty — nothing to publish")

    seen: set[str] = set()
    for spec in outputs:
        missing = [field for field in REQUIRED_OUTPUT_FIELDS if not spec.get(field)]
        if missing:
            raise ValueError(f"api.yaml: output {spec} is missing {missing}")
        if spec["shape"] not in PUBLISH_BUILDERS:
            raise ValueError(
                f"api.yaml: output '{spec['name']}' has unknown shape "
                f"'{spec['shape']}' (known: {', '.join(sorted(PUBLISH_BUILDERS))})"
            )
        if spec["name"] in seen:
            raise ValueError(f"api.yaml: duplicate output name '{spec['name']}'")
        seen.add(spec["name"])


def publish_api(
    analysis_folder: Path,
    api_root: Path,
    *,
    strict: bool = False,
    config_path: Path | None = None,
) -> bool:
    """Build every configured output into ``api/v<schema_version>/``.

    Returns False when ``strict`` is set and a previously published row changed value.
    """
    config = load_api_config(config_path)
    _assert_config_valid(config)

    schema_version = config["schema_version"]
    version_root = api_root / f"v{schema_version}"

    print(f"📦 Publishing {version_root} from {analysis_folder}")

    entries: dict[str, Any] = {}
    history_changed = False

    for spec in config["outputs"]:
        name = spec["name"]
        file_name = f"{name}.json"
        destination = version_root / file_name

        records, missing_cities = PUBLISH_BUILDERS[spec["shape"]](analysis_folder, spec)

        # Diff before overwriting — the file on disk is the previous release.
        diff = diff_against_published(destination, records, spec["key"])
        if diff and diff["changed"]:
            history_changed = True

        # Minified: this is CDN payload, not something to read in a diff. The manifest and
        # the release notes below are the human-readable view.
        write_json(destination, records, minified=True)
        payload = destination.read_text(encoding="utf-8")

        entries[name] = build_output_entry(file_name, records, payload, missing_cities)
        city_count = len(entries[name]["cities"])
        size_kb = entries[name]["bytes"] / 1024
        print(
            f"  {file_name}  {len(records):,} rows  {city_count} cities  {size_kb:,.0f} KB"
        )
        for line in format_diff(diff, spec["key"]):
            print(line)

    manifest_path = version_root / "manifest.json"
    write_json(manifest_path, build_manifest(schema_version, entries))
    print(f"  manifest.json  {len(entries)} outputs")

    if history_changed and strict:
        print("❌ --strict: previously published rows changed value")
        return False
    return True


def read_manifest(api_root: Path, schema_version: int) -> dict[str, Any] | None:
    path = api_root / f"v{schema_version}" / "manifest.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))
