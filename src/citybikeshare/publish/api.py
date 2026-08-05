"""The publish stage: per-city analysis JSON -> the `api/` tree served over jsDelivr.

Publish never reads parquet or recomputes anything; it only reshapes what `analyze` already
wrote. That keeps the public payload a pure function of `analysis/`, so a release can be
rebuilt byte-for-byte from a checkout.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from citybikeshare.publish.builders import PUBLISH_BUILDERS, Records
from citybikeshare.publish.manifest import build_manifest, build_output_entry
from citybikeshare.utils.io import write_json

REQUIRED_OUTPUT_FIELDS = ("name", "shape", "key")


@dataclass(frozen=True)
class BuiltOutput:
    """One output assembled in memory, before anything has been written to ``api/``."""

    spec: dict[str, Any]
    destination: Path
    records: Records

    @property
    def name(self) -> str:
        return self.spec["name"]


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


def _build_all_outputs(
    analysis_folder: Path, version_root: Path, config: dict[str, Any]
) -> list[BuiltOutput]:
    """Assemble every output in memory, writing nothing.

    Keeping the whole build ahead of the whole write is what makes a failed publish a no-op:
    a builder that raises on the third output (duplicate keys, schema drift, a city missing
    its section) leaves the previous release intact rather than half-replaced, with a
    manifest that no longer matches the payloads beside it.
    """
    built = []
    for spec in config["outputs"]:
        destination = version_root / f"{spec['name']}.json"
        records = PUBLISH_BUILDERS[spec["shape"]](analysis_folder, spec)
        built.append(BuiltOutput(spec, destination, records))
    return built


def _write_all_outputs(
    built: list[BuiltOutput], schema_version: int, manifest_path: Path
) -> dict[str, Any]:
    """Write every payload and the manifest; return the manifest's per-output entries."""
    entries: dict[str, Any] = {}
    for output in built:
        # Pretty-printed on purpose. Minifying saves ~1 KB gzipped (jsDelivr compresses
        # everything) but collapses the payload onto one line, which makes `git diff`
        # useless for reviewing a release — the one place a restated count would show up.
        write_json(output.destination, output.records)
        # Hash what actually landed on disk, so the manifest can't describe an intent that
        # the file doesn't match.
        payload = output.destination.read_text(encoding="utf-8")
        entries[output.name] = build_output_entry(
            output.destination.name, output.records, payload
        )

    write_json(manifest_path, build_manifest(schema_version, entries, manifest_path))
    return entries


def _report_release(entries: dict[str, Any]) -> None:
    """What landed. Reviewing *what changed* is `git diff api/` — the payload is
    pretty-printed so an added month and a restated count both read plainly there."""
    for entry in entries.values():
        print(
            f"  {entry['file']}  {entry['rows']:,} rows  "
            f"{len(entry['cities'])} cities  {entry['bytes'] / 1024:,.0f} KB"
        )
    print(f"  manifest.json  {len(entries)} outputs")


def publish_api(analysis_folder: Path, api_root: Path, config: dict[str, Any]) -> None:
    """Build every configured output into ``api/v<schema_version>/``."""
    _assert_config_valid(config)

    schema_version = config["schema_version"]
    version_root = api_root / f"v{schema_version}"

    print(f"📦 Publishing {version_root} from {analysis_folder}")

    built = _build_all_outputs(analysis_folder, version_root, config)
    entries = _write_all_outputs(built, schema_version, version_root / "manifest.json")
    _report_release(entries)
