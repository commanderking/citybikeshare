import json

import pytest

from citybikeshare.publish.api import publish_api
from citybikeshare.publish.builders import build_merged_section
from citybikeshare.publish.manifest import (
    diff_against_published,
    format_diff,
    summarize_city_coverage,
)

SPEC = {
    "name": "all_cities_volume_by_month",
    "shape": "merge_cities",
    "source": "visuals",
    "section": "volume_by_month",
    "key": ["city", "year", "month"],
}


def write_city(analysis_folder, city, section, name="volume_by_month"):
    city_dir = analysis_folder / city
    city_dir.mkdir(parents=True, exist_ok=True)
    (city_dir / "visuals.json").write_text(
        json.dumps({name: section}), encoding="utf-8"
    )


def month(year, m, trips):
    return {"year": year, "month": m, "trips": trips}


@pytest.fixture
def analysis_folder(tmp_path):
    folder = tmp_path / "analysis"
    folder.mkdir()
    write_city(folder, "boston", [month(2025, 1, 100), month(2025, 2, 200)])
    write_city(folder, "austin", [month(2025, 1, 50)])
    return folder


def api_config(**overrides):
    return {"schema_version": 1, "outputs": [{**SPEC, **overrides}]}


def test_merges_cities_and_tags_each_record(analysis_folder):
    records = build_merged_section(analysis_folder, SPEC)

    assert [r["city"] for r in records] == ["austin", "boston", "boston"]
    assert records[0] == {"city": "austin", "year": 2025, "month": 1, "trips": 50}


def test_missing_section_raises_rather_than_dropping_the_city(analysis_folder):
    write_city(analysis_folder, "helsinki", [], name="by_hour")

    with pytest.raises(ValueError, match="helsinki"):
        build_merged_section(analysis_folder, SPEC)


def test_disagreeing_record_fields_raise(analysis_folder):
    write_city(analysis_folder, "oslo", [{"year": 2025, "month": 1, "rides": 7}])

    with pytest.raises(ValueError, match="disagree on record fields"):
        build_merged_section(analysis_folder, SPEC)


def test_duplicate_keys_raise(analysis_folder):
    write_city(analysis_folder, "oslo", [month(2025, 1, 10), month(2025, 1, 10)])

    with pytest.raises(ValueError, match="duplicate keys"):
        build_merged_section(analysis_folder, SPEC)


def test_key_field_absent_from_records_raises(analysis_folder):
    with pytest.raises(ValueError, match="`key` names"):
        build_merged_section(analysis_folder, {**SPEC, "key": ["city", "quarter"]})


def test_city_coverage_spans_months():
    coverage = summarize_city_coverage(
        [
            {"city": "boston", "year": 2024, "month": 11, "trips": 5},
            {"city": "boston", "year": 2025, "month": 2, "trips": 7},
        ]
    )

    assert coverage == {
        "rows": 2,
        "trips": 12,
        "first_month": "2024-11",
        "last_month": "2025-02",
    }


def test_diff_separates_appended_rows_from_restated_history(tmp_path):
    published = tmp_path / "out.json"
    published.write_text(
        json.dumps(
            [
                {"city": "boston", "year": 2025, "month": 1, "trips": 100},
                {"city": "boston", "year": 2025, "month": 2, "trips": 200},
            ]
        ),
        encoding="utf-8",
    )

    diff = diff_against_published(
        published,
        [
            {"city": "boston", "year": 2025, "month": 1, "trips": 111},
            {"city": "boston", "year": 2025, "month": 3, "trips": 300},
        ],
        ["city", "year", "month"],
    )

    assert diff["added"] == [("boston", 2025, 3)]
    assert diff["removed"] == [("boston", 2025, 2)]
    assert [c["key"] for c in diff["changed"]] == [
        {"city": "boston", "year": 2025, "month": 1}
    ]
    assert diff["changed"][0]["from"]["trips"] == 100
    assert diff["changed"][0]["to"]["trips"] == 111


def test_first_publish_has_no_diff(tmp_path):
    assert diff_against_published(tmp_path / "absent.json", [], ["city"]) is None


KEY = ["city", "year", "month"]


def test_release_notes_for_a_first_publish():
    assert format_diff(None, KEY) == [
        "   first publish — no previous release to compare against"
    ]


def test_release_notes_when_nothing_moved():
    empty = {"added": [], "removed": [], "changed": []}

    assert format_diff(empty, KEY) == ["   no change since the previous release"]


def test_release_notes_tally_added_and_removed_rows_by_city():
    diff = {
        "added": [("boston", 2026, 1), ("boston", 2026, 2), ("chicago", 2026, 1)],
        "removed": [("austin", 2019, 5)],
        "changed": [],
    }

    assert format_diff(diff, KEY) == [
        "   + 3 new rows: boston (2), chicago (1)",
        "   - 1 rows removed: austin (1)",
    ]


def test_release_notes_call_out_restated_history():
    diff = {
        "added": [],
        "removed": [],
        "changed": [
            {
                "key": {"city": "boston", "year": 2025, "month": 11},
                "from": {"trips": 100},
                "to": {"trips": 1148},
            }
        ],
    }

    lines = format_diff(diff, KEY)

    assert "1 previously published row CHANGED" in lines[0]
    assert lines[1] == "       boston 2025-11  trips: 100 → 1,148"


def test_release_notes_truncate_a_long_list_of_changes():
    changed = [
        {
            "key": {"city": "boston", "year": 2025, "month": m},
            "from": {"trips": 1},
            "to": {"trips": 2},
        }
        for m in range(1, 13)
    ]

    lines = format_diff({"added": [], "removed": [], "changed": changed}, KEY)

    assert "12 previously published rows CHANGED" in lines[0]
    assert lines[-1] == "       … and 2 more"


def test_publish_writes_payload_and_manifest(tmp_path, analysis_folder):
    api_root = tmp_path / "api"

    assert publish_api(analysis_folder, api_root, api_config())

    payload = json.loads(
        (api_root / "v1" / "all_cities_volume_by_month.json").read_text(
            encoding="utf-8"
        )
    )
    assert len(payload) == 3

    manifest = json.loads(
        (api_root / "v1" / "manifest.json").read_text(encoding="utf-8")
    )
    entry = manifest["outputs"]["all_cities_volume_by_month"]
    assert manifest["schema_version"] == 1
    assert entry["rows"] == 3
    assert entry["cities"]["boston"] == {
        "rows": 2,
        "trips": 300,
        "first_month": "2025-01",
        "last_month": "2025-02",
    }


def test_republishing_unchanged_analysis_is_byte_identical(tmp_path, analysis_folder):
    api_root = tmp_path / "api"
    published = api_root / "v1" / "all_cities_volume_by_month.json"
    manifest = api_root / "v1" / "manifest.json"

    publish_api(analysis_folder, api_root, api_config())
    first_payload = published.read_bytes()
    first_manifest = manifest.read_bytes()
    publish_api(analysis_folder, api_root, api_config())

    assert published.read_bytes() == first_payload
    # The manifest too — a no-op republish must not dirty it with a fresh wall-clock stamp.
    assert manifest.read_bytes() == first_manifest


def stamp_manifest(api_root, generated_at):
    """Backdate the published manifest so timestamp assertions don't race the clock."""
    path = api_root / "v1" / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["generated_at"] = generated_at
    path.write_text(json.dumps(manifest), encoding="utf-8")


def read_generated_at(api_root):
    manifest = json.loads(
        (api_root / "v1" / "manifest.json").read_text(encoding="utf-8")
    )
    return manifest["generated_at"]


def test_generated_at_tracks_data_changes_not_publish_runs(tmp_path, analysis_folder):
    api_root = tmp_path / "api"
    sentinel = "2020-01-01T00:00:00Z"

    publish_api(analysis_folder, api_root, api_config())
    stamp_manifest(api_root, sentinel)

    publish_api(analysis_folder, api_root, api_config())
    assert read_generated_at(api_root) == sentinel

    write_city(analysis_folder, "austin", [month(2025, 1, 50), month(2025, 2, 60)])
    publish_api(analysis_folder, api_root, api_config())
    assert read_generated_at(api_root) != sentinel


def test_strict_fails_when_published_history_is_restated(tmp_path, analysis_folder):
    api_root = tmp_path / "api"
    publish_api(analysis_folder, api_root, api_config())

    write_city(analysis_folder, "boston", [month(2025, 1, 999), month(2025, 2, 200)])

    assert not publish_api(analysis_folder, api_root, api_config(), strict=True)
    # Appending a month is not a restatement, so strict stays happy.
    write_city(
        analysis_folder,
        "boston",
        [month(2025, 1, 999), month(2025, 2, 200), month(2025, 3, 300)],
    )
    assert publish_api(analysis_folder, api_root, api_config(), strict=True)


def test_builder_failure_leaves_the_previous_release_untouched(
    tmp_path, analysis_folder
):
    """A later output blowing up must not half-replace the release — otherwise the manifest
    describes a state the payloads beside it no longer match."""
    api_root = tmp_path / "api"
    payload = api_root / "v1" / "all_cities_volume_by_month.json"
    manifest = api_root / "v1" / "manifest.json"

    publish_api(analysis_folder, api_root, api_config())
    published_payload = payload.read_bytes()
    published_manifest = manifest.read_bytes()

    # New data for the first output, and a second output whose `key` names a field the
    # records don't have — passes config validation, fails inside the builder.
    write_city(analysis_folder, "boston", [month(2025, 1, 100), month(2026, 4, 400)])
    config = api_config()
    config["outputs"].append({**SPEC, "name": "second", "key": ["city", "quarter"]})

    with pytest.raises(ValueError, match="`key` names"):
        publish_api(analysis_folder, api_root, config)

    assert payload.read_bytes() == published_payload
    assert manifest.read_bytes() == published_manifest
    assert not (api_root / "v1" / "second.json").exists()


def test_unknown_shape_fails_before_writing_anything(tmp_path, analysis_folder):
    api_root = tmp_path / "api"

    with pytest.raises(ValueError, match="unknown shape"):
        publish_api(analysis_folder, api_root, api_config(shape="merge_planets"))

    assert not api_root.exists()
