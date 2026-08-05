import json

import pytest

from citybikeshare.publish.api import publish_api
from citybikeshare.publish.builders import build_merged_section

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


def test_publish_writes_the_payload(tmp_path, analysis_folder):
    api_root = tmp_path / "api"

    publish_api(analysis_folder, api_root, api_config())

    payload = json.loads(
        (api_root / "v1" / "all_cities_volume_by_month.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload == [
        {"city": "austin", "year": 2025, "month": 1, "trips": 50},
        {"city": "boston", "year": 2025, "month": 1, "trips": 100},
        {"city": "boston", "year": 2025, "month": 2, "trips": 200},
    ]


def test_republishing_unchanged_analysis_is_byte_identical(tmp_path, analysis_folder):
    api_root = tmp_path / "api"
    published = api_root / "v1" / "all_cities_volume_by_month.json"

    publish_api(analysis_folder, api_root, api_config())
    first = published.read_bytes()
    publish_api(analysis_folder, api_root, api_config())

    assert published.read_bytes() == first


def test_builder_failure_leaves_the_previous_release_untouched(
    tmp_path, analysis_folder
):
    """A later output blowing up must not half-replace the release — otherwise `api/` mixes
    files from this run with files from the last, and the tag you cut publishes that."""
    api_root = tmp_path / "api"
    payload = api_root / "v1" / "all_cities_volume_by_month.json"

    publish_api(analysis_folder, api_root, api_config())
    published_payload = payload.read_bytes()

    # New data for the first output, and a second output whose `key` names a field the
    # records don't have — passes config validation, fails inside the builder.
    write_city(analysis_folder, "boston", [month(2025, 1, 100), month(2026, 4, 400)])
    config = api_config()
    config["outputs"].append({**SPEC, "name": "second", "key": ["city", "quarter"]})

    with pytest.raises(ValueError, match="`key` names"):
        publish_api(analysis_folder, api_root, config)

    assert payload.read_bytes() == published_payload
    assert not (api_root / "v1" / "second.json").exists()


def test_unknown_shape_fails_before_writing_anything(tmp_path, analysis_folder):
    api_root = tmp_path / "api"

    with pytest.raises(ValueError, match="unknown shape"):
        publish_api(analysis_folder, api_root, api_config(shape="merge_planets"))

    assert not api_root.exists()
