import shutil
import zipfile
from pathlib import Path

import pytest

from citybikeshare.context import PipelineContext
from citybikeshare.etl.extract import extract_city_data
from citybikeshare.etl.state import (
    file_signature,
    is_unchanged,
    load_state,
    write_state,
)
from citybikeshare.etl.transform import transform_city_data

TEST_DATA = Path(__file__).parent.parent / "test_data"
BOSTON_2024 = TEST_DATA / "202401-bluebikes-tripdata.csv"


class TestStateHelpers:
    def test_file_signature_has_size_and_mtime(self, tmp_path):
        f = tmp_path / "a.csv"
        f.write_text("hello")
        sig = file_signature(f)
        assert sig["bytes"] == 5
        assert "mtime" in sig

    def test_load_missing_state_returns_empty(self, tmp_path):
        assert load_state(tmp_path / "nope.json") == {}

    def test_write_then_load_round_trip(self, tmp_path):
        path = tmp_path / "m.json"
        data = {"a.csv": {"bytes": 1, "mtime": 2.0, "outputs": ["a.parquet"]}}
        write_state(path, data)
        assert load_state(path) == data

    def test_is_unchanged_true_for_matching_signature(self, tmp_path):
        f = tmp_path / "a.csv"
        f.write_text("hello")
        assert is_unchanged(f, file_signature(f)) is True

    def test_is_unchanged_false_when_bytes_differ(self, tmp_path):
        f = tmp_path / "a.csv"
        f.write_text("hello")
        recorded = file_signature(f)
        f.write_text("hello world")  # size changes
        assert is_unchanged(f, recorded) is False

    def test_is_unchanged_false_for_none_record(self, tmp_path):
        f = tmp_path / "a.csv"
        f.write_text("hello")
        assert is_unchanged(f, None) is False

    def test_is_unchanged_false_for_missing_file(self, tmp_path):
        assert is_unchanged(tmp_path / "ghost.csv", {"bytes": 1, "mtime": 2.0}) is False


@pytest.fixture
def boston_context(tmp_path):
    ctx = PipelineContext(
        city="boston",
        data_root=tmp_path / "data",
        transformed_root=tmp_path / "output",
        analysis_root=tmp_path / "analysis",
    )
    ctx.raw_directory.mkdir(parents=True, exist_ok=True)
    ctx.parquet_directory.mkdir(parents=True, exist_ok=True)
    shutil.copy(BOSTON_2024, ctx.raw_directory / BOSTON_2024.name)
    return ctx


class TestTransformIncremental:
    def _parquet_path(self, ctx):
        return ctx.parquet_directory / BOSTON_2024.name.replace(".csv", ".parquet")

    def test_first_run_creates_parquet_and_state(self, boston_context):
        transform_city_data(boston_context)
        assert self._parquet_path(boston_context).exists()
        state = load_state(boston_context.transform_state_path)
        assert BOSTON_2024.name in state
        assert state[BOSTON_2024.name]["bytes"] == BOSTON_2024.stat().st_size

    def test_second_run_skips_unchanged_file(self, boston_context):
        transform_city_data(boston_context)
        parquet = self._parquet_path(boston_context)
        first_mtime = parquet.stat().st_mtime_ns

        transform_city_data(boston_context)
        # Skipped → the per-file parquet was not rewritten.
        assert parquet.stat().st_mtime_ns == first_mtime

    def test_modified_input_is_reprocessed(self, boston_context):
        transform_city_data(boston_context)
        state_before = load_state(boston_context.transform_state_path)
        recorded_bytes = state_before[BOSTON_2024.name]["bytes"]

        # Append a row so the size (and thus signature) changes.
        raw_file = boston_context.raw_directory / BOSTON_2024.name
        with open(raw_file, "a") as f:
            f.write(
                '"EXTRA123","classic_bike","2024-01-15 10:00:00","2024-01-15 10:05:00",'
                '"Ames St at Main St","M32037","Central Square","M32011",'
                "42.36,-71.08,42.36,-71.10,\"member\"\n"
            )

        transform_city_data(boston_context)
        state_after = load_state(boston_context.transform_state_path)
        assert state_after[BOSTON_2024.name]["bytes"] != recorded_bytes
        assert state_after[BOSTON_2024.name]["bytes"] == raw_file.stat().st_size

    def test_removed_input_drops_orphan_parquet(self, boston_context):
        transform_city_data(boston_context)
        parquet = self._parquet_path(boston_context)
        assert parquet.exists()

        # Remove the source CSV, then re-run.
        (boston_context.raw_directory / BOSTON_2024.name).unlink()
        transform_city_data(boston_context)

        assert not parquet.exists()
        state = load_state(boston_context.transform_state_path)
        assert BOSTON_2024.name not in state

    def test_no_incremental_rebuilds(self, boston_context):
        transform_city_data(boston_context)
        parquet = self._parquet_path(boston_context)
        first_mtime = parquet.stat().st_mtime_ns

        transform_city_data(boston_context, incremental=False)
        # Forced rebuild → parquet rewritten (new mtime).
        assert parquet.stat().st_mtime_ns != first_mtime


@pytest.fixture
def extract_context(tmp_path):
    ctx = PipelineContext(
        city="boston",
        data_root=tmp_path / "data",
        transformed_root=tmp_path / "output",
        analysis_root=tmp_path / "analysis",
    )
    ctx.download_directory.mkdir(parents=True, exist_ok=True)
    ctx.raw_directory.mkdir(parents=True, exist_ok=True)
    return ctx


def _make_zip(path, members: dict):
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)


class TestExtractIncremental:
    def test_appledouble_files_are_not_extracted(self, extract_context):
        _make_zip(
            extract_context.download_directory / "202603-tripdata.zip",
            {
                "202603-tripdata.csv": "a,b\n1,2\n",
                "._202603-tripdata.csv": "garbage",  # AppleDouble metadata
                "__MACOSX/._202603-tripdata.csv": "garbage",
            },
        )
        extract_city_data(extract_context)

        raw = extract_context.raw_directory
        assert (raw / "202603-tripdata.csv").exists()
        assert not (raw / "._202603-tripdata.csv").exists()
        # AppleDouble names must not be recorded as outputs (this is what broke skips).
        state = load_state(extract_context.extract_state_path)
        outputs = state["202603-tripdata.zip"]["outputs"]
        assert outputs == ["202603-tripdata.csv"]

    def test_unchanged_archive_is_skipped(self, extract_context):
        _make_zip(
            extract_context.download_directory / "202603-tripdata.zip",
            {"202603-tripdata.csv": "a,b\n1,2\n", "._202603-tripdata.csv": "junk"},
        )
        extract_city_data(extract_context)
        extracted = extract_context.raw_directory / "202603-tripdata.csv"
        first_mtime = extracted.stat().st_mtime_ns

        # Second run: archive unchanged + output present → skip (no re-extract).
        extract_city_data(extract_context)
        assert extracted.stat().st_mtime_ns == first_mtime

    def test_cumulative_archive_only_rewrites_changed_members(self, extract_context):
        # A cumulative archive (like Daejeon) re-bundles every month each release.
        zip_path = extract_context.download_directory / "trips.zip"
        _make_zip(
            zip_path,
            {"a.csv": "x,y\n1,2\n", "b.csv": "x,y\n1,2\n"},
        )
        extract_city_data(extract_context)
        raw = extract_context.raw_directory
        a_mtime = (raw / "a.csv").stat().st_mtime_ns

        # New release: a unchanged, b grew, c is new. Overwrite the same archive.
        _make_zip(
            zip_path,
            {
                "a.csv": "x,y\n1,2\n",  # identical
                "b.csv": "x,y\n1,2\n3,4\n5,6\n",  # larger
                "c.csv": "x,y\n9,9\n",  # new
            },
        )
        b_mtime_before = (raw / "b.csv").stat().st_mtime_ns
        extract_city_data(extract_context)

        # Unchanged member keeps its mtime (so transform skips it); changed/new are written.
        assert (raw / "a.csv").stat().st_mtime_ns == a_mtime
        assert (raw / "b.csv").stat().st_mtime_ns != b_mtime_before
        assert (raw / "c.csv").exists()
