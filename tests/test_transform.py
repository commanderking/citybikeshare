import polars as pl
import pytest
from pathlib import Path

from citybikeshare.context import PipelineContext
from citybikeshare.config.loader import load_city_config
from citybikeshare.etl.pipelines.common import convert_columns_to_datetime
from citybikeshare.etl.transform import (
    create_parquet,
    determine_has_header,
    filter_filenames,
    get_csv_scan_params,
)

TEST_DATA = Path(__file__).parent.parent / "test_data"
BOSTON_2018 = TEST_DATA / "201805-bluebikes-tripdata.csv"
BOSTON_2023 = TEST_DATA / "202301-bluebikes-tripdata.csv"
BOSTON_2024 = TEST_DATA / "202401-bluebikes-tripdata.csv"


@pytest.fixture
def boston_config():
    return load_city_config("boston")


@pytest.fixture
def boston_context(tmp_path):
    ctx = PipelineContext(
        city="boston",
        data_root=tmp_path / "data",
        transformed_root=tmp_path / "output",
        analysis_root=tmp_path / "analysis",
    )
    ctx.parquet_directory.mkdir(parents=True, exist_ok=True)
    return ctx


class TestFilterFilenames:
    def test_includes_files_matching_pattern(self):
        config = {"file_matcher": ["-tripdata"], "excluded_filenames": []}
        files = [
            "/data/boston/201805-bluebikes-tripdata.csv",
            "/data/boston/stations.csv",
        ]
        assert filter_filenames(files, config) == [
            "/data/boston/201805-bluebikes-tripdata.csv"
        ]

    def test_excludes_explicitly_listed_filenames(self):
        config = {"file_matcher": ["-tripdata"], "excluded_filenames": ["202001"]}
        files = [
            "/data/boston/201805-bluebikes-tripdata.csv",
            "/data/boston/202001-bluebikes-tripdata.csv",
        ]
        result = filter_filenames(files, config)
        assert result == ["/data/boston/201805-bluebikes-tripdata.csv"]

    def test_empty_file_list_returns_empty(self):
        config = {"file_matcher": ["-tripdata"], "excluded_filenames": []}
        assert filter_filenames([], config) == []

    def test_multiple_matchers(self):
        config = {"file_matcher": ["-tripdata", "trips"], "excluded_filenames": []}
        files = [
            "/data/boston/201805-bluebikes-tripdata.csv",
            "/data/philly/2022Q1-trips.csv",
            "/data/boston/stations.csv",
        ]
        result = filter_filenames(files, config)
        assert len(result) == 2
        assert "/data/boston/stations.csv" not in result


class TestCreateParquet:
    REQUIRED_COLUMNS = {
        "start_time",
        "end_time",
        "start_station_name",
        "end_station_name",
    }

    def _load_output(self, context: PipelineContext, source_csv: Path) -> pl.DataFrame:
        parquet_path = context.parquet_directory / source_csv.name.replace(
            ".csv", ".parquet"
        )
        assert parquet_path.exists(), f"Parquet not written: {parquet_path}"
        return pl.read_parquet(parquet_path)

    def _assert_base_schema(self, df: pl.DataFrame):
        assert self.REQUIRED_COLUMNS.issubset(set(df.columns))
        assert df.schema["start_time"] == pl.Datetime("ms")
        assert df.schema["end_time"] == pl.Datetime("ms")
        assert df.schema["start_station_name"] == pl.String
        assert df.schema["end_station_name"] == pl.String
        assert len(df) > 0

    def test_2018_format_produces_valid_parquet(self, boston_context, boston_config):
        create_parquet(str(BOSTON_2018), boston_context, boston_config)
        df = self._load_output(boston_context, BOSTON_2018)
        self._assert_base_schema(df)

    def test_2023_format_produces_valid_parquet(self, boston_context, boston_config):
        create_parquet(str(BOSTON_2023), boston_context, boston_config)
        df = self._load_output(boston_context, BOSTON_2023)
        self._assert_base_schema(df)

    def test_2024_format_produces_valid_parquet(self, boston_context, boston_config):
        create_parquet(str(BOSTON_2024), boston_context, boston_config)
        df = self._load_output(boston_context, BOSTON_2024)
        self._assert_base_schema(df)

    def test_2018_format_includes_membership_and_demographics(
        self, boston_context, boston_config
    ):
        # Old format has usertype, birth year, gender — these map to membership_type, birth_year, gender
        create_parquet(str(BOSTON_2018), boston_context, boston_config)
        df = self._load_output(boston_context, BOSTON_2018)
        assert "membership_type" in df.columns
        assert "birth_year" in df.columns
        assert "gender" in df.columns
        assert df["membership_type"].drop_nulls().len() > 0

    def test_2024_format_includes_bike_type(self, boston_context, boston_config):
        # New format has rideable_type which maps to bike_type
        create_parquet(str(BOSTON_2024), boston_context, boston_config)
        df = self._load_output(boston_context, BOSTON_2024)
        assert "bike_type" in df.columns
        assert df.schema["bike_type"] == pl.String
        assert df["bike_type"].drop_nulls().len() > 0

    def test_start_times_are_not_null(self, boston_context, boston_config):
        create_parquet(str(BOSTON_2024), boston_context, boston_config)
        df = self._load_output(boston_context, BOSTON_2024)
        assert df["start_time"].null_count() == 0

    def test_config_param_is_used(self, boston_context, tmp_path):
        # Regression: create_parquet previously ignored its config arg and reloaded from disk.
        # Verify a config with a broken pipeline raises rather than silently loading the wrong config.
        bad_config = load_city_config("boston") | {
            "processing_pipeline": ["nonexistent_step"]
        }
        with pytest.raises(KeyError):
            create_parquet(str(BOSTON_2024), boston_context, bad_config)


class TestGetCsvScanParams:
    def test_defaults_to_has_header_true(self):
        params = get_csv_scan_params("/some/file.csv", {})
        assert params["has_header"] is True
        assert params["encoding"] == "utf8-lossy"
        assert params["infer_schema_length"] == 0

    def test_explicit_has_header_false_includes_new_columns(self):
        opts = {"has_header": False, "new_columns": ["col_a", "col_b"]}
        params = get_csv_scan_params("/some/file.csv", opts)
        assert params["has_header"] is False
        assert params["new_columns"] == ["col_a", "col_b"]

    def test_extra_options_are_passed_through(self):
        opts = {"separator": ";"}
        params = get_csv_scan_params("/some/file.csv", opts)
        assert params["separator"] == ";"


class TestDetermineHasHeader:
    def test_returns_true_when_first_row_matches_expected_columns(self, tmp_path):
        csv = tmp_path / "with_header.csv"
        csv.write_text("start_time,end_time,station_name\n2024-01-01,2024-01-01,A\n")
        assert (
            determine_has_header(str(csv), ["start_time", "end_time", "station_name"])
            is True
        )

    def test_returns_false_when_first_row_is_data(self, tmp_path):
        csv = tmp_path / "no_header.csv"
        csv.write_text("2024-01-01 00:00:00,2024-01-01 00:10:00,StationA\n")
        assert (
            determine_has_header(str(csv), ["start_time", "end_time", "station_name"])
            is False
        )

    def test_returns_false_when_only_some_columns_match(self, tmp_path):
        csv = tmp_path / "partial.csv"
        csv.write_text("start_time,end_time,station_name\n")
        # expected_columns is missing station_name, so not all first-row items are in the set
        assert determine_has_header(str(csv), ["start_time", "end_time"]) is False


class TestDatetimeGuard:
    FMT = ["%Y-%m-%d %H:%M:%S"]

    def test_unparseable_present_value_raises(self):
        # A real value in a format we didn't declare must fail loudly, not become null.
        lf = pl.LazyFrame({"start_time": ["2024-01-01 00:00:00", "2025-03-24 18:04"]})
        fn = convert_columns_to_datetime(["start_time"], self.FMT)
        with pytest.raises(ValueError, match="matched none of"):
            fn(lf)

    def test_error_names_column_and_examples(self):
        lf = pl.LazyFrame({"start_time": ["2025-03-25 8:18"]})
        fn = convert_columns_to_datetime(["start_time"], self.FMT)
        with pytest.raises(ValueError, match=r"start_time.*2025-03-25 8:18"):
            fn(lf)

    def test_empty_and_null_values_are_allowed(self):
        # Genuinely missing dates are fine — they pass through as null without raising.
        lf = pl.LazyFrame(
            {"start_time": ["2024-01-01 00:00:00", "", None]},
            schema={"start_time": pl.Utf8},
        )
        fn = convert_columns_to_datetime(["start_time"], self.FMT)
        out = fn(lf).collect()
        assert out["start_time"].null_count() == 2

    def test_all_parseable_passes(self):
        lf = pl.LazyFrame({"start_time": ["2024-01-01 00:00:00", "2025-03-24 18:04"]})
        fn = convert_columns_to_datetime(
            ["start_time"], ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"]
        )
        out = fn(lf).collect()
        assert out["start_time"].null_count() == 0
