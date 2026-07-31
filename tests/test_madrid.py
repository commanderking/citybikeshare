"""High-value tests for the Madrid (BiciMAD) pipeline.

Each class targets one correctness-critical behaviour where a regression would silently
corrupt data (wrong timezone, wrong duration unit, double-counted month, malformed cells) or
where a fail-loud guard must fire. Inputs are kept to the minimum rows/columns that still
exercise the branch under test.
"""

import json
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from citybikeshare.etl.pipelines.common import (
    _add_madrid_duration_seconds,
    _madrid_local_datetime_expr,
    reconcile_madrid_times,
    select_final_columns,
)
from citybikeshare.utils.io_clean import (
    _BICIMAD_MOVEMENT_COLUMNS,
    _bicimad_month_covered_by_csv,
    _write_bicimad_movements_csv,
    convert_bicimad_movements_json,
)

DATE_FORMATS = ["%Y-%m-%dT%H:%M:%S"]


# 1. Timezone: the three encodings must all land on naive Madrid local time. The movements
#    "...Z" era is genuine UTC and must shift (+1 winter / +2 summer); the others are already
#    local. A regression here silently moves ~2.5 years of data 1–2h.
class TestMadridTimezone:
    def _local(self, values):
        return (
            pl.DataFrame({"t": values})
            .with_columns(_madrid_local_datetime_expr("t", DATE_FORMATS).alias("t"))["t"]
            .to_list()
        )

    def test_movements_utc_shifts_to_madrid_local_across_dst(self):
        # Z = UTC -> Madrid local: +1h in winter (CET), +2h in summer (CEST).
        assert self._local(["2020-01-01T00:00:00Z", "2020-07-01T00:00:00Z"]) == [
            datetime(2020, 1, 1, 1, 0, 0),
            datetime(2020, 7, 1, 2, 0, 0),
        ]

    def test_usage_offset_and_csv_naive_stay_local(self):
        # Usage's offset already reflects local wall-clock; CSV is naive local. Both unchanged.
        assert self._local(
            ["2018-07-01T01:00:00.000+0200", "2022-01-01T00:02:20"]
        ) == [datetime(2018, 7, 1, 1, 0, 0), datetime(2022, 1, 1, 0, 2, 20)]

    def test_empty_timestamp_stays_null(self):
        assert self._local([""]) == [None]


# 2. Duration: JSON ships seconds, CSV ships minutes — they must converge on seconds (×60),
#    and a frame with neither column must fail loud rather than produce null durations.
class TestMadridDuration:
    def _seconds(self, data):
        out = _add_madrid_duration_seconds(pl.LazyFrame(data), list(data))
        return out.collect()["duration_seconds"].to_list()

    def test_json_seconds_passthrough(self):
        assert self._seconds({"duration_seconds": ["154"]}) == [154.0]

    def test_csv_minutes_converted_to_seconds(self):
        assert self._seconds({"duration_minutes": ["16.28"]}) == pytest.approx([976.8])

    def test_missing_duration_column_fails_loud(self):
        with pytest.raises(ValueError, match="no duration column"):
            _add_madrid_duration_seconds(pl.LazyFrame({"foo": ["x"]}), ["foo"])


# 3. JSON->CSV converter: Mongo extended-JSON scalar wrappers must be unwrapped (else every
#    cell is a dict repr), and a record without `travel_time` must fail loud (a station file
#    mis-named as movements, or a schema change).
class TestMadridJsonConverter:
    def test_unwraps_mongo_scalars(self, tmp_path):
        raw = tmp_path / "201704_Usage_Bicimad.json"
        record = {
            "_id": {"$oid": "abc123"},
            "unplug_hourTime": {"$date": "2017-04-01T01:00:00.000+0200"},
            "travel_time": 169,
            "idunplug_station": 41,
            "idplug_station": 50,
            "idunplug_base": 1,
            "idplug_base": 17,
            "user_type": 1,
            "ageRange": 4,
            "user_day_code": "df84",
            "zip_code": "28005",
        }
        raw.write_text(json.dumps(record) + "\n", encoding="utf-8")
        out = tmp_path / "201704_Usage_Bicimad.csv"

        assert _write_bicimad_movements_csv(raw, out) == 1
        lines = out.read_text(encoding="utf-8").splitlines()
        assert lines[0] == ";".join(_BICIMAD_MOVEMENT_COLUMNS)
        row = dict(zip(lines[0].split(";"), lines[1].split(";")))
        # $date / $oid unwrapped to their scalar, not written as "{'$date': ...}"
        assert row["unplug_hourTime"] == "2017-04-01T01:00:00.000+0200"
        assert row["travel_time"] == "169"

    def test_record_without_travel_time_fails_loud(self, tmp_path):
        raw = tmp_path / "202001_movements.json"
        raw.write_text(json.dumps({"_id": 1, "stations": []}) + "\n", encoding="utf-8")
        with pytest.raises(ValueError, match="travel_time"):
            _write_bicimad_movements_csv(raw, tmp_path / "202001_movements.csv")


# 4. Dedup: a movements month that also ships as CSV (e.g. 2021-06) must be skipped so it isn't
#    counted twice; a month with no CSV sibling must convert normally.
class TestMadridDedup:
    def test_month_with_csv_sibling_is_covered(self):
        csv_files = [Path("trips_21_06_June.csv.gz")]
        assert _bicimad_month_covered_by_csv("202106_movements.json.gz", csv_files) is True

    def test_month_without_csv_sibling_is_not_covered(self):
        csv_files = [Path("trips_21_06_June.csv.gz")]
        assert _bicimad_month_covered_by_csv("202105_movements.json.gz", csv_files) is False

    def test_converter_skips_covered_month_and_writes_nothing(self, tmp_path):
        raw = tmp_path / "202106_movements.json"
        raw.write_text(json.dumps({"travel_time": 100}) + "\n", encoding="utf-8")
        out = tmp_path / "202106_movements.csv"

        result = convert_bicimad_movements_json(
            raw, out, {}, [Path("trips_21_06_June.csv.gz")]
        )
        assert result is None
        assert not out.exists()


# 5. Two-era convergence: after reconcile + select_final_columns, the JSON and CSV eras must
#    produce the *same* canonical schema (so per-file parquets combine — the Null-vs-String
#    partition bug), with end_time derived for JSON and kept precise for CSV, and columns
#    absent in an era present as typed-null Utf8.
class TestMadridTwoEraConvergence:
    CONFIG = {
        "date_formats": DATE_FORMATS,
        "final_columns": [
            "start_time",
            "end_time",
            "duration_seconds",
            "start_station_name",
            "trip_id",
        ],
    }
    EXPECTED_SCHEMA = {
        "start_time": pl.Datetime("ms"),
        "end_time": pl.Datetime("ms"),
        "duration_seconds": pl.Float64,
        "start_station_name": pl.Utf8,
        "trip_id": pl.Utf8,
    }

    def _canonical(self, data):
        df = reconcile_madrid_times(pl.LazyFrame(data), self.CONFIG, None)
        return select_final_columns(df, self.CONFIG["final_columns"]).collect()

    def _json_era(self):
        # hour-truncated UTC start, seconds duration, NO end_time, ids-only (no name/trip_id)
        return self._canonical(
            {"start_time": ["2020-01-01T00:00:00Z"], "duration_seconds": ["600"]}
        )

    def _csv_era(self):
        return self._canonical(
            {
                "start_time": ["2022-01-01T00:02:20"],
                "end_time": ["2022-01-01T00:18:37"],
                "duration_minutes": ["16.28"],
                "start_station_name": ["Plaza"],
                "trip_id": ["t1"],
            }
        )

    def test_json_era_derives_end_time_and_types_missing_columns(self):
        out = self._json_era()
        assert out["start_time"].to_list() == [datetime(2020, 1, 1, 1, 0, 0)]  # UTC+1
        assert out["end_time"].to_list() == [datetime(2020, 1, 1, 1, 10, 0)]  # +600s
        # name absent in JSON era -> present as Utf8 null (not Null dtype), so parquets combine
        assert out.schema["start_station_name"] == pl.Utf8
        assert out["start_station_name"].to_list() == [None]

    def test_csv_era_keeps_precise_end_time(self):
        out = self._csv_era()
        assert out["end_time"].to_list() == [datetime(2022, 1, 1, 0, 18, 37)]  # kept, not derived
        assert out["start_station_name"].to_list() == ["Plaza"]

    def test_both_eras_produce_identical_canonical_schema(self):
        assert dict(self._json_era().schema) == self.EXPECTED_SCHEMA
        assert dict(self._csv_era().schema) == self.EXPECTED_SCHEMA
