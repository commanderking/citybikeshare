import polars as pl
import pytest

from citybikeshare.etl.pipelines.common import handle_odd_hour_duration


def _durations(values):
    lf = pl.LazyFrame({"duration": values})
    return handle_odd_hour_duration(lf).collect()["duration"].to_list()


def test_parses_hms_to_seconds():
    assert _durations(["00:02:01", "01:26:49", "00:00:00"]) == [121, 5209, 0]


def test_hours_can_exceed_24():
    # Taipei durations can run past 24h; 25:00:00 -> 90000s
    assert _durations(["25:00:00"]) == [90000]


def test_empty_or_whitespace_duration_is_null_not_error():
    # Genuinely missing values pass through as null without raising.
    assert _durations(["", "   ", "00:05:00"]) == [None, None, 300]


def test_malformed_duration_hard_errors():
    # Known-corrupt values are removed upstream by remove_invalid_rows;
    # anything malformed that reaches here must surface, not be nulled.
    with pytest.raises(ValueError, match="did not match HH:MM:SS"):
        _durations(["17520:06:", "00:05:00"])
