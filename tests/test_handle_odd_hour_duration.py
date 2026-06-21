import polars as pl
import pytest

from citybikeshare.etl.pipelines.common import handle_odd_hour_duration


def _durations(values, excluded=()):
    lf = pl.LazyFrame({"duration": values})
    return handle_odd_hour_duration(lf, excluded).collect()["duration"].to_list()


def test_parses_hms_to_seconds():
    assert _durations(["00:02:01", "01:26:49", "00:00:00"]) == [121, 5209, 0]


def test_hours_can_exceed_24():
    # Taipei durations can run past 24h; 25:00:00 -> 90000s
    assert _durations(["25:00:00"]) == [90000]


def test_empty_or_whitespace_duration_is_null_not_error():
    # Genuinely missing values pass through as null without raising.
    assert _durations(["", "   ", "00:05:00"]) == [None, None, 300]


def test_malformed_duration_hard_errors():
    # Real corrupt value from 202509.csv: empty seconds field. Must surface, not null.
    with pytest.raises(ValueError, match="did not match HH:MM:SS"):
        _durations(["17520:06:", "00:05:00"])


def test_excluded_value_is_dropped_and_does_not_raise():
    # The known-corrupt row is excluded -> dropped; the good row survives.
    assert _durations(["17520:06:", "00:05:00"], excluded=["17520:06:"]) == [300]


def test_excluding_one_bad_value_does_not_mask_another():
    # Excluding the known value must not suppress a *different* unparseable value.
    with pytest.raises(ValueError, match="did not match HH:MM:SS"):
        _durations(["17520:06:", "99:99:"], excluded=["17520:06:"])
