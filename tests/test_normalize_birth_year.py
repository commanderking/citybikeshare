import polars as pl
import pytest

from citybikeshare.etl.pipelines.common import normalize_birth_year

CONFIG = {"birth_year_min": 1900}


def _run(values, config=CONFIG):
    df = pl.LazyFrame({"birth_year": values})
    return normalize_birth_year(df, config, None).collect()


def test_strips_float_suffix_and_casts_to_int():
    out = _run(["1994.0", "1982"])
    assert out["birth_year"].dtype == pl.Int64
    assert out["birth_year"].to_list() == [1994, 1982]


def test_missing_markers_pass_through_as_null():
    out = _run(["NA", "", None, "1990"])
    assert out["birth_year"].to_list() == [None, None, None, 1990]


def test_nulls_values_below_min():
    out = _run(["1", "199", "200", "1990"])
    assert out["birth_year"].to_list() == [None, None, None, 1990]


def test_keeps_high_years_including_infants():
    out = _run(["2021.0", "2023", "1990"])
    assert out["birth_year"].to_list() == [2021, 2023, 1990]


def test_raises_on_non_numeric():
    # strict cast surfaces a present, non-numeric value instead of nulling it silently
    with pytest.raises(pl.exceptions.InvalidOperationError, match="garbage"):
        _run(["garbage", "1990"])


def test_custom_missing_marker():
    out = _run(["--", "1990"], {"birth_year_min": 1900, "birth_year_missing": ["--"]})
    assert out["birth_year"].to_list() == [None, 1990]
