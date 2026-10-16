from datetime import datetime
from types import SimpleNamespace

import polars as pl
import pytest

from citybikeshare.etl.pipelines.common import restrict_to_file_date_range
from citybikeshare.etl.transform import _assert_file_scoped_keys_match_inputs

CONFIG = {"file_date_ranges": {"indego-trips-2021-q4.csv.gz": ["2021-10-01", "2022-01-01"]}}
TRIPS = pl.LazyFrame(
    {
        "start_time": [
            datetime(2021, 9, 30, 23, 59),  # repeated from Q3: dropped
            datetime(2021, 10, 1, 0, 0),    # range start is inclusive
            datetime(2021, 12, 31, 23, 59),
            datetime(2022, 1, 1, 0, 0),     # range end is exclusive
            None,                           # kept for the null checks
        ]
    }
)


def _context(name):
    return SimpleNamespace(source_file=f"data/philadelphia/raw/{name}")


def test_listed_file_keeps_only_its_range_and_nulls():
    out = restrict_to_file_date_range(TRIPS, CONFIG, _context("indego-trips-2021-q4.csv.gz"))
    assert out.collect()["start_time"].to_list() == [
        datetime(2021, 10, 1, 0, 0),
        datetime(2021, 12, 31, 23, 59),
        None,
    ]


def test_unlisted_file_passes_through():
    out = restrict_to_file_date_range(TRIPS, CONFIG, _context("indego-trips-2021-q3.csv.gz"))
    assert out.collect().height == 5


def test_file_scoped_key_naming_no_input_fails_loud():
    inputs = ["data/philadelphia/raw/indego-trips-2021-q3.csv.gz"]
    with pytest.raises(ValueError, match="indego-trips-2021-q4.csv.gz"):
        _assert_file_scoped_keys_match_inputs(CONFIG, inputs)


def test_file_scoped_keys_matching_inputs_pass():
    inputs = ["data/philadelphia/raw/indego-trips-2021-q4.csv.gz"]
    _assert_file_scoped_keys_match_inputs(CONFIG, inputs)
