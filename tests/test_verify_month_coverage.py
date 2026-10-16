import pytest

from citybikeshare.analysis.verify_month_coverage import (
    _assert_known_gaps_current,
    _assert_months_complete,
    _expand_months,
    _find_incomplete_months,
)


def test_finds_missing_and_partial_months_between_first_and_last():
    days_by_month = {
        "2023-01": 3,   # first month: exempt however sparse
        "2023-02": 28,  # complete
        "2023-03": 24,  # 7 empty days (London's missing week)
        # 2023-04 missing entirely
        "2023-05": 27,  # 4 empty days (a blizzard): under the threshold
        "2023-06": 1,   # last month: exempt
    }
    assert _find_incomplete_months(days_by_month) == {"2023-03": 7, "2023-04": 30}


def test_expands_single_months_and_ranges_across_a_year_boundary():
    assert _expand_months(["2020-04", "2014-11..2015-02"]) == {
        "2020-04", "2014-11", "2014-12", "2015-01", "2015-02",
    }


def test_rejects_a_reversed_range():
    with pytest.raises(ValueError, match="empty or reversed"):
        _expand_months(["2015-04..2014-11"])


def test_unlisted_incomplete_month_fails_loud():
    with pytest.raises(ValueError, match=r"2023-02 \(28 days empty\)"):
        _assert_months_complete("dc", {"2023-02": 28, "2018-01": 31}, allowed={"2018-01"})


def test_listed_incomplete_months_pass():
    _assert_months_complete("dc", {"2018-01": 31}, allowed={"2018-01"})


def test_known_gap_that_is_now_complete_fails_loud():
    with pytest.raises(ValueError, match="now complete: 2023-02"):
        _assert_known_gaps_current("dc", {"2018-01": 31}, known={"2018-01", "2023-02"})
