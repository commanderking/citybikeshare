import pytest

from citybikeshare.etl.custom_downloaders.mexico_city import (
    _apply_url_overrides,
    _assert_unique_hrefs,
)

# The page as found: the "2025-02" label links January's file.
PAGE_LINKS = [
    ("2025-01", "/wp-content/uploads/2025/02/2025-01.csv"),
    ("2025-02", "/wp-content/uploads/2025/02/2025-01.csv"),
    ("2025-03", "/wp-content/uploads/2025/04/2025-03.csv"),
]


def test_mislinked_month_fails_loud():
    with pytest.raises(ValueError, match=r"2025-01\.csv <- \['2025-01', '2025-02'\]"):
        _assert_unique_hrefs(PAGE_LINKS)


def test_override_repoints_a_listed_label():
    overrides = {"2025-02": "/wp-content/uploads/2025/03/2025-02.csv"}
    links = _apply_url_overrides(PAGE_LINKS, overrides)
    assert ("2025-02", "/wp-content/uploads/2025/03/2025-02.csv") in links
    assert len(links) == len(PAGE_LINKS)
    _assert_unique_hrefs(links)


def test_override_adds_an_unlisted_label():
    overrides = {"2020-12": "/wp-content/uploads/2022/08/2020-12.csv"}
    links = _apply_url_overrides(PAGE_LINKS[:1], overrides)
    assert links == [
        ("2025-01", "/wp-content/uploads/2025/02/2025-01.csv"),
        ("2020-12", "/wp-content/uploads/2022/08/2020-12.csv"),
    ]
