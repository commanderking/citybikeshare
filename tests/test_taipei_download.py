from citybikeshare.etl.custom_downloaders.taipei import (
    _expected_output,
    clean_filename,
)

REAL_URL = (
    "https://tcgbusfs.blob.core.windows.net/dotapp/"
    "youbike_second_ticket_opendata/2026/2026-02/202602_YouBike2.0租借站點資訊.zip"
)


def test_clean_filename_extracts_yyyymm():
    assert clean_filename("202602_YouBike2.0租借站點資訊.csv") == "202602.csv"


def test_expected_output_derives_month_from_url(tmp_path):
    """The skip key from the URL must match what the member name would produce,
    so the pre-fetch check and the per-member write agree."""
    out = _expected_output(REAL_URL, str(tmp_path))
    assert out == str(tmp_path / "202602.csv")


def test_skip_key_matches_member_derived_name():
    member_inside_zip = "202602_YouBike2.0租借站點資訊.csv"
    from_url = clean_filename(_expected_output(REAL_URL, "").split("/")[-1][:6])
    from_member = clean_filename(member_inside_zip)
    assert from_url == from_member == "202602.csv"
