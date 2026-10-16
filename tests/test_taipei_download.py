from io import BytesIO
from zipfile import ZipFile

import pytest

from citybikeshare.etl.custom_downloaders.taipei import (
    _expected_output,
    _extract_month_from_url,
    _write_month_csv,
)

REAL_URL = (
    "https://tcgbusfs.blob.core.windows.net/dotapp/"
    "youbike_second_ticket_opendata/2026/2026-02/202602_YouBike2.0租借站點資訊.zip"
)

CSV_BODY = b"rent_time,rent_station\n2026-03-01 00:00:00,a\n"


def _zip_bytes(members: dict[str, bytes]) -> bytes:
    """An in-memory zip; a name ending in '/' becomes a directory entry."""
    buffer = BytesIO()
    with ZipFile(buffer, "w") as zip_file:
        for name, body in members.items():
            if name.endswith("/"):
                zip_file.mkdir(name.rstrip("/"))
            else:
                zip_file.writestr(name, body)
    return buffer.getvalue()


def test_expected_output_derives_month_from_url(tmp_path):
    out = _expected_output(REAL_URL, str(tmp_path))
    assert out == str(tmp_path / "202602.csv")


def test_extract_month_rejects_url_without_month():
    with pytest.raises(ValueError, match="no YYYYMM_ prefix"):
        _extract_month_from_url("https://example.com/YouBike2.0_202603.zip")


@pytest.mark.parametrize(
    ("month", "members"),
    [
        ("202602", {"202602_YouBike2.0票證刷卡資料.csv": CSV_BODY}),
        (
            "202112",
            {
                "202112_YouBike2.0/": b"",
                "202112_YouBike2.0/202112_YouBike2.0.csv": CSV_BODY,
            },
        ),
        ("202603", {"YouBike2.0票證刷卡資料_202603.csv": CSV_BODY}),
    ],
    ids=["month-prefix", "wrapped-in-directory", "month-suffix"],
)
def test_write_month_csv_handles_each_member_layout(tmp_path, month, members):
    target = tmp_path / f"{month}.csv"
    _write_month_csv(_zip_bytes(members), month, str(target))
    assert target.read_bytes() == CSV_BODY
    assert not (tmp_path / f"{month}.csv.part").exists()


def test_write_month_csv_rejects_multiple_csvs(tmp_path):
    zip_bytes = _zip_bytes({"202603_a.csv": CSV_BODY, "202603_b.csv": CSV_BODY})
    with pytest.raises(ValueError, match="exactly one CSV"):
        _write_month_csv(zip_bytes, "202603", str(tmp_path / "202603.csv"))
