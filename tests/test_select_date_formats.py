from types import SimpleNamespace

from citybikeshare.etl.pipelines.common import select_date_formats

CONFIG = {
    "date_formats": ["%m/%d/%Y %H:%M", "%d/%m/%Y %H:%M"],
    "file_date_formats": {"Bikeshare Ridership (2017 Q1).csv.gz": ["%d/%m/%Y %H:%M"]},
}


def _context(path):
    return SimpleNamespace(source_file=path)


def test_listed_file_uses_its_own_formats():
    ctx = _context("data/toronto/cleaned/Bikeshare Ridership (2017 Q1).csv.gz")
    assert select_date_formats(CONFIG, ctx) == ["%d/%m/%Y %H:%M"]


def test_other_files_use_city_formats():
    ctx = _context("data/toronto/cleaned/Bikeshare Ridership (2017 Q3).csv.gz")
    assert select_date_formats(CONFIG, ctx) == CONFIG["date_formats"]


def test_city_without_overrides_uses_city_formats():
    config = {"date_formats": ["%Y-%m-%d %H:%M:%S"]}
    assert select_date_formats(config, _context("x.csv")) == config["date_formats"]
