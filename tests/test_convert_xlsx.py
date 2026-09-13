import gzip
import zipfile
from xml.sax.saxutils import escape

import pytest

from citybikeshare.context import PipelineContext
from citybikeshare.etl import clean as clean_mod
from citybikeshare.utils.io_clean import convert_xlsx_sheets

# Excel date serial for 2016-01-10 00:00 — how a Q4 "10/01/2016" (d/m) cell ended up stored
# after Excel re-parsed it as m/d.
SWAPPED_SERIAL = 42379.0

_STYLES = (
    '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
    "<fonts count='1'><font/></fonts><fills count='1'><fill/></fills>"
    "<borders count='1'><border/></borders>"
    '<cellXfs count="2"><xf numFmtId="0"/><xf numFmtId="22" applyNumberFormat="1"/></cellXfs>'
    "</styleSheet>"
)


def _cell(ref, value):
    """A cell element: text → inline string; ('date', serial) → numeric with the date style."""
    if isinstance(value, tuple) and value[0] == "date":
        return f'<c r="{ref}" s="1"><v>{value[1]}</v></c>'
    if isinstance(value, (int, float)):
        return f'<c r="{ref}"><v>{value}</v></c>'
    return f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'


def write_xlsx(path, sheets: dict):
    """Write a minimal OOXML workbook: {sheet name: [row, row, ...]}, each row a list of cells."""
    ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    rel_ns = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(
            "[Content_Types].xml",
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
            + "".join(
                f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                for i in range(1, len(sheets) + 1)
            )
            + "</Types>",
        )
        z.writestr(
            "_rels/.rels",
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
            "</Relationships>",
        )
        z.writestr(
            "xl/workbook.xml",
            f'<workbook xmlns="{ns}" xmlns:r="{rel_ns}"><sheets>'
            + "".join(
                f'<sheet name="{escape(name)}" sheetId="{i}" r:id="rId{i}"/>'
                for i, name in enumerate(sheets, 1)
            )
            + "</sheets></workbook>",
        )
        z.writestr(
            "xl/_rels/workbook.xml.rels",
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            + "".join(
                f'<Relationship Id="rId{i}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{i}.xml"/>'
                for i in range(1, len(sheets) + 1)
            )
            + f'<Relationship Id="rId{len(sheets) + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
            "</Relationships>",
        )
        z.writestr("xl/styles.xml", _STYLES)
        for i, rows in enumerate(sheets.values(), 1):
            body = "".join(
                f'<row r="{r}">'
                + "".join(_cell(f"{chr(65 + c)}{r}", v) for c, v in enumerate(row))
                + "</row>"
                for r, row in enumerate(rows, 1)
            )
            z.writestr(
                f"xl/worksheets/sheet{i}.xml",
                f'<worksheet xmlns="{ns}"><sheetData>{body}</sheetData></worksheet>',
            )


HEADER = ["trip_id", "trip_start_time", "trip_stop_time", "user_type"]
Q4_SHEET = {
    "date_columns": ["trip_start_time", "trip_stop_time"],
    "text_date_format": "%d/%m/%Y %H:%M",
    "serial_dates_day_month_swapped": True,
    "expected_months": [10, 11, 12],
}


def _read_gz_lines(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return f.read().splitlines()


def _convert(tmp_path, sheets_data, sheets_cfg):
    raw = tmp_path / "book.xlsx"
    write_xlsx(raw, sheets_data)
    return convert_xlsx_sheets(
        raw, tmp_path / "book.csv.gz", {}, [], options={"sheets": sheets_cfg}
    )


class TestConvertXlsxSheets:
    def test_unswaps_serial_dates_and_parses_text_dates_to_one_format(self, tmp_path):
        rows = [
            HEADER,
            [462305, ("date", SWAPPED_SERIAL), ("date", SWAPPED_SERIAL + 0.005), "Casual"],
            [462306, "13/10/2016 0:05", "13/10/2016 0:20", "Member"],
        ]
        (out,) = _convert(tmp_path, {"tor_trips_2016_Q4": rows}, {"tor_trips_2016_Q4": Q4_SHEET})

        assert out.name == "book__tor_trips_2016_Q4.csv.gz"
        lines = _read_gz_lines(out)
        assert lines[1] == "462305,2016-10-01 00:00:00,2016-10-01 00:07:12,Casual"
        assert lines[2] == "462306,2016-10-13 00:05:00,2016-10-13 00:20:00,Member"

    def test_sheet_name_slug_strips_trailing_space(self, tmp_path):
        rows = [HEADER, [1, ("date", 42560.0), ("date", 42560.0), "Member"]]
        cfg = {"tor_trips_2016_Q3 ": {"date_columns": ["trip_start_time", "trip_stop_time"]}}
        (out,) = _convert(tmp_path, {"tor_trips_2016_Q3 ": rows}, cfg)
        assert out.name == "book__tor_trips_2016_Q3.csv.gz"
        assert _read_gz_lines(out)[1].startswith("1,2016-07-09 00:00:00,")

    def test_reads_every_cell_as_text(self, tmp_path):
        # trip_id must not come back as a float ("53279.0"): polars' Excel default would.
        rows = [HEADER, [53279, "x", "y", "Member"]]
        (out,) = _convert(tmp_path, {"s": rows}, {"s": {}})
        assert _read_gz_lines(out)[1] == "53279,x,y,Member"

    def test_undeclared_or_missing_sheet_raises(self, tmp_path):
        rows = [HEADER]
        with pytest.raises(ValueError, match="not in config: \\['extra'\\]"):
            _convert(tmp_path, {"s": rows, "extra": rows}, {"s": {}})
        with pytest.raises(ValueError, match="not in workbook: \\['typo'\\]"):
            _convert(tmp_path, {"s": rows}, {"typo": {}})

    def test_text_date_matching_no_format_raises(self, tmp_path):
        rows = [HEADER, [1, "Oct 13 2016", "Oct 13 2016", "Member"]]
        with pytest.raises(ValueError, match="could not be parsed.*Oct 13 2016"):
            _convert(tmp_path, {"q4": rows}, {"q4": Q4_SHEET})

    def test_wrong_swap_assumption_trips_expected_months(self, tmp_path):
        # A genuine 2016-01-10 serial, un-swapped, would land in October; declaring
        # expected_months for Q1 catches a swap that shouldn't have been configured.
        rows = [HEADER, [1, ("date", SWAPPED_SERIAL), ("date", SWAPPED_SERIAL), "Member"]]
        cfg = {"q1": {**Q4_SHEET, "expected_months": [1, 2, 3]}}
        with pytest.raises(ValueError, match="months \\[10\\] outside expected"):
            _convert(tmp_path, {"q1": rows}, cfg)

    def test_serial_with_day_over_12_cannot_be_swapped(self, tmp_path):
        rows = [HEADER, [1, ("date", 42656.0), ("date", 42656.0), "Member"]]  # 2016-10-13
        with pytest.raises(ValueError, match="day > 12"):
            _convert(tmp_path, {"q4": rows}, {"q4": Q4_SHEET})


class TestCleanStageXlsxInputs:
    def _ctx(self, tmp_path):
        ctx = PipelineContext(
            city="toronto",
            data_root=tmp_path / "data",
            transformed_root=tmp_path / "output",
            analysis_root=tmp_path / "analysis",
        )
        ctx.raw_directory.mkdir(parents=True)
        return ctx

    def _config(self, monkeypatch, cfg):
        monkeypatch.setattr(clean_mod, "load_city_config", lambda city: cfg)

    def test_unclaimed_xlsx_raises(self, tmp_path, monkeypatch):
        ctx = self._ctx(tmp_path)
        write_xlsx(ctx.raw_directory / "stray.xlsx", {"s": [HEADER]})
        self._config(monkeypatch, {"clean_pipeline": ["normalize_newlines"]})
        with pytest.raises(ValueError, match="no converter step targets: \\['stray.xlsx'\\]"):
            clean_mod.clean_city_data(ctx)

    def test_ignored_xlsx_is_skipped_and_targeted_one_converted(self, tmp_path, monkeypatch):
        ctx = self._ctx(tmp_path)
        write_xlsx(ctx.raw_directory / "readme.xlsx", {"s": [HEADER]})
        rows = [HEADER, [1, ("date", SWAPPED_SERIAL), ("date", SWAPPED_SERIAL), "Member"]]
        write_xlsx(tmp_path / "trips.xlsx", {"q4": rows})
        with open(tmp_path / "trips.xlsx", "rb") as f, gzip.open(
            ctx.raw_directory / "trips.xlsx.gz", "wb"
        ) as g:
            g.write(f.read())
        self._config(
            monkeypatch,
            {
                "clean_pipeline": [
                    {
                        "step": "convert_xlsx_sheets",
                        "files": ["trips.xlsx.gz"],
                        "sheets": {"q4": Q4_SHEET},
                    }
                ],
                "compress_cleaned": True,
                "ignored_raw_files": ["readme.xlsx"],
            },
        )

        clean_mod.clean_city_data(ctx)

        assert sorted(p.name for p in ctx.cleaned_directory.iterdir()) == ["trips__q4.csv.gz"]
        assert "2016-10-01 00:00:00" in _read_gz_lines(ctx.cleaned_directory / "trips__q4.csv.gz")[1]
