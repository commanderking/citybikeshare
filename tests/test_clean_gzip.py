import gzip

from citybikeshare.context import PipelineContext
from citybikeshare.etl.transform import _parquet_name
from citybikeshare.utils.io_clean import stream_clean_to_gzip
from citybikeshare.utils.paths import get_csv_files

SEOUL_PIPELINE = ["encode_utf8", "clean_seoul_files"]
SEOUL_CFG = {"cleaning_options": {"source_encoding": "EUC-KR"}}


def _read_gz(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return f.read()


class TestDropUnbalancedQuotes:
    def test_drops_only_odd_quote_rows(self, tmp_path):
        raw = tmp_path / "정보_2021.03.csv"
        # row 2 has a stray quote (odd count) -> must be dropped; others kept
        raw.write_text(
            '"a","b","c"\n'
            '"SPB-1","ok","0"\n'
            '"SPB-2","교", ""0"\n'  # corrupt: unbalanced quotes
            '"SPB-3","ok","0"\n',
            encoding="utf-8",
        )
        out = tmp_path / "정보_2021.03.csv.gz"
        stream_clean_to_gzip(
            raw, out, ["drop_unbalanced_quote_lines"], SEOUL_CFG
        )
        lines = _read_gz(out).splitlines()
        assert len(lines) == 3  # header + 2 good rows
        assert all(line.count('"') % 2 == 0 for line in lines)
        assert "SPB-2" not in _read_gz(out)


class TestPrependHeader:
    def test_prepends_header_for_known_headerless_file(self, tmp_path):
        raw = tmp_path / "서울특별시 공공자전거 대여정보_201812.csv"
        raw.write_text("SPB-1,2018-12-01 00:00:16,01135,강서구의회\n", encoding="utf-8")
        out = tmp_path / "대여정보_201812.csv.gz"
        stream_clean_to_gzip(raw, out, ["seoul_prepend_header"], SEOUL_CFG)
        text = _read_gz(out)
        assert text.startswith("자전거번호,대여일시,")
        assert "SPB-1" in text

    def test_no_header_for_normal_file(self, tmp_path):
        raw = tmp_path / "정보_2021.01.csv"
        raw.write_text("자전거번호,대여일시\nSPB-1,2021-01-01 00:00:00\n", encoding="utf-8")
        out = tmp_path / "정보_2021.01.csv.gz"
        stream_clean_to_gzip(raw, out, ["seoul_prepend_header"], SEOUL_CFG)
        # unchanged: no extra header prepended
        assert _read_gz(out).count("자전거번호") == 1


class TestStreamCleanToGzip:
    def test_applies_seoul_line_fix_and_compresses(self, tmp_path):
        raw = tmp_path / "공공자전거 대여이력_2306.csv"
        raw.write_text("date\n2323-06-23\nfine\n", encoding="utf-8")
        out = tmp_path / "공공자전거 대여이력_2306.csv.gz"

        stream_clean_to_gzip(raw, out, SEOUL_PIPELINE, SEOUL_CFG)

        text = _read_gz(out)
        assert "2023-06-23" in text and "2323-06-23" not in text

    def test_decodes_euc_kr_source_to_utf8(self, tmp_path):
        # Korean headers stored as EUC-KR (as Seoul's raw files are) must come out UTF-8.
        raw = tmp_path / "대여이력_2019.csv"
        raw.write_bytes("자전거번호,대여일시\n1,2019-01-01 00:00:00\n".encode("euc-kr"))
        out = tmp_path / "대여이력_2019.csv.gz"

        stream_clean_to_gzip(raw, out, SEOUL_PIPELINE, SEOUL_CFG)

        assert "자전거번호" in _read_gz(out)

    def test_output_is_smaller_than_raw(self, tmp_path):
        raw = tmp_path / "대여이력_2019.csv"
        raw.write_text("a,b\n" + "1,2019-01-01 00:00:00\n" * 5000, encoding="utf-8")
        out = tmp_path / "대여이력_2019.csv.gz"

        stream_clean_to_gzip(raw, out, SEOUL_PIPELINE, SEOUL_CFG)

        assert out.stat().st_size < raw.stat().st_size


class TestTransformReadsGzip:
    def test_parquet_name_handles_csv_gz(self):
        assert _parquet_name("/x/대여이력_2019.csv.gz") == "대여이력_2019.parquet"
        assert _parquet_name("/x/foo.csv") == "foo.parquet"

    def test_get_csv_files_picks_up_gz_and_plain(self, tmp_path):
        (tmp_path / "a.csv").write_text("x")
        (tmp_path / "b.csv.gz").write_bytes(b"x")
        (tmp_path / "ignore.txt").write_text("x")
        found = {p.rsplit("/", 1)[-1] for p in get_csv_files(tmp_path)}
        assert found == {"a.csv", "b.csv.gz"}

    def test_transform_input_directory_recognizes_gz(self, tmp_path):
        ctx = PipelineContext(
            city="seoul",
            data_root=tmp_path / "data",
            transformed_root=tmp_path / "output",
            analysis_root=tmp_path / "analysis",
        )
        ctx.cleaned_directory.mkdir(parents=True)
        (ctx.cleaned_directory / "x.csv.gz").write_bytes(b"x")
        assert ctx.transform_input_directory == ctx.cleaned_directory
