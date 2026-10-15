from citybikeshare.utils.io_clean import normalize_newlines


def _normalize(tmp_path, data: bytes) -> bytes:
    f = tmp_path / "trips.csv"
    f.write_bytes(data)
    normalize_newlines(f, {})
    return f.read_bytes()


def test_doubled_carriage_return_does_not_add_blank_lines(tmp_path):
    # Mexico City 2020-12 ends every line with \r\r\n.
    assert _normalize(tmp_path, b"a,b\r\r\n1,2\r\r\n") == b"a,b\n1,2\n"


def test_crlf_and_lone_cr_become_lf(tmp_path):
    assert _normalize(tmp_path, b"a,b\r\n1,2\r3,4\n") == b"a,b\n1,2\n3,4\n"
