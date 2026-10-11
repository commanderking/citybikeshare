import pytest

from citybikeshare.utils.io_clean import (
    assert_uniform_field_count,
    clean_rosario_files,
    normalize_delimiters,
)


def test_rosario_2021_both_layouts_become_six_fields(tmp_path):
    # The source switches from `\t"\t`-separated rows to plain tabs on 2021-06-01.
    raw = (
        'Id\t"\tOrigen"\t"\tFecha Origen"\t"\tDestino"\t"\tFecha Destino"\t"\tTransbordo"\r\n'
        '1\t"\tPaseo Pellegrini"\t"\t2021-05-31 23:57:16"\t"\tParque II"\t"\t2021-06-01 00:13:53"\t"\t-"\r\n'
        "2\tEstadio Municipal\t2021-06-01 00:00\tPlaza San Mart\xedn\t2021-06-01 00:14\t-\r\n"
    )
    csv_file = tmp_path / "Mi bici tu bici 2021.csv"
    csv_file.write_bytes(raw.encode("latin1"))

    clean_rosario_files(csv_file, {})
    normalize_delimiters(csv_file, {})
    assert_uniform_field_count(csv_file, {})

    lines = csv_file.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "Id,Origen,Fecha Origen,Destino,Fecha Destino,Transbordo"
    assert lines[2] == "2,Estadio Municipal,2021-06-01 00:00,Plaza San Martín,2021-06-01 00:14,-"


def test_assert_uniform_field_count_raises_on_short_row(tmp_path):
    csv_file = tmp_path / "x.csv"
    csv_file.write_text("a,b,c\n1,2,3\n4,5\n")
    with pytest.raises(ValueError, match="1 row"):
        assert_uniform_field_count(csv_file, {})


def test_assert_uniform_field_count_respects_quoted_commas(tmp_path):
    csv_file = tmp_path / "x.csv"
    csv_file.write_text('a,b\n"x, y",2\n')
    assert_uniform_field_count(csv_file, {})
