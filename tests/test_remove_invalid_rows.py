import polars as pl

from citybikeshare.etl.pipelines.common import remove_invalid_rows


def _run(rows, invalid_values):
    lf = pl.LazyFrame(rows)
    return remove_invalid_rows(lf, invalid_values).collect()


def test_drops_rows_matching_curated_values():
    out = _run(
        {"duration": ["00:05:00", "17520:06:", "00:10:00", "87648:04:"]},
        {"duration": ["17520:06:", "87648:04:"]},
    )
    assert out["duration"].to_list() == ["00:05:00", "00:10:00"]


def test_no_config_is_a_no_op():
    rows = {"duration": ["00:05:00", "17520:06:"]}
    assert _run(rows, {}).height == 2
    assert _run(rows, {"duration": []}).height == 2


def test_only_drops_on_the_named_column():
    # A value listed for 'duration' must not remove a row that merely has the same
    # string elsewhere.
    out = _run(
        {"duration": ["00:05:00"], "note": ["17520:06:"]},
        {"duration": ["17520:06:"]},
    )
    assert out.height == 1


def test_logs_what_it_drops(capsys):
    _run({"duration": ["17520:06:", "17520:06:"]}, {"duration": ["17520:06:"]})
    out = capsys.readouterr().out
    assert "remove_invalid_rows" in out
    assert "17520:06:" in out
