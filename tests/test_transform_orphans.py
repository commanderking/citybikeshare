from pathlib import Path

from citybikeshare.context import PipelineContext
from citybikeshare.etl.transform import _remove_orphan_parquets


def _ctx(tmp_path):
    ctx = PipelineContext(
        city="x",
        data_root=tmp_path / "data",
        transformed_root=tmp_path / "output",
        analysis_root=tmp_path / "analysis",
    )
    ctx.parquet_directory.mkdir(parents=True)
    return ctx


def test_removes_true_orphan(tmp_path):
    ctx = _ctx(tmp_path)
    (ctx.parquet_directory / "old.parquet").write_bytes(b"x")
    (ctx.parquet_directory / "keep.parquet").write_bytes(b"x")
    _remove_orphan_parquets(ctx, {"keep.parquet"})
    assert sorted(p.name for p in ctx.parquet_directory.iterdir()) == ["keep.parquet"]


def test_keeps_case_variant_that_is_the_same_file(tmp_path):
    # Simulate a case-only rename: the directory entry is `2019-q3.parquet` but the expected
    # name is `2019-Q3.parquet`. On a case-insensitive FS they're one file and must survive.
    ctx = _ctx(tmp_path)
    old = ctx.parquet_directory / "2019-q3.parquet"
    old.write_bytes(b"x")
    new = ctx.parquet_directory / "2019-Q3.parquet"
    case_insensitive = new.exists()
    _remove_orphan_parquets(ctx, {"2019-Q3.parquet"})
    if case_insensitive:
        assert new.exists() and old.exists()  # same file, untouched
    else:
        assert not old.exists()  # case-sensitive FS: genuinely a different, orphaned file
