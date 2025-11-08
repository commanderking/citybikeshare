import polars as pl


def append_duration_column(lf):
    """Adds duration column if needed"""
    columns = lf.collect_schema().names()
    has_duration = "duration" in columns

    if not has_duration and {"start_time", "end_time"}.issubset(columns):
        lf = lf.with_columns(
            (pl.col("end_time") - pl.col("start_time"))
            .dt.total_seconds()
            .alias("duration")
        ).filter(pl.col("end_time") - pl.col("start_time") > 0)

    return lf
