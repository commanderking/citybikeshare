import polars as pl


def derive_duration_column(lf):
    """Add a `duration` column (seconds) from start/end times if not already present.

    Does **not** filter — the frame keeps every row, so callers that need data-quality
    metrics (total counts, null counts) still see the full, unfiltered data.
    """
    columns = lf.collect_schema().names()
    has_duration = "duration" in columns

    if not has_duration and {"start_time", "end_time"}.issubset(columns):
        lf = lf.with_columns(
            (pl.col("end_time") - pl.col("start_time"))
            .dt.total_seconds()
            .alias("duration")
        )

    return lf


def append_duration_column(lf):
    """Derive `duration` (if needed) and drop rows with null/negative duration.

    For duration-distribution analyses (e.g. duration buckets) that should only count
    valid trips. Summaries that report data-quality metrics should use
    `derive_duration_column` and restrict duration stats with a mask instead, so dropped
    rows are surfaced rather than silently removed.
    """
    lf = derive_duration_column(lf)
    return lf.filter(
        (pl.col("end_time") - pl.col("start_time") >= 0)
        & (pl.col("duration").is_not_null())
    )
