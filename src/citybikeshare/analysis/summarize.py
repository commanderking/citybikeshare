import json
import polars as pl
from src.citybikeshare.context import PipelineContext


def summarize_city(context: PipelineContext):
    """
    Summarize transformed Parquet data by year for a single city.
    """

    print(f"Preparing summary for: {context.city}")

    input_directory = context.transformed_directory
    analysis_directory = context.analysis_directory

    # Read all parquet files
    lf = pl.scan_parquet(input_directory / "**/*.parquet")
    columns = lf.collect_schema().names()
    # Ensure a datetime column exists
    if "start_time" not in lf.collect_schema().names():
        raise ValueError(
            f"{context.city}: missing 'start_time' column in transformed data"
        )

    # Determine duration column
    has_duration = "duration" in columns

    # Add a duration_in_seconds column if needed
    if not has_duration and {"start_time", "end_time"}.issubset(columns):
        lf = lf.with_columns(
            (pl.col("end_time") - pl.col("start_time"))
            .dt.total_seconds()
            .alias("duration_seconds")
        )
        duration_col = "duration_seconds"
    else:
        duration_col = "duration"  # assuming already in seconds

    summary = (
        lf.group_by("year")
        .agg(
            [
                pl.count().alias("trip_count"),
                pl.col(duration_col).median().alias("median_duration"),
                pl.col(duration_col).quantile(0.25).alias("first_quantile_duration"),
                pl.col(duration_col).quantile(0.75).alias("third_quantile_duration"),
                # Counts if null is any column of row
                pl.sum_horizontal(
                    [pl.col(c).is_null().cast(pl.Int64) for c in lf.columns]
                )
                .sum()
                .alias("null_rows"),
            ]
        )
        .sort("year")
        .collect()
        .to_dicts()
    )

    analysis_directory = context.analysis_directory
    analysis_directory.mkdir(parents=True, exist_ok=True)

    # Write JSON output
    output_file = analysis_directory / f"summary.json"
    with open(output_file, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"✅ Wrote summary for {context.city} to {output_file}")
