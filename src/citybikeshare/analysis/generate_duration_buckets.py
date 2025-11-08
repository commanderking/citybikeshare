import polars as pl
import json
from src.citybikeshare.context import PipelineContext
from src.citybikeshare.analysis.utils import append_duration_column


def generate_duration_buckets(context: PipelineContext):
    input_directory = context.transformed_directory
    city = context.city

    lf = pl.scan_parquet(input_directory / "**/*.parquet")

    bucket_size = 3 * 60  # seconds per bin
    max_duration = 120 * 60  # cap point in seconds
    lf_buckets = (
        lf.pipe(append_duration_column)
        .with_columns(
            [
                pl.when(pl.col("duration") > max_duration)
                .then(max_duration)
                .otherwise(pl.col("duration"))
                .alias("duration_capped"),
                # use capped duration for bucketing
                (
                    (
                        pl.when(pl.col("duration") > max_duration)
                        .then(max_duration)
                        .otherwise(pl.col("duration"))
                        / bucket_size
                    ).floor()
                    * bucket_size
                )
                .alias("bucket")
                .cast(pl.Int32),
            ]
        )
        .group_by(["bucket", "year"])
        .agg(pl.count().alias("count"))
        .sort(["year", "bucket"])
        .with_columns(pl.lit(city).alias("city"))
        .collect()
        .select(["bucket", "count", "year", "city"])
        .to_dicts()
    )

    analysis_directory = context.analysis_directory
    analysis_directory.mkdir(parents=True, exist_ok=True)

    # Write JSON output

    # Export JSON (bucket_label instead of bucket)
    output_file = analysis_directory / f"duration_buckets.json"
    with open(output_file, "w") as f:
        json.dump(lf_buckets, f, indent=2)
