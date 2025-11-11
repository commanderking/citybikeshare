import polars as pl
import json
from src.citybikeshare.context import PipelineContext
from src.citybikeshare.analysis.utils import append_duration_column


def generate_duration_buckets(context: PipelineContext):
    input_directory = context.transformed_directory
    city = context.city

    lf = pl.scan_parquet(input_directory / "**/*.parquet")

    bucket_size = 1 * 60  # seconds per bucket
    max_duration = 120 * 60  # cap point in seconds
    buckets = (
        lf.pipe(append_duration_column)
        .with_columns(
            ### Capping duration at 120 minutes, anything after goes in the 120 bucket
            [
                pl.when(pl.col("duration") > max_duration)
                .then(max_duration)
                .otherwise(pl.col("duration"))
                .alias("duration_capped"),
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
        ## Because cities like Vancouver don't go through start_date, end_date cleaning, filter years that are null for this analysis
        .filter(pl.col("year").is_not_null())
        .group_by(["year", "bucket"])
        .agg(pl.count().alias("count"))
        .sort(["year", "bucket"])
        .with_columns(pl.lit(city).alias("city"))
        .collect()
        .select(["bucket", "count", "year", "city"])
        .to_dicts()
    )

    analysis_directory = context.analysis_directory
    analysis_directory.mkdir(parents=True, exist_ok=True)
    output_file = analysis_directory / f"duration_buckets.json"
    with open(output_file, "w") as f:
        json.dump(buckets, f, indent=2)

    print(f"✅ Generated Duration Buckets for {city}!")
