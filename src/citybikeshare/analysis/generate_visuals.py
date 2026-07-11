import polars as pl
from citybikeshare.context import PipelineContext
from citybikeshare.analysis.utils import derive_duration_column
from citybikeshare.utils.io import write_json

# Duration band edges (seconds) and labels. Edges are right-closed by polars `cut`
# (default), so e.g. exactly 300s falls in "<5".
_DURATION_BREAKS = [300, 900, 1800, 3600]
_DURATION_LABELS = ["<5", "5-15", "15-30", "30-60", "60+"]


def generate_visuals(context: PipelineContext):
    """Chart-ready aggregations for the client dashboard, written to ``visuals.json``.

    Every section is a count/sum aggregation (no quantiles), so the whole thing runs
    under the streaming engine and is safe even for the largest cities. Time-based
    sections are keyed by ``year`` so the client can show change over time; an all-time
    view is just the client summing across years.

    Sections:
      - volume_by_month   : (year, month) -> trips
      - by_hour           : (year, hour 0-23) -> trips
      - by_dow            : (year, dow 1=Mon..7=Sun) -> trips
      - stations          : (year, station) -> start_trips, end_trips, total_trips
                            (subsumes top-N and the active-station count)
      - round_trip_share  : (year) -> trips, round_trips, round_trip_share
      - duration_bands    : (year, band) -> trips, share (within year)
    """
    print(f"Generating visuals for: {context.city}")
    input_directory = context.transformed_directory
    analysis_directory = context.analysis_directory

    lf = pl.scan_parquet(input_directory / "**/*.parquet")
    if "start_time" not in lf.collect_schema().names():
        raise ValueError(
            f"{context.city}: missing 'start_time' column in transformed data"
        )

    lf = (
        lf.pipe(derive_duration_column)
        .with_columns(
            [
                pl.col("year").cast(pl.Int32),
                pl.col("month").cast(pl.Int32),
                pl.col("start_time").dt.hour().alias("hour"),
                pl.col("start_time").dt.weekday().alias("dow"),  # 1=Mon … 7=Sun
            ]
        )
        # Rows with a null start_time land in a null year/month partition and can't sit
        # on a timeline — drop them here (same choice as generate_duration_buckets).
        .filter(pl.col("year").is_not_null())
    )

    valid_dur = (pl.col("end_time") - pl.col("start_time") >= 0) & pl.col(
        "duration"
    ).is_not_null()

    volume_by_month = (
        lf.group_by("year", "month").agg(pl.len().alias("trips")).sort("year", "month")
    )
    by_hour = (
        lf.group_by("year", "hour").agg(pl.len().alias("trips")).sort("year", "hour")
    )
    by_dow = lf.group_by("year", "dow").agg(pl.len().alias("trips")).sort("year", "dow")

    # Per-station departures and arrivals. Full join keeps stations that only ever
    # appear on one side; total_trips = "either start or end" appearances.
    starts = lf.group_by("year", "start_station_name").agg(
        pl.len().alias("start_trips")
    )
    ends = lf.group_by("year", "end_station_name").agg(pl.len().alias("end_trips"))
    stations = (
        starts.join(
            ends,
            left_on=["year", "start_station_name"],
            right_on=["year", "end_station_name"],
            how="full",
            coalesce=True,
        )
        .rename({"start_station_name": "station"})
        .with_columns(
            [pl.col("start_trips").fill_null(0), pl.col("end_trips").fill_null(0)]
        )
        .with_columns(
            (pl.col("start_trips") + pl.col("end_trips")).alias("total_trips")
        )
        .filter(pl.col("station").is_not_null())
        .sort(["year", "total_trips"], descending=[False, True])
    )

    round_trip_share = (
        lf.group_by("year")
        .agg(
            [
                pl.len().alias("trips"),
                # `==` is null when either side is null; treat that as "not a round trip".
                (pl.col("start_station_name") == pl.col("end_station_name"))
                .fill_null(False)
                .sum()
                .alias("round_trips"),
            ]
        )
        .with_columns(
            (pl.col("round_trips") / pl.col("trips")).round(4).alias("round_trip_share")
        )
        .sort("year")
    )

    duration_bands = (
        lf.filter(valid_dur)
        .with_columns(
            pl.col("duration")
            .cut(_DURATION_BREAKS, labels=_DURATION_LABELS)
            .alias("band")
        )
        .group_by("year", "band")
        .agg(pl.len().alias("trips"))
        .with_columns(
            (pl.col("trips") / pl.col("trips").sum().over("year"))
            .round(4)
            .alias("share")
        )
        .with_columns(pl.col("band").cast(pl.String))
        .sort("year")
    )

    section_frames = {
        "volume_by_month": volume_by_month,
        "by_hour": by_hour,
        "by_dow": by_dow,
        "stations": stations,
        "round_trip_share": round_trip_share,
        "duration_bands": duration_bands,
    }
    # collect_all runs all sections as one optimized job; common-subplan elimination
    # shares the single parquet scan across them.
    collected = pl.collect_all(list(section_frames.values()), engine="streaming")
    sections = {name: df.to_dicts() for name, df in zip(section_frames, collected)}

    # `cut` yields a categorical sorted by physical order, so impose the intended band
    # order explicitly for stable, readable output.
    band_order = {label: i for i, label in enumerate(_DURATION_LABELS)}
    sections["duration_bands"].sort(
        key=lambda r: (r["year"], band_order.get(r["band"], 99))
    )

    # sort_keys for stable diffs in the committed snapshot; write_json keeps non-ASCII
    # station names (Korean, accented Spanish) as readable UTF-8.
    output_file = analysis_directory / "visuals.json"
    write_json(output_file, sections, sort_keys=True)

    print(f"✅ Wrote visuals for {context.city} to {output_file}")
