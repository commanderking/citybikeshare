import json
from datetime import timedelta
import polars as pl

from dateutil.parser import parse


def get_recent_year_df(date_column):
    """Returns all rows one year from the last date"""

    def inner(df):
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        max_date = df.select(pl.max(date_column)).to_series()[0]
        one_year_ago = max_date - timedelta(days=365)

        # Filter the DataFrame for the last year of data
        last_year_df = df.filter(pl.col(date_column) >= one_year_ago)

        return last_year_df

    return inner


def get_bookend_dates(df):
    # Use min and max to find the earliest start_time and latest end_time
    result = df.select(
        pl.col("start_time").min().alias("earliest_start_time"),
        pl.col("end_time").max().alias("latest_end_time"),
    )

    print(result)

    # Extract values from the resulting DataFrame
    earliest_start_time = result["earliest_start_time"][0].isoformat()
    latest_end_time = result["latest_end_time"][0].isoformat()
    return earliest_start_time, latest_end_time


def fill_missing_years(data, start_year, end_year):
    # Convert the list to a dictionary for quick lookup by year
    year_dict = {entry["year"]: entry["has_null"] for entry in data}

    # Generate the full range of years and fill in missing ones with has_null: 0
    filled_data = [
        {"year": year, "has_null": year_dict.get(year, 0)}
        for year in range(start_year, end_year + 1)
    ]

    return filled_data


def get_null_rows_by_year(df, **kwargs):
    headers = kwargs.get("null_headers", df.columns)
    start_time, end_time = get_bookend_dates(df)

    start_year = parse(start_time).year
    end_year = parse(end_time).year
    lazy_df = df.lazy()
    null_df = (
        lazy_df.select(headers)
        .filter(
            pl.any_horizontal(pl.all().is_null())  # Keep rows with any NULL values
        )
        .with_columns(
            pl.col("start_time").dt.year().alias("year"),
            pl.any_horizontal(pl.all().is_null()).alias("has_null"),
        )
        .groupby("year")
        .agg(pl.col("has_null").sum())
        .sort("year")
    )

    collected_null_df = null_df.collect()

    print(collected_null_df)

    total_null_rows = collected_null_df["has_null"].sum()

    print(total_null_rows)
    nulls_by_year = fill_missing_years(
        collected_null_df.to_dicts(), start_year, end_year
    )

    return nulls_by_year, total_null_rows


def log_final_results(df, args, **kwargs):
    """Print all rows that have NULL in at least one column"""

    city = args.city
    city_json = {}
    json_data = {}

    output_directory = get_output_directory()
    summary_path = output_directory / "system_statistics.json"

    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    try:
        with open(summary_path, "r") as f:
            json_data = json.load(f)
    except Exception as e:
        print(f"No logging file found, will create new one. Error: {e}")
    null_headers = kwargs.get("null_headers", df.columns)

    null_rows_by_year, total_null_rows = get_null_rows_by_year(
        df, null_headers=null_headers
    )
    for header in null_headers:
        null_count = df.select(pl.col(header).is_null().sum()).item()
        city_json[f"null_{header}"] = null_count
    city_json["null_by_year"] = null_rows_by_year

    first_trip, last_trip = get_bookend_dates(df)
    city_json = city_json | {
        "total_rows": df.height,
        "null_rows": total_null_rows,
        "percent_complete": 100 - round(((total_null_rows / df.height) * 100), 2),
        "first_trip": first_trip,
        "last_trip": last_trip,
    }

    json_data[city] = city_json
    with open(summary_path, "w") as f:
        json.dump(json_data, f, indent=4)

    return df


def print_null_data(df):
    df_null_rows = df.filter(pl.any_horizontal(pl.all().is_null()))
    print(df_null_rows)
    return df


def assess_null_data(df):
    headers = df.columns
    for header in headers:
        null_count = df.select(pl.col(header).is_null().sum()).item()
        if null_count != 0:
            print(f"{header} has {null_count} rows with null values")
    return df
