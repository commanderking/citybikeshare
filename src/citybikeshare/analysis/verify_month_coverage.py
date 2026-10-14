import calendar

import polars as pl

from citybikeshare.config.loader import load_city_config
from citybikeshare.context import PipelineContext

# A month is incomplete when this many of its days have no trips at all. The worst genuine
# weather gap seen across the cities is 4 days (the Jan 2016 blizzard in NYC/DC/Philadelphia);
# the smallest real data gap is 6 (Toronto 2017's day/month swap), then 7 (London's week 49).
MIN_EMPTY_DAYS = 5


def verify_month_coverage(context: PipelineContext) -> list[str]:
    """Fail when a month inside the city's data span is missing or has
    MIN_EMPTY_DAYS+ days without trips, unless the city's config lists it.

    Two config keys allow a month: `expected_gaps` for permanent, genuine gaps (seasonal
    closures, shutdowns, months the source never published) and `known_gaps` for defects not
    yet fixed. Returns the known gaps so callers can keep them visible. The first and last
    months are exempt, being naturally partial.
    """
    config = load_city_config(context.city)
    days_by_month = _count_trip_days_by_month(context)
    incomplete = _find_incomplete_months(days_by_month)

    expected = _expand_months(config.get("expected_gaps", []))
    known = _expand_months(config.get("known_gaps", []))
    _assert_months_complete(context.city, incomplete, expected | known)
    _assert_known_gaps_current(context.city, incomplete, known)
    return sorted(known)


def _count_trip_days_by_month(context: PipelineContext) -> dict[str, int]:
    """Map YYYY-MM to the number of distinct days in that month with at least one trip."""
    days = (
        pl.scan_parquet(context.transformed_directory / "**/*.parquet")
        .select(pl.col("start_time").dt.date().alias("day"))
        .drop_nulls()
        .unique()
        .group_by(pl.col("day").dt.strftime("%Y-%m").alias("month"))
        .len()
        .collect()
    )
    return dict(zip(days["month"], days["len"]))


def _find_incomplete_months(days_by_month: dict[str, int]) -> dict[str, int]:
    """Months strictly between the first and last data month with MIN_EMPTY_DAYS+ empty
    days, mapped to their empty-day count."""
    if not days_by_month:
        return {}
    first, last = min(days_by_month), max(days_by_month)
    incomplete = {}
    for month in _months_between(first, last)[1:-1]:
        year, mon = map(int, month.split("-"))
        empty = calendar.monthrange(year, mon)[1] - days_by_month.get(month, 0)
        if empty >= MIN_EMPTY_DAYS:
            incomplete[month] = empty
    return incomplete


def _expand_months(entries: list[str]) -> set[str]:
    """Expand config entries — "YYYY-MM" or an inclusive "YYYY-MM..YYYY-MM" range."""
    months = set()
    for entry in entries:
        start, _, end = str(entry).partition("..")
        months.update(_months_between(start, end or start))
    return months


def _months_between(start: str, end: str) -> list[str]:
    """Inclusive list of YYYY-MM months from start to end."""
    year, mon = map(int, start.split("-"))
    months = []
    while (month := f"{year}-{mon:02d}") <= end:
        months.append(month)
        year, mon = (year + 1, 1) if mon == 12 else (year, mon + 1)
    if not months:
        raise ValueError(f"Month range {start}..{end} is empty or reversed")
    return months


def _assert_months_complete(city, incomplete, allowed):
    unexplained = {m: n for m, n in incomplete.items() if m not in allowed}
    if unexplained:
        listing = ", ".join(f"{m} ({n} days empty)" for m, n in sorted(unexplained.items()))
        raise ValueError(
            f"{city}: incomplete months: {listing}. Find the missing data, or list the month "
            "under expected_gaps (genuine gap) or known_gaps (defect to fix) in the city config."
        )


def _assert_known_gaps_current(city, incomplete, known):
    """A known gap whose month is now complete means the defect was fixed; the stale entry
    must go so known_gaps only ever lists live defects."""
    fixed = sorted(m for m in known if m not in incomplete)
    if fixed:
        raise ValueError(
            f"{city}: known_gaps lists months that are now complete: {', '.join(fixed)}. "
            "Remove them from the city config."
        )
