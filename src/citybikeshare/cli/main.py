#!/usr/bin/env python3
"""
Unified CLI for CityBikeshare ETL pipeline.

Usage examples:
    poetry run pipeline vancouver
    pipenv run clean seoul
"""

from pathlib import Path
import typer
from src.citybikeshare.etl.download import download_city_data
from src.citybikeshare.etl.extract import extract_city_data
from src.citybikeshare.etl.clean import clean_city_data
from src.citybikeshare.etl.transform import transform_city_data
from src.citybikeshare.context import PipelineContext
from src.citybikeshare.analysis.summarize import summarize_city
from src.citybikeshare.analysis.merge_summaries import merge_city_summaries
from src.citybikeshare.analysis.generate_duration_buckets import (
    generate_duration_buckets,
)
from src.citybikeshare.analysis.merge_duration_buckets import merge_duration_buckets
from src.citybikeshare.etl.inspect import analyze_headers

app = typer.Typer(help="Unified CLI for the CityBikeshare ETL pipeline")


# --------------------------------------------------
# Individual commands (same behavior as before)
# --------------------------------------------------


def build_context(city: str) -> PipelineContext:
    data_root = Path("data")
    city_dir = data_root / city
    for sub in ["download", "raw", "metadata", "parquet"]:
        (city_dir / sub).mkdir(parents=True, exist_ok=True)

    return PipelineContext(
        city=city,
        data_root=Path("data"),
        transformed_root=Path("output"),
        analysis_root=Path("analysis"),
    )


@app.command()
def sync(
    city: str = typer.Argument(..., help="City name (e.g. montreal, boston, seoul)"),
):
    """Download or update raw bikeshare data."""
    context = build_context(city)
    typer.echo(f"🌐 Syncing data for {city}")
    download_city_data(context)
    typer.secho(f"✅ Successfully synced data for {city}", fg=typer.colors.GREEN)


@app.command()
def extract(
    city: str = typer.Argument(..., help="City name (e.g. montreal, boston, seoul)"),
    overwrite: bool = typer.Option(
        False, "--overwrite", "-o", help="Clear old extracted files first?"
    ),
):
    """Extract ZIP or CSV files for the given city."""
    typer.echo(f"📦 Extracting files for {city}")
    context = build_context(city)

    csv_files = extract_city_data(context, overwrite=overwrite)
    typer.secho(
        f"✅ Extracted {len(csv_files)} CSV files for {city}", fg=typer.colors.GREEN
    )


@app.command()
def clean(
    city: str = typer.Argument(..., help="City name (e.g. montreal, taipei, boston)"),
):
    """Clean and normalize raw city data."""
    context = build_context(city)
    typer.echo(f"🧼 Cleaning data for {city}")
    clean_city_data(context)
    typer.secho(f"✅ Cleaning complete for {city}", fg=typer.colors.GREEN)


@app.command()
def inspect(
    city: str = typer.Argument(..., help="City name (e.g. montreal, taipei, boston)"),
):
    """Inspect csv files for headers"""
    context = build_context(city)
    typer.echo(f"🧼 Inspecting {city} files for headers")
    analyze_headers(context)
    typer.secho(f"✅ Cleaning complete for {city}", fg=typer.colors.GREEN)


@app.command()
def transform(
    city: str = typer.Argument(..., help="City name (e.g. montreal, taipei, boston)"),
):
    """Combine and standardize all cleaned CSVs."""
    context = build_context(city)
    typer.echo(f"🔧 Transforming data for {city}")
    transform_city_data(context)
    typer.secho(f"✅ Transform complete for {city}", fg=typer.colors.GREEN)


@app.command()
def analyze(
    city: str = typer.Argument(..., help="City name to summarize"),
):
    """
    Analyze transformed Parquet data and generate per-year summary JSON.
    """
    context = build_context(city)
    summarize_city(context)
    generate_duration_buckets(context)


@app.command()
def analyze_all(
    duration_buckets: bool = typer.Option(
        False,
        "--duration_buckets",
        help="Only generate duration bucket analysis",
    ),
):
    for city_dir in (Path("output")).iterdir():
        if not city_dir.is_dir():
            continue
        context = build_context(city_dir.name)

        if duration_buckets:
            # Only run duration buckets if flag passed
            generate_duration_buckets(context)
        else:
            # Default: run both
            summarize_city(context)
            generate_duration_buckets(context)


@app.command()
def merge_summaries():
    analysis_folder = Path("analysis")
    merge_city_summaries(analysis_folder)
    merge_duration_buckets(analysis_folder)


# --------------------------------------------------
# Pipeline command that chains all steps
# --------------------------------------------------


@app.command()
def pipeline(
    city: str = typer.Argument(..., help="City name (e.g. montreal, seoul, taipei)"),
    skip_sync: bool = typer.Option(False, help="Skip sync step"),
):
    """Run the full pipeline: sync → extract → clean → transform."""
    typer.echo(f"🚴 Starting full pipeline for {city}")

    if not skip_sync:
        typer.echo("Syncing")
        sync(city)
    else:
        print("Skipping sync")

    typer.echo("Step 2: Extract")
    extract(city)  #

    typer.echo("Step 3: Clean")
    clean(city)

    typer.echo("Step 4: Transform")
    transform(city)

    typer.secho(f"✅ Pipeline complete for {city}", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()
