#!/usr/bin/env python3
"""
Unified CLI for CityBikeshare ETL pipeline.

Usage examples:
    poetry run pipeline vancouver
    pipenv run clean seoul
"""

from pathlib import Path
import typer
from citybikeshare.etl.download import download_city_data
from citybikeshare.etl.extract import extract_city_data
from citybikeshare.etl.clean import clean_city_data
from citybikeshare.etl.transform import transform_city_data
from citybikeshare.context import build_context
from citybikeshare.analysis.summarize import summarize_city
from citybikeshare.analysis.merge_summaries import merge_city_summaries
from citybikeshare.analysis.generate_duration_buckets import (
    generate_duration_buckets,
)
from citybikeshare.analysis.generate_visuals import generate_visuals
from citybikeshare.analysis.merge_duration_buckets import merge_duration_buckets
from citybikeshare.etl.inspect import analyze_headers
from citybikeshare.cli.transform_all import app as transform_all_app
from citybikeshare.cli.pipeline_all import run_pipeline_all

app = typer.Typer(help="Unified CLI for the CityBikeshare ETL pipeline")
app.add_typer(transform_all_app, name="transform-all")


# --------------------------------------------------
# Individual commands (same behavior as before)
# --------------------------------------------------


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
    typer.secho(f"✅ Finished inspecting headers for {city}", fg=typer.colors.GREEN)


@app.command()
def transform(
    city: str = typer.Argument(..., help="City name (e.g. montreal, taipei, boston)"),
    incremental: bool = typer.Option(
        True,
        "--incremental/--no-incremental",
        help="Skip inputs unchanged since the last run (default). Use --no-incremental to rebuild every file.",
    ),
):
    """Combine and standardize all cleaned CSVs."""
    context = build_context(city)
    typer.echo(f"🔧 Transforming data for {city}")
    transform_city_data(context, incremental=incremental)
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
    generate_visuals(context)


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
            # Default: run all per-city analyses
            summarize_city(context)
            generate_duration_buckets(context)
            generate_visuals(context)


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

    context = build_context(city)

    if not skip_sync:
        typer.echo("Step 1: Sync")
        download_city_data(context)
    else:
        print("Skipping sync")

    typer.echo("Step 2: Extract")
    extract_city_data(context)

    typer.echo("Step 3: Clean")
    clean_city_data(context)

    typer.echo("Step 4: Transform")
    transform_city_data(context)

    typer.secho(f"✅ Pipeline complete for {city}", fg=typer.colors.GREEN)


@app.command(name="pipeline-all")
def pipeline_all(
    skip_sync: bool = typer.Option(False, help="Skip the sync step for every city."),
    max_workers: int = typer.Option(
        4,
        "--max-workers",
        "-w",
        help="Number of cities to process in parallel.",
    ),
):
    """Run the full pipeline (sync → extract → clean → transform) for every city."""
    run_pipeline_all(skip_sync=skip_sync, max_workers=max_workers)


if __name__ == "__main__":
    app()
