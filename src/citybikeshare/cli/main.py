#!/usr/bin/env python3
"""
Unified CLI for CityBikeshare ETL pipeline.

Usage examples:
    poetry run pipeline vancouver
    pipenv run clean seoul
"""

import typer
from src.citybikeshare.etl.download import download_city_data
from src.citybikeshare.etl.extract import extract_city_data
from src.citybikeshare.etl.clean import clean_city_data
from src.citybikeshare.etl.transform import transform_city_data


app = typer.Typer(help="Unified CLI for the CityBikeshare ETL pipeline")


class Args:
    """Common Args class reused across steps"""

    def __init__(self, city: str, no_write: bool = False, sample: int = 0):
        self.city = city
        self.no_write = no_write
        self.sample = sample


# --------------------------------------------------
# Individual commands (same behavior as before)
# --------------------------------------------------


@app.command()
def sync(
    city: str = typer.Argument(..., help="City name (e.g. montreal, boston, seoul)"),
):
    """Download or update raw bikeshare data."""
    typer.echo(f"🌐 Syncing data for {city}")
    download_city_data(city)
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
    csv_files = extract_city_data(city, overwrite=overwrite)
    typer.secho(
        f"✅ Extracted {len(csv_files)} CSV files for {city}", fg=typer.colors.GREEN
    )


@app.command()
def clean(
    city: str = typer.Argument(..., help="City name (e.g. montreal, taipei, boston)"),
):
    """Clean and normalize raw city data."""
    args = Args(city=city)
    typer.echo(f"🧼 Cleaning data for {city}")
    clean_city_data(args)
    typer.secho(f"✅ Cleaning complete for {city}", fg=typer.colors.GREEN)


@app.command()
def transform(
    city: str = typer.Argument(..., help="City name (e.g. montreal, taipei, boston)"),
):
    """Combine and standardize all cleaned CSVs."""
    args = Args(city=city)
    typer.echo(f"🔧 Transforming data for {city}")
    transform_city_data(args)
    typer.secho(f"✅ Transform complete for {city}", fg=typer.colors.GREEN)


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
        typer.echo("Step 1: Sync")
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
