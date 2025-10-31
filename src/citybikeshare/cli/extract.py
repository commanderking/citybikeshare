#!/usr/bin/env python3
"""
CLI command for extracting downloaded bikeshare data by city.

Usage:
    pipenv run extract_new <city_name>

Examples:
    pipenv run extract_new montreal
    pipenv run extract_new boston --overwrite
"""

import typer
from src.citybikeshare.etl.extract import extract_city_data

app = typer.Typer(help="Extract bikeshare ZIP and CSV files for a given city.")


@app.command()
def main(
    city: str = typer.Argument(
        ..., help="Name of the city (e.g. montreal, taipei, boston)"
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        "-o",
        help="Whether to clear out existing extracted files before running.",
    ),
):
    """
    Extract ZIP or CSV files for the given city into its /data/<city>/raw folder.
    """
    typer.echo(f"📦 Starting extract for city: {city}")
    csv_files = extract_city_data(city, overwrite=overwrite)
    typer.secho(
        f"✅ Extract complete! {len(csv_files)} CSV files extracted.",
        fg=typer.colors.GREEN,
    )


if __name__ == "__main__":
    app()
