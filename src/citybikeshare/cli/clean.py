#!/usr/bin/env python3
"""
CLI command for clean bikeshare data city

Usage:
    pipenv run clean <city_name>

Examples:
    pipenv run clean vancouver
    pipenv run clean seoul
"""

import typer
from src.citybikeshare.etl.clean import clean
from src.citybikeshare.cli.transform import Args

app = typer.Typer(help="Clean all csv files in folder")


@app.command()
def main(
    city: str = typer.Argument(
        ..., help="Name of the city (e.g. montreal, taipei, boston)"
    ),
):
    """
    Extract ZIP or CSV files for the given city into its /data/<city>/raw folder.
    """
    args = Args(city=city)
    typer.echo(f"📦 Starting to clean: {city}")
    clean(args)


if __name__ == "__main__":
    app()
