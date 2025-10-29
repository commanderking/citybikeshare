#!/usr/bin/env python3
"""
CLI for downloading bikeshare data by city.

Usage:
    pipenv run sync_new <city_name>

Examples:
    pipenv run sync_new montreal
    pipenv run sync_new boston
"""

import typer
from src.citybikeshare.etl.download import download_city_data

app = typer.Typer(help="Sync bikeshare data for a given city.")


@app.command()
def main(
    city: str = typer.Argument(
        ..., help="Name of the city (e.g. montreal, boston, nyc)"
    ),
):
    """
    Download raw bikeshare data for the specified city.
    """

    typer.echo(f"📦 Starting sync for city: {city}")
    download_city_data(city)
    typer.secho(f"✅ Successfully synced data for {city}", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()
