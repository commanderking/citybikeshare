#!/usr/bin/env python3
"""
CLI command for transforming bikeshare data (cleaning + combining CSVs).

Usage:
    pipenv run transform_new <city_name>

Examples:
    pipenv run transform_new montreal

"""

import typer
from src.citybikeshare.etl.transform import transform

app = typer.Typer(help="Transform (clean and combine) bikeshare CSVs for a given city.")


class Args:
    def __init__(self, city: str, no_write: bool = False, sample: int = 0):
        self.city = city
        self.no_write = no_write
        self.sample = sample


@app.command()
def main(
    city: str = typer.Argument(
        ..., help="Name of the city (e.g. montreal, taipei, boston)"
    ),
):
    """
    Clean, standardize, and combine extracted CSV files for the given city.
    """
    try:
        ## Temporary - a remnant from moving over from argparse where args.city was used to access city
        args = Args(city=city)

        typer.echo(f"🧼 Starting transform for city: {city}")
        transform(args)

        typer.echo("✅ Success!")

    except FileNotFoundError as e:
        typer.secho(f"❌ No CSV files found for {city}: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.secho(f"❌ Transform failed for {city}: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
