#!/usr/bin/env python3
"""
Transform all city datasets in parallel.
"""

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import typer
from src.citybikeshare.etl.transform import transform_city_data
from src.citybikeshare.cli.transform import Args
from src.citybikeshare.config.loader import CONFIG_DIR

app = typer.Typer(help="Run transformations for all configured cities.")


def get_all_cities() -> list[str]:
    """Return list of cities based on YAML config filenames."""
    config_path = Path(CONFIG_DIR)
    return [p.stem for p in config_path.glob("*.yaml") if p.is_file()]


def transform_single_city(city: str) -> tuple[str, bool, str]:
    """Run transform for one city. Returns (city, success, message)."""
    try:
        args = Args(city=city)
        transform_city_data(args)
        return (city, True, "✅ Finished successfully")

    except Exception as e:
        return (city, False, f"❌ Failed: {e}")


@app.command()
def main(
    max_workers: int = typer.Option(
        4,
        "--max-workers",
        "-w",
        help="Number of cities to process in parallel.",
    ),
):
    """
    Transform all cities in parallel using ThreadPoolExecutor.
    """
    cities = get_all_cities()
    typer.echo(f"🌍 Found {len(cities)} city configs: {', '.join(cities)}\n")

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_city = {
            executor.submit(transform_single_city, city): city for city in cities
        }

        for future in as_completed(future_to_city):
            city = future_to_city[future]
            try:
                city, success, message = future.result()
                color = typer.colors.GREEN if success else typer.colors.RED
                typer.secho(f"{city}: {message}", fg=color)
                results.append((city, success))
            except Exception as e:
                typer.secho(f"{city}: ❌ Unexpected failure: {e}", fg=typer.colors.RED)
                results.append((city, False))

    # 🧾 Summary
    total = len(results)
    failed = [c for c, ok in results if not ok]
    typer.echo("\n🧮 Summary:")
    typer.echo(f"  Total: {total}")
    typer.echo(f"  Successful: {total - len(failed)}")
    typer.echo(f"  Failed: {len(failed)}")

    if failed:
        typer.echo(f"  ❌ Failed cities: {', '.join(failed)}")
    else:
        typer.echo("  ✅ All cities transformed successfully!")

    typer.echo("🎉 All transformations complete!")


if __name__ == "__main__":
    app()
