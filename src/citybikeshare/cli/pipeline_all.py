#!/usr/bin/env python3
"""
Run the full ETL pipeline (sync → extract → clean → transform) for every
configured city.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import typer
from citybikeshare.etl.download import download_city_data
from citybikeshare.etl.extract import extract_city_data
from citybikeshare.etl.clean import clean_city_data
from citybikeshare.etl.transform import transform_city_data
from citybikeshare.context import build_context
from citybikeshare.cli.transform_all import get_all_cities


def pipeline_single_city(city: str, skip_sync: bool) -> tuple[str, bool, str]:
    """Run the full pipeline for one city. Returns (city, success, message)."""
    try:
        context = build_context(city)
        if not skip_sync:
            download_city_data(context)
        extract_city_data(context)
        clean_city_data(context)
        transform_city_data(context)
        return (city, True, "✅ Finished successfully")

    except Exception as e:
        return (city, False, f"❌ Failed: {e}")


def run_pipeline_all(skip_sync: bool = False, max_workers: int = 4):
    """
    Run sync → extract → clean → transform for all cities in parallel.
    """
    cities = get_all_cities()
    typer.echo(f"🌍 Found {len(cities)} city configs: {', '.join(cities)}\n")

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_city = {
            executor.submit(pipeline_single_city, city, skip_sync): city
            for city in cities
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
        typer.echo("  ✅ All cities completed successfully!")

    typer.echo("🎉 Full pipeline complete for all cities!")
