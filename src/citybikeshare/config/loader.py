import polars as pl
from pathlib import Path
from typing import Any, Dict
import yaml

CONFIG_DIR = Path(__file__).parent / "cities"

DTYPE_MAP = {
    "Utf8": pl.Utf8,
    "Int64": pl.Int64,
    "Float64": pl.Float64,
    "Date": pl.Date,
    "Datetime": pl.Datetime,
    "Boolean": pl.Boolean,
}


def load_city_config(city: str) -> Dict[str, Any]:
    """Return configuration for a given city, from YAML if available."""
    yaml_path = CONFIG_DIR / f"{city}.yaml"

    if yaml_path.exists():
        config = yaml.safe_load(yaml_path.read_text())

        # Convert schema overrides to actual Polars dtypes
        read_csv_opts = config.get("read_csv_options", {})
        overrides = read_csv_opts.get("schema_overrides", {})

        # convert string → Polars dtype objects
        read_csv_opts["schema_overrides"] = {
            col: DTYPE_MAP.get(dtype_str, pl.Utf8)
            for col, dtype_str in overrides.items()
        }

        return config
    else:
        raise Exception(f"No YAML file found for {city}")
