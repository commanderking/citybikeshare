from pathlib import Path
from typing import Any, Dict
import yaml

CONFIG_DIR = Path(__file__).parent / "metadata"


def load_city_config(city: str) -> Dict[str, Any]:
    """Return configuration for a given city, from YAML if available."""
    yaml_path = CONFIG_DIR / f"{city}.yaml"

    if yaml_path.exists():
        return yaml.safe_load(yaml_path.read_text())

    # fallback to old Python constants until migration is complete
    from scripts import constants

    if city not in constants.config:
        raise KeyError(f"No config found for city: {city}")
    return constants.config[city]
