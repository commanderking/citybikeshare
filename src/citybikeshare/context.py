from dataclasses import dataclass
from pathlib import Path
import typer


@dataclass(frozen=True)
class PipelineContext:
    """Shared runtime context for all ETL steps."""

    city: str
    data_root: Path
    transformed_root: Path
    analysis_root: Path

    @property
    def download_directory(self) -> Path:
        """Path to the city's download data folder."""

        return self.data_root / self.city / "download"

    @property
    def raw_directory(self) -> Path:
        """Path to the city's raw data folder."""
        return self.data_root / self.city / "raw"

    @property
    def metadata_directory(self) -> Path:
        """Path to the city's metadata data folder."""
        return self.data_root / self.city / "metadata"

    @property
    def parquet_directory(self) -> Path:
        """Path to the city's parquet data folder."""
        return self.data_root / self.city / "parquet"

    @property
    def transformed_directory(self) -> Path:
        """Path to the city's transformed data folder (parquet)."""
        return self.transformed_root / self.city

    @property
    def analysis_directory(self) -> Path:
        """Path to the city's output folder."""
        return self.analysis_root / self.city


def build_context(city: str) -> "PipelineContext":
    if not Path("pyproject.toml").exists():
        typer.secho(
            "Error: must be run from the project root (no pyproject.toml found here).",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    data_root = Path("data")
    city_dir = data_root / city
    for sub in ["download", "raw", "metadata", "parquet"]:
        (city_dir / sub).mkdir(parents=True, exist_ok=True)

    return PipelineContext(
        city=city,
        data_root=data_root,
        transformed_root=Path("output"),
        analysis_root=Path("analysis"),
    )
