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
    # The specific input file currently being transformed, set per-file by the
    # transform stage so steps can target file-scoped fixes (e.g. null_corrupt_times).
    source_file: Path | str | None = None

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
    def cleaned_directory(self) -> Path:
        """Path to the city's cleaned data folder (cleaned copies of raw CSVs)."""
        return self.data_root / self.city / "cleaned"

    @property
    def parquet_directory(self) -> Path:
        """Path to the city's parquet data folder."""
        return self.data_root / self.city / "parquet"

    @property
    def extract_state_path(self) -> Path:
        """Path to the extract stage's state file."""
        return self.data_root / self.city / "extract.state.json"

    @property
    def clean_state_path(self) -> Path:
        """Path to the clean stage's state file."""
        return self.data_root / self.city / "clean.state.json"

    @property
    def transform_state_path(self) -> Path:
        """Path to the transform stage's state file."""
        return self.data_root / self.city / "transform.state.json"

    @property
    def transform_input_directory(self) -> Path:
        """Directory transform reads from: cleaned/ when it has CSVs, else raw/."""
        cleaned = self.cleaned_directory
        if cleaned.exists() and (
            any(cleaned.glob("*.csv")) or any(cleaned.glob("*.csv.gz"))
        ):
            return cleaned
        return self.raw_directory

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
    # cleaned/ is created on demand by the clean stage (only clean_pipeline cities)

    return PipelineContext(
        city=city,
        data_root=data_root,
        transformed_root=Path("output"),
        analysis_root=Path("analysis"),
    )
