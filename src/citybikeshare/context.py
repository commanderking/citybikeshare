from dataclasses import dataclass
from pathlib import Path


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
        """Path to the city's output folder."""
        return self.transformed_root / self.city

    @property
    def analysis_directory(self) -> Path:
        """Path to the city's output folder."""
        return self.analysis_root / self.city
