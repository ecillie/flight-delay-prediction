from dataclasses import dataclass
from enum import StrEnum, auto
from pathlib import Path

import pyarrow as pa

from flightdelay.storage.errors import PartitionError


class Dataset(StrEnum):
    FLIGHTS = auto()
    WEATHER = auto()
    WEATHER_YEAR = auto()
    AIRPORTS = auto()


@dataclass(slots=True, kw_only=True, frozen=True)
class StorageOptions:
    root: Path = Path("data")
    min_free_bytes: int = 5_368_709_120
    max_stage_bytes: int = 536_870_912
    batch_rows: int = 25_000
    duckdb_threads: int = 2
    duckdb_memory: str = "512MB"
    schema_version: int = 1
    manifest_timeout_seconds: float = 30.0

    @property
    def manifest_path(self) -> Path:
        return self.root.joinpath("metadata", "ingestion.sqlite")


@dataclass(slots=True, kw_only=True, frozen=True)
class Partition:
    dataset: Dataset
    year: int | None = None
    month: int | None = None
    airport: str | None = None

    @property
    def year_value(self) -> int:
        if self.year is None:
            raise PartitionError(partition=self.dataset, reason="year required")
        return self.year

    @property
    def key(self) -> str:
        if self.dataset == Dataset.FLIGHTS:
            return f"{self.year:04d}-{self.month:02d}"
        if self.dataset == Dataset.WEATHER:
            return f"{self.year:04d}/{self.airport}"
        if self.dataset == Dataset.WEATHER_YEAR:
            return str(self.year)
        return "current"

    @property
    def directories(self) -> tuple[str, ...]:
        if self.dataset == Dataset.FLIGHTS:
            return ("flights", f"year={self.year}", f"month={self.month:02d}")
        if self.dataset == Dataset.WEATHER:
            return ("weather", f"year={self.year}", f"airport={self.airport}")
        if self.dataset == Dataset.WEATHER_YEAR:
            return ("weather", f"year={self.year}", "compacted")
        return ("airports",)


@dataclass(slots=True, kw_only=True, frozen=True)
class Schemas:
    raw: pa.Schema
    cleaned: pa.Schema


@dataclass(slots=True, kw_only=True, frozen=True)
class BatchPair:
    raw: pa.Table
    cleaned: pa.Table
