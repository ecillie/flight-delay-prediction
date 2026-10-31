from dataclasses import dataclass, field
from pathlib import Path

from flightdelay.storage.domain import Dataset, Partition, StorageOptions


@dataclass(slots=True, kw_only=True, frozen=True)
class MigrationOptions:
    source: Path = Path("data/database/flights.sqlite")
    storage: StorageOptions = field(default_factory=StorageOptions)
    evidence: Path = Path("docs/data-validation")
    capacity_margin: float = 1.5
    manifest_allowance: int = 16_777_216
    benchmark_repeats: int = 3


@dataclass(slots=True, kw_only=True, frozen=True)
class Task:
    partition: Partition
    rows: int
    first_date: str | None = None
    last_date: str | None = None

    @property
    def table(self) -> str:
        if self.partition.dataset == Dataset.FLIGHTS:
            return "raw_ontime_flights"
        if self.partition.dataset == Dataset.AIRPORTS:
            return "airports"
        return "weather_hourly"

    @property
    def predicate(self) -> str:
        if self.partition.dataset == Dataset.FLIGHTS:
            return "Year=? AND Month=?"
        if self.partition.dataset == Dataset.AIRPORTS:
            return "1=1"
        if self.partition.dataset == Dataset.WEATHER:
            return "airport_code=? AND weather_date BETWEEN ? AND ?"
        return "weather_date BETWEEN ? AND ?"

    @property
    def parameters(self) -> tuple[str | int, ...]:
        if self.partition.dataset == Dataset.FLIGHTS:
            assert self.partition.year is not None and self.partition.month is not None
            return (self.partition.year, self.partition.month)
        if self.partition.dataset == Dataset.AIRPORTS:
            return ()
        dates = (f"{self.partition.year}-01-01", f"{self.partition.year}-12-31")
        if self.partition.dataset == Dataset.WEATHER:
            assert self.partition.airport is not None
            return (self.partition.airport, *dates)
        return dates

    @property
    def order(self) -> str:
        if self.partition.dataset == Dataset.WEATHER_YEAR:
            return "airport_code, weather_datetime"
        if self.partition.dataset == Dataset.WEATHER:
            return "weather_datetime"
        return "rowid"


@dataclass(slots=True, kw_only=True, frozen=True)
class TableSchema:
    name: str
    columns: tuple[tuple[str, str], ...]
    ddl: str


@dataclass(slots=True, kw_only=True, frozen=True)
class Inventory:
    source_identity: str
    source_bytes: int
    source_sha256: str
    schemas: tuple[TableSchema, ...]
    tasks: tuple[Task, ...]

    @property
    def weather_years(self) -> tuple[int, ...]:
        return tuple(
            sorted(
                {
                    task.partition.year_value
                    for task in self.tasks
                    if task.partition.dataset == Dataset.WEATHER
                }
            )
        )
