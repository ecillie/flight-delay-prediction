import argparse
import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from pathlib import Path

import requests

from flightdelay.collection.domain import Downloader, HttpDownloader, NetworkOptions
from flightdelay.collection.services import (
    airport_coordinates,
    collect_airports,
    collect_flights,
    collect_weather,
)
from flightdelay.storage.analytics import audit_store, compact_weather
from flightdelay.storage.domain import Dataset, Partition, StorageOptions
from flightdelay.storage.policy import find_partition_violation
from flightdelay.storage.schemas import PartitionRecord
from flightdelay.storage.store import Store, create_store


class Command(StrEnum):
    FLIGHTS = "collect-flights"
    AIRPORTS = "collect-airports"
    WEATHER = "collect-weather"
    COMPACT = "compact-weather"
    AUDIT = "audit"


class ExitCode(IntEnum):
    SUCCESS = 0


@dataclass(slots=True, kw_only=True, frozen=True)
class RunOptions:
    command: Command
    root: Path = Path("data")
    year: int | None = None
    month: int | None = None
    airport: str | None = None
    force: bool = False


def record_summary(record: PartitionRecord) -> dict[str, object]:
    return {
        "dataset": record.dataset,
        "partition": record.partition_key,
        "generation": record.generation,
        "rows": record.rows,
        "status": record.status,
    }


@dataclass(slots=True, kw_only=True, frozen=True)
class Pipeline:
    store: Store
    downloader: Downloader

    def flights(self, options: RunOptions) -> dict[str, object]:
        partition = Partition(
            dataset=Dataset.FLIGHTS, year=options.year, month=options.month
        )
        return record_summary(
            collect_flights(self.store, self.downloader, partition, force=options.force)
        )

    def airports(self, options: RunOptions) -> dict[str, object]:
        return record_summary(
            collect_airports(self.store, self.downloader, force=options.force)
        )

    def weather(self, options: RunOptions) -> dict[str, object]:
        records = [
            collect_weather(
                self.store,
                self.downloader,
                Partition(dataset=Dataset.WEATHER, year=options.year, airport=airport),
                latitude,
                longitude,
                force=options.force,
            )
            for airport, latitude, longitude in airport_coordinates(
                self.store, options.airport
            )
        ]
        return {
            "complete_partitions": len(records),
            "rows": sum(record.rows for record in records),
        }

    def compact(self, options: RunOptions) -> dict[str, object]:
        assert options.year is not None
        return record_summary(
            compact_weather(self.store, options.year, force=options.force)
        )

    def audit(self, options: RunOptions) -> dict[str, object]:
        return audit_store(self.store)


def find_option_violation(options: RunOptions) -> ValueError | None:
    if options.command == Command.FLIGHTS:
        return find_partition_violation(
            Partition(
                dataset=Dataset.FLIGHTS,
                year=options.year,
                month=options.month,
                airport=options.airport,
            )
        )
    if options.command in (Command.WEATHER, Command.COMPACT):
        if options.year is None:
            return ValueError("--year is required")
        if options.command == Command.COMPACT and options.airport is not None:
            return ValueError("compaction covers a year; --airport does not apply")
        dataset = Dataset.WEATHER_YEAR if options.airport is None else Dataset.WEATHER
        return find_partition_violation(
            Partition(
                dataset=dataset,
                year=options.year,
                month=options.month,
                airport=options.airport,
            )
        )
    if any(
        value is not None for value in (options.year, options.month, options.airport)
    ):
        return ValueError("year, month and airport do not apply to this command")
    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect and validate partitioned Parquet datasets"
    )
    parser.add_argument("command", type=Command, choices=list(Command))
    parser.add_argument("--root", type=Path, default=StorageOptions().root)
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--airport")
    parser.add_argument("--force", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    options = RunOptions(**vars(parser.parse_args(argv)))
    if error := find_option_violation(options):
        parser.error(str(error))
    logging.basicConfig(level=logging.INFO)
    store = create_store(StorageOptions(root=options.root))
    with requests.Session() as session:
        pipeline = Pipeline(
            store=store,
            downloader=HttpDownloader(session=session, options=NetworkOptions()),
        )
        handlers = {
            Command.FLIGHTS: pipeline.flights,
            Command.AIRPORTS: pipeline.airports,
            Command.WEATHER: pipeline.weather,
            Command.COMPACT: pipeline.compact,
            Command.AUDIT: pipeline.audit,
        }
        print(json.dumps(handlers[options.command](options), indent=2))
    return ExitCode.SUCCESS


if __name__ == "__main__":
    raise SystemExit(main())
