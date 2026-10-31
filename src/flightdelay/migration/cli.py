import argparse
import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from enum import IntEnum, StrEnum, auto
from pathlib import Path

from flightdelay.migration.domain import MigrationOptions
from flightdelay.migration.services import (
    convert_history,
    estimate_storage,
    save_inventory,
)
from flightdelay.migration.source import inspect_source, open_legacy
from flightdelay.migration.validation import benchmark_history, validate_history
from flightdelay.storage.domain import StorageOptions


class Operation(StrEnum):
    INVENTORY = auto()
    ESTIMATE = auto()
    CONVERT = auto()
    VALIDATE = auto()
    BENCHMARK = auto()
    ALL = auto()


class ExitCode(IntEnum):
    SUCCESS = 0


@dataclass(slots=True, kw_only=True, frozen=True)
class CommandOptions:
    operation: Operation
    source: Path = MigrationOptions().source
    root: Path = StorageOptions().root
    evidence: Path = MigrationOptions().evidence


def build_parser() -> argparse.ArgumentParser:
    defaults = MigrationOptions()
    parser = argparse.ArgumentParser(
        description="Read-only legacy SQLite to Parquet migration"
    )
    parser.add_argument(
        "--operation", type=Operation, choices=list(Operation), required=True
    )
    parser.add_argument("--source", type=Path, default=defaults.source)
    parser.add_argument("--root", type=Path, default=defaults.storage.root)
    parser.add_argument("--evidence", type=Path, default=defaults.evidence)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    command = CommandOptions(**vars(build_parser().parse_args(argv)))
    options = MigrationOptions(
        source=command.source,
        storage=StorageOptions(root=command.root),
        evidence=command.evidence,
    )
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    with open_legacy(options) as source:
        inventory = inspect_source(source)
        save_inventory(inventory, options)
        if command.operation == Operation.INVENTORY:
            print(
                json.dumps(
                    {
                        "source_identity": inventory.source_identity,
                        "partitions": len(inventory.tasks),
                    }
                )
            )
            return ExitCode.SUCCESS
        if command.operation in (Operation.ESTIMATE, Operation.CONVERT, Operation.ALL):
            estimate = estimate_storage(source, inventory, options)
            if command.operation == Operation.ESTIMATE:
                print(json.dumps(estimate, indent=2))
                return ExitCode.SUCCESS
            convert_history(source, inventory, estimate, options)
        if command.operation in (Operation.VALIDATE, Operation.ALL):
            validate_history(source, inventory, options)
        if command.operation in (Operation.BENCHMARK, Operation.ALL):
            benchmark_history(source, options)
        source.verify_digest()
    return ExitCode.SUCCESS


if __name__ == "__main__":
    raise SystemExit(main())
