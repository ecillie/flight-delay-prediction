import sqlite3
from collections.abc import Generator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from flightdelay.storage.domain import Dataset, Partition, StorageOptions
from flightdelay.storage.errors import IntegrityError
from flightdelay.storage.schemas import PartitionRecord


@runtime_checkable
class ManifestPort(Protocol):
    def get(self, partition: Partition) -> PartitionRecord | None: ...

    def records(
        self, dataset: Dataset | None = None
    ) -> tuple[PartitionRecord, ...]: ...

    def commit(self, record: PartitionRecord) -> None: ...


@dataclass(slots=True, kw_only=True, frozen=True)
class Manifest:
    options: StorageOptions

    def get(self, partition: Partition) -> PartitionRecord | None:
        with open_manifest(self.options) as connection:
            row = connection.execute(
                "SELECT record_json FROM partitions "
                "WHERE dataset=? AND partition_key=?",
                (partition.dataset.value, partition.key),
            ).fetchone()
        if row is None:
            return None
        return PartitionRecord.model_validate_json(row[0])

    def records(self, dataset: Dataset | None = None) -> tuple[PartitionRecord, ...]:
        with open_manifest(self.options) as connection:
            if dataset is None:
                rows = connection.execute(
                    "SELECT record_json FROM partitions ORDER BY dataset, partition_key"
                ).fetchall()
                return tuple(
                    PartitionRecord.model_validate_json(row[0]) for row in rows
                )
            rows = connection.execute(
                "SELECT record_json FROM partitions "
                "WHERE dataset=? ORDER BY partition_key",
                (dataset.value,),
            ).fetchall()
        return tuple(PartitionRecord.model_validate_json(row[0]) for row in rows)

    def commit(self, record: PartitionRecord) -> None:
        with open_manifest(self.options) as connection, connection:
            connection.execute(
                "INSERT INTO partitions VALUES (?, ?, ?, ?) "
                "ON CONFLICT(dataset, partition_key) DO UPDATE SET "
                "generation=excluded.generation, record_json=excluded.record_json",
                (
                    record.dataset.value,
                    record.partition_key,
                    record.generation,
                    record.model_dump_json(),
                ),
            )


@contextmanager
def open_manifest(options: StorageOptions) -> Generator[sqlite3.Connection]:
    with closing(
        sqlite3.connect(options.manifest_path, timeout=options.manifest_timeout_seconds)
    ) as connection:
        connection.execute("PRAGMA synchronous=FULL")
        yield connection


def initialize_manifest(options: StorageOptions) -> Manifest:
    options.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open_manifest(options) as connection, connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, options.schema_version):
            raise IntegrityError(
                path=options.manifest_path, reason="manifest version mismatch"
            )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS partitions ("
            "dataset TEXT NOT NULL, partition_key TEXT NOT NULL, "
            "generation TEXT NOT NULL, record_json TEXT NOT NULL, "
            "PRIMARY KEY (dataset, partition_key))"
        )
        connection.execute(f"PRAGMA user_version={options.schema_version}")
    return Manifest(options=options)
