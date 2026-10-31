import argparse
import os
from collections.abc import Generator
from dataclasses import dataclass, replace
from pathlib import Path

import pyarrow.parquet as pq

from flightdelay.storage.domain import BatchPair, Dataset, Partition, StorageOptions
from flightdelay.storage.manifest import ManifestPort
from flightdelay.storage.schemas import PartitionRecord
from flightdelay.storage.store import create_store


@dataclass(slots=True, kw_only=True, frozen=True)
class TerminatingManifest:
    delegate: ManifestPort
    exit_code: int = 71

    def get(self, partition: Partition) -> PartitionRecord | None:
        return self.delegate.get(partition)

    def records(self, dataset: Dataset | None = None) -> tuple[PartitionRecord, ...]:
        return self.delegate.records(dataset)

    def commit(self, record: PartitionRecord) -> None:
        os._exit(self.exit_code)


def interrupted_pairs(pair: BatchPair) -> Generator[BatchPair]:
    yield pair
    os._exit(71)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("mode", choices=("mid-write", "before-commit"))
    return parser


def main() -> int:
    arguments = build_parser().parse_args()
    store = create_store(StorageOptions(root=arguments.root, min_free_bytes=0))
    partition = Partition(dataset=Dataset.FLIGHTS, year=2023, month=1)
    record = store.manifest.get(partition)
    assert record is not None
    pair = BatchPair(
        raw=pq.ParquetFile(store.resolve(record.raw.relative_path)).read(),
        cleaned=pq.ParquetFile(store.resolve(record.cleaned.relative_path)).read(),
    )
    if arguments.mode == "mid-write":
        store.publish(partition, interrupted_pairs(pair), provenance={}, force=True)
    failing_store = replace(
        store, manifest=TerminatingManifest(delegate=store.manifest)
    )
    failing_store.publish(partition, (pair,), provenance={}, force=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
