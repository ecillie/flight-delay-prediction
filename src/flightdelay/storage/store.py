import fcntl
import json
import os
import shutil
import tempfile
import uuid
from collections.abc import Generator, Iterable
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from flightdelay.storage.domain import BatchPair, Dataset, Partition, StorageOptions
from flightdelay.storage.errors import (
    DataValidationError,
    IntegrityError,
    StorageSpaceError,
    WriterBusyError,
)
from flightdelay.storage.manifest import ManifestPort, initialize_manifest
from flightdelay.storage.policy import (
    ValidationProgress,
    expected_weather_rows,
    find_partition_violation,
    validate_pair,
)
from flightdelay.storage.schemas import (
    FileInfo,
    PartitionRecord,
    flight_schemas,
    get_schemas,
)
from flightdelay.storage.util import (
    file_hash,
    schema_hash,
    sync_directory,
    sync_directory_chain,
    sync_file,
    table_fingerprint,
    verify_written_file,
)


@dataclass(slots=True, kw_only=True, frozen=True)
class Store:
    options: StorageOptions
    manifest: ManifestPort

    def resolve(self, relative_path: str) -> Path:
        path = self.options.root.joinpath(relative_path).resolve()
        if not path.is_relative_to(self.options.root.resolve()):
            raise IntegrityError(path=path, reason="manifest path escapes storage root")
        return path

    def is_complete(self, partition: Partition) -> bool:
        if error := find_partition_violation(partition):
            raise error
        record = self.manifest.get(partition)
        if record is None:
            return False
        return record_is_current(self, record)

    def publish(
        self,
        partition: Partition,
        pairs: Iterable[BatchPair],
        *,
        provenance: dict[str, object],
        expected_rows: int | None = None,
        force: bool = False,
    ) -> PartitionRecord:
        if error := find_partition_violation(partition):
            raise error
        with writer_lock(self.options):
            current = self.manifest.get(partition)
            if current is not None and not force and record_is_current(self, current):
                return current
            return publish_locked(self, partition, pairs, provenance, expected_rows)


def create_store(options: StorageOptions | None = None) -> Store:
    settings = options if options is not None else StorageOptions()
    settings.root.joinpath(".staging").mkdir(parents=True, exist_ok=True)
    manifest = initialize_manifest(settings)
    sync_directory_chain(settings.manifest_path.parent, settings.root)
    sync_directory(settings.root.parent)
    return Store(options=settings, manifest=manifest)


@contextmanager
def writer_lock(options: StorageOptions) -> Generator[None]:
    path = options.manifest_path.with_suffix(".lock")
    with path.open("a+b") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise WriterBusyError(path=path) from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def record_is_current(store: Store, record: PartitionRecord) -> bool:
    if record.schema_version != store.options.schema_version:
        return False
    schemas = get_schemas(record.dataset)
    for info, schema in ((record.raw, schemas.raw), (record.cleaned, schemas.cleaned)):
        path = store.resolve(info.relative_path)
        if not path.is_file():
            return False
        stat = path.stat()
        if (stat.st_size, stat.st_mtime_ns) != (info.size_bytes, info.mtime_ns):
            return False
        accepted_hashes = {schema_hash(schema)}
        if record.dataset == Dataset.FLIGHTS:
            legacy = flight_schemas(legacy=True)
            legacy_schema = legacy.raw if info == record.raw else legacy.cleaned
            accepted_hashes.add(schema_hash(legacy_schema))
        if info.schema_sha256 not in accepted_hashes:
            return False
    return True


def audit_record(store: Store, record: PartitionRecord) -> None:
    if not record_is_current(store, record):
        raise IntegrityError(
            path=store.options.manifest_path,
            reason="missing or modified partition files",
        )
    for info in (record.raw, record.cleaned):
        path = store.resolve(info.relative_path)
        if file_hash(path) != info.sha256:
            raise IntegrityError(path=path, reason="checksum mismatch")


def check_stage_capacity(
    options: StorageOptions, stage: Path, incoming_bytes: int = 0
) -> None:
    used = sum(path.stat().st_size for path in stage.iterdir() if path.is_file())
    allowance = 2 * incoming_bytes + 1_048_576
    if used + allowance > options.max_stage_bytes:
        raise StorageSpaceError(
            available=options.max_stage_bytes - used, required=allowance
        )
    free = shutil.disk_usage(options.root).free
    if free < options.min_free_bytes + allowance:
        raise StorageSpaceError(
            available=free, required=options.min_free_bytes + allowance
        )


def write_stage(
    store: Store,
    partition: Partition,
    pairs: Iterable[BatchPair],
    stage: Path,
    expected_rows: int | None,
) -> ValidationProgress:
    schemas = get_schemas(partition.dataset)
    fingerprints: dict[str, list[bytes]] = {"raw": [], "cleaned": []}
    progress = ValidationProgress()
    with ExitStack() as stack:
        writers = {
            layer: stack.enter_context(
                pq.ParquetWriter(
                    stage.joinpath(f"{layer}.parquet"), schema, compression="zstd"
                )
            )
            for layer, schema in (("raw", schemas.raw), ("cleaned", schemas.cleaned))
        }
        for pair in pairs:
            if pair.cleaned.num_rows > store.options.batch_rows:
                raise DataValidationError(
                    dataset=partition.dataset, reason="batch exceeds row allowance"
                )
            check_stage_capacity(
                store.options, stage, pair.raw.nbytes + pair.cleaned.nbytes
            )
            progress = validate_pair(partition, pair, progress)
            for layer, table in (("raw", pair.raw), ("cleaned", pair.cleaned)):
                writers[layer].write_table(
                    table, row_group_size=store.options.batch_rows
                )
                fingerprints[layer].append(table_fingerprint(table))
    if progress.rows == 0:
        raise DataValidationError(dataset=partition.dataset, reason="empty partition")
    if partition.dataset == Dataset.WEATHER:
        expected_rows = expected_weather_rows(partition.year_value)
    if expected_rows is not None and progress.rows != expected_rows:
        raise DataValidationError(
            dataset=partition.dataset, reason="incomplete partition"
        )
    if partition.dataset == Dataset.FLIGHTS and progress.legacy_rows not in (
        0,
        progress.rows,
    ):
        raise DataValidationError(
            dataset="flights", reason="mixed legacy identifier provenance"
        )
    for layer, schema in (("raw", schemas.raw), ("cleaned", schemas.cleaned)):
        path = stage.joinpath(f"{layer}.parquet")
        sync_file(path)
        verify_written_file(path, schema, progress.rows, tuple(fingerprints[layer]))
    check_stage_capacity(store.options, stage)
    return progress


def publish_file(
    store: Store, partition: Partition, stage: Path, layer: str, generation: str
) -> FileInfo:
    source = stage.joinpath(f"{layer}.parquet")
    target = store.options.root.joinpath(
        layer, *partition.directories, f"{generation}.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    os.replace(source, target)
    sync_directory_chain(target.parent, store.options.root)
    stat = target.stat()
    return FileInfo(
        relative_path=str(target.relative_to(store.options.root)),
        size_bytes=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        sha256=file_hash(target),
        schema_sha256=schema_hash(pq.read_schema(target)),
    )


def publish_locked(
    store: Store,
    partition: Partition,
    pairs: Iterable[BatchPair],
    provenance: dict[str, object],
    expected_rows: int | None,
) -> PartitionRecord:
    if partition.dataset == Dataset.WEATHER:
        expected_rows = expected_weather_rows(partition.year_value)
    generation = uuid.uuid4().hex
    with tempfile.TemporaryDirectory(
        dir=store.options.root.joinpath(".staging")
    ) as temporary:
        stage = Path(temporary)
        check_stage_capacity(store.options, stage)
        progress = write_stage(store, partition, pairs, stage, expected_rows)
        raw = publish_file(store, partition, stage, "raw", generation)
        cleaned = publish_file(store, partition, stage, "cleaned", generation)
        record = PartitionRecord(
            dataset=partition.dataset,
            partition_key=partition.key,
            year=partition.year,
            month=partition.month,
            airport=partition.airport,
            generation=generation,
            rows=progress.rows,
            legacy_rows=progress.legacy_rows,
            expected_rows=expected_rows,
            schema_version=store.options.schema_version,
            raw=raw,
            cleaned=cleaned,
            provenance_json=json.dumps(provenance, sort_keys=True),
        )
        store.manifest.commit(record)
    sync_directory(store.options.manifest_path.parent)
    return record
