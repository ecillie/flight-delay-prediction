import json
import logging
import shutil
from collections.abc import Iterable
from contextlib import ExitStack
from dataclasses import asdict

import pyarrow as pa
import pyarrow.parquet as pq

from flightdelay.migration.domain import Inventory, MigrationOptions, Task
from flightdelay.migration.errors import MigrationConflictError
from flightdelay.migration.source import LegacySource, read_pairs
from flightdelay.storage.analytics import compact_weather, weather_generations
from flightdelay.storage.domain import BatchPair, Dataset, Partition
from flightdelay.storage.errors import IntegrityError, StorageSpaceError
from flightdelay.storage.policy import ValidationProgress, validate_pair
from flightdelay.storage.schemas import PartitionRecord, get_schemas
from flightdelay.storage.store import Store, audit_record, create_store

logger = logging.getLogger(__name__)


def save_evidence(options: MigrationOptions, name: str, value: object) -> None:
    options.evidence.mkdir(parents=True, exist_ok=True)
    path = options.evidence.joinpath(f"phase-4-{name}.json")
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(value, indent=2, default=str) + "\n")
    temporary.replace(path)


def encode_size(
    pairs: Iterable[BatchPair], task: Task, options: MigrationOptions
) -> dict[str, int]:
    schemas = get_schemas(task.partition.dataset)
    sinks = {"raw": pa.BufferOutputStream(), "cleaned": pa.BufferOutputStream()}
    progress = ValidationProgress()
    with ExitStack() as stack:
        writers = {
            layer: stack.enter_context(
                pq.ParquetWriter(sinks[layer], schema, compression="zstd")
            )
            for layer, schema in (("raw", schemas.raw), ("cleaned", schemas.cleaned))
        }
        for pair in pairs:
            progress = validate_pair(task.partition, pair, progress)
            writers["raw"].write_table(
                pair.raw, row_group_size=options.storage.batch_rows
            )
            writers["cleaned"].write_table(
                pair.cleaned, row_group_size=options.storage.batch_rows
            )
    if progress.rows != task.rows:
        raise MigrationConflictError(key=f"source row count: {task.partition.key}")
    return {layer: sink.getvalue().size for layer, sink in sinks.items()}


def estimate_storage(
    source: LegacySource, inventory: Inventory, options: MigrationOptions
) -> dict[str, object]:
    flights = tuple(
        task for task in inventory.tasks if task.partition.dataset == Dataset.FLIGHTS
    )
    probes = tuple(dict.fromkeys((flights[0], max(flights, key=task_rows))))
    probes_data = []
    flight_bytes = 0
    flight_rows = 0
    for task in probes:
        logger.info("Estimating production flights %s in memory", task.partition.key)
        sizes = encode_size(read_pairs(source, task, options), task, options)
        flight_bytes += sum(sizes.values())
        flight_rows += task.rows
        probes_data.append(
            {
                "dataset": "flights",
                "partition": task.partition.key,
                "rows": task.rows,
                "bytes": sizes,
            }
        )
    projected = flight_bytes / flight_rows * sum(task.rows for task in flights)
    weather_fragments = 0.0
    compacted = 0
    for year in inventory.weather_years:
        tasks = tuple(
            task
            for task in inventory.tasks
            if task.partition.dataset == Dataset.WEATHER and task.partition.year == year
        )
        probe = tasks[0]
        logger.info("Estimating weather fragments for %s in memory", year)
        sizes = encode_size(read_pairs(source, probe, options), probe, options)
        weather_fragments += (
            sum(sizes.values()) / probe.rows * sum(task.rows for task in tasks)
        )
        combined = Task(
            partition=Partition(dataset=Dataset.WEATHER_YEAR, year=year),
            rows=sum(task.rows for task in tasks),
        )
        logger.info("Measuring full compacted weather year %s in memory", year)
        year_sizes = encode_size(
            read_pairs(source, combined, options), combined, options
        )
        compacted += sum(year_sizes.values())
        probes_data.append(
            {
                "dataset": "weather",
                "partition": probe.partition.key,
                "rows": probe.rows,
                "bytes": sizes,
                "full_year_compacted_bytes": year_sizes,
            }
        )
    reference = next(
        task for task in inventory.tasks if task.partition.dataset == Dataset.AIRPORTS
    )
    reference_bytes = sum(
        encode_size(read_pairs(source, reference, options), reference, options).values()
    )
    source.assert_unchanged()
    total = int(projected + weather_fragments + compacted + reference_bytes)
    result = {
        "source_identity": inventory.source_identity,
        "probes": probes_data,
        "projected_flight_bytes": int(projected),
        "projected_weather_fragment_bytes": int(weather_fragments),
        "measured_compacted_weather_bytes": compacted,
        "reference_bytes": reference_bytes,
        "projected_combined_data_bytes": total,
        "capacity_margin": options.capacity_margin,
        "staging_and_spill_allowance_bytes": options.storage.max_stage_bytes * 2,
        "reserve_bytes": options.storage.min_free_bytes,
        "required_free_bytes_before_export": int(total * options.capacity_margin)
        + options.manifest_allowance
        + options.storage.max_stage_bytes * 2
        + options.storage.min_free_bytes,
        "free_bytes_at_estimate": shutil.disk_usage(options.storage.root).free,
        "estimation_wrote_no_dataset_files": True,
    }
    save_evidence(options, "estimate", result)
    return result


def task_rows(task: Task) -> int:
    return task.rows


def validate_existing_records(store: Store, inventory: Inventory) -> None:
    for record in store.manifest.records():
        if record.dataset == Dataset.WEATHER_YEAR:
            continue
        provenance = json.loads(record.provenance_json)
        if provenance.get("source_identity") != inventory.source_identity:
            raise MigrationConflictError(key=record.partition_key)


def can_resume(store: Store, record: PartitionRecord, task: Task) -> bool:
    if record.rows != task.rows:
        return False
    try:
        audit_record(store, record)
    except IntegrityError:
        return False
    return True


def check_capacity(
    store: Store, estimate: dict[str, object], options: MigrationOptions
) -> dict[str, int]:
    projected = estimate["projected_combined_data_bytes"]
    assert isinstance(projected, int)
    existing = sum(
        resumable_bytes(store, record) for record in store.manifest.records()
    )
    remaining = max(0, projected - existing)
    required = (
        int(remaining * options.capacity_margin)
        + options.manifest_allowance
        + options.storage.max_stage_bytes * 2
        + options.storage.min_free_bytes
    )
    free = shutil.disk_usage(options.storage.root).free
    result = {
        "free_bytes": free,
        "required_free_bytes": required,
        "estimated_remaining_output_bytes": remaining,
    }
    save_evidence(options, "capacity", result)
    if free < required:
        raise StorageSpaceError(available=free, required=required)
    return result


def resumable_bytes(store: Store, record: PartitionRecord) -> int:
    try:
        audit_record(store, record)
    except IntegrityError:
        return 0
    if record.dataset == Dataset.WEATHER_YEAR:
        sources = weather_generations(store.manifest.records(), record.year)
        if json.loads(record.provenance_json).get("source_generations") != sources:
            return 0
    return record.raw.size_bytes + record.cleaned.size_bytes


def convert_history(
    source: LegacySource,
    inventory: Inventory,
    estimate: dict[str, object],
    options: MigrationOptions,
) -> dict[str, object]:
    store = create_store(options.storage)
    validate_existing_records(store, inventory)
    check_capacity(store, estimate, options)
    completed = []
    for task in inventory.tasks:
        source.assert_unchanged()
        prior = store.manifest.get(task.partition)
        if prior is not None and can_resume(store, prior, task):
            logger.info(
                "Resuming verified partition %s %s",
                task.partition.dataset,
                task.partition.key,
            )
            completed.append(
                {
                    "dataset": task.partition.dataset,
                    "key": task.partition.key,
                    "rows": prior.rows,
                    "resumed": True,
                }
            )
            continue
        logger.info(
            "Converting %s %s: %s rows",
            task.partition.dataset,
            task.partition.key,
            task.rows,
        )
        record = store.publish(
            task.partition,
            read_pairs(source, task, options),
            expected_rows=task.rows,
            force=prior is not None,
            provenance={
                "source_identity": inventory.source_identity,
                "source_sha256": inventory.source_sha256,
                "source_path": str(source.path),
                "source_table": task.table,
                "source_partition": task.partition.key,
                "converter_version": 1,
                "timezone": None,
                "timezone_note": "legacy database did not retain API timezone metadata",
            },
        )
        source.assert_unchanged()
        completed.append(
            {
                "dataset": record.dataset,
                "key": record.partition_key,
                "rows": record.rows,
                "resumed": False,
            }
        )
        save_evidence(
            options,
            "conversion",
            {
                "source_identity": inventory.source_identity,
                "partitions": completed,
                "complete": False,
            },
        )
    for year in inventory.weather_years:
        logger.info("Compacting historical weather %s", year)
        compact_weather(store, year)
        source.assert_unchanged()
    result = {
        "source_identity": inventory.source_identity,
        "partitions": completed,
        "complete": True,
    }
    save_evidence(options, "conversion", result)
    return result


def save_inventory(inventory: Inventory, options: MigrationOptions) -> None:
    save_evidence(options, "inventory", asdict(inventory))
