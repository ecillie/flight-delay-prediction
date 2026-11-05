import json
from collections.abc import Generator
from contextlib import closing
from pathlib import Path

import duckdb
import pyarrow as pa

from flightdelay.storage.domain import BatchPair, Dataset, Partition, StorageOptions
from flightdelay.storage.errors import IntegrityError, MissingDatasetError
from flightdelay.storage.schemas import PartitionRecord, get_schemas
from flightdelay.storage.store import (
    Store,
    audit_record,
    create_store,
    publish_locked,
    record_is_current,
    writer_lock,
)


def open_research_connection(root: Path | None = None) -> duckdb.DuckDBPyConnection:
    options = StorageOptions() if root is None else StorageOptions(root=root)
    if not options.manifest_path.is_file():
        raise MissingDatasetError(
            dataset="Parquet data; run the historical migration first"
        )
    store = create_store(options)
    records = store.manifest.records()
    for dataset in (Dataset.FLIGHTS, Dataset.AIRPORTS, Dataset.WEATHER):
        if not any(record.dataset == dataset for record in records):
            raise MissingDatasetError(dataset=dataset)
    connection = open_analytics(store)
    try:
        create_views(store, connection)
    except Exception:
        connection.close()
        raise
    return connection


def open_analytics(store: Store) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(
        config={
            "threads": store.options.duckdb_threads,
            "memory_limit": store.options.duckdb_memory,
            "temp_directory": str(
                store.options.root.joinpath(".staging", "duckdb-spill")
            ),
            "max_temp_directory_size": f"{store.options.max_stage_bytes}B",
        }
    )


def weather_generations(
    records: tuple[PartitionRecord, ...], year: int | None
) -> dict[str, str]:
    return {
        record.partition_key: record.generation
        for record in records
        if record.dataset == Dataset.WEATHER and record.year == year
    }


def select_weather_records(
    store: Store, records: tuple[PartitionRecord, ...]
) -> tuple[PartitionRecord, ...]:
    compacted = {
        record.year: record
        for record in records
        if record.dataset == Dataset.WEATHER_YEAR
        and json.loads(record.provenance_json).get("source_generations")
        == weather_generations(records, record.year)
        and record_is_current(store, record)
    }
    return tuple(
        record
        for record in records
        if (record.dataset == Dataset.WEATHER and record.year not in compacted)
        or (
            record.dataset == Dataset.WEATHER_YEAR
            and record == compacted.get(record.year)
        )
    )


def committed_records(store: Store, dataset: Dataset) -> tuple[PartitionRecord, ...]:
    records = store.manifest.records()
    if dataset == Dataset.WEATHER:
        return select_weather_records(store, records)
    return tuple(record for record in records if record.dataset == dataset)


def register_dataset(
    store: Store,
    connection: duckdb.DuckDBPyConnection,
    dataset: Dataset,
    name: str,
    records: tuple[PartitionRecord, ...],
) -> None:
    if any(not record_is_current(store, record) for record in records):
        raise IntegrityError(
            path=store.options.manifest_path,
            reason="dataset files are missing or modified",
        )
    if len(records) == 0:
        connection.from_arrow(
            pa.Table.from_batches([], schema=get_schemas(dataset).cleaned)
        ).create_view(name)
        return
    paths = [str(store.resolve(record.cleaned.relative_path)) for record in records]
    relation = connection.read_parquet(
        paths, hive_partitioning=False, union_by_name=True
    )
    if dataset == Dataset.FLIGHTS:
        sql_types = {
            pa.string(): "VARCHAR",
            pa.int64(): "BIGINT",
            pa.float64(): "DOUBLE",
        }
        additions = [
            f'CAST(NULL AS {sql_types[field.type]}) AS "{field.name}"'
            for field in get_schemas(dataset).cleaned
            if field.name not in relation.columns
        ]
        relation = relation.project(", ".join(["*", *additions]))
    relation.create_view(name)


def create_views(store: Store, connection: duckdb.DuckDBPyConnection) -> None:
    records = store.manifest.records()
    register_dataset(
        store,
        connection,
        Dataset.FLIGHTS,
        "flight_records",
        tuple(record for record in records if record.dataset == Dataset.FLIGHTS),
    )
    connection.execute(
        "CREATE VIEW raw_ontime_flights AS SELECT *, "
        "COALESCE(source_rowid, source_record_number) AS rowid FROM flight_records"
    )
    register_dataset(
        store,
        connection,
        Dataset.WEATHER,
        "weather_hourly",
        select_weather_records(store, records),
    )
    register_dataset(
        store,
        connection,
        Dataset.AIRPORTS,
        "airports",
        tuple(record for record in records if record.dataset == Dataset.AIRPORTS),
    )


def compacted_pairs(
    store: Store, records: tuple[PartitionRecord, ...]
) -> Generator[BatchPair]:
    schemas = get_schemas(Dataset.WEATHER)
    raw_paths = [str(store.resolve(record.raw.relative_path)) for record in records]
    cleaned_paths = [
        str(store.resolve(record.cleaned.relative_path)) for record in records
    ]
    with (
        closing(open_analytics(store)) as raw_connection,
        closing(open_analytics(store)) as clean_connection,
    ):
        raw_reader = (
            raw_connection.read_parquet(raw_paths, hive_partitioning=False)
            .order("airport_code, weather_datetime")
            .to_arrow_reader(batch_size=store.options.batch_rows)
        )
        clean_reader = (
            clean_connection.read_parquet(cleaned_paths, hive_partitioning=False)
            .order("airport_code, weather_datetime")
            .to_arrow_reader(batch_size=store.options.batch_rows)
        )
        for raw, cleaned in zip(raw_reader, clean_reader, strict=True):
            yield BatchPair(
                raw=pa.Table.from_batches([raw]).cast(schemas.raw),
                cleaned=pa.Table.from_batches([cleaned]).cast(schemas.cleaned),
            )


def compact_weather(store: Store, year: int, *, force: bool = False) -> PartitionRecord:
    partition = Partition(dataset=Dataset.WEATHER_YEAR, year=year)
    with writer_lock(store.options):
        records = tuple(
            record
            for record in store.manifest.records(Dataset.WEATHER)
            if record.year == year
        )
        if len(records) == 0:
            raise MissingDatasetError(dataset=f"weather for {year}")
        for record in records:
            audit_record(store, record)
        generations = weather_generations(records, year)
        current = store.manifest.get(partition)
        if current is not None and not force and record_is_current(store, current):
            if (
                json.loads(current.provenance_json).get("source_generations")
                == generations
            ):
                return current
        return publish_locked(
            store,
            partition,
            compacted_pairs(store, records),
            {
                "source_generations": generations,
                "sort": "airport_code, weather_datetime",
            },
            sum(record.rows for record in records),
        )


def audit_store(store: Store) -> dict[str, object]:
    records = store.manifest.records()
    for record in records:
        audit_record(store, record)
    referenced = {
        store.resolve(info.relative_path)
        for record in records
        for info in (record.raw, record.cleaned)
    }
    published = {
        path.resolve()
        for layer in ("raw", "cleaned")
        for path in store.options.root.joinpath(layer).rglob("*.parquet")
    }
    return {
        "complete_partitions": len(records),
        "unreferenced_files": sorted(str(path) for path in published - referenced),
        "staging_entries": sorted(
            str(path) for path in store.options.root.joinpath(".staging").iterdir()
        ),
    }
