import json
import time
from collections import Counter
from contextlib import closing
from dataclasses import dataclass

import pandas as pd
import pandas.testing as pdt
import pyarrow as pa
import pyarrow.parquet as pq

from flightdelay.migration.domain import Inventory, MigrationOptions, Task
from flightdelay.migration.errors import MigrationConflictError, MigrationParityError
from flightdelay.migration.services import save_evidence, validate_existing_records
from flightdelay.migration.source import (
    LegacySource,
    declared_arrow_schema,
    read_schema,
    read_tables,
)
from flightdelay.storage.analytics import (
    audit_store,
    create_views,
    open_analytics,
    weather_generations,
)
from flightdelay.storage.domain import Dataset, Partition
from flightdelay.storage.schemas import PartitionRecord
from flightdelay.storage.store import Store, create_store
from flightdelay.storage.util import rebatch_tables
from flightdelay.verification import model_query


def validate_partition(
    source: LegacySource,
    task: Task,
    record: PartitionRecord,
    store: Store,
    options: MigrationOptions,
) -> dict[str, object]:
    raw = pq.ParquetFile(store.resolve(record.raw.relative_path))
    cleaned = pq.ParquetFile(store.resolve(record.cleaned.relative_path))
    rows = 0
    nulls: Counter[str] = Counter()
    for expected, raw_batch, cleaned_batch in zip(
        read_tables(source, task, options),
        raw.iter_batches(batch_size=options.storage.batch_rows),
        cleaned.iter_batches(batch_size=options.storage.batch_rows),
        strict=True,
    ):
        actual = pa.Table.from_batches([cleaned_batch]).select(expected.column_names)
        if not expected.equals(actual):
            raise MigrationParityError(
                key=task.partition.key,
                reason="existing values, types, or nulls changed",
            )
        raw_table = pa.Table.from_batches([raw_batch])
        names = [
            name for name in raw_table.column_names if name in expected.column_names
        ]
        if not expected.select(names).equals(raw_table.select(names)):
            raise MigrationParityError(
                key=task.partition.key, reason="raw layer differs"
            )
        for name, column in zip(expected.column_names, expected.columns, strict=True):
            nulls[name] += column.null_count
        rows += len(expected)
    if rows != task.rows or record.rows != task.rows:
        raise MigrationParityError(
            key=task.partition.key, reason="partition count mismatch"
        )
    return {
        "dataset": task.partition.dataset,
        "key": task.partition.key,
        "rows": rows,
        "all_existing_columns_equal": True,
        "raw_values_equal": True,
        "null_counts": dict(nulls),
        "first_date": task.first_date,
        "last_date": task.last_date,
    }


@dataclass(slots=True, kw_only=True, frozen=True)
class ModelScenario:
    name: str
    target: str
    predicate: str


def model_scenarios() -> tuple[ModelScenario, ...]:
    return (
        ModelScenario(
            name="cancelled", target="Cancelled", predicate="Cancelled IS NOT NULL"
        ),
        ModelScenario(
            name="delayed",
            target="DepDel15",
            predicate="Cancelled=0 AND DepDel15 IS NOT NULL",
        ),
        ModelScenario(
            name="diverted",
            target="Diverted",
            predicate="Cancelled=0 AND Diverted IS NOT NULL",
        ),
    )


def validate_model_sample(
    source: LegacySource,
    store: Store,
    scenario: ModelScenario,
    options: MigrationOptions,
) -> dict[str, object]:
    query = model_query(
        "raw_ontime_flights", scenario.target, scenario.name, scenario.predicate
    )
    query = query.replace("SELECT Year,", "SELECT rowid AS source_rowid, Year,")
    source_frames = pd.read_sql_query(
        query,
        source.connection,
        chunksize=options.storage.batch_rows,
        dtype_backend="pyarrow",
    )
    rows = 0
    targets: Counter[int] = Counter()
    years: Counter[int] = Counter()
    source_schema = declared_arrow_schema(read_schema(source, "raw_ontime_flights"))
    types = {field.name: field.type for field in source_schema}
    types.update({"source_rowid": pa.int64(), f"target_{scenario.name}": pa.int64()})
    with closing(open_analytics(store)) as connection:
        create_views(store, connection)
        reader = connection.sql(query).to_arrow_reader(
            batch_size=options.storage.batch_rows
        )
        nonempty = (frame for frame in source_frames if len(frame) > 0)
        tables = rebatch_tables(reader, options.storage.batch_rows)
        for frame, table in zip(nonempty, tables, strict=True):
            schema = pa.schema(
                [pa.field(str(name), types[str(name)]) for name in frame.columns]
            )
            expected = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)
            actual = table.cast(expected.schema)
            if not expected.equals(actual):
                raise MigrationParityError(
                    key=scenario.name, reason="model IDs, features, or targets changed"
                )
            targets.update(frame[f"target_{scenario.name}"].tolist())
            years.update(frame["Year"].tolist())
            rows += len(frame)
            source.assert_unchanged()
    return {
        "sample": scenario.name,
        "rows": rows,
        "exact_membership_features_targets_equal": True,
        "target_counts": dict(targets),
        "year_counts": dict(years),
        "train_rows_2023_2024": years[2023] + years[2024],
        "test_rows_2025": years[2025],
    }


def join_query() -> str:
    return (
        "SELECT COUNT(*) AS joined_rows, COUNT(w.airport_code) AS matched_rows, "
        "AVG(w.temperature_2m) AS average_temperature FROM raw_ontime_flights f "
        "LEFT JOIN weather_hourly w ON f.Origin=w.airport_code "
        "AND f.FlightDate=w.weather_date AND f.CRSDepHour=w.weather_hour "
        "AND w.weather_date BETWEEN ? AND ? WHERE f.Year=? AND f.Month=?"
    )


def validate_joins(
    source: LegacySource, store: Store, inventory: Inventory
) -> list[dict[str, object]]:
    tasks = [
        task for task in inventory.tasks if task.partition.dataset == Dataset.FLIGHTS
    ]
    probes = tuple(dict.fromkeys((tasks[0], tasks[-1])))
    results: list[dict[str, object]] = []
    with closing(open_analytics(store)) as connection:
        create_views(store, connection)
        for task in probes:
            params = (
                task.first_date,
                task.last_date,
                task.partition.year,
                task.partition.month,
            )
            expected = pd.read_sql_query(
                join_query(), source.connection, params=params
            ).convert_dtypes()
            actual = connection.execute(join_query(), params).fetchdf().convert_dtypes()
            pdt.assert_frame_equal(
                expected, actual, check_dtype=False, atol=1e-10, rtol=1e-12
            )
            joined = int(actual["joined_rows"].tolist()[0])
            matched = int(actual["matched_rows"].tolist()[0])
            if joined != task.rows:
                raise MigrationParityError(
                    key=task.partition.key, reason="weather join multiplies flights"
                )
            results.append(
                {
                    "partition": task.partition.key,
                    "joined_rows": joined,
                    "matched_rows": matched,
                    "unmatched_rows": joined - matched,
                    "equivalent": True,
                }
            )
    return results


def validate_history(
    source: LegacySource, inventory: Inventory, options: MigrationOptions
) -> dict[str, object]:
    store = create_store(options.storage)
    validate_existing_records(store, inventory)
    results = []
    for task in inventory.tasks:
        record = store.manifest.get(task.partition)
        if record is None:
            raise MigrationConflictError(key=f"missing: {task.partition.key}")
        results.append(validate_partition(source, task, record, store, options))
        save_evidence(
            options, "partition-validation", {"partitions": results, "complete": False}
        )
    for year in inventory.weather_years:
        tasks = tuple(
            task
            for task in inventory.tasks
            if task.partition.dataset == Dataset.WEATHER and task.partition.year == year
        )
        task = Task(
            partition=Partition(dataset=Dataset.WEATHER_YEAR, year=year),
            rows=sum(item.rows for item in tasks),
        )
        record = store.manifest.get(task.partition)
        if record is None or json.loads(record.provenance_json).get(
            "source_generations"
        ) != weather_generations(store.manifest.records(), year):
            raise MigrationConflictError(key=f"missing or stale compaction: {year}")
        results.append(validate_partition(source, task, record, store, options))
    samples = [
        validate_model_sample(source, store, scenario, options)
        for scenario in model_scenarios()
    ]
    joins = validate_joins(source, store, inventory)
    audit = audit_store(store)
    source.verify_digest()
    save_evidence(
        options,
        "partition-validation",
        {
            "source_identity": inventory.source_identity,
            "partitions": results,
            "complete": True,
        },
    )
    files = [
        path
        for layer in ("raw", "cleaned")
        for path in options.storage.root.joinpath(layer).rglob("*.parquet")
    ]
    size = sum(path.stat().st_size for path in files) + sum(
        path.stat().st_size
        for path in options.storage.manifest_path.parent.glob("ingestion.sqlite*")
    )
    result = {
        "source_identity": inventory.source_identity,
        "partitions": results,
        "samples": samples,
        "joins": joins,
        "audit": audit,
        "actual_combined_storage_bytes": size,
        "source_bytes": inventory.source_bytes,
        "storage_saving_percent": 100 * (1 - size / inventory.source_bytes),
        "source_digest_unchanged": True,
        "generations": {
            f"{record.dataset}:{record.partition_key}": record.generation
            for record in store.manifest.records()
        },
        "complete": True,
    }
    save_evidence(options, "validation", result)
    return result


def benchmark_history(
    source: LegacySource, options: MigrationOptions
) -> dict[str, object]:
    store = create_store(options.storage)
    path = options.evidence.joinpath("phase-4-validation.json")
    if not path.exists():
        raise MigrationConflictError(
            key="run full parity validation before benchmarking"
        )
    validation = json.loads(path.read_text())
    generations = {
        f"{record.dataset}:{record.partition_key}": record.generation
        for record in store.manifest.records()
    }
    if (
        not validation.get("complete")
        or validation.get("source_identity") != source.identity
        or validation.get("generations") != generations
    ):
        raise MigrationConflictError(
            key="benchmark requires current full parity validation"
        )
    cases = []
    for repetition in range(options.benchmark_repeats):
        for scenario in model_scenarios():
            query = model_query(
                "raw_ontime_flights", scenario.target, scenario.name, scenario.predicate
            )
            order = (
                ("sqlite", "parquet") if repetition % 2 == 0 else ("parquet", "sqlite")
            )
            for backend in order:
                seconds, rows = time_model_load(source, store, query, backend, options)
                cases.append(
                    {
                        "repetition": repetition + 1,
                        "sample": scenario.name,
                        "backend": backend,
                        "seconds": seconds,
                        "rows": rows,
                    }
                )
                save_evidence(options, "benchmark", {"cases": cases, "complete": False})
                source.assert_unchanged()
            if cases[-1]["rows"] != cases[-2]["rows"]:
                raise MigrationParityError(
                    key=scenario.name, reason="benchmark workloads have different rows"
                )
    result = {
        "cases": cases,
        "complete": True,
        "materialization": "bounded pandas frames",
        "network_and_training_excluded": True,
    }
    save_evidence(options, "benchmark", result)
    return result


def time_model_load(
    source: LegacySource,
    store: Store,
    query: str,
    backend: str,
    options: MigrationOptions,
) -> tuple[float, int]:
    if backend == "sqlite":
        before = time.perf_counter()
        rows = sum(
            len(frame)
            for frame in pd.read_sql_query(
                query, source.connection, chunksize=options.storage.batch_rows
            )
        )
        return time.perf_counter() - before, rows
    with closing(open_analytics(store)) as connection:
        create_views(store, connection)
        before = time.perf_counter()
        reader = connection.sql(query).to_arrow_reader(
            batch_size=options.storage.batch_rows
        )
        rows = sum(len(pa.Table.from_batches([batch]).to_pandas()) for batch in reader)
        return time.perf_counter() - before, rows
