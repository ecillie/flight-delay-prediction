import hashlib
import json
import sqlite3
from collections.abc import Generator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow as pa

from flightdelay.migration.domain import Inventory, MigrationOptions, TableSchema, Task
from flightdelay.migration.errors import SourceChangedError
from flightdelay.storage.domain import BatchPair, Dataset, Partition
from flightdelay.storage.errors import DataValidationError
from flightdelay.storage.policy import find_partition_violation
from flightdelay.storage.schemas import get_schemas
from flightdelay.storage.util import file_hash


def source_state(path: Path) -> tuple[tuple[str, int, int], ...]:
    candidates = (path, Path(str(path) + "-wal"))
    return tuple(
        (str(item), item.stat().st_size, item.stat().st_mtime_ns)
        for item in candidates
        if item.exists()
    )


@dataclass(slots=True, kw_only=True, frozen=True)
class LegacySource:
    path: Path
    connection: sqlite3.Connection
    state: tuple[tuple[str, int, int], ...]
    data_version: int
    sha256: str
    identity: str

    def assert_unchanged(self) -> None:
        version = self.connection.execute("PRAGMA data_version").fetchone()[0]
        if source_state(self.path) != self.state or version != self.data_version:
            raise SourceChangedError(path=self.path)

    def verify_digest(self) -> None:
        self.assert_unchanged()
        if file_hash(self.path) != self.sha256:
            raise SourceChangedError(path=self.path)
        self.assert_unchanged()


@contextmanager
def open_legacy(options: MigrationOptions) -> Generator[LegacySource]:
    path = options.source.resolve()
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        connection.execute("PRAGMA temp_store=MEMORY")
        connection.execute("BEGIN")
        connection.execute("SELECT rootpage FROM sqlite_schema LIMIT 1").fetchone()
        state = source_state(path)
        digest = file_hash(path)
        wal = Path(str(path) + "-wal")
        wal_digest = file_hash(wal) if wal.exists() else None
        schema = connection.execute(
            "SELECT name,sql FROM sqlite_schema ORDER BY name"
        ).fetchall()
        identity = hashlib.sha256(
            json.dumps([digest, wal_digest, schema]).encode()
        ).hexdigest()
        source = LegacySource(
            path=path,
            connection=connection,
            state=state,
            data_version=connection.execute("PRAGMA data_version").fetchone()[0],
            sha256=digest,
            identity=identity,
        )
        source.assert_unchanged()
        try:
            yield source
        finally:
            connection.rollback()


def read_schema(source: LegacySource, table: str) -> TableSchema:
    columns = tuple(
        (row[1], row[2])
        for row in source.connection.execute(f'PRAGMA table_info("{table}")')
    )
    row = source.connection.execute(
        "SELECT sql FROM sqlite_schema WHERE name=? AND type='table'", (table,)
    ).fetchone()
    if row is None:
        raise DataValidationError(dataset=table, reason="source table missing")
    return TableSchema(name=table, columns=columns, ddl=row[0])


def arrow_source_schema(source: LegacySource, task: Task) -> pa.Schema:
    schema = declared_arrow_schema(read_schema(source, task.table))
    if task.partition.dataset == Dataset.FLIGHTS:
        return pa.schema([pa.field("source_rowid", pa.int64()), *schema])
    return schema


def declared_arrow_schema(schema: TableSchema) -> pa.Schema:
    types = {"INTEGER": pa.int64(), "REAL": pa.float64(), "TEXT": pa.string()}
    return pa.schema(
        [pa.field(name, types[kind.upper()]) for name, kind in schema.columns]
    )


def inspect_source(source: LegacySource) -> Inventory:
    source.assert_unchanged()
    schemas = tuple(
        read_schema(source, name)
        for name in ("raw_ontime_flights", "weather_hourly", "airports")
    )
    flights = source.connection.execute(
        "SELECT Year,Month,COUNT(*),MIN(FlightDate),MAX(FlightDate) "
        "FROM raw_ontime_flights GROUP BY Year,Month ORDER BY Year,Month"
    ).fetchall()
    weather = source.connection.execute(
        "SELECT airport_code,substr(weather_date,1,4),COUNT(*),"
        "MIN(weather_date),MAX(weather_date) "
        "FROM weather_hourly GROUP BY airport_code,substr(weather_date,1,4) "
        "ORDER BY airport_code,substr(weather_date,1,4)"
    ).fetchall()
    tasks = [
        Task(
            partition=Partition(dataset=Dataset.FLIGHTS, year=row[0], month=row[1]),
            rows=row[2],
            first_date=row[3],
            last_date=row[4],
        )
        for row in flights
    ]
    tasks.extend(
        Task(
            partition=Partition(
                dataset=Dataset.WEATHER, airport=row[0], year=int(row[1])
            ),
            rows=row[2],
            first_date=row[3],
            last_date=row[4],
        )
        for row in weather
    )
    count = source.connection.execute("SELECT COUNT(*) FROM airports").fetchone()[0]
    tasks.append(Task(partition=Partition(dataset=Dataset.AIRPORTS), rows=count))
    for task in tasks:
        if error := find_partition_violation(task.partition):
            raise error
        expected = get_schemas(task.partition.dataset).cleaned.names
        added = {"source_rowid", "source_record_number", "record_id"}
        names = {name for name, _ in read_schema(source, task.table).columns}
        if names != set(expected) - added:
            raise DataValidationError(
                dataset=task.table, reason="legacy source schema differs"
            )
    source.assert_unchanged()
    return Inventory(
        source_identity=source.identity,
        source_bytes=source.path.stat().st_size,
        source_sha256=source.sha256,
        schemas=schemas,
        tasks=tuple(tasks),
    )


def read_tables(
    source: LegacySource, task: Task, options: MigrationOptions
) -> Generator[pa.Table]:
    prefix = (
        "rowid AS source_rowid," if task.partition.dataset == Dataset.FLIGHTS else ""
    )
    query = (
        f'SELECT {prefix}* FROM "{task.table}" '
        f"WHERE {task.predicate} ORDER BY {task.order}"
    )
    schema = arrow_source_schema(source, task)
    for frame in pd.read_sql_query(
        query,
        source.connection,
        params=task.parameters,
        chunksize=options.storage.batch_rows,
        dtype_backend="pyarrow",
    ):
        source.assert_unchanged()
        yield pa.Table.from_pandas(frame, schema=schema, preserve_index=False)


def convert_table(table: pa.Table, task: Task, first_record: int) -> BatchPair:
    schemas = get_schemas(task.partition.dataset)
    if task.partition.dataset != Dataset.FLIGHTS:
        return BatchPair(
            raw=table.select(schemas.raw.names).cast(schemas.raw),
            cleaned=table.select(schemas.cleaned.names).cast(schemas.cleaned),
        )
    new_fields = {"record_id", "source_record_number"}
    raw = table.select([name for name in schemas.raw.names if name not in new_fields])
    numbers = list(range(first_record, first_record + table.num_rows))
    raw = raw.append_column("source_record_number", pa.array(numbers, pa.int64()))
    raw = raw.append_column(
        "record_id",
        pa.array(
            [
                f"bts:{task.partition.year}:{task.partition.month:02d}:{number}"
                for number in numbers
            ]
        ),
    )
    raw = raw.select(schemas.raw.names).cast(schemas.raw)
    cleaned = raw.append_column("CRSDepHour", table.column("CRSDepHour"))
    cleaned = cleaned.append_column("CRSArrHour", table.column("CRSArrHour"))
    return BatchPair(raw=raw, cleaned=cleaned.cast(schemas.cleaned))


def read_pairs(
    source: LegacySource, task: Task, options: MigrationOptions
) -> Generator[BatchPair]:
    first_record = 1
    for table in read_tables(source, task, options):
        yield convert_table(table, task, first_record)
        first_record += table.num_rows
