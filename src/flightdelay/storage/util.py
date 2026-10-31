import hashlib
import os
from collections.abc import Generator, Iterable
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from flightdelay.storage.errors import IntegrityError


def rebatch_tables(
    batches: Iterable[pa.RecordBatch], batch_rows: int
) -> Generator[pa.Table]:
    if batch_rows <= 0:
        raise ValueError("batch_rows must be positive")
    pending: pa.Table | None = None
    for batch in batches:
        table = pa.Table.from_batches([batch])
        if pending is not None:
            table = pa.concat_tables((pending, table))
        while table.num_rows >= batch_rows:
            yield table.slice(0, batch_rows)
            table = table.slice(batch_rows)
        pending = table if table.num_rows > 0 else None
    if pending is not None:
        yield pending


def schema_hash(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.remove_metadata().serialize().to_pybytes()).hexdigest()


def file_hash(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def table_fingerprint(table: pa.Table) -> bytes:
    digest = hashlib.sha256()
    frame = table.to_pandas()
    digest.update(pd.util.hash_pandas_object(frame, index=False).to_numpy().tobytes())
    for column in table.columns:
        digest.update(column.is_null().to_numpy().tobytes())
    return digest.digest()


def sync_file(path: Path) -> None:
    with path.open("r+b") as handle:
        handle.flush()
        os.fsync(handle.fileno())


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def verify_written_file(
    path: Path, schema: pa.Schema, rows: int, fingerprints: tuple[bytes, ...]
) -> None:
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows != rows or not parquet.schema_arrow.equals(
        schema, check_metadata=False
    ):
        raise IntegrityError(path=path, reason="written schema or row count differs")
    actual = tuple(
        table_fingerprint(parquet.read_row_group(index))
        for index in range(parquet.num_row_groups)
    )
    if actual != fingerprints:
        raise IntegrityError(path=path, reason="read-back values or nulls differ")


def sync_directory_chain(path: Path, boundary: Path) -> None:
    for directory in (path, *path.parents):
        if directory.resolve().is_relative_to(boundary.resolve()):
            sync_directory(directory)
