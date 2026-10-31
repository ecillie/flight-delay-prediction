import argparse
import json
import sqlite3
import statistics
import tempfile
import time
import zipfile
from collections.abc import Generator, Sequence
from contextlib import closing
from dataclasses import dataclass
from enum import IntEnum
from io import BytesIO
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pyarrow as pa
import pyarrow.parquet as pq

from flightdelay.collection.domain import RequestParams
from flightdelay.collection.services import collect_flights
from flightdelay.collection.transforms import frame_pairs, prepare_flight_pair
from flightdelay.storage.analytics import (
    audit_store,
    compact_weather,
    create_views,
    open_analytics,
)
from flightdelay.storage.domain import BatchPair, Dataset, Partition, StorageOptions
from flightdelay.storage.schemas import (
    PartitionRecord,
    flight_source_schema,
    get_schemas,
)
from flightdelay.storage.store import Store, create_store


class ExitCode(IntEnum):
    SUCCESS = 0


@dataclass(slots=True, kw_only=True, frozen=True)
class VerificationOptions:
    pilot: Path = Path("data/parquet-pilot/zstd/cleaned")
    evidence: Path = Path("docs/data-validation/phase-3-verification.json")
    scratch: Path = Path("data/.phase3-verification")
    sample_rows: int = 25_000
    repeats: int = 3


@dataclass(slots=True, kw_only=True, frozen=True)
class RecordedDownload:
    payload: bytes

    def fetch(self, url: str, params: RequestParams | None = None) -> bytes:
        return self.payload


def model_query(
    table: str,
    target: str = "Cancelled",
    name: str = "cancelled",
    predicate: str = "Cancelled IS NOT NULL",
) -> str:
    return (
        "SELECT Year, Month, DayofMonth, DayOfWeek, FlightDate, Reporting_Airline, "
        "Flight_Number_Reporting_Airline, Origin, Dest, CRSDepTime, CRSDepHour, "
        "CRSArrTime, CRSArrHour, CRSElapsedTime, Distance, DistanceGroup, "
        f"CAST({target} AS INTEGER) AS target_{name} FROM {table} "
        f"WHERE {predicate} AND rowid % 10 = 0 ORDER BY rowid"
    )


def legacy_hour(value: float) -> int | None:
    if pd.isna(value):
        return None
    number = int(value)
    if number == 2400:
        return 0
    return number // 100


def sqlite_pipeline(payload: bytes, path: Path) -> tuple[float, pd.DataFrame]:
    types = {"int64": "INTEGER", "double": "REAL", "string": "TEXT"}
    definitions = [
        f'"{field.name}" {types[str(field.type)]}' for field in flight_source_schema()
    ]
    definitions.extend(
        f'"{name}" INTEGER'
        for name in ("source_year", "source_month", "CRSDepHour", "CRSArrHour")
    )
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(
            f"CREATE TABLE raw_ontime_flights ({', '.join(definitions)})"
        )
        connection.execute(
            "CREATE INDEX origin_time ON "
            "raw_ontime_flights(Origin, FlightDate, CRSDepHour)"
        )
        connection.execute("CREATE INDEX year_month ON raw_ontime_flights(Year, Month)")
        connection.execute("CREATE INDEX delay_target ON raw_ontime_flights(DepDel15)")
        started = time.perf_counter()
        with (
            zipfile.ZipFile(BytesIO(payload)) as archive,
            archive.open("flights.csv") as handle,
        ):
            frame = pd.read_csv(handle)
        frame["source_year"] = 2023
        frame["source_month"] = 1
        frame["CRSDepHour"] = frame["CRSDepTime"].apply(legacy_hour).astype("Int64")
        frame["CRSArrHour"] = frame["CRSArrTime"].apply(legacy_hour).astype("Int64")
        frame.to_sql("raw_ontime_flights", connection, if_exists="append", index=False)
        result = pd.read_sql_query(model_query("raw_ontime_flights"), connection)
        return time.perf_counter() - started, result


def parquet_pipeline(payload: bytes, root: Path) -> tuple[float, pd.DataFrame]:
    store = create_store(StorageOptions(root=root))
    started = time.perf_counter()
    collect_flights(
        store,
        RecordedDownload(payload=payload),
        Partition(dataset=Dataset.FLIGHTS, year=2023, month=1),
    )
    with closing(open_analytics(store)) as connection:
        create_views(store, connection)
        frame = connection.execute(model_query("raw_ontime_flights")).fetchdf()
    return time.perf_counter() - started, frame


def measure_pipeline(
    backend: str, payload: bytes, scratch: Path, repetition: int
) -> tuple[float, pd.DataFrame]:
    if backend == "sqlite":
        return sqlite_pipeline(
            payload, scratch.joinpath(f"baseline-{repetition}.sqlite")
        )
    return parquet_pipeline(payload, scratch.joinpath(f"pipeline-{repetition}"))


def verify_pipeline_timings(
    options: VerificationOptions, scratch: Path
) -> dict[str, object]:
    sample = next(
        pq.ParquetFile(
            options.pilot.joinpath("raw_ontime_flights.parquet")
        ).iter_batches(batch_size=options.sample_rows)
    )
    native = (
        pa.Table.from_batches([sample]).select(flight_source_schema().names).to_pandas()
    )
    output = BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("flights.csv", native.to_csv(index=False))
    payload = output.getvalue()
    timings: dict[str, list[float]] = {"sqlite": [], "parquet": []}
    for repetition in range(options.repeats):
        order = ("sqlite", "parquet") if repetition % 2 == 0 else ("parquet", "sqlite")
        measured = {
            backend: measure_pipeline(backend, payload, scratch, repetition)
            for backend in order
        }
        baseline_seconds, expected = measured["sqlite"]
        seconds, actual = measured["parquet"]
        pdt.assert_frame_equal(
            expected.convert_dtypes(),
            actual.convert_dtypes(),
            check_dtype=False,
            check_exact=True,
        )
        timings["sqlite"].append(baseline_seconds)
        timings["parquet"].append(seconds)
    baseline = statistics.median(timings["sqlite"])
    measured = statistics.median(timings["parquet"])
    return {
        "rows": len(native),
        "repetitions": options.repeats,
        "timings_seconds": timings,
        "sqlite_median_seconds": baseline,
        "parquet_median_seconds": measured,
        "change_percent": 100 * (measured / baseline - 1),
        "model_inputs_equal": True,
    }


def legacy_flight_pairs(options: VerificationOptions) -> Generator[BatchPair]:
    first_record = 1
    partition = Partition(dataset=Dataset.FLIGHTS, year=2023, month=1)
    source = pq.ParquetFile(options.pilot.joinpath("raw_ontime_flights.parquet"))
    for batch in source.iter_batches(batch_size=StorageOptions().batch_rows):
        table = pa.Table.from_batches([batch])
        frame = table.select(flight_source_schema().names).to_pandas()
        yield prepare_flight_pair(
            frame,
            partition,
            first_record,
            table.column("source_rowid").combine_chunks(),
        )
        first_record += len(frame)


def verify_flight_roundtrip(
    options: VerificationOptions, store: Store, record: PartitionRecord
) -> None:
    original = pq.ParquetFile(options.pilot.joinpath("raw_ontime_flights.parquet"))
    written = pq.ParquetFile(store.resolve(record.cleaned.relative_path))
    for expected_batch, actual_batch in zip(
        original.iter_batches(batch_size=store.options.batch_rows),
        written.iter_batches(batch_size=store.options.batch_rows),
        strict=True,
    ):
        expected = pa.Table.from_batches([expected_batch])
        actual = pa.Table.from_batches([actual_batch]).select(expected.column_names)
        assert expected.equals(actual), "Historical flight values or nulls changed"


def verify_real_partitions(
    options: VerificationOptions, root: Path
) -> dict[str, object]:
    store = create_store(StorageOptions(root=root))
    flight_partition = Partition(dataset=Dataset.FLIGHTS, year=2023, month=1)
    count = pq.ParquetFile(
        options.pilot.joinpath("raw_ontime_flights.parquet")
    ).metadata.num_rows
    flight = store.publish(
        flight_partition,
        legacy_flight_pairs(options),
        expected_rows=count,
        provenance={"verification_source": "phase-2-pilot"},
    )
    verify_flight_roundtrip(options, store, flight)
    weather = pq.read_table(
        options.pilot.joinpath("weather_hourly.parquet"),
        filters=[("airport_code", "=", "ATL")],
    )
    schemas = get_schemas(Dataset.WEATHER)
    raw = weather.select(schemas.raw.names).cast(schemas.raw)
    cleaned = weather.select(schemas.cleaned.names).cast(schemas.cleaned)
    record = store.publish(
        Partition(dataset=Dataset.WEATHER, year=2023, airport="ATL"),
        frame_pairs(raw, cleaned, store.options.batch_rows),
        provenance={"verification_source": "phase-2-pilot"},
    )
    written_weather = pq.ParquetFile(store.resolve(record.cleaned.relative_path)).read()
    assert weather.equals(written_weather.select(weather.column_names))
    airports = pq.read_table(options.pilot.joinpath("airports.parquet"))
    schema = get_schemas(Dataset.AIRPORTS).raw
    reference = airports.select(schema.names).cast(schema)
    airport_record = store.publish(
        Partition(dataset=Dataset.AIRPORTS),
        (BatchPair(raw=reference, cleaned=reference),),
        provenance={"verification_source": "phase-2-pilot"},
    )
    written_airports = pq.ParquetFile(
        store.resolve(airport_record.cleaned.relative_path)
    ).read()
    assert airports.equals(written_airports.select(airports.column_names))
    compaction_started = time.perf_counter()
    compacted = compact_weather(store, 2023)
    compaction_seconds = time.perf_counter() - compaction_started
    with closing(open_analytics(store)) as connection:
        create_views(store, connection)
        connection.read_parquet(
            str(options.pilot.joinpath("raw_ontime_flights.parquet")),
            hive_partitioning=False,
        ).create_view("reference_flights")
        sample_counts = {}
        for name, target, predicate in (
            ("cancelled", "Cancelled", "Cancelled IS NOT NULL"),
            ("delayed", "DepDel15", "Cancelled = 0 AND DepDel15 IS NOT NULL"),
            ("diverted", "Diverted", "Cancelled = 0 AND Diverted IS NOT NULL"),
        ):
            actual = (
                connection.execute(
                    model_query("raw_ontime_flights", target, name, predicate)
                )
                .fetchdf()
                .convert_dtypes()
            )
            query = model_query("reference_flights", target, name, predicate).replace(
                "rowid", "source_rowid"
            )
            expected = connection.execute(query).fetchdf().convert_dtypes()
            pdt.assert_frame_equal(
                expected, actual, check_dtype=False, check_exact=True
            )
            sample_counts[name] = len(actual)
        weather_count = connection.execute(
            "SELECT COUNT(*) FROM weather_hourly"
        ).fetchone()
        assert weather_count == (record.rows,)
    before = time.perf_counter()
    checks = [
        store.is_complete(Partition(dataset=Dataset.WEATHER, year=2023, airport="ATL"))
        for _ in range(100)
    ]
    completion_seconds = (time.perf_counter() - before) / len(checks)
    assert all(checks)
    audit = audit_store(store)
    return {
        "flight_rows": flight.rows,
        "preserved_legacy_rows": flight.legacy_rows,
        "weather_rows": record.rows,
        "airport_rows": len(reference),
        "compacted_rows": compacted.rows,
        "compaction_seconds": compaction_seconds,
        "model_sample_rows": sample_counts,
        "model_inputs_equal": True,
        "all_existing_columns_equal": True,
        "completion_mean_seconds": completion_seconds,
        "audit": audit,
    }


def build_parser() -> argparse.ArgumentParser:
    defaults = VerificationOptions()
    parser = argparse.ArgumentParser()
    parser.add_argument("--pilot", type=Path, default=defaults.pilot)
    parser.add_argument("--evidence", type=Path, default=defaults.evidence)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    options = VerificationOptions(**vars(build_parser().parse_args(argv)))
    options.scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=options.scratch) as temporary:
        scratch = Path(temporary)
        real = verify_real_partitions(options, scratch.joinpath("real-partitions"))
        timings = verify_pipeline_timings(options, scratch)
    options.scratch.rmdir()
    options.evidence.write_text(
        json.dumps({"real_partitions": real, "offline_pipeline": timings}, indent=2)
        + "\n"
    )
    return ExitCode.SUCCESS


if __name__ == "__main__":
    raise SystemExit(main())
