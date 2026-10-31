from collections.abc import Generator
from contextlib import closing
from dataclasses import dataclass, replace

import duckdb
import pytest

from flightdelay.storage.analytics import audit_store, create_views, open_analytics
from flightdelay.storage.domain import BatchPair, Dataset, Partition
from flightdelay.storage.errors import (
    DataValidationError,
    IntegrityError,
    StorageSpaceError,
    WriterBusyError,
)
from flightdelay.storage.manifest import ManifestPort
from flightdelay.storage.schemas import PartitionRecord
from flightdelay.storage.store import Store, writer_lock


@dataclass(slots=True, kw_only=True, frozen=True)
class RejectingManifest:
    delegate: ManifestPort
    reason: str = "simulated manifest failure"

    def get(self, partition: Partition) -> PartitionRecord | None:
        return self.delegate.get(partition)

    def records(self, dataset: Dataset | None = None) -> tuple[PartitionRecord, ...]:
        return self.delegate.records(dataset)

    def commit(self, record: PartitionRecord) -> None:
        raise OSError(f"{self.reason}: {record.generation}")


def interrupted_pairs(pair: BatchPair) -> Generator[BatchPair]:
    yield pair
    raise OSError("interrupted download stream")


class TestPublication:
    @pytest.fixture
    def prior(
        self, published_flights: Store, flight_partition: Partition
    ) -> PartitionRecord:
        record = published_flights.manifest.get(flight_partition)
        assert record is not None
        return record

    @pytest.fixture
    def failing_store(self, published_flights: Store) -> Store:
        return replace(
            published_flights,
            manifest=RejectingManifest(delegate=published_flights.manifest),
        )

    @pytest.fixture
    def broken_stream(self, flight_pair: BatchPair) -> Generator[BatchPair]:
        return interrupted_pairs(flight_pair)

    @pytest.fixture
    def broken_schema(self, flight_pair: BatchPair) -> BatchPair:
        return replace(flight_pair, raw=flight_pair.raw.drop(["Distance"]))

    @pytest.fixture
    def damaged(self, published_flights: Store, prior: PartitionRecord) -> Store:
        published_flights.resolve(prior.raw.relative_path).write_bytes(b"corrupted")
        return published_flights

    @pytest.fixture
    def constrained(self, store: Store) -> Store:
        return replace(store, options=replace(store.options, max_stage_bytes=1))

    @pytest.fixture
    def old_connection(
        self, published_flights: Store
    ) -> Generator[duckdb.DuckDBPyConnection]:
        with closing(open_analytics(published_flights)) as connection:
            create_views(published_flights, connection)
            yield connection

    def test_rerun_skips_without_consuming_input(
        self,
        published_flights: Store,
        flight_partition: Partition,
        prior: PartitionRecord,
    ) -> None:
        result = published_flights.publish(flight_partition, (), provenance={})
        assert result == prior
        assert len(published_flights.manifest.records()) == 1

    def test_force_keeps_old_reader_valid_and_one_active_partition(
        self,
        published_flights: Store,
        flight_partition: Partition,
        flight_pair: BatchPair,
        prior: PartitionRecord,
        old_connection: duckdb.DuckDBPyConnection,
    ) -> None:
        result = published_flights.publish(
            flight_partition, (flight_pair,), provenance={}, force=True
        )
        assert result.generation != prior.generation
        assert len(published_flights.manifest.records()) == 1
        assert old_connection.execute(
            "SELECT COUNT(*) FROM raw_ontime_flights"
        ).fetchone() == (3,)

    def test_commit_failure_keeps_previous_generation(
        self,
        failing_store: Store,
        flight_partition: Partition,
        flight_pair: BatchPair,
        prior: PartitionRecord,
    ) -> None:
        with pytest.raises(OSError, match="manifest failure"):
            failing_store.publish(
                flight_partition, (flight_pair,), provenance={}, force=True
            )
        assert failing_store.manifest.get(flight_partition) == prior
        assert failing_store.is_complete(flight_partition)
        orphans = audit_store(failing_store)["unreferenced_files"]
        assert isinstance(orphans, list)
        assert len(orphans) == 2

    def test_stream_failure_does_not_publish(
        self,
        published_flights: Store,
        flight_partition: Partition,
        broken_stream: Generator[BatchPair],
        prior: PartitionRecord,
    ) -> None:
        with pytest.raises(OSError, match="interrupted"):
            published_flights.publish(
                flight_partition, broken_stream, provenance={}, force=True
            )
        assert published_flights.manifest.get(flight_partition) == prior
        assert audit_store(published_flights)["staging_entries"] == []

    def test_schema_drift_never_marks_complete(
        self,
        store: Store,
        flight_partition: Partition,
        broken_schema: BatchPair,
    ) -> None:
        with pytest.raises(DataValidationError, match="schema drift"):
            store.publish(flight_partition, (broken_schema,), provenance={})
        assert not store.is_complete(flight_partition)

    def test_corrupted_file_is_not_complete(
        self, damaged: Store, flight_partition: Partition
    ) -> None:
        assert not damaged.is_complete(flight_partition)
        with pytest.raises(IntegrityError):
            audit_store(damaged)

    def test_storage_guard_runs_before_publication(
        self, constrained: Store, flight_partition: Partition, flight_pair: BatchPair
    ) -> None:
        with pytest.raises(StorageSpaceError):
            constrained.publish(flight_partition, (flight_pair,), provenance={})
        assert constrained.manifest.get(flight_partition) is None

    def test_writer_lock_excludes_second_publisher(
        self, store: Store, flight_partition: Partition, flight_pair: BatchPair
    ) -> None:
        with writer_lock(store.options):
            with pytest.raises(WriterBusyError):
                store.publish(flight_partition, (flight_pair,), provenance={})
        assert store.manifest.get(flight_partition) is None
