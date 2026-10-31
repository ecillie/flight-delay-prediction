import subprocess
import sys
from pathlib import Path

import pytest

from flightdelay.storage.analytics import audit_store
from flightdelay.storage.domain import BatchPair, Partition
from flightdelay.storage.schemas import PartitionRecord
from flightdelay.storage.store import Store


class TestCrashRecovery:
    @pytest.fixture
    def prior(
        self, published_flights: Store, flight_partition: Partition
    ) -> PartitionRecord:
        record = published_flights.manifest.get(flight_partition)
        assert record is not None
        return record

    @pytest.fixture(
        params=[
            pytest.param("mid-write", id="during-file-write"),
            pytest.param("before-commit", id="after-rename-before-commit"),
        ]
    )
    def crashed(
        self,
        request: pytest.FixtureRequest,
        published_flights: Store,
        prior: PartitionRecord,
    ) -> Store:
        result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("crash_worker.py")),
                str(published_flights.options.root),
                request.param,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 71, result.stderr
        return published_flights

    def test_process_crash_preserves_previous_batch_and_allows_retry(
        self,
        crashed: Store,
        prior: PartitionRecord,
        flight_partition: Partition,
        flight_pair: BatchPair,
    ) -> None:
        assert crashed.manifest.get(flight_partition) == prior
        assert crashed.is_complete(flight_partition)
        result = crashed.publish(
            flight_partition, (flight_pair,), provenance={}, force=True
        )
        assert result.generation != prior.generation
        assert result.rows == 3
        assert audit_store(crashed)["complete_partitions"] == 1
