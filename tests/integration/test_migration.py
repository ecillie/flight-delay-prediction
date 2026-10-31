import json
import os
import sqlite3
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from flightdelay.collection.domain import Downloader
from flightdelay.collection.schemas import WeatherResponse
from flightdelay.collection.transforms import (
    prepare_airport_pair,
    prepare_weather_batches,
)
from flightdelay.migration.domain import MigrationOptions
from flightdelay.migration.errors import (
    MigrationConflictError,
    SourceChangedError,
)
from flightdelay.migration.services import convert_history, estimate_storage
from flightdelay.migration.source import inspect_source, open_legacy
from flightdelay.migration.validation import validate_history
from flightdelay.storage.analytics import open_research_connection
from flightdelay.storage.domain import Dataset, Partition
from flightdelay.storage.errors import MissingDatasetError, StorageSpaceError
from flightdelay.storage.policy import scheduled_hours
from flightdelay.storage.store import Store, create_store


class TestHistoricalMigration:
    @pytest.fixture
    def options(
        self,
        tmp_path: Path,
        store: Store,
        flight_frame: pd.DataFrame,
        weather_body: dict[str, object],
        airport_downloader: Downloader,
    ) -> MigrationOptions:
        path = tmp_path.joinpath("legacy.sqlite")
        schema = (
            Path(__file__)
            .resolve()
            .parents[2]
            .joinpath("docs/data-validation/sqlite-schema.sql")
        )
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.executescript(schema.read_text())
            frame = flight_frame.copy()
            frame["source_year"] = 2023
            frame["source_month"] = 1
            frame["CRSDepHour"] = scheduled_hours(frame["CRSDepTime"])
            frame["CRSArrHour"] = scheduled_hours(frame["CRSArrTime"])
            frame.to_sql(
                "raw_ontime_flights", connection, if_exists="append", index=False
            )
            connection.executemany(
                "UPDATE raw_ontime_flights SET rowid=? WHERE rowid=?",
                ((10, 1), (20, 2), (21, 3)),
            )
            payload = WeatherResponse.model_validate(weather_body)
            pairs = prepare_weather_batches(
                payload,
                Partition(dataset=Dataset.WEATHER, year=2023, airport="ATL"),
                33.6,
                -84.4,
                25000,
            )
            for pair in pairs:
                pair.cleaned.to_pandas().to_sql(
                    "weather_hourly", connection, if_exists="append", index=False
                )
            reference = prepare_airport_pair(
                airport_downloader.fetch("fixture"), ("ATL", "JFK")
            )
            reference.cleaned.to_pandas().to_sql(
                "airports", connection, if_exists="append", index=False
            )
        return MigrationOptions(
            source=path, storage=store.options, evidence=tmp_path.joinpath("evidence")
        )

    @pytest.fixture
    def converted(self, options: MigrationOptions) -> MigrationOptions:
        with open_legacy(options) as source:
            inventory = inspect_source(source)
            estimate = estimate_storage(source, inventory, options)
            convert_history(source, inventory, estimate, options)
        return options

    @pytest.fixture
    def constrained(self, options: MigrationOptions) -> MigrationOptions:
        free = 1 << 60
        return replace(options, storage=replace(options.storage, min_free_bytes=free))

    @pytest.fixture
    def changed(self, converted: MigrationOptions) -> MigrationOptions:
        with closing(sqlite3.connect(converted.source)) as connection, connection:
            connection.execute(
                "UPDATE raw_ontime_flights SET Distance=999 WHERE rowid=10"
            )
        return converted

    @pytest.fixture
    def damaged(self, converted: MigrationOptions) -> MigrationOptions:
        store = create_store(converted.storage)
        record = store.manifest.get(
            Partition(dataset=Dataset.FLIGHTS, year=2023, month=1)
        )
        assert record is not None
        store.resolve(record.cleaned.relative_path).write_bytes(b"truncated")
        return converted

    def test_full_conversion_preserves_all_columns_and_samples(
        self, converted: MigrationOptions
    ) -> None:
        with open_legacy(converted) as source:
            inventory = inspect_source(source)
            result = validate_history(source, inventory, converted)
        assert result["complete"] is True
        assert result["source_digest_unchanged"] is True
        evidence = json.loads(
            converted.evidence.joinpath("phase-4-validation.json").read_text()
        )
        assert evidence["samples"][0]["rows"] == 2
        assert evidence["samples"][0]["target_counts"] == {"0": 1, "1": 1}

    def test_resume_does_not_append_or_rewrite(
        self, converted: MigrationOptions
    ) -> None:
        store = create_store(converted.storage)
        before = store.manifest.records()
        with open_legacy(converted) as source:
            inventory = inspect_source(source)
            estimate = estimate_storage(source, inventory, converted)
            result = convert_history(source, inventory, estimate, converted)
        assert create_store(converted.storage).manifest.records() == before
        partitions = result["partitions"]
        assert isinstance(partitions, list)
        assert all(item["resumed"] for item in partitions)

    def test_capacity_failure_never_exports_data(
        self, constrained: MigrationOptions
    ) -> None:
        with open_legacy(constrained) as source:
            inventory = inspect_source(source)
            estimate = estimate_storage(source, inventory, constrained)
            with pytest.raises(StorageSpaceError):
                convert_history(source, inventory, estimate, constrained)
        assert create_store(constrained.storage).manifest.records() == ()

    def test_different_source_cannot_resume_existing_partitions(
        self, changed: MigrationOptions
    ) -> None:
        with open_legacy(changed) as source:
            inventory = inspect_source(source)
            estimate = estimate_storage(source, inventory, changed)
            with pytest.raises(MigrationConflictError):
                convert_history(source, inventory, estimate, changed)

    def test_damaged_partition_is_rebuilt_on_resume(
        self, damaged: MigrationOptions
    ) -> None:
        with open_legacy(damaged) as source:
            inventory = inspect_source(source)
            estimate = estimate_storage(source, inventory, damaged)
            convert_history(source, inventory, estimate, damaged)
            assert validate_history(source, inventory, damaged)["complete"] is True

    def test_source_connection_is_read_only(self, options: MigrationOptions) -> None:
        with open_legacy(options) as source:
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                source.connection.execute("DELETE FROM airports")

    def test_source_metadata_change_is_detected(
        self, options: MigrationOptions
    ) -> None:
        with open_legacy(options) as source:
            stat = options.source.stat()
            os.utime(options.source, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1000000))
            with pytest.raises(SourceChangedError):
                source.assert_unchanged()

    def test_research_connection_loads_committed_legacy_ids(
        self, converted: MigrationOptions
    ) -> None:
        with closing(open_research_connection(converted.storage.root)) as connection:
            assert connection.execute(
                "SELECT rowid FROM raw_ontime_flights ORDER BY rowid"
            ).fetchall() == [(10,), (20,), (21,)]

    def test_missing_migration_does_not_create_an_empty_dataset(
        self, tmp_path: Path
    ) -> None:
        missing = tmp_path.joinpath("absent")
        with pytest.raises(MissingDatasetError):
            open_research_connection(missing)
        assert not missing.exists()
