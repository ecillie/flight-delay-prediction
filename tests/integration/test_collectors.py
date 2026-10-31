import json
from collections.abc import Generator
from contextlib import closing
from dataclasses import replace

import pandas as pd
import pytest

from flightdelay.collection.domain import Downloader
from flightdelay.collection.schemas import WeatherResponse
from flightdelay.collection.services import (
    collect_airports,
    collect_flights,
    collect_weather,
)
from flightdelay.collection.transforms import prepare_weather_batches
from flightdelay.storage.analytics import (
    committed_records,
    compact_weather,
    create_views,
    open_analytics,
)
from flightdelay.storage.domain import BatchPair, Dataset, Partition
from flightdelay.storage.errors import DataValidationError
from flightdelay.storage.store import Store


class TestCollectors:
    @pytest.fixture
    def collected_flights(
        self, store: Store, bts_downloader: Downloader, flight_partition: Partition
    ) -> Store:
        collect_flights(store, bts_downloader, flight_partition)
        return store

    @pytest.fixture
    def damaged_download(
        self, collected_flights: Store, flight_partition: Partition
    ) -> Store:
        record = collected_flights.manifest.get(flight_partition)
        assert record is not None
        collected_flights.resolve(record.raw.relative_path).write_bytes(b"truncated")
        return collected_flights

    def test_download_write_and_rerun(
        self,
        collected_flights: Store,
        invalid_downloader: Downloader,
        flight_partition: Partition,
    ) -> None:
        record = collect_flights(
            collected_flights, invalid_downloader, flight_partition
        )
        assert record.rows == 3
        assert record.legacy_rows == 0
        assert collected_flights.is_complete(flight_partition)

    def test_airports_follow_committed_flights(
        self, collected_flights: Store, airport_downloader: Downloader
    ) -> None:
        record = collect_airports(collected_flights, airport_downloader)
        assert record.rows == 2

    def test_rerun_repairs_truncated_new_download(
        self,
        damaged_download: Store,
        bts_downloader: Downloader,
        flight_partition: Partition,
    ) -> None:
        record = collect_flights(damaged_download, bts_downloader, flight_partition)
        assert record.rows == 3
        assert damaged_download.is_complete(flight_partition)
        assert len(damaged_download.manifest.records(Dataset.FLIGHTS)) == 1

    def test_weather_preserves_timezone_and_skips_completed(
        self,
        store: Store,
        weather_downloader: Downloader,
        invalid_downloader: Downloader,
        weather_partition: Partition,
    ) -> None:
        record = collect_weather(
            store, weather_downloader, weather_partition, 33.6, -84.4
        )
        repeat = collect_weather(
            store, invalid_downloader, weather_partition, 33.6, -84.4
        )
        assert record == repeat
        assert record.rows == 8760
        assert record.expected_rows == 8760
        assert json.loads(record.provenance_json)["timezone"] == "America/New_York"

    def test_migrated_ids_are_not_overwritten_by_download(
        self,
        published_flights: Store,
        invalid_downloader: Downloader,
        flight_partition: Partition,
    ) -> None:
        with pytest.raises(DataValidationError, match="legacy IDs"):
            collect_flights(
                published_flights, invalid_downloader, flight_partition, force=True
            )


class TestWeatherValidation:
    @pytest.fixture
    def leap_pairs(self, weather_body: dict[str, object]) -> Generator[BatchPair]:
        response = WeatherResponse.model_validate(weather_body)
        values = {
            name: [1.0] * 8784 for name in response.hourly.model_dump(exclude={"time"})
        }
        values["time"] = (
            pd.date_range("2024-01-01", periods=8784, freq="h")
            .strftime("%Y-%m-%dT%H:%M")
            .tolist()
        )
        payload = WeatherResponse.model_validate({"hourly": values})
        partition = Partition(dataset=Dataset.WEATHER, year=2024, airport="ATL")
        return prepare_weather_batches(payload, partition, 33.6, -84.4, 25000)

    def test_leap_year_requires_and_keeps_all_hours(
        self, store: Store, leap_pairs: Generator[BatchPair]
    ) -> None:
        record = store.publish(
            Partition(dataset=Dataset.WEATHER, year=2024, airport="ATL"),
            leap_pairs,
            provenance={},
        )
        assert record.rows == record.expected_rows == 8784

    @pytest.fixture(
        params=[
            pytest.param("missing", id="missing-hour"),
            pytest.param("duplicate", id="duplicate-hour"),
        ]
    )
    def invalid_pairs(
        self,
        request: pytest.FixtureRequest,
        weather_body: dict[str, object],
        weather_partition: Partition,
    ) -> Generator[BatchPair]:
        response = WeatherResponse.model_validate(weather_body)
        values = response.hourly.model_dump()
        if request.param == "missing":
            values = {key: value[:-1] for key, value in values.items()}
        if request.param == "duplicate":
            times = list(values["time"])
            times[1] = times[0]
            values["time"] = times
        payload = WeatherResponse.model_validate({"hourly": values})
        return prepare_weather_batches(payload, weather_partition, 33.6, -84.4, 25000)

    def test_rejects_incomplete_or_duplicate_hours(
        self,
        store: Store,
        weather_partition: Partition,
        invalid_pairs: Generator[BatchPair],
    ) -> None:
        with pytest.raises(DataValidationError):
            store.publish(weather_partition, invalid_pairs, provenance={})
        assert not store.is_complete(weather_partition)


class TestWeatherCompaction:
    @pytest.fixture
    def compacted(
        self, published_weather: Store, weather_body: dict[str, object]
    ) -> Store:
        payload = WeatherResponse.model_validate(weather_body)
        partition = Partition(dataset=Dataset.WEATHER, year=2023, airport="JFK")
        pairs = prepare_weather_batches(
            payload, partition, 40.6, -73.7, published_weather.options.batch_rows
        )
        published_weather.publish(partition, pairs, provenance={})
        compact_weather(published_weather, 2023)
        return published_weather

    @pytest.fixture
    def updated(
        self,
        compacted: Store,
        weather_body: dict[str, object],
        weather_partition: Partition,
    ) -> Store:
        payload = WeatherResponse.model_validate(weather_body)
        hourly = payload.hourly.model_dump()
        hourly["temperature_2m"] = [77.0, *hourly["temperature_2m"][1:]]
        payload = WeatherResponse.model_validate({"hourly": hourly})
        pairs = prepare_weather_batches(
            payload, weather_partition, 33.6, -84.4, compacted.options.batch_rows
        )
        compacted.publish(weather_partition, pairs, provenance={}, force=True)
        return compacted

    def test_compaction_preserves_rows_without_double_counting(
        self, compacted: Store
    ) -> None:
        with closing(open_analytics(compacted)) as connection:
            create_views(compacted, connection)
            assert connection.execute(
                "SELECT COUNT(*) FROM weather_hourly"
            ).fetchone() == (17520,)
        assert len(compacted.manifest.records(Dataset.WEATHER_YEAR)) == 1

    def test_updated_airport_invalidates_compacted_snapshot(
        self, updated: Store
    ) -> None:
        with closing(open_analytics(updated)) as connection:
            create_views(updated, connection)
            assert connection.execute(
                "SELECT COUNT(*) FROM weather_hourly"
            ).fetchone() == (17520,)
            assert connection.execute(
                "SELECT temperature_2m FROM weather_hourly "
                "WHERE airport_code='ATL' ORDER BY weather_datetime LIMIT 1"
            ).fetchone() == (77.0,)
        assert all(
            record.dataset == Dataset.WEATHER
            for record in committed_records(updated, Dataset.WEATHER)
        )

    def test_compaction_rerun_is_idempotent(self, compacted: Store) -> None:
        before = compacted.manifest.get(
            Partition(dataset=Dataset.WEATHER_YEAR, year=2023)
        )
        assert compact_weather(compacted, 2023) == before

    def test_compaction_aligns_multiple_arrow_batches(self, compacted: Store) -> None:
        bounded = replace(
            compacted, options=replace(compacted.options, batch_rows=5000)
        )
        result = compact_weather(bounded, 2023, force=True)
        assert result.rows == 17520
        with closing(open_analytics(bounded)) as connection:
            create_views(bounded, connection)
            assert connection.execute(
                "SELECT COUNT(*) FROM weather_hourly"
            ).fetchone() == (17520,)
