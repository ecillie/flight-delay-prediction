import json
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from flightdelay.collection.domain import RequestParams
from flightdelay.collection.schemas import WeatherResponse
from flightdelay.collection.transforms import (
    prepare_flight_pair,
    prepare_weather_batches,
)
from flightdelay.storage.domain import BatchPair, Dataset, Partition, StorageOptions
from flightdelay.storage.schemas import flight_source_schema
from flightdelay.storage.store import Store, create_store


@dataclass(slots=True, kw_only=True, frozen=True)
class PayloadDownloader:
    payload: bytes

    def fetch(self, url: str, params: RequestParams | None = None) -> bytes:
        return self.payload


@pytest.fixture
def store(tmp_path: Path) -> Store:
    return create_store(
        StorageOptions(
            root=tmp_path.joinpath("lake"),
            min_free_bytes=0,
            max_stage_bytes=16_777_216,
        )
    )


@pytest.fixture
def flight_partition() -> Partition:
    return Partition(dataset=Dataset.FLIGHTS, year=2023, month=1)


@pytest.fixture
def flight_frame() -> pd.DataFrame:
    frame = pd.DataFrame({field.name: [None] * 3 for field in flight_source_schema()})
    values = {
        "Year": [2023] * 3,
        "Month": [1] * 3,
        "FlightDate": ["2023-01-01"] * 3,
        "Origin": ["ATL", "JFK", "ATL"],
        "Dest": ["JFK", "ATL", "JFK"],
        "CRSDepTime": [0, 2359, 2400],
        "CRSArrTime": [100, 100, 100],
        "Distance": [100.0, None, 200.0],
        "Cancelled": [0.0, 1.0, 0.0],
        "DepDel15": [0.0, None, 1.0],
        "Diverted": [0.0, 0.0, 1.0],
    }
    for name, column in values.items():
        frame[name] = column
    return frame


@pytest.fixture
def flight_pair(flight_frame: pd.DataFrame, flight_partition: Partition) -> BatchPair:
    return prepare_flight_pair(
        flight_frame, flight_partition, 1, pa.array([10, 20, 21], pa.int64())
    )


@pytest.fixture
def published_flights(
    store: Store, flight_partition: Partition, flight_pair: BatchPair
) -> Store:
    store.publish(flight_partition, (flight_pair,), provenance={"source": "fixture"})
    return store


@pytest.fixture
def bts_downloader(flight_frame: pd.DataFrame) -> PayloadDownloader:
    stream = BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("flights.csv", flight_frame.to_csv(index=False))
    return PayloadDownloader(payload=stream.getvalue())


@pytest.fixture
def invalid_downloader() -> PayloadDownloader:
    return PayloadDownloader(payload=b"must not be downloaded for a completed batch")


@pytest.fixture
def airport_downloader() -> PayloadDownloader:
    frame = pd.DataFrame(
        {
            "iata_code": ["ATL", "JFK"],
            "icao_code": ["KATL", "KJFK"],
            "name": ["Atlanta", "New York"],
            "type": ["large_airport"] * 2,
            "latitude_deg": [33.6, 40.6],
            "longitude_deg": [-84.4, -73.7],
            "elevation_ft": [1000.0, 13.0],
            "iso_country": ["US"] * 2,
            "iso_region": ["US-GA", "US-NY"],
            "municipality": ["Atlanta", "New York"],
            "scheduled_service": ["yes"] * 2,
        }
    )
    return PayloadDownloader(payload=frame.to_csv(index=False).encode())


@pytest.fixture
def weather_body() -> dict[str, object]:
    times = pd.date_range("2023-01-01", periods=8760, freq="h")
    hourly = {
        name: [1.0] * len(times)
        for name in (
            "temperature_2m",
            "precipitation",
            "rain",
            "snowfall",
            "wind_speed_10m",
            "wind_gusts_10m",
            "visibility",
            "cloud_cover",
        )
    }
    hourly["time"] = times.strftime("%Y-%m-%dT%H:%M").tolist()
    return {
        "hourly": hourly,
        "timezone": "America/New_York",
        "utc_offset_seconds": -18000,
    }


@pytest.fixture
def weather_downloader(weather_body: dict[str, object]) -> PayloadDownloader:
    return PayloadDownloader(payload=json.dumps(weather_body).encode())


@pytest.fixture
def weather_partition() -> Partition:
    return Partition(dataset=Dataset.WEATHER, year=2023, airport="ATL")


@pytest.fixture
def published_weather(
    store: Store, weather_body: dict[str, object], weather_partition: Partition
) -> Store:
    payload = WeatherResponse.model_validate(weather_body)
    pairs = prepare_weather_batches(
        payload, weather_partition, 33.6, -84.4, store.options.batch_rows
    )
    store.publish(weather_partition, pairs, provenance={"source": "fixture"})
    return store
