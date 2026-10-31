import json
from importlib.resources import files
from typing import Literal

import pyarrow as pa
from pydantic import BaseModel, ConfigDict

from flightdelay.storage.domain import Dataset, Schemas


class FileInfo(BaseModel):
    model_config = ConfigDict(frozen=True)
    relative_path: str
    size_bytes: int
    mtime_ns: int
    sha256: str
    schema_sha256: str


class PartitionRecord(BaseModel):
    model_config = ConfigDict(frozen=True)
    dataset: Dataset
    partition_key: str
    year: int | None
    month: int | None
    airport: str | None
    generation: str
    rows: int
    expected_rows: int | None
    legacy_rows: int = 0
    schema_version: int
    raw: FileInfo
    cleaned: FileInfo
    provenance_json: str
    status: Literal["complete"] = "complete"


def flight_source_schema() -> pa.Schema:
    entries = json.loads(
        files("flightdelay.resources").joinpath("flights.json").read_text()
    )
    types = {"INTEGER": pa.int64(), "REAL": pa.float64(), "TEXT": pa.string()}
    return pa.schema([pa.field(name, types[kind]) for name, kind in entries])


def flight_schemas() -> Schemas:
    source = flight_source_schema()
    raw = pa.schema(
        [
            *source,
            pa.field("source_year", pa.int64()),
            pa.field("source_month", pa.int64()),
            pa.field("source_rowid", pa.int64()),
            pa.field("source_record_number", pa.int64()),
            pa.field("record_id", pa.string()),
        ]
    )
    return Schemas(
        raw=raw,
        cleaned=pa.schema(
            [
                *raw,
                pa.field("CRSDepHour", pa.int64()),
                pa.field("CRSArrHour", pa.int64()),
            ]
        ),
    )


def weather_schemas() -> Schemas:
    raw = pa.schema(
        [
            pa.field("airport_code", pa.string()),
            pa.field("weather_datetime", pa.string()),
            pa.field("latitude", pa.float64()),
            pa.field("longitude", pa.float64()),
            *[
                pa.field(name, pa.float64())
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
            ],
        ]
    )
    return Schemas(
        raw=raw,
        cleaned=pa.schema(
            [
                *raw,
                pa.field("weather_date", pa.string()),
                pa.field("weather_hour", pa.int64()),
            ]
        ),
    )


def airport_schemas() -> Schemas:
    schema = pa.schema(
        [
            *[
                pa.field(name, pa.string())
                for name in (
                    "airport_code",
                    "icao_code",
                    "airport_name",
                    "type",
                )
            ],
            pa.field("latitude", pa.float64()),
            pa.field("longitude", pa.float64()),
            pa.field("elevation_ft", pa.float64()),
            *[
                pa.field(name, pa.string())
                for name in (
                    "country",
                    "region",
                    "municipality",
                    "scheduled_service",
                )
            ],
        ]
    )
    return Schemas(raw=schema, cleaned=schema)


def get_schemas(dataset: Dataset) -> Schemas:
    factories = {
        Dataset.FLIGHTS: flight_schemas,
        Dataset.WEATHER: weather_schemas,
        Dataset.WEATHER_YEAR: weather_schemas,
        Dataset.AIRPORTS: airport_schemas,
    }
    return factories[dataset]()
