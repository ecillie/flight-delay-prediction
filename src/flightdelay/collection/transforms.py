import zipfile
from collections.abc import Generator, Iterable
from io import BytesIO

import pandas as pd
import pyarrow as pa

from flightdelay.collection.schemas import WeatherResponse
from flightdelay.storage.domain import BatchPair, Dataset, Partition
from flightdelay.storage.errors import DataValidationError
from flightdelay.storage.policy import scheduled_hours
from flightdelay.storage.schemas import flight_source_schema, get_schemas


def flight_csv_frames(payload: bytes, batch_rows: int) -> Generator[pd.DataFrame]:
    schema = pa.unify_schemas(
        [flight_source_schema(), flight_source_schema(marketing=True)]
    )
    dtypes = {"int64": "Int64", "double": "Float64", "string": "string"}
    columns = {field.name: dtypes[str(field.type)] for field in schema}
    columns["Operating_Airline "] = "string"
    with zipfile.ZipFile(BytesIO(payload)) as archive:
        csv_files = [
            name for name in archive.namelist() if name.lower().endswith(".csv")
        ]
        if len(csv_files) != 1:
            raise DataValidationError(
                dataset="flights", reason="expected exactly one CSV in ZIP"
            )
        with archive.open(csv_files[0]) as handle:
            for frame in pd.read_csv(handle, chunksize=batch_rows, dtype=columns):
                frame = frame.rename(columns=str.strip)
                source = flight_source_schema(
                    marketing="Marketing_Airline_Network" in frame
                )
                trailing = f"Unnamed: {len(source)}"
                if trailing in frame.columns:
                    if frame[trailing].notna().any():
                        raise DataValidationError(
                            dataset="flights", reason="unexpected trailing column data"
                        )
                    frame = frame.drop(columns=[trailing])
                yield frame


def prepare_flight_pair(
    frame: pd.DataFrame,
    partition: Partition,
    first_record: int,
    legacy_ids: pa.Array | None = None,
) -> BatchPair:
    schema = flight_source_schema(marketing="Marketing_Airline_Network" in frame)
    if set(frame.columns) != set(schema.names):
        raise DataValidationError(
            dataset="flights", reason="source columns differ from schema"
        )
    raw = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)
    count = len(frame)
    numbers = list(range(first_record, first_record + count))
    identifiers = legacy_ids if legacy_ids is not None else pa.nulls(count, pa.int64())
    additions = (
        ("source_year", pa.array([partition.year] * count, pa.int64())),
        ("source_month", pa.array([partition.month] * count, pa.int64())),
        ("source_rowid", identifiers),
        ("source_record_number", pa.array(numbers, pa.int64())),
        (
            "record_id",
            pa.array(
                [
                    f"bts:{partition.year}:{partition.month:02d}:{number}"
                    for number in numbers
                ]
            ),
        ),
    )
    for name, array in additions:
        raw = raw.append_column(name, array)
    schemas = get_schemas(Dataset.FLIGHTS)
    for field in schemas.raw:
        if field.name not in raw.column_names:
            raw = raw.append_column(field, pa.nulls(count, field.type))
    raw = raw.select(schemas.raw.names).cast(schemas.raw)
    cleaned = raw.append_column(
        "CRSDepHour", pa.array(scheduled_hours(frame["CRSDepTime"]), pa.int64())
    )
    cleaned = cleaned.append_column(
        "CRSArrHour", pa.array(scheduled_hours(frame["CRSArrTime"]), pa.int64())
    )
    return BatchPair(raw=raw, cleaned=cleaned.cast(schemas.cleaned))


def prepare_flight_batches(
    frames: Iterable[pd.DataFrame], partition: Partition
) -> Generator[BatchPair]:
    first_record = 1
    for frame in frames:
        yield prepare_flight_pair(frame, partition, first_record)
        first_record += len(frame)


def frame_pairs(
    raw: pa.Table, cleaned: pa.Table, batch_rows: int
) -> Generator[BatchPair]:
    for start in range(0, raw.num_rows, batch_rows):
        yield BatchPair(
            raw=raw.slice(start, batch_rows), cleaned=cleaned.slice(start, batch_rows)
        )


def prepare_weather_batches(
    payload: WeatherResponse,
    partition: Partition,
    latitude: float,
    longitude: float,
    batch_rows: int,
) -> Generator[BatchPair]:
    values = payload.hourly.model_dump(exclude={"time"})
    if any(len(column) != len(payload.hourly.time) for column in values.values()):
        raise DataValidationError(
            dataset="weather", reason="hourly arrays have unequal lengths"
        )
    times = pd.to_datetime(pd.Series(payload.hourly.time))
    if times.dt.tz is not None:
        raise DataValidationError(
            dataset="weather",
            reason="expected local timestamps without timezone conversion",
        )
    frame = pd.DataFrame(values)
    frame["airport_code"] = partition.airport
    frame["latitude"] = latitude
    frame["longitude"] = longitude
    frame["weather_datetime"] = times.dt.strftime("%Y-%m-%d %H:%M:%S")
    schemas = get_schemas(Dataset.WEATHER)
    raw = pa.Table.from_pandas(frame, schema=schemas.raw, preserve_index=False)
    frame["weather_date"] = times.dt.strftime("%Y-%m-%d")
    frame["weather_hour"] = times.dt.hour
    cleaned = pa.Table.from_pandas(frame, schema=schemas.cleaned, preserve_index=False)
    yield from frame_pairs(raw, cleaned, batch_rows)


def prepare_airport_pair(payload: bytes, codes: tuple[str, ...]) -> BatchPair:
    names = {
        "iata_code": "airport_code",
        "icao_code": "icao_code",
        "name": "airport_name",
        "type": "type",
        "latitude_deg": "latitude",
        "longitude_deg": "longitude",
        "elevation_ft": "elevation_ft",
        "iso_country": "country",
        "iso_region": "region",
        "municipality": "municipality",
        "scheduled_service": "scheduled_service",
    }
    frame = pd.read_csv(
        BytesIO(payload), dtype={"iata_code": "string", "icao_code": "string"}
    )
    if not set(names).issubset(frame.columns):
        raise DataValidationError(dataset="airports", reason="source columns missing")
    frame["iata_code"] = frame["iata_code"].str.upper()
    frame = frame.loc[frame["iata_code"].isin(codes), list(names)].rename(columns=names)
    reference = pd.DataFrame({"airport_code": codes}).merge(
        frame, on="airport_code", how="left", validate="one_to_one"
    )
    schema = get_schemas(Dataset.AIRPORTS).raw
    table = pa.Table.from_pandas(reference, schema=schema, preserve_index=False)
    return BatchPair(raw=table, cleaned=table)
