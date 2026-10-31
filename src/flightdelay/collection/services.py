import hashlib
import json
import logging
from contextlib import closing

from flightdelay.collection.domain import Downloader, NetworkOptions, RequestParams
from flightdelay.collection.schemas import WeatherResponse
from flightdelay.collection.transforms import (
    flight_csv_frames,
    prepare_airport_pair,
    prepare_flight_batches,
    prepare_weather_batches,
)
from flightdelay.storage.analytics import (
    committed_records,
    open_analytics,
    register_dataset,
)
from flightdelay.storage.domain import Dataset, Partition
from flightdelay.storage.errors import DataValidationError, MissingDatasetError
from flightdelay.storage.policy import expected_weather_rows, find_partition_violation
from flightdelay.storage.schemas import PartitionRecord
from flightdelay.storage.store import Store

logger = logging.getLogger(__name__)


def collect_flights(
    store: Store,
    downloader: Downloader,
    partition: Partition,
    *,
    options: NetworkOptions | None = None,
    force: bool = False,
) -> PartitionRecord:
    settings = options if options is not None else NetworkOptions()
    if error := find_partition_violation(partition):
        raise error
    if not force and store.is_complete(partition):
        logger.info("Skipping completed flights %s", partition.key)
        record = store.manifest.get(partition)
        assert record is not None
        return record
    prior = store.manifest.get(partition)
    if prior is not None and prior.legacy_rows > 0:
        raise DataValidationError(
            dataset="flights",
            reason=(
                "restore migrated legacy IDs from SQLite; "
                "downloading would renumber them"
            ),
        )
    name = (
        "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_"
        f"{partition.year}_{partition.month}.zip"
    )
    url = f"{settings.bts_url}/{name}"
    logger.info("Downloading flights %s", partition.key)
    payload = downloader.fetch(url)
    pairs = prepare_flight_batches(
        flight_csv_frames(payload, store.options.batch_rows), partition
    )
    return store.publish(
        partition,
        pairs,
        force=force,
        provenance={
            "provider": "BTS",
            "url": url,
            "source_sha256": hashlib.sha256(payload).hexdigest(),
            "identifiers": (
                "new-download source_rowid is null; "
                "source_record_number follows CSV order"
            ),
        },
    )


def get_airport_codes(store: Store) -> tuple[str, ...]:
    if len(committed_records(store, Dataset.FLIGHTS)) == 0:
        raise MissingDatasetError(dataset="flights")
    with closing(open_analytics(store)) as connection:
        register_dataset(
            store,
            connection,
            Dataset.FLIGHTS,
            "raw_ontime_flights",
            committed_records(store, Dataset.FLIGHTS),
        )
        rows = connection.execute(
            "SELECT Origin AS airport_code FROM raw_ontime_flights "
            "WHERE Origin IS NOT NULL UNION SELECT Dest FROM raw_ontime_flights "
            "WHERE Dest IS NOT NULL ORDER BY airport_code"
        ).fetchall()
    return tuple(row[0] for row in rows)


def collect_airports(
    store: Store,
    downloader: Downloader,
    *,
    options: NetworkOptions | None = None,
    force: bool = False,
) -> PartitionRecord:
    settings = options if options is not None else NetworkOptions()
    partition = Partition(dataset=Dataset.AIRPORTS)
    codes = get_airport_codes(store)
    prior = store.manifest.get(partition)
    if prior is not None and not force and store.is_complete(partition):
        if json.loads(prior.provenance_json).get("airport_codes") == list(codes):
            return prior
    payload = downloader.fetch(settings.airports_url)
    logger.info("Preparing airport reference for %s codes", len(codes))
    pair = prepare_airport_pair(payload, codes)
    return store.publish(
        partition,
        (pair,),
        force=True,
        expected_rows=len(codes),
        provenance={
            "provider": "OurAirports",
            "url": settings.airports_url,
            "source_sha256": hashlib.sha256(payload).hexdigest(),
            "airport_codes": codes,
        },
    )


def airport_coordinates(
    store: Store, airport: str | None = None
) -> tuple[tuple[str, float, float], ...]:
    if len(committed_records(store, Dataset.AIRPORTS)) == 0:
        raise MissingDatasetError(dataset="airports")
    with closing(open_analytics(store)) as connection:
        register_dataset(
            store,
            connection,
            Dataset.AIRPORTS,
            "airports",
            committed_records(store, Dataset.AIRPORTS),
        )
        rows = connection.execute(
            "SELECT airport_code, latitude, longitude FROM airports "
            "WHERE latitude IS NOT NULL AND longitude IS NOT NULL ORDER BY airport_code"
        ).fetchall()
    selected = tuple(row for row in rows if airport is None or row[0] == airport)
    if airport is not None and len(selected) == 0:
        raise DataValidationError(
            dataset="airports", reason=f"no coordinates for {airport}"
        )
    return selected


def collect_weather(
    store: Store,
    downloader: Downloader,
    partition: Partition,
    latitude: float,
    longitude: float,
    *,
    options: NetworkOptions | None = None,
    force: bool = False,
) -> PartitionRecord:
    settings = options if options is not None else NetworkOptions()
    if error := find_partition_violation(partition):
        raise error
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise DataValidationError(dataset="weather", reason="invalid coordinates")
    if not force and store.is_complete(partition):
        logger.info("Skipping completed weather %s", partition.key)
        record = store.manifest.get(partition)
        assert record is not None
        return record
    parameters: RequestParams = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": f"{partition.year}-01-01",
        "end_date": f"{partition.year}-12-31",
        "hourly": (
            "temperature_2m,precipitation,rain,snowfall,wind_speed_10m,"
            "wind_gusts_10m,visibility,cloud_cover"
        ),
        "timezone": "auto",
    }
    body = downloader.fetch(settings.weather_url, parameters)
    response = WeatherResponse.model_validate_json(body)
    pairs = prepare_weather_batches(
        response, partition, latitude, longitude, store.options.batch_rows
    )
    return store.publish(
        partition,
        pairs,
        force=force,
        expected_rows=expected_weather_rows(partition.year_value),
        provenance={
            "provider": "Open-Meteo",
            "url": settings.weather_url,
            "request": dict(parameters),
            "source_sha256": hashlib.sha256(body).hexdigest(),
            "timezone": response.timezone,
            "timezone_abbreviation": response.timezone_abbreviation,
            "utc_offset_seconds": response.utc_offset_seconds,
        },
    )
