import calendar
import re
from dataclasses import dataclass
from itertools import pairwise

import pandas as pd

from flightdelay.storage.domain import BatchPair, Dataset, Partition
from flightdelay.storage.errors import DataValidationError, PartitionError
from flightdelay.storage.schemas import get_schemas


@dataclass(slots=True, kw_only=True, frozen=True)
class ValidationProgress:
    rows: int = 0
    legacy_rows: int = 0
    last_legacy_id: int | None = None


def find_partition_violation(partition: Partition) -> PartitionError | None:
    if partition.dataset == Dataset.AIRPORTS:
        if any(
            value is not None
            for value in (partition.year, partition.month, partition.airport)
        ):
            return PartitionError(
                partition="airports", reason="unexpected partition fields"
            )
        return None
    if partition.year is None or not 1900 <= partition.year <= 9998:
        return PartitionError(partition=str(partition.year), reason="invalid year")
    if partition.dataset == Dataset.FLIGHTS:
        if (
            partition.month is None
            or not 1 <= partition.month <= 12
            or partition.airport is not None
        ):
            return PartitionError(
                partition=str(partition.month), reason="invalid flight month"
            )
        return None
    if partition.month is not None:
        return PartitionError(partition=str(partition.month), reason="unexpected month")
    if partition.dataset == Dataset.WEATHER:
        if (
            partition.airport is None
            or re.fullmatch(r"[A-Z0-9]{3}", partition.airport) is None
        ):
            return PartitionError(
                partition=str(partition.airport), reason="invalid airport"
            )
        return None
    if partition.airport is not None:
        return PartitionError(partition=partition.airport, reason="unexpected airport")
    return None


def expected_weather_rows(year: int) -> int:
    return 24 * (365 + calendar.isleap(year))


def scheduled_hours(times: pd.Series) -> pd.Series:
    invalid = times.notna() & ~(
        (times.between(0, 2359) & times.mod(100).lt(60)) | times.eq(2400)
    )
    if invalid.any():
        raise DataValidationError(
            dataset="flights", reason="invalid scheduled HHMM time"
        )
    return (times.mask(times.eq(2400), 0) // 100).astype("Int64")


def find_pair_violation(
    partition: Partition, pair: BatchPair
) -> DataValidationError | None:
    schemas = get_schemas(partition.dataset)
    if not pair.raw.schema.equals(schemas.raw, check_metadata=False):
        return DataValidationError(dataset=partition.dataset, reason="raw schema drift")
    if not pair.cleaned.schema.equals(schemas.cleaned, check_metadata=False):
        return DataValidationError(
            dataset=partition.dataset, reason="cleaned schema drift"
        )
    if pair.raw.num_rows == 0 or pair.raw.num_rows != pair.cleaned.num_rows:
        return DataValidationError(
            dataset=partition.dataset, reason="empty or unequal layers"
        )
    if not pair.raw.equals(pair.cleaned.select(schemas.raw.names)):
        return DataValidationError(
            dataset=partition.dataset, reason="raw values changed in cleaning"
        )
    return None


def validate_flight_pair(
    partition: Partition, pair: BatchPair, progress: ValidationProgress
) -> ValidationProgress:
    frame = pair.cleaned.to_pandas()
    if frame[["Year", "Month", "FlightDate"]].isna().any().any():
        raise DataValidationError(dataset="flights", reason="missing partition keys")
    dates = pd.to_datetime(frame["FlightDate"], format="%Y-%m-%d", errors="coerce")
    if (
        dates.isna().any()
        or not (
            dates.dt.year.eq(partition.year) & dates.dt.month.eq(partition.month)
        ).all()
    ):
        raise DataValidationError(dataset="flights", reason="invalid flight dates")
    numbers = list(range(progress.rows + 1, progress.rows + len(frame) + 1))
    expected_ids = [
        f"bts:{partition.year}:{partition.month:02d}:{number}" for number in numbers
    ]
    checks = (
        frame["Year"].eq(partition.year).all(),
        frame["Month"].eq(partition.month).all(),
        frame["source_year"].eq(partition.year).all(),
        frame["source_month"].eq(partition.month).all(),
        frame["source_record_number"].tolist() == numbers,
        frame["record_id"].tolist() == expected_ids,
        frame["FlightDate"]
        .str.startswith(f"{partition.year}-{partition.month:02d}-")
        .all(),
    )
    if not all(checks):
        raise DataValidationError(
            dataset="flights", reason="partition or record identifier mismatch"
        )
    ids = [int(value) for value in frame["source_rowid"].dropna()]
    if any(value <= 0 for value in ids) or any(
        left >= right for left, right in pairwise(ids)
    ):
        raise DataValidationError(
            dataset="flights", reason="legacy IDs are not unique and ordered"
        )
    if (
        len(ids) > 0
        and progress.last_legacy_id is not None
        and ids[0] <= progress.last_legacy_id
    ):
        raise DataValidationError(
            dataset="flights", reason="duplicate legacy IDs across batches"
        )
    for field in ("Dep", "Arr"):
        times = frame[f"CRS{field}Time"]
        hours = scheduled_hours(times)
        actual = frame[f"CRS{field}Hour"].astype("Int64")
        if not actual.equals(hours):
            raise DataValidationError(dataset="flights", reason="derived hour mismatch")
    last_id = progress.last_legacy_id if len(ids) == 0 else ids[-1]
    return ValidationProgress(
        rows=progress.rows + len(frame),
        legacy_rows=progress.legacy_rows + len(ids),
        last_legacy_id=last_id,
    )


def validate_weather_pair(
    partition: Partition, pair: BatchPair, progress: ValidationProgress
) -> ValidationProgress:
    frame = pair.cleaned.to_pandas()
    start = f"{partition.year}-01-01"
    expected = pd.date_range(
        start, periods=expected_weather_rows(partition.year_value), freq="h"
    )
    times = expected[progress.rows : progress.rows + len(frame)]
    checks = (
        frame["airport_code"].eq(partition.airport).all(),
        frame["weather_datetime"].tolist()
        == times.strftime("%Y-%m-%d %H:%M:%S").tolist(),
        frame["weather_date"].tolist() == times.strftime("%Y-%m-%d").tolist(),
        frame["weather_hour"].tolist() == times.hour.tolist(),
    )
    if not all(checks):
        raise DataValidationError(
            dataset="weather", reason="missing, duplicate, or incorrect local hours"
        )
    return ValidationProgress(rows=progress.rows + len(frame))


def validate_airport_pair(
    pair: BatchPair, progress: ValidationProgress
) -> ValidationProgress:
    codes = pair.cleaned.column("airport_code")
    if codes.null_count > 0 or len(codes.unique()) != len(codes):
        raise DataValidationError(
            dataset="airports", reason="missing or duplicate airport codes"
        )
    if progress.rows > 0:
        raise DataValidationError(
            dataset="airports", reason="reference must be one batch"
        )
    if not all(
        re.fullmatch(r"[A-Z0-9]{3}", code) is not None for code in codes.to_pylist()
    ):
        raise DataValidationError(dataset="airports", reason="invalid airport codes")
    return ValidationProgress(rows=len(codes))


def validate_pair(
    partition: Partition, pair: BatchPair, progress: ValidationProgress
) -> ValidationProgress:
    if error := find_pair_violation(partition, pair):
        raise error
    if partition.dataset == Dataset.FLIGHTS:
        return validate_flight_pair(partition, pair, progress)
    if partition.dataset == Dataset.WEATHER:
        return validate_weather_pair(partition, pair, progress)
    if partition.dataset == Dataset.AIRPORTS:
        return validate_airport_pair(pair, progress)
    return ValidationProgress(rows=progress.rows + pair.cleaned.num_rows)
