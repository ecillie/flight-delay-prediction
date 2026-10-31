import pandas as pd
import pandas.testing as pdt
import pytest

from flightdelay.storage.domain import Dataset, Partition
from flightdelay.storage.errors import DataValidationError
from flightdelay.storage.policy import (
    expected_weather_rows,
    find_partition_violation,
    scheduled_hours,
)


class TestHours:
    valid_times = (0.0, 2359.0, 2400.0, None)
    expected_hours = (0, 23, 0, None)

    @pytest.fixture
    def source(self) -> pd.Series:
        return pd.Series(self.valid_times)

    @pytest.fixture
    def expected(self) -> pd.Series:
        return pd.Series(self.expected_hours, dtype="Int64")

    @pytest.fixture(
        params=[
            pytest.param(-1.0, id="negative"),
            pytest.param(2360.0, id="invalid-minute"),
            pytest.param(2401.0, id="after-midnight-sentinel"),
        ]
    )
    def invalid(self, request: pytest.FixtureRequest) -> pd.Series:
        return pd.Series([request.param])

    def test_preserves_midnight_and_null_semantics(
        self, source: pd.Series, expected: pd.Series
    ) -> None:
        pdt.assert_series_equal(scheduled_hours(source), expected)

    def test_rejects_invalid_hhmm(self, invalid: pd.Series) -> None:
        with pytest.raises(DataValidationError, match="HHMM"):
            scheduled_hours(invalid)


class TestPartitionPolicy:
    @pytest.fixture(
        params=[
            pytest.param(
                Partition(dataset=Dataset.FLIGHTS, year=2023, month=13),
                id="invalid-month",
            ),
            pytest.param(
                Partition(dataset=Dataset.WEATHER, year=2023, airport="../"),
                id="unsafe-airport",
            ),
            pytest.param(
                Partition(dataset=Dataset.WEATHER, airport="ATL"), id="missing-year"
            ),
            pytest.param(
                Partition(dataset=Dataset.AIRPORTS, year=2023),
                id="unexpected-reference-year",
            ),
        ]
    )
    def invalid(self, request: pytest.FixtureRequest) -> Partition:
        return request.param

    def test_rejects_invalid_partitions(self, invalid: Partition) -> None:
        assert find_partition_violation(invalid) is not None


class TestCalendar:
    @pytest.fixture(
        params=[
            pytest.param((2023, 8760), id="ordinary-year"),
            pytest.param((2024, 8784), id="leap-year"),
            pytest.param((1900, 8760), id="non-leap-century"),
        ]
    )
    def scenario(self, request: pytest.FixtureRequest) -> tuple[int, int]:
        return request.param

    def test_expected_distinct_hours(self, scenario: tuple[int, int]) -> None:
        assert expected_weather_rows(scenario[0]) == scenario[1]
