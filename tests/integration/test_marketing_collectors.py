import json
import zipfile
from contextlib import closing
from dataclasses import dataclass
from io import BytesIO

import pandas as pd
import pyarrow.parquet as pq
import pytest

from flightdelay.cli import Command, RunOptions, find_option_violation
from flightdelay.collection.domain import Downloader, RequestParams
from flightdelay.collection.services import collect_flights
from flightdelay.storage.analytics import audit_store, create_views, open_analytics
from flightdelay.storage.domain import Dataset, Partition
from flightdelay.storage.schemas import flight_schemas, flight_source_schema
from flightdelay.storage.store import Store
from flightdelay.storage.util import file_hash, schema_hash


@dataclass(slots=True, kw_only=True, frozen=True)
class MarketingDownloader:
    payload: bytes
    filename: str

    def fetch(self, url: str, params: RequestParams | None = None) -> bytes:
        assert url.endswith(self.filename)
        return self.payload


class TestMarketingCollectors:
    @pytest.fixture(
        params=[
            pytest.param((2023, 2), id="normal-archive"),
            pytest.param((2023, 12), id="parenthesized-archive"),
        ]
    )
    def marketing_partition(self, request: pytest.FixtureRequest) -> Partition:
        year, month = request.param
        return Partition(dataset=Dataset.FLIGHTS, year=year, month=month)

    @pytest.fixture
    def marketing_downloader(
        self, marketing_frame: pd.DataFrame, marketing_partition: Partition
    ) -> MarketingDownloader:
        schema = flight_source_schema(marketing=True)
        frame = marketing_frame.reindex(columns=schema.names)
        frame["Month"] = marketing_partition.month
        frame["FlightDate"] = (
            f"{marketing_partition.year}-{marketing_partition.month:02d}-01"
        )
        frame["Marketing_Airline_Network"] = ["DL", "AA", "UA"]
        frame["DOT_ID_Marketing_Airline"] = [19790, 19805, 19977]
        frame["IATA_Code_Marketing_Airline"] = ["DL", "AA", "UA"]
        frame["Flight_Number_Marketing_Airline"] = [123, 456, 789]
        frame["Operating_Airline"] = ["9E", "OH", "OO"]
        frame["Duplicate"] = ["N", "N", "N"]
        frame = frame.rename(columns={"Operating_Airline": "Operating_Airline "})
        frame[""] = None
        stream = BytesIO()
        with zipfile.ZipFile(stream, "w") as archive:
            archive.writestr("marketing.csv", frame.to_csv(index=False))
        beginning = "Beginning_January_2018"
        if marketing_partition.month == 12:
            beginning = f"({beginning})"
        return MarketingDownloader(
            payload=stream.getvalue(),
            filename=(
                f"On_Time_Marketing_Carrier_On_Time_Performance_{beginning}_"
                f"{marketing_partition.year}_{marketing_partition.month}.zip"
            ),
        )

    @pytest.fixture
    def collected(
        self,
        published_flights: Store,
        marketing_partition: Partition,
        marketing_downloader: MarketingDownloader,
    ) -> Store:
        collect_flights(published_flights, marketing_downloader, marketing_partition)
        return published_flights

    def test_preserves_carrier_fields_in_both_layers(
        self, collected: Store, marketing_partition: Partition
    ) -> None:
        record = collected.manifest.get(marketing_partition)
        assert record is not None
        assert record.rows == 3
        for info in (record.raw, record.cleaned):
            table = pq.ParquetFile(collected.resolve(info.relative_path)).read()
            assert table["Marketing_Airline_Network"].to_pylist() == ["DL", "AA", "UA"]
            assert table["Operating_Airline"].to_pylist() == ["9E", "OH", "OO"]
            assert table["DOT_ID_Marketing_Airline"].to_pylist() == [
                19790,
                19805,
                19977,
            ]
            assert table["Flight_Number_Marketing_Airline"].to_pylist() == [
                123,
                456,
                789,
            ]
            assert table["record_id"][0].as_py().startswith("bts:")
        assert "Marketing_Carrier" in json.loads(record.provenance_json)["url"]
        assert len(collected.manifest.records(Dataset.FLIGHTS)) == 2

    def test_view_exposes_marketing_and_reporting_rows(self, collected: Store) -> None:
        with closing(open_analytics(collected)) as connection:
            create_views(collected, connection)
            assert connection.execute(
                "SELECT Marketing_Airline_Network, Operating_Airline "
                "FROM raw_ontime_flights WHERE Marketing_Airline_Network IS NOT NULL "
                "ORDER BY source_record_number"
            ).fetchall() == [("DL", "9E"), ("AA", "OH"), ("UA", "OO")]
            assert connection.execute(
                "SELECT COUNT(*) FROM raw_ontime_flights"
            ).fetchone() == (6,)

    def test_completed_partition_skips_download(
        self,
        collected: Store,
        marketing_partition: Partition,
        invalid_downloader: Downloader,
    ) -> None:
        before = collected.manifest.get(marketing_partition)
        assert (
            collect_flights(collected, invalid_downloader, marketing_partition)
            == before
        )

    def test_force_replaces_marketing_partition(
        self,
        collected: Store,
        marketing_partition: Partition,
        marketing_downloader: MarketingDownloader,
    ) -> None:
        before = collected.manifest.get(marketing_partition)
        assert before is not None
        after = collect_flights(
            collected, marketing_downloader, marketing_partition, force=True
        )
        assert after.generation != before.generation
        assert len(collected.manifest.records(Dataset.FLIGHTS)) == 2

    def test_cli_accepts_marketing_command(
        self, marketing_partition: Partition
    ) -> None:
        assert (
            find_option_violation(
                RunOptions(
                    command=Command.FLIGHTS,
                    year=marketing_partition.year,
                    month=marketing_partition.month,
                )
            )
            is None
        )


class TestExistingFlightFiles:
    @pytest.fixture
    def legacy_store(
        self, published_flights: Store, flight_partition: Partition
    ) -> Store:
        record = published_flights.manifest.get(flight_partition)
        assert record is not None
        schemas = flight_schemas(legacy=True)
        updates = {}
        for layer, schema in (("raw", schemas.raw), ("cleaned", schemas.cleaned)):
            info = getattr(record, layer)
            path = published_flights.resolve(info.relative_path)
            table = pq.ParquetFile(path).read().select(schema.names)
            pq.write_table(table, path)
            stat = path.stat()
            updates[layer] = info.model_copy(
                update={
                    "size_bytes": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                    "sha256": file_hash(path),
                    "schema_sha256": schema_hash(schema),
                }
            )
        published_flights.manifest.commit(record.model_copy(update=updates))
        return published_flights

    def test_old_files_remain_complete_and_skip_download(
        self,
        legacy_store: Store,
        flight_partition: Partition,
        invalid_downloader: Downloader,
    ) -> None:
        assert legacy_store.is_complete(flight_partition)
        assert (
            collect_flights(legacy_store, invalid_downloader, flight_partition).rows
            == 3
        )
        audit_store(legacy_store)

    def test_old_view_preserves_ids_and_exposes_null_marketing_fields(
        self, legacy_store: Store
    ) -> None:
        with closing(open_analytics(legacy_store)) as connection:
            create_views(legacy_store, connection)
            assert connection.execute(
                "SELECT rowid, Marketing_Airline_Network "
                "FROM raw_ontime_flights ORDER BY rowid"
            ).fetchall() == [(10, None), (20, None), (21, None)]

    @pytest.fixture
    def old_downloader(self, flight_frame: pd.DataFrame) -> MarketingDownloader:
        frame = flight_frame.copy()
        frame["Year"] = 2017
        frame["FlightDate"] = "2017-01-01"
        stream = BytesIO()
        with zipfile.ZipFile(stream, "w") as archive:
            archive.writestr("reporting.csv", frame.to_csv(index=False))
        return MarketingDownloader(
            payload=stream.getvalue(),
            filename="On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2017_1.zip",
        )

    def test_before_2018_uses_reporting_archive(
        self, store: Store, old_downloader: MarketingDownloader
    ) -> None:
        record = collect_flights(
            store,
            old_downloader,
            Partition(dataset=Dataset.FLIGHTS, year=2017, month=1),
        )
        assert record.rows == 3
        table = pq.ParquetFile(store.resolve(record.cleaned.relative_path)).read()
        assert table["Marketing_Airline_Network"].null_count == 3
