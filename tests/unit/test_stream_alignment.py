import pyarrow as pa
import pytest

from flightdelay.storage.util import rebatch_tables


class TestStreamAlignment:
    @pytest.fixture
    def batches(self) -> tuple[pa.RecordBatch, ...]:
        return tuple(
            pa.record_batch([pa.array(range(start, end))], names=["value"])
            for start, end in ((0, 5), (5, 10), (10, 13), (13, 18))
        )

    def test_short_internal_batch_does_not_change_rows(
        self, batches: tuple[pa.RecordBatch, ...]
    ) -> None:
        tables = tuple(rebatch_tables(batches, 5))
        assert [len(table) for table in tables] == [5, 5, 5, 3]
        assert pa.concat_tables(tables).column("value").to_pylist() == list(range(18))

    def test_empty_stream_has_no_extra_batch(self) -> None:
        assert tuple(rebatch_tables((), 5)) == ()
