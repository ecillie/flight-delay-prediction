# FlightDelayModel

Flight-delay collection and research. The collectors now write Zstandard Parquet
with a small SQLite ingestion manifest. Research notebooks read the validated
Parquet dataset.

## Setup

Use Python 3.14 and uv:

```sh
uv sync --locked
uv run flightdelay --help
```

## Collection

Collect an explicit batch, followed by its reference and weather data:

```sh
uv run flightdelay collect-flights --year 2026 --month 1
uv run flightdelay collect-airports
uv run flightdelay collect-weather --year 2026 --airport ATL
uv run flightdelay compact-weather --year 2026
uv run flightdelay audit
```

`collect-flights` downloads BTS marketing-carrier data for January 2018 onward,
including marketing, operating, and code-share carrier fields. Earlier months
use reporting-carrier data. Both use the existing `flights` storage, manifest,
and `raw_ontime_flights` SQL view. The existing `flights.json` defines both source
schemas. Fields absent from a source are null; reporting-carrier fields are not
inferred from marketing or operating carrier codes.

Completed historical batches are still skipped and retain their IDs. This change
applies to future downloads; it does not backfill marketing fields into existing
batches. `--force` can replace a downloaded batch, but migrated legacy-ID batches
remain protected from replacement.

Omit `--airport` to collect a year for every airport with coordinates. Each
completed airport/year is skipped on restart. HTTP errors, including rate limits,
stop the command; rerun later to resume. Requests have a bounded timeout and do
not wait indefinitely on rate limits. Use `--root PATH` for a separate dataset.

If restoring the original 2023–2025 SQLite snapshot, use the historical converter; do not
redownload it to populate Parquet. Conversion preserves the original sample IDs,
and notebook cutover follows parity validation. The examples above illustrate
the collection interface, not commands to run as part of setup. Airport and
weather collection require committed Parquet flight/reference data first; they
do not silently fall back to SQLite.

Existing collection module paths also work with the new arguments, for example:

```sh
uv run python -m src.data_collection.bts_flight_collection --year 2026 --month 1
```

## Storage and safe reruns

```text
data/
  raw/flights/year=YYYY/month=MM/GENERATION.parquet
  cleaned/flights/year=YYYY/month=MM/GENERATION.parquet
  raw/weather/year=YYYY/airport=AAA/GENERATION.parquet
  cleaned/weather/year=YYYY/airport=AAA/GENERATION.parquet
  raw/weather/year=YYYY/compacted/GENERATION.parquet
  cleaned/weather/year=YYYY/compacted/GENERATION.parquet
  raw/airports/GENERATION.parquet
  cleaned/airports/GENERATION.parquet
  metadata/ingestion.sqlite
```

Raw files contain parsed source records with provenance, rather than the
original ZIP/JSON payload bytes. Cleaned files add derived fields. The manifest
records source URLs and hashes, schemas, counts, checksums, publication status,
and weather timezone metadata. Weather times remain local; they are not
implicitly converted to UTC. Scheduled `2400` retains the existing hour-zero
behavior on the original flight date.

Writers validate both layers, read their values back, fsync files and directories,
then commit their paths together. A single publisher lock excludes competing
writes. A batch is complete only when its manifest entry and file metadata agree.
Every weather airport/year must contain the exact distinct local hourly grid,
including 8,784 hours in leap years.

Reruns skip completed batches. `--force` publishes a replacement generation
without appending duplicate rows. Old files remain to protect existing readers;
`audit` checks active checksums and reports unreferenced files and abandoned
staging entries. It does not delete them. An interrupted publication can be
retried; readers continue using the previous committed generation.

Weather compaction combines committed airport/year files, sorted by airport and
timestamp. If an airport is updated later, readers use the current airport files
until compaction is rerun, preventing stale results or double counting.

Fresh BTS downloads have batch-local `record_id` and `source_record_number`
fields, deterministic for the same CSV row order. Their `source_rowid` is null
because a downloaded
CSV cannot establish the original SQLite row ID. Historical conversion supplies
the original IDs. Collectors refuse to replace a migrated legacy-ID batch with a
new download; restore or reconvert it from the original database instead.

The default guards retain 5 GiB free space and limit a staged publication to
512 MiB. These protect individual writes, not the capacity for a full migration.
Full migration needs its own fresh capacity check.

## SQL access

Use committed manifest paths rather than globbing all generations:

```python
from contextlib import closing

from flightdelay.storage.analytics import create_views, open_analytics
from flightdelay.storage.store import create_store

store = create_store()
with closing(open_analytics(store)) as connection:
    create_views(store, connection)
    flights = connection.execute(
        "SELECT Year, Month, Origin, Dest FROM raw_ontime_flights LIMIT 10"
    ).fetchdf()
```

Views are `raw_ontime_flights`, `weather_hourly`, and `airports`. A connection
uses the generation snapshot captured when its views were created; create a new
connection to see later publications. The flight view exposes `rowid` as the
preserved legacy ID when available, otherwise the new batch-local record number.
This preserves existing historical `rowid % 10` samples after migration.
Globbing the raw/cleaned roots would also read retired generations and must be
avoided.

## Verification

```sh
uv run ruff check
uv run ruff format --check
uv run ty check
uv run pytest
uv build --wheel
```

Tests separate unit policies from filesystem/collector integration checks,
including abrupt-process crash recovery. Verify the current production data with:

```sh
uv run flightdelay audit
uv run python -m flightdelay.migration.notebooks --verify
```

These commands check committed file integrity and notebook sample distributions
without training models or modifying notebook outputs. Back up
`data/raw`, `data/cleaned`, and `data/metadata` together for recovery.
