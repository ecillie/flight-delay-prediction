from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import requests

type RequestParams = Mapping[str, str | int | float]


@dataclass(slots=True, kw_only=True, frozen=True)
class NetworkOptions:
    timeout_seconds: float = 120.0
    bts_url: str = "https://www.transtats.bts.gov/PREZIP"
    airports_url: str = "https://davidmegginson.github.io/ourairports-data/airports.csv"
    weather_url: str = "https://archive-api.open-meteo.com/v1/archive"


@runtime_checkable
class Downloader(Protocol):
    def fetch(self, url: str, params: RequestParams | None = None) -> bytes: ...


@dataclass(slots=True, kw_only=True, frozen=True)
class HttpDownloader:
    session: requests.Session
    options: NetworkOptions

    def fetch(self, url: str, params: RequestParams | None = None) -> bytes:
        with self.session.get(
            url, params=params, timeout=self.options.timeout_seconds
        ) as response:
            response.raise_for_status()
            return response.content
