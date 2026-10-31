from pydantic import BaseModel, ConfigDict, FiniteFloat

type Reading = FiniteFloat | None


class WeatherHourly(BaseModel):
    model_config = ConfigDict(frozen=True)
    time: tuple[str, ...]
    temperature_2m: tuple[Reading, ...]
    precipitation: tuple[Reading, ...]
    rain: tuple[Reading, ...]
    snowfall: tuple[Reading, ...]
    wind_speed_10m: tuple[Reading, ...]
    wind_gusts_10m: tuple[Reading, ...]
    visibility: tuple[Reading, ...]
    cloud_cover: tuple[Reading, ...]


class WeatherResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    hourly: WeatherHourly
    timezone: str | None = None
    timezone_abbreviation: str | None = None
    utc_offset_seconds: int | None = None
