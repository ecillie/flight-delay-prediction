"""
Author : Evan Cillie
Last Edit : June 27th 2026
Purpose : Collect weather info related to the dates and times of flights
"""

import sys
import sqlite3
from pathlib import Path
import time

import pandas as pd
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.logger_config import setup_logger


logger = setup_logger(__name__, "weather_collector")

DB_PATH = "data/database/flights.sqlite"
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

START_YEAR = 2023
END_YEAR = 2025

TABLE_NAME = "weather_hourly"


SLEEP_AFTER_RATE_LIMIT = 1800

HOURLY_VARIABLES = [
    "temperature_2m",
    "precipitation",
    "rain",
    "snowfall",
    "wind_speed_10m",
    "wind_gusts_10m",
    "visibility",
    "cloud_cover"
]


def get_expected_rows_for_year(year):
    if year == 2024:
        return 8784

    return 8760


def get_airports_from_db(conn, logger):
    query = """
    SELECT airport_code, latitude, longitude
    FROM airports
    WHERE latitude IS NOT NULL
      AND longitude IS NOT NULL
    ORDER BY airport_code;
    """

    airports_df = pd.read_sql_query(query, conn)

    logger.info(f"Found {len(airports_df)} airports with latitude and longitude")

    return airports_df


def weather_year_already_exists(conn, airport_code, year):
    expected_rows = get_expected_rows_for_year(year)

    query = """
    SELECT COUNT(*) AS row_count
    FROM weather_hourly
    WHERE airport_code = ?
      AND weather_date BETWEEN ? AND ?;
    """

    result = pd.read_sql_query(
        query,
        conn,
        params=(
            airport_code,
            f"{year}-01-01",
            f"{year}-12-31"
        )
    )

    existing_rows = result.loc[0, "row_count"]

    return existing_rows >= expected_rows


def get_missing_airports(conn, logger, all_airports):
    expected_total_rows = sum(
        get_expected_rows_for_year(year)
        for year in range(START_YEAR, END_YEAR + 1)
    )

    query = """
    SELECT airport_code, COUNT(*) AS row_count
    FROM weather_hourly
    GROUP BY airport_code
    HAVING COUNT(*) >= ?
    ORDER BY airport_code;
    """

    completed_df = pd.read_sql_query(
        query,
        conn,
        params=(expected_total_rows,)
    )

    completed_airports = set(completed_df["airport_code"].dropna())
    all_airports_set = set(all_airports["airport_code"].dropna())

    missing_airports = all_airports_set - completed_airports

    logger.info(f"Total airports in airports table: {len(all_airports_set)}")
    logger.info(f"Airports with complete weather data: {len(completed_airports)}")
    logger.info(f"Airports still missing weather data: {len(missing_airports)}")

    if len(missing_airports) > 0:
        logger.info(f"First 20 missing airports: {sorted(missing_airports)[:20]}")

    missing_airports_df = all_airports[
        all_airports["airport_code"].isin(missing_airports)
    ].copy()

    return missing_airports_df


def fetch_weather_for_airport_year(airport_code, latitude, longitude, year, logger):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "auto"
    }

    while True:
        logger.info(f"Requesting weather for {airport_code} in {year}")

        try:
            response = requests.get(BASE_URL, params=params, timeout=60)

            if response.status_code == 429:
                logger.warning(
                    f"Rate limited for {airport_code} {year}. "
                    f"Waiting {SLEEP_AFTER_RATE_LIMIT} seconds before retrying."
                )
                time.sleep(SLEEP_AFTER_RATE_LIMIT)
                continue

            response.raise_for_status()

        except requests.exceptions.RequestException as error:
            logger.error(f"Weather request failed for {airport_code} {year}: {error}")
            return pd.DataFrame()

        data = response.json()

        if "hourly" not in data:
            logger.warning(f"No hourly weather data returned for {airport_code} {year}")
            logger.warning(f"Response: {data}")
            return pd.DataFrame()

        weather_df = pd.DataFrame(data["hourly"])

        if weather_df.empty:
            logger.warning(f"Empty weather dataframe for {airport_code} {year}")
            return pd.DataFrame()

        weather_df["airport_code"] = airport_code
        weather_df["latitude"] = latitude
        weather_df["longitude"] = longitude

        weather_df["weather_datetime"] = pd.to_datetime(weather_df["time"])
        weather_df["weather_date"] = weather_df["weather_datetime"].dt.date.astype(str)
        weather_df["weather_hour"] = weather_df["weather_datetime"].dt.hour

        weather_df = weather_df.drop(columns=["time"])

        logger.info(f"Received {len(weather_df)} rows for {airport_code} {year}")

        return weather_df


def insert_weather_data(conn, weather_df, airport_code, year, logger):
    weather_df.to_sql(
        TABLE_NAME,
        conn,
        if_exists="append",
        index=False
    )

    logger.info(
        f"{airport_code} {year}: inserted {len(weather_df)} rows into {TABLE_NAME}"
    )


def main():
    conn = sqlite3.connect(DB_PATH)

    airports_df = get_airports_from_db(conn, logger)
    airports_df = get_missing_airports(conn, logger, airports_df)

    total_rows_inserted = 0

    for _, airport in airports_df.iterrows():
        airport_code = airport["airport_code"]
        latitude = airport["latitude"]
        longitude = airport["longitude"]

        for year in range(START_YEAR, END_YEAR + 1):

            if weather_year_already_exists(conn, airport_code, year):
                logger.info(f"Skipping {airport_code} {year}, already complete")
                continue

            weather_df = fetch_weather_for_airport_year(
                airport_code=airport_code,
                latitude=latitude,
                longitude=longitude,
                year=year,
                logger=logger
            )

            if weather_df.empty:
                logger.warning(f"{airport_code} {year}: received 0 weather rows")
                continue

            insert_weather_data(conn, weather_df, airport_code, year, logger)

            total_rows_inserted += len(weather_df)

            

    conn.close()

    logger.info(f"Total weather rows inserted this run: {total_rows_inserted}")


if __name__ == "__main__":
    main()