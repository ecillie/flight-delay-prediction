"""
Author : Evan Cillie
Last Edit : June 23rd 2026
Purpose : Collect info on all airports in the flights database
"""

import sys
import time
import sqlite3
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.logger_config import setup_logger


logger = setup_logger(__name__, "airports_scraper")

DB_PATH = "data/database/flights.sqlite"
AIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
OUTPUT_TABLE = "airports"


def get_airport_codes_from_db(conn):
    query = """
    SELECT Origin AS airport_code
    FROM raw_ontime_flights
    WHERE Origin IS NOT NULL

    UNION

    SELECT Dest AS airport_code
    FROM raw_ontime_flights
    WHERE Dest IS NOT NULL

    ORDER BY airport_code;
    """

    return pd.read_sql_query(query, conn)


def get_airport_info_from_github():
    airports_df = pd.read_csv(AIRPORTS_URL)

    airports_df = airports_df[
        airports_df["iata_code"].notna()
    ][[
        "iata_code",
        "icao_code",
        "name",
        "type",
        "latitude_deg",
        "longitude_deg",
        "elevation_ft",
        "iso_country",
        "iso_region",
        "municipality",
        "scheduled_service"
    ]]

    airports_df = airports_df.rename(columns={
        "iata_code": "airport_code",
        "icao_code": "icao_code",
        "name": "airport_name",
        "latitude_deg": "latitude",
        "longitude_deg": "longitude",
        "iso_country": "country",
        "iso_region": "region"
    })

    airports_df["airport_code"] = airports_df["airport_code"].str.upper()

    return airports_df


def main():
    start_time = time.perf_counter()

    conn = sqlite3.connect(DB_PATH)

    airport_codes_df = get_airport_codes_from_db(conn)
    logger.info(f"Found {len(airport_codes_df)} unique airport codes in flights database")

    github_airports_df = get_airport_info_from_github()
    logger.info(f"Downloaded {len(github_airports_df)} airports from OurAirports GitHub data")

    merged_df = airport_codes_df.merge(
        github_airports_df,
        on="airport_code",
        how="left"
    )

    missing_df = merged_df[merged_df["airport_name"].isna()]

    if len(missing_df) > 0:
        logger.warning(f"Missing info for {len(missing_df)} airport codes")
        logger.warning(missing_df["airport_code"].tolist())

    merged_df.to_sql(
        OUTPUT_TABLE,
        conn,
        if_exists="replace",
        index=False
    )

    conn.close()

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    logger.info(f"Saved {len(merged_df)} airport records to table: {OUTPUT_TABLE}")
    logger.info(f"Script took {elapsed_time:.4f} seconds")

    print(merged_df.head())
    print(f"Total airports saved: {len(merged_df)}")


if __name__ == "__main__":
    main()
