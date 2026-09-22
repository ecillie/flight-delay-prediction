"""

Author : Evan Cillie

Last Edit : June 21st 2026

Purpose : Collect flight information from BTS relating to cancelations and delays

"""


import requests
import zipfile
import pandas as pd
from io import BytesIO
import sys
from pathlib import Path
import sqlite3

conn = sqlite3.connect("data/database/flights.sqlite")
cursor = conn.cursor()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.logger_config import setup_logger


logger = setup_logger(__name__, "download_bts")

BASE_URL = "https://www.transtats.bts.gov/PREZIP"

YEARS = [2023, 2024, 2025]
MONTHS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]


def download_csv_to_dataframe(year, month):
    file_name = f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
    url = f"{BASE_URL}/{file_name}"

    logger.info(f"Downloading {year}-{month:02d}")
    logger.info(f"URL: {url}")

    try:
        response = requests.get(url, timeout=120)
        logger.info(f"Status code: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"Download failed for {year}-{month:02d}")
            return None

        zip_bytes = BytesIO(response.content)

        if not zipfile.is_zipfile(zip_bytes):
            logger.error(f"Downloaded file is not a valid ZIP for {year}-{month:02d}")
            return None

        zip_bytes.seek(0)

        with zipfile.ZipFile(zip_bytes, "r") as zip_ref:
            csv_files = [
                file_name for file_name in zip_ref.namelist()
                if file_name.endswith(".csv")
            ]

            if not csv_files:
                logger.error(f"No CSV found inside ZIP for {year}-{month:02d}")
                return None

            csv_file = csv_files[0]
            logger.info(f"Reading CSV from ZIP memory: {csv_file}")

            with zip_ref.open(csv_file) as file:
                df = pd.read_csv(file)

        logger.info(f"Loaded dataframe for {year}-{month:02d} with {len(df)} rows")
        return df

    except requests.RequestException as error:
        logger.error(f"Request failed for {year}-{month:02d}: {error}")
        return None
    
def load_to_sqlite (df):
    if "Unnamed: 109" in df.columns:
        df = df.drop(columns=["Unnamed: 109"])
        logger.info("Dropped Column 109")

    df.to_sql(
        "raw_ontime_flights",
        conn,
        if_exists="append",
        index=False
    )

def get_hour_from_bts_time(time_value):
    if pd.isna(time_value):
        return None

    time_value = int(time_value)

    if time_value == 2400:
        return 0

    return time_value // 100

    
def main():
    logger.info("Starting BTS download script")

    for year in YEARS:
        for month in MONTHS:
            df = download_csv_to_dataframe(year, month)

            if df is None:
                logger.warning(f"No data returned for {year}-{month:02d}")
                continue

            if "Unnamed: 109" in df.columns:
                df = df.drop(columns=["Unnamed: 109"])

            df["source_year"] = year
            df["source_month"] = month

            df["CRSDepHour"] = df["CRSDepTime"].apply(get_hour_from_bts_time).astype("Int64")
            df["CRSArrHour"] = df["CRSArrTime"].apply(get_hour_from_bts_time).astype("Int64")

            logger.info(f"Created hour fields for {year}-{month:02d}")
            logger.info(f"Ready to load {year}-{month:02d} into SQLite")

            load_to_sqlite(df)

    logger.info("Finished BTS download script")
if __name__ == '__main__':
    main()