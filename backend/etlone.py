#!/usr/bin/env python3
import os
import logging
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

import pandas as pd
import pytz
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import TIMESTAMP

load_dotenv()

API_BASE = "https://sg-mdh-api.mpa.gov.sg/v1"
API_KEY = os.getenv("SG_MDH_APIKEY")
DB_URL = os.getenv("DATABASE_URL")
TZ = pytz.UTC

print(f"API_KEY length: {len(API_KEY)}")
print(f"API_KEY: '{API_KEY}'")


if not API_KEY:
    raise RuntimeError("Missing SG_MDH_APIKEY in .env file")
if not DB_URL:
    raise RuntimeError("Missing DATABASE_URL in .env file")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("etl_dwell")

engine = create_engine(DB_URL, pool_pre_ping=True)

# Requests Session with Retry 
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))
session.headers.update({"apikey": API_KEY})

# Data Fetch

def fetch_api_data(endpoint: str):
    """Fetch data from API endpoint with retries."""
    url = f"{API_BASE}{endpoint}"
    logger.info("Fetching: %s", url)
    try:
        r = session.get(url, timeout=60)
        if not r.ok:
            logger.error("API failed [%s]: %s", r.status_code, r.text)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error("Request failed: %s", e)
        raise

def fetch_arrivals(anchor_date: str, hours: int):
    return fetch_api_data(f"/vessel/arrivals/date/{anchor_date}/hours/{hours}")

def fetch_departures(anchor_date: str, hours: int):
    # Only pass date (YYYY-MM-DD), no time
    return fetch_api_data(f"/vessel/departure/date/{anchor_date}/hours/{hours}")

# Transform

def records_to_df(records, time_field):
    """Convert raw JSON records to DataFrame."""
    if not records:
        return pd.DataFrame(columns=["imo_number", "call_sign", "vessel_name", time_field, "raw", "location_from", "location_to"])

    rows = []

    for r in records:
        vp = r.get("vesselParticulars", {})
        
        # Extract time field
        time_val = r.get("arrivedTime") if time_field == "arrived_time" else r.get("departedTime")
        
        # Extract locations
        location_from = r.get("locationFrom")
        location_to = r.get("locationTo")

        rows.append({
            "imo_number": vp.get("imoNumber"),
            "call_sign": vp.get("callSign"),
            "vessel_name": vp.get("vesselName"),
            time_field: time_val,
            "location_from": location_from,
            "location_to": location_to,
            "raw": r
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df[time_field] = pd.to_datetime(df[time_field], utc=True, errors="coerce")
    return df

# Save Raw

def save_raw(df, table_name, fetched_at):
    """Save raw DataFrame to Postgres with JSONB and timestamptz."""
    if df.empty:
        logger.info("No rows to save for %s", table_name)
        return

    df = df.rename(columns={"raw": "raw_json"})
    df["fetched_at"] = fetched_at

    dtypes = {
        "raw_json": JSONB,
        "fetched_at": TIMESTAMP(timezone=True)
    }
    if "arrived_time" in df.columns:
        dtypes["arrived_time"] = TIMESTAMP(timezone=True)
    if "departed_time" in df.columns:
        dtypes["departed_time"] = TIMESTAMP(timezone=True)

    df.to_sql(table_name, engine, if_exists="append", index=False, dtype=dtypes, method="multi")
    logger.info("Saved %d rows to %s", len(df), table_name)

# Match & Dwell

def match_and_compute_dwell(arr_df, dep_df):
    """Match arrivals & departures, compute dwell hours."""
    if arr_df.empty or dep_df.empty:
        logger.warning("No arrivals or departures to match")
        return pd.DataFrame()

    # Ensure required columns exist
    for col in ["imo_number", "call_sign", "vessel_name"]: 
        if col not in arr_df.columns:
            arr_df[col] = None
        if col not in dep_df.columns:
            dep_df[col] = None

    # Filter out rows where imo_number is null, "0", or empty string
    arr_df_clean = arr_df.dropna(subset=["imo_number"]).query('imo_number != "0" and imo_number != ""')
    dep_df_clean = dep_df.dropna(subset=["imo_number"]).query('imo_number != "0" and imo_number != ""')

    if arr_df_clean.empty or dep_df_clean.empty:
        logger.warning("No valid arrivals or departures to match after filtering")
        return pd.DataFrame()

    # Ensure datetime columns are properly parsed
    for col in ["arrived_time", "departed_time"]:
        if col in arr_df_clean.columns:
            arr_df_clean[col] = pd.to_datetime(arr_df_clean[col], utc=True, errors="coerce")
        if col in dep_df_clean.columns:
            dep_df_clean[col] = pd.to_datetime(dep_df_clean[col], utc=True, errors="coerce")

    # Drop rows where datetime is null after parsing
    arr_df_clean = arr_df_clean.dropna(subset=["arrived_time"])
    dep_df_clean = dep_df_clean.dropna(subset=["departed_time"])

    df = pd.merge_asof(
        dep_df_clean.sort_values("departed_time"),
        arr_df_clean.sort_values("arrived_time"),
        by="imo_number",
        left_on="departed_time",
        right_on="arrived_time",
        direction="backward",
        allow_exact_matches=True
    )

    df = df.dropna(subset=["arrived_time", "departed_time"])
    df["dwell_hours"] = (df["departed_time"] - df["arrived_time"]).dt.total_seconds() / 3600
    df = df[(df["dwell_hours"] >= 0) & (df["dwell_hours"] <= 30 * 24)]
    df["departure_date"] = df["departed_time"].dt.date

    # Ensure final columns exist
    required_cols = ["imo_number", "call_sign", "vessel_name", "arrived_time", "departed_time", "dwell_hours", "departure_date"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    return df[required_cols]

# Aggregate & Upsert

def aggregate_and_upsert(df):
    if df.empty:
        logger.info("No data to aggregate")
        return

    agg = df.groupby("departure_date").agg(
        avg_dwell_hours=("dwell_hours", "mean"),
        min_dwell_hours=("dwell_hours", "min"),
        max_dwell_hours=("dwell_hours", "max"),
        num_vessels=("dwell_hours", "count")
    ).reset_index()

    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS daily_dwell_agg (
            departure_date DATE PRIMARY KEY,
            avg_dwell_hours DOUBLE PRECISION,
            min_dwell_hours DOUBLE PRECISION,
            max_dwell_hours DOUBLE PRECISION,
            num_vessels INTEGER
        )
        """))

        conn.execute(text("""
        INSERT INTO daily_dwell_agg
        (departure_date, avg_dwell_hours, min_dwell_hours, max_dwell_hours, num_vessels)
        VALUES (:departure_date, :avg_dwell_hours, :min_dwell_hours, :max_dwell_hours, :num_vessels)
        ON CONFLICT (departure_date) DO UPDATE SET
            avg_dwell_hours = EXCLUDED.avg_dwell_hours,
            min_dwell_hours = EXCLUDED.min_dwell_hours,
            max_dwell_hours = EXCLUDED.max_dwell_hours,
            num_vessels = EXCLUDED.num_vessels
        """), agg.to_dict(orient="records"))

    logger.info("Upserted %d rows into daily_dwell_agg", len(agg))

# Runner

def run_bulk_etl(anchor_date: str, hours: int):
    fetched_at = datetime.now(timezone.utc)

    try:
        arrivals = fetch_arrivals(anchor_date, hours)
        departures = fetch_departures(anchor_date, hours)
    except Exception as e:
        logger.error("Failed to fetch data for date %s: %s", anchor_date, e)
        raise

    arr_df = records_to_df(arrivals, "arrived_time")
    dep_df = records_to_df(departures, "departed_time")

    save_raw(arr_df, "arrivals_raw", fetched_at)
    save_raw(dep_df, "departures_raw", fetched_at)

    matched = match_and_compute_dwell(arr_df, dep_df)
    aggregate_and_upsert(matched)
    return matched

if __name__ == "__main__":
    # Run for last 20 days
    for days_back in range(1, 28):  
        anchor_dt = (datetime.now(timezone.utc) - timedelta(days=days_back)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        anchor_str = anchor_dt.strftime("%Y-%m-%d %H:%M:%S").replace(" ", "%20")

        logger.info("Starting ETL for date: %s", anchor_str)

        try:
            df = run_bulk_etl(anchor_str, 24)
            logger.info("ETL complete for %s. Rows processed: %d", anchor_str, len(df))
        except Exception as e:
            logger.error("ETL failed for %s: %s", anchor_str, e)
            continue  # Keep going even if one day fails