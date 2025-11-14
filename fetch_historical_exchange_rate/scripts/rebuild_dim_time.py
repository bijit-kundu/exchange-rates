"""Regenerate dim_time from the dates currently present in fact_exchange_rate.

Usage:
    python3 scripts/rebuild_dim_time.py

Requires BQ_PROJECT, BQ_DATASET (and optional BQ_LOCATION) plus credentials.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

PROJECT_ID = os.getenv("BQ_PROJECT")
DATASET_ID = os.getenv("BQ_DATASET")
TABLE_FACT = f"{PROJECT_ID}.{DATASET_ID}.fact_exchange_rate"
TABLE_DIM = f"{PROJECT_ID}.{DATASET_ID}.dim_time"

if not PROJECT_ID or not DATASET_ID:
    raise SystemExit("BQ_PROJECT and BQ_DATASET must be set in the environment")

client = bigquery.Client(project=PROJECT_ID)

try:
    fact_dates = client.query(
        f"""
        SELECT DISTINCT
            date_key,
            DATE(PARSE_DATE('%Y%m%d', CAST(date_key AS STRING))) AS calendar_date
        FROM `{TABLE_FACT}`
        WHERE date_key IS NOT NULL
        ORDER BY date_key
        """
    ).to_dataframe()
except NotFound:
    raise SystemExit(f"fact table not found: {TABLE_FACT}")

if fact_dates.empty:
    raise SystemExit("fact table has no rows; nothing to build in dim_time")

fact_dates["calendar_date"] = pd.to_datetime(fact_dates["calendar_date"])

# build attributes similar to main ETL
fact_dates["day_of_week"] = fact_dates["calendar_date"].dt.dayofweek + 1
fact_dates["day_name"] = fact_dates["calendar_date"].dt.day_name()
fact_dates["is_weekend"] = fact_dates["day_of_week"].isin([6, 7])
fact_dates["week_start_date"] = fact_dates["calendar_date"] - pd.to_timedelta(
    fact_dates["calendar_date"].dt.dayofweek, unit="D"
)
fact_dates["month"] = fact_dates["calendar_date"].dt.month
fact_dates["month_name"] = fact_dates["calendar_date"].dt.strftime("%B")
fact_dates["quarter"] = fact_dates["calendar_date"].dt.quarter
fact_dates["year"] = fact_dates["calendar_date"].dt.year

# keep consistent types
fact_dates["calendar_date"] = fact_dates["calendar_date"].dt.date
fact_dates["week_start_date"] = fact_dates["week_start_date"].dt.date

cols = [
    "date_key",
    "calendar_date",
    "day_of_week",
    "day_name",
    "is_weekend",
    "week_start_date",
    "month",
    "month_name",
    "quarter",
    "year",
]
dim_time_df = fact_dates[cols].rename(columns={"calendar_date": "date"})

schema = [
    bigquery.SchemaField("date_key", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("day_of_week", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("day_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("is_weekend", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("week_start_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("month", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("month_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("quarter", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("year", "INT64", mode="REQUIRED"),
]

load_cfg = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=schema,
)

job = client.load_table_from_dataframe(dim_time_df, TABLE_DIM, job_config=load_cfg)
job.result()
print(f"Rebuilt {TABLE_DIM} with {len(dim_time_df)} rows sourced from {TABLE_FACT}.")
