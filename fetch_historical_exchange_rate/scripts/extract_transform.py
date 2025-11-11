"""
extract_transform.py

Reads historical_exchange_rates.json, flattens the nested "rates" structure into a tidy
DataFrame, and writes the results into BigQuery tables:
- fact_exchange_rate
- dim_time (calendar dimension regenerated each run)

Behaviours:
- Resolves file paths relative to this script.
- Skips malformed entries.
- Converts exchange_date to datetime and sorts.
- Creates the fact table if missing.
- Avoids inserting duplicate (exchange_date, base_currency, target_currency) rows by
  comparing keys already in the DB.
- Disposes SQLAlchemy engine cleanly.
"""
import os
import json
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

base_dir = Path(__file__).resolve().parents[1]
data_dir = base_dir / "data"
file_path = data_dir / "historical_exchange_rates.json"

# Load JSON file, exit with a clear message if missing or invalid
try:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
except FileNotFoundError:
    raise SystemExit(f"Input file not found: {file_path}")
except json.JSONDecodeError as e:
    raise SystemExit(f"Invalid JSON in {file_path}: {e}")

# Flatten nested JSON into a list of row dicts
rows = []
for entry in data:
    if not isinstance(entry, dict):
        continue
    rates = entry.get("rates")
    if not isinstance(rates, dict):
        continue

    base_currency = entry.get("base")
    exchange_date = entry.get("date")
    timestamp = entry.get("timestamp")
    fetched_at = entry.get("fetched_at")

    for target, rate in rates.items():
        rows.append({
            "base_currency": base_currency,
            "target_currency": target,
            "exchange_date": exchange_date,
            "rate": rate,
            "timestamp": timestamp,
            "fetched_at": fetched_at
        })

df = pd.DataFrame(rows)
if df.empty:
    print("No rows to process. Exiting.")
    raise SystemExit(0)

# Normalize exchange_date and drop rows that fail parsing
df["exchange_date"] = pd.to_datetime(df["exchange_date"], errors="coerce")
df = df.dropna(subset=["exchange_date"])
df = df.sort_values(by=["exchange_date", "target_currency"])

# Build dim_time rows from the exchange_date column
dim_time_df = (
    df["exchange_date"]
    .dt.normalize()
    .drop_duplicates()
    .sort_values()
    .to_frame(name="date")
)

if dim_time_df.empty:
    print("No calendar dates derived from exchange_rate data. Exiting.")
    raise SystemExit(0)

dim_time_df["date_key"] = dim_time_df["date"].dt.strftime("%Y%m%d").astype(int)
dim_time_df["day_of_week"] = dim_time_df["date"].dt.dayofweek + 1  # 1 = Monday
dim_time_df["day_name"] = dim_time_df["date"].dt.day_name()
dim_time_df["month"] = dim_time_df["date"].dt.month
dim_time_df["month_name"] = dim_time_df["date"].dt.strftime("%B")
dim_time_df["quarter"] = dim_time_df["date"].dt.quarter
dim_time_df["year"] = dim_time_df["date"].dt.year
dim_time_df["week_start_date"] = (
    dim_time_df["date"] - pd.to_timedelta(dim_time_df["date"].dt.dayofweek, unit="D")
)
dim_time_df["is_weekend"] = dim_time_df["day_of_week"].isin([6, 7]).astype(int)

dim_time_df["date"] = dim_time_df["date"].dt.date
dim_time_df["week_start_date"] = dim_time_df["week_start_date"].dt.date
dim_time_df = dim_time_df[
    [
        "date_key",
        "date",
        "day_of_week",
        "day_name",
        "is_weekend",
        "week_start_date",
        "month",
        "month_name",
        "quarter",
        "year",
    ]
]

# --- BigQuery configuration ---
project_id = os.getenv("BQ_PROJECT", "my-project-lab-477712")
dataset_id = os.getenv("BQ_DATASET", "exchange-rates")
dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

client = bigquery.Client(project=project_id)

try:
    client.get_dataset(dataset_ref)
except NotFound:
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = os.getenv("BQ_LOCATION")
    dataset = client.create_dataset(dataset)
    print(f"Created dataset {dataset.full_dataset_id}")

fact_table_id = f"{project_id}.{dataset_id}.fact_exchange_rate"
dim_time_table_id = f"{project_id}.{dataset_id}.dim_time"


def ensure_table(table_id: str, schema, time_partitioning=None):
    try:
        client.get_table(table_id)
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        if time_partitioning:
            table.time_partitioning = time_partitioning
        client.create_table(table)
        print(f"Created table {table_id}")


fact_schema = [
    bigquery.SchemaField("exchange_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("base_currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("target_currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("rate", "FLOAT"),
    bigquery.SchemaField("timestamp", "STRING"),
    bigquery.SchemaField("fetched_at", "TIMESTAMP"),
]

dim_time_schema = [
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

ensure_table(
    fact_table_id,
    fact_schema,
    time_partitioning=bigquery.TimePartitioning(field="exchange_date"),
)
ensure_table(dim_time_table_id, dim_time_schema)

# Load/refresh dim_time (truncate + reload)
dim_time_job_cfg = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=dim_time_schema,
)
dim_time_load = client.load_table_from_dataframe(dim_time_df, dim_time_table_id, job_config=dim_time_job_cfg)
dim_time_load.result()
print(f"Loaded {len(dim_time_df)} rows into {dim_time_table_id}.")

# Prepare fact data and drop duplicates already present in BigQuery
df_to_insert = df.copy()
df_to_insert["exchange_date"] = df_to_insert["exchange_date"].dt.date
df_to_insert["base_currency"] = df_to_insert["base_currency"].astype(str)
df_to_insert["target_currency"] = df_to_insert["target_currency"].astype(str)
df_to_insert["rate"] = pd.to_numeric(df_to_insert["rate"], errors="coerce")
df_to_insert["timestamp"] = df_to_insert["timestamp"].astype(str)
df_to_insert["fetched_at"] = pd.to_datetime(df_to_insert["fetched_at"], errors="coerce")

min_date = df_to_insert["exchange_date"].min()
max_date = df_to_insert["exchange_date"].max()

existing_keys = set()
query = f"""
SELECT exchange_date, base_currency, target_currency
FROM `{fact_table_id}`
WHERE exchange_date BETWEEN @min_date AND @max_date
"""
query_job = client.query(
    query,
    job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("min_date", "DATE", min_date),
            bigquery.ScalarQueryParameter("max_date", "DATE", max_date),
        ]
    ),
)
for row in query_job:
    existing_keys.add((row.exchange_date, row.base_currency, row.target_currency))

df_to_insert["__key"] = list(
    zip(
        df_to_insert["exchange_date"],
        df_to_insert["base_currency"],
        df_to_insert["target_currency"],
    )
)
df_to_insert = df_to_insert[~df_to_insert["__key"].isin(existing_keys)].drop(columns="__key")

if df_to_insert.empty:
    print("No new rows to insert into fact_exchange_rate.")
else:
    fact_job_cfg = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=fact_schema,
    )
    load_job = client.load_table_from_dataframe(df_to_insert, fact_table_id, job_config=fact_job_cfg)
    load_job.result()
    print(f"Inserted {len(df_to_insert)} new rows into {fact_table_id}.")

count_job = client.query(f"SELECT COUNT(*) AS cnt FROM `{fact_table_id}`")
total_rows = next(count_job.result()).cnt
print(f"Total rows in {fact_table_id}: {total_rows}")
