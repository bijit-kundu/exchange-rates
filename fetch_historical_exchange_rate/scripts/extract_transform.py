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
import pyarrow as pa
from decimal import Decimal, ROUND_HALF_UP, getcontext
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
dataset_id = os.getenv("BQ_DATASET", "exchange_rates")
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
    bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("date_key", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("base_currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("target_currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("rate", "NUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
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
)
ensure_table(dim_time_table_id, dim_time_schema)

ARROW_TYPE_MAP = {
    "INT64": pa.int64(),
    "DATE": pa.date32(),
    "STRING": pa.string(),
    "BOOL": pa.bool_(),
    "NUMERIC": pa.decimal128(38, 9),
    "TIMESTAMP": pa.timestamp("us"),
}


def validate_against_schema(dataframe: pd.DataFrame, schema_fields):
    """Attempt Arrow conversion per column to catch schema/dtype mismatches early."""
    for field in schema_fields:
        if field.name not in dataframe.columns:
            raise ValueError(f"Missing required column '{field.name}' for BigQuery load.")
        arrow_type = ARROW_TYPE_MAP.get(field.field_type)
        if arrow_type is None:
            continue
        series = dataframe[field.name]
        try:
            pa.Array.from_pandas(series, type=arrow_type)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as err:
            raise ValueError(
                f"Column '{field.name}' (dtype={series.dtype}) failed conversion to {field.field_type}: {err}"
            ) from err

# Load/refresh dim_time (truncate + reload)
dim_time_job_cfg = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=dim_time_schema,
)
dim_time_load = client.load_table_from_dataframe(dim_time_df, dim_time_table_id, job_config=dim_time_job_cfg)
dim_time_load.result()
print(f"Loaded {len(dim_time_df)} rows into {dim_time_table_id}.")

# Prepare fact data and drop duplicates already present in BigQuery
getcontext().prec = 38

df_to_insert = df.copy()
df_to_insert["exchange_date"] = df_to_insert["exchange_date"].dt.date
df_to_insert["base_currency"] = df_to_insert["base_currency"].astype(str)
df_to_insert["target_currency"] = df_to_insert["target_currency"].astype(str)

numeric_rates = pd.to_numeric(df_to_insert["rate"], errors="coerce")

def to_decimal(value):
    if pd.isna(value):
        return None
    return Decimal(str(value)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)

df_to_insert["rate"] = numeric_rates.apply(to_decimal)
df_to_insert["timestamp"] = (
    pd.to_datetime(df_to_insert["timestamp"], unit="s", errors="coerce", utc=True)
    .dt.tz_convert(None)
    .dt.floor("us")
)
df_to_insert["fetched_at"] = (
    pd.to_datetime(df_to_insert["fetched_at"], errors="coerce", utc=True)
    .dt.tz_convert(None)
    .dt.floor("us")
)

df_to_insert = df_to_insert.merge(
    dim_time_df[["date", "date_key"]],
    left_on="exchange_date",
    right_on="date",
    how="left",
    validate="many_to_one",
)

if df_to_insert["date_key"].isna().any():
    missing = sorted(df_to_insert.loc[df_to_insert["date_key"].isna(), "exchange_date"].unique())
    raise SystemExit(f"Unable to find date_key for dates: {missing}")

df_to_insert["date_key"] = df_to_insert["date_key"].astype("int64")
df_to_insert = df_to_insert.drop(columns=["date"])

min_key = int(df_to_insert["date_key"].min())
max_key = int(df_to_insert["date_key"].max())

existing_keys = set()
query = f"""
SELECT date_key, base_currency, target_currency
FROM `{fact_table_id}`
WHERE date_key BETWEEN @min_key AND @max_key
"""
query_job = client.query(
    query,
    job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("min_key", "INT64", min_key),
            bigquery.ScalarQueryParameter("max_key", "INT64", max_key),
        ]
    ),
)
for row in query_job:
    existing_keys.add((row.date_key, row.base_currency, row.target_currency))

df_to_insert["__key"] = list(
    zip(
        df_to_insert["date_key"],
        df_to_insert["base_currency"],
        df_to_insert["target_currency"],
    )
)
df_to_insert = df_to_insert[~df_to_insert["__key"].isin(existing_keys)].drop(columns="__key")

if not df_to_insert.empty:
    df_to_insert = df_to_insert.drop(columns=["exchange_date"])
    max_id_query = client.query(f"SELECT COALESCE(MAX(id), 0) AS max_id FROM `{fact_table_id}`")
    max_id = int(next(max_id_query.result()).max_id)
    start_id = max_id + 1
    df_to_insert["id"] = range(start_id, start_id + len(df_to_insert))
    df_to_insert = df_to_insert[
        ["id", "date_key", "base_currency", "target_currency", "rate", "timestamp", "fetched_at"]
    ]

if df_to_insert.empty:
    print("No new rows to insert into fact_exchange_rate.")
else:
    fact_job_cfg = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=fact_schema,
    )
    try:
        validate_against_schema(df_to_insert, fact_schema)
        load_job = client.load_table_from_dataframe(df_to_insert, fact_table_id, job_config=fact_job_cfg)
        load_job.result()
    except Exception as exc:
        print("BigQuery load failed. Column diagnostics:")
        for col in df_to_insert.columns:
            series = df_to_insert[col]
            sample = series.dropna().iloc[0] if not series.dropna().empty else "NaN"
            print(f"- {col}: dtype={series.dtype}, sample={sample}")
        raise
    print(f"Inserted {len(df_to_insert)} new rows into {fact_table_id}.")

count_job = client.query(f"SELECT COUNT(*) AS cnt FROM `{fact_table_id}`")
total_rows = next(count_job.result()).cnt
print(f"Total rows in {fact_table_id}: {total_rows}")
