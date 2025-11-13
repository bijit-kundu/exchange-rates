import csv
import os
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# Resolve repo paths once so the script works regardless of CWD
base_dir = Path(__file__).resolve().parents[1]
csv_path = base_dir / "data" / "currencies.csv"

default_rows = [
    ("EUR", "Euro"),
    ("GBP", "British Pound"),
    ("AUD", "Australian Dollar"),
    ("USD", "US Dollar"),
]

project_id = os.getenv("BQ_PROJECT", "my-project-lab-477712")
dataset_id = os.getenv("BQ_DATASET", "exchange_rates")
dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

client = bigquery.Client(project=project_id)

try:
    client.get_dataset(dataset_ref)
except NotFound:
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = os.getenv("BQ_LOCATION")
    client.create_dataset(dataset)
    print(f"Created dataset {dataset.full_dataset_id}")

table_id = f"{project_id}.{dataset_id}.dim_currency"
stage_table_id = f"{project_id}.{dataset_id}.dim_currency_stage"
schema = [
    bigquery.SchemaField("currency_key", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("currency_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("currency_name", "STRING"),
]

try:
    client.get_table(table_id)
except NotFound:
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)
    print(f"Created table {table_id}")

rows = default_rows
if csv_path.exists():
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        rows = []
        # Some rows contain multiple ISO codes (e.g., "MXN MXV"); split and treat individually
        for idx, r in enumerate(reader, start=2):
            if not r:
                continue
            try:
                codes = [token.strip().upper() for token in r[0].split() if token.strip()]
                name = r[1].strip() if len(r) > 1 else ""
            except Exception as exc:
                raise ValueError(f"Failed parsing row {idx} in {csv_path}: {r}") from exc

            for code in codes:
                if len(code) == 3:
                    rows.append((code, name))

if not rows:
    raise SystemExit("No currency rows found to load.")

# Build stable payload for load job (sorted to make debugging easier)
# Existing codes retain their numeric key; new codes get sequential ids so fact table
# can reference a durable surrogate key.
existing_map = {}
max_key = 0
try:
    query = client.query(f"SELECT currency_code, currency_key FROM `{table_id}`")
    for row in query:
        existing_map[row.currency_code] = row.currency_key
        max_key = max(max_key, row.currency_key or 0)
except NotFound:
    pass

rows_payload = []
next_key = max_key + 1
for code, name in sorted(rows, key=lambda item: item[0]):
    key = existing_map.get(code)
    if key is None:
        key = next_key
        next_key += 1
        existing_map[code] = key
    rows_payload.append({
        "currency_key": key,
        "currency_code": code,
        "currency_name": name or None
    })

# Truncate/reload staging table on every run so merges see a clean snapshot
load_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=schema,
)

load_job = client.load_table_from_json(rows_payload, stage_table_id, job_config=load_config)
load_job.result()
print(f"Loaded {len(rows_payload)} rows into staging table {stage_table_id}.")

merge_sql = f"""
MERGE `{table_id}` T
USING `{stage_table_id}` S
ON T.currency_code = S.currency_code
WHEN MATCHED THEN UPDATE SET currency_name = S.currency_name
WHEN NOT MATCHED THEN INSERT (currency_key, currency_code, currency_name)
VALUES (S.currency_key, S.currency_code, S.currency_name)
"""

try:
    client.query(merge_sql).result()
except Exception as exc:
    preview = rows_payload[:5]
    raise RuntimeError(
        f"BigQuery merge failed while processing dim_currency rows (first rows={preview})"
    ) from exc

try:
    client.delete_table(stage_table_id)
    print(f"Dropped staging table {stage_table_id}.")
except NotFound:
    pass

print(f"dim_currency upserted {len(rows_payload)} rows into {table_id}")
print(f"To extend the dimension, edit CSV at {csv_path}")
