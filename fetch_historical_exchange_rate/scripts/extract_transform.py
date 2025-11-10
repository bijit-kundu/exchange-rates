"""
extract_transform.py

Reads historical_exchange_rates.json, flattens the nested "rates" structure into a tidy
DataFrame, and loads new rows into the SQLite fact table `fact_exchange_rate`.

Behaviours:
- Resolves file paths relative to this script.
- Skips malformed entries.
- Converts exchange_date to datetime and sorts.
- Creates the fact table if missing.
- Avoids inserting duplicate (exchange_date, base_currency, target_currency) rows by
  comparing keys already in the DB.
- Disposes SQLAlchemy engine cleanly.
"""
import pandas as pd
import json
from pathlib import Path
from sqlalchemy import create_engine, text

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

# Database path and engine configuration
db_path = base_dir / "exchange_rates.db"
engine = create_engine(f"sqlite:///{db_path}", connect_args={"timeout": 30, "check_same_thread": False})

try:
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fact_exchange_rate (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        exchange_date DATE NOT NULL,
        base_currency CHAR(3) NOT NULL,
        target_currency CHAR(3) NOT NULL,
        rate REAL,
        timestamp TEXT,
        fetched_at TEXT
    );
    """

    with engine.begin() as conn:
        # Ensure table exists
        conn.execute(text(create_table_sql))

        # Read existing keys to prevent duplicates
        existing_keys = set()
        try:
            existing = pd.read_sql_query(
                "SELECT exchange_date, base_currency, target_currency FROM fact_exchange_rate",
                conn
            )
            existing["exchange_date"] = pd.to_datetime(existing["exchange_date"], errors="coerce").dt.date.astype(str)
            existing_keys = set(zip(existing["exchange_date"], existing["base_currency"], existing["target_currency"]))
        except Exception:
            existing_keys = set()

        # Prepare df for insertion and filter out already-present rows
        df_to_insert = df.copy()
        df_to_insert["exchange_date"] = df_to_insert["exchange_date"].dt.date.astype(str)
        df_to_insert["base_currency"] = df_to_insert["base_currency"].astype(str)
        df_to_insert["target_currency"] = df_to_insert["target_currency"].astype(str)

        df_to_insert["__key"] = list(zip(
            df_to_insert["exchange_date"],
            df_to_insert["base_currency"],
            df_to_insert["target_currency"]
        ))
        df_to_insert = df_to_insert[~df_to_insert["__key"].isin(existing_keys)].drop(columns=["__key"])

        if df_to_insert.empty:
            print("No new rows to insert into fact_exchange_rate.")
        else:
            df_to_insert.to_sql("fact_exchange_rate", conn, if_exists="append", index=False)
            print(f"Inserted {len(df_to_insert)} new rows into fact_exchange_rate.")

    # Verify total rows after load
    verification = pd.read_sql_query("SELECT COUNT(*) AS cnt FROM fact_exchange_rate", engine)
    total_rows = int(verification.at[0, "cnt"])
    print(f"Total rows in fact_exchange_rate: {total_rows}")

finally:
    engine.dispose()