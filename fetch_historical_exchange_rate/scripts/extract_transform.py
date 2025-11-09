import pandas as pd
import json
from pathlib import Path
from sqlalchemy import create_engine, text

file_path = "../data/historical_exchange_rates.json"

# Step 1: Load JSON
with open(file_path) as f:
    data = json.load(f)

# Step 2: Flatten nested JSON into records
rows = []
for entry in data:
    for target, rate in entry["rates"].items():
        rows.append({
            "base_currency": entry["base"],
            "target_currency": target,
            "exchange_date": entry["date"],
            "rate": rate,
            "timestamp": entry.get("timestamp"),
            "fetched_at": entry.get("fetched_at")
        })

df = pd.DataFrame(rows)

# Step 3: Data validation
df["exchange_date"] = pd.to_datetime(df["exchange_date"])
df = df.sort_values(by=["exchange_date", "target_currency"])

print("DataFrame head:")
print(df.head())

# Step 4: Load to SQL (SQLite placed one level up: fetch_historical_exchange_rate/)
engine = None
try:
    # place DB in fetch_historical_exchange_rate directory (one level up from scripts/)
    base_dir = Path(__file__).resolve().parents[1]
    db_path = base_dir / "exchange_rates.db"

    engine = create_engine(f"sqlite:///{db_path}")

    # If you want a specific schema, create the table first (no-op if exists)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fact_exchange_rate (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        exchange_date DATE NOT NULL,
        base_currency CHAR(3) NOT NULL,
        target_currency CHAR(3) NOT NULL,
        rate DECIMAL(10,6) NOT NULL,
        timestamp BIGINT,
        fetched_at DATETIME,
        FOREIGN KEY (base_currency) REFERENCES dim_currency(code),
        FOREIGN KEY (target_currency) REFERENCES dim_currency(code),
        FOREIGN KEY (exchange_date) REFERENCES dim_date(date)
    );
    """

    with engine.begin() as conn:
        # Enable FK enforcement for this connection
        conn.execute(text("PRAGMA foreign_keys = ON;"))

        # Create table with foreign keys (adjust referenced table/columns to your schema)
        conn.execute(text(create_table_sql))

        # --- Avoid inserting duplicates ---
        # Build a set of existing keys (exchange_date, base_currency, target_currency)
        table_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='fact_exchange_rate'")
        ).fetchone() is not None

        if table_exists:
            try:
                existing = pd.read_sql_query(
                    "SELECT exchange_date, base_currency, target_currency FROM fact_exchange_rate",
                    conn
                )
                # normalize existing dates to YYYY-MM-DD strings for comparison
                existing["exchange_date"] = pd.to_datetime(existing["exchange_date"]).dt.date.astype(str)
                existing_keys = set(zip(existing["exchange_date"], existing["base_currency"], existing["target_currency"]))
            except Exception:
                existing_keys = set()
        else:
            existing_keys = set()

        # prepare dataframe for insert - normalize exchange_date to YYYY-MM-DD string
        df_to_insert = df.copy()
        df_to_insert["exchange_date"] = df_to_insert["exchange_date"].dt.date.astype(str)
        df_to_insert["base_currency"] = df_to_insert["base_currency"].astype(str)
        df_to_insert["target_currency"] = df_to_insert["target_currency"].astype(str)

        # create tuple keys and filter out rows already present
        df_to_insert["__key"] = list(zip(df_to_insert["exchange_date"], df_to_insert["base_currency"], df_to_insert["target_currency"]))
        df_to_insert = df_to_insert[~df_to_insert["__key"].isin(existing_keys)].drop(columns=["__key"])

        if df_to_insert.empty:
            print("ℹ️ No new rows to insert into fact_exchange_rate (all rows are duplicates).")
        else:
            # append only new rows
            df_to_insert.to_sql("fact_exchange_rate", conn, if_exists="append", index=False)
            print(f"✅ Inserted {len(df_to_insert)} new rows into fact_exchange_rate")

    # Verify that rows were written
    verification = pd.read_sql_query("SELECT COUNT(*) AS cnt FROM fact_exchange_rate", engine)
    loaded_count = int(verification.at[0, "cnt"])
    print(f"✅ Total rows in SQLite table fact_exchange_rate (db: {db_path}): {loaded_count}")

except Exception as e:
    print(f"❌ Failed to load data into SQLite: {e}")

finally:
    if engine is not None:
        engine.dispose()