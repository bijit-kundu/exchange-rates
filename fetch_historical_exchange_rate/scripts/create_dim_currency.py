import csv
from pathlib import Path
from sqlalchemy import create_engine, text

# DB path 
base_dir = Path(__file__).resolve().parents[1]
db_path = base_dir / "exchange_rates.db"
engine = create_engine(f"sqlite:///{db_path}")

# SQL for creating dim_currency
create_sql = """
CREATE TABLE IF NOT EXISTS dim_currency (
    currency_code CHAR(3) PRIMARY KEY,
    currency_name VARCHAR(50)
);
"""

# CSV format: currency_code,currency_name,region
default_rows = [
    ("EUR", "Euro"),
    ("GBP", "British Pound"),
    ("AUD", "Australian Dollar"),
    ("USD", "US Dollar"),
]

csv_path = base_dir / "data" / "currencies.csv"  # path to the input CSV (data/currencies.csv) used to seed dim_currency; header row is skipped

with engine.begin() as conn:
    conn.execute(text(create_sql))
    # Optionally drop and recreate if you want a fresh table:
    # conn.execute(text("DROP TABLE IF EXISTS dim_currency;"))
    # conn.execute(text(create_sql))

    # Load from CSV if present, otherwise use default_rows
    rows = default_rows
    if csv_path.exists():
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            # skip header row
            next(reader, None)
            rows = []
            for r in reader:
                if not r:
                    continue
                code = r[0].strip().upper()
                name = r[1].strip() if len(r) > 1 else ""
                if len(code) == 3:
                    rows.append((code, name))

    # Insert using parameterized statements; ignore duplicates via INSERT OR IGNORE
    # Use named parameters and pass a dict for each row (safe and compatible with text())
    insert_sql = "INSERT OR IGNORE INTO dim_currency (currency_code, currency_name) VALUES (:code, :name);"
    try:
        for code, name in rows:
            conn.execute(text(insert_sql), {"code": code, "name": name})
    except Exception:
        raise

print(f"✅ dim_currency created/updated in {db_path} — inserted {len(rows)} candidate rows (duplicates ignored).")
print(f"ℹ️ To load a full list of currencies, create a CSV at: {csv_path} with: currency_code,currency_name")