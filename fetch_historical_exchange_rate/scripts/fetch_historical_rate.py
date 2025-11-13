"""
Fetch recent exchange rates and append responses to the historical JSON cache.

- Loads API key from repo root .env
- Resolves file paths relative to this script
- Fetches the last N days (default: 1, configurable via FETCH_DAYS env var)
- Writes a Perth (Australia) timestamp into each record (fetched_at)
- Appends results to data/historical_exchange_rates.json (creates file if missing)
"""
from pathlib import Path
import os
import json
import time
from datetime import date, timedelta, datetime
import requests
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# --- Path and configuration ---
# base_dir points to fetch_historical_exchange_rate/
base_dir = Path(__file__).resolve().parents[1]
# project root is two levels up (where .env lives)
project_root = Path(__file__).resolve().parents[2]
# load .env from repo root (don't commit .env)
load_dotenv(project_root / ".env")

API_KEY = os.getenv("EXCHANGE_API_KEY")
if not API_KEY:
    raise SystemExit("Missing EXCHANGE_API_KEY in environment or .env")

# API details (adjust BASE_URL for your provider)
BASE_URL = "https://api.exchangeratesapi.io/v1/"
BASE_CURRENCY = os.getenv("BASE_CURRENCY", "AUD")
SYMBOLS = os.getenv("SYMBOLS", "EUR,USD,GBP,SGD")
FETCH_DAYS = max(1, int(os.getenv("FETCH_DAYS", "1")))

# Ensure data directory exists and point to output file
data_dir = base_dir / "data"
data_dir.mkdir(parents=True, exist_ok=True)
output_file = data_dir / "historical_exchange_rates.json"

# Perth timezone object for consistent timestamps
perth_tz = ZoneInfo("Australia/Perth")

# --- Load existing data (if present) ---
try:
    with open(output_file, "r", encoding="utf-8") as f:
        existing_data = json.load(f)
        if not isinstance(existing_data, list):
            # ensure we work with a list of entries
            existing_data = []
except FileNotFoundError:
    existing_data = []
except json.JSONDecodeError:
    # if the file is corrupted or empty, start clean
    existing_data = []

# Track dates already present so we can skip redundant requests (API friendly)
existing_dates = {item.get("date") for item in existing_data if isinstance(item, dict) and item.get("date")}

# --- Prepare request parameters ---
# Template shared by every request; only the URL path changes per date
params_template = {
    "access_key": API_KEY,
    "base": BASE_CURRENCY,
    "symbols": SYMBOLS
}
headers = {}

# Fetch last N days (default 1)
today = date.today()
for i in range(FETCH_DAYS):
    d = today - timedelta(days=i)
    date_str = d.isoformat()  # YYYY-MM-DD
    if date_str in existing_dates:
        # skip dates already stored
        continue

    endpoint = f"{BASE_URL}{date_str}"
    try:
        resp = requests.get(endpoint, params=params_template, headers=headers, timeout=15)
    except requests.RequestException as e:
        # network/timeout issues — skip this date but continue others
        print(f"Request failed for {date_str}: {e}")
        continue

    if resp.status_code != 200:
        print(f"Non-200 for {date_str}: {resp.status_code} - {resp.text}")
        continue

    try:
        payload = resp.json()
    except ValueError:
        print(f"Invalid JSON for {date_str}")
        continue

    # Add Perth fetched timestamp for traceability
    payload["fetched_at"] = datetime.now(perth_tz).isoformat()
    # Ensure date field exists (some APIs echo it, some don't)
    payload.setdefault("date", date_str)

    existing_data.append(payload)
    existing_dates.add(date_str)
    # be polite to API
    time.sleep(0.2)

# --- Persist results ---
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(existing_data, f, indent=2, ensure_ascii=False)

print(f"Wrote {len(existing_data)} records to {output_file}")
