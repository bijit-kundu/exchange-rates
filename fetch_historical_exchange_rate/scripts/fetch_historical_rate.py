import requests
import json
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from pathlib import Path
import os
from dotenv import load_dotenv, find_dotenv

base_dir = Path(__file__).resolve().parents[1]   # fetch_historical_exchange_rate/
load_dotenv(find_dotenv())  # finds the nearest .env up the dirs (works if project root contains .env)

API_KEY = os.getenv("EXCHANGE_API_KEY")
if not API_KEY:
    raise SystemExit("Missing EXCHANGE_API_KEY in .env or environment")
BASE_URL = "https://api.exchangeratesapi.io/v1/"  # keep trailing slash
BASE_CURRENCY = "EUR"     # You can change this to any valid currency (e.g., 'EUR', 'AUD')
SYMBOLS = "GBP,AUD,USD"

# --- Fetch Data ---
headers = {}
params_template = {
    "access_key": API_KEY,
    "base": BASE_CURRENCY,
    "symbols": SYMBOLS
}

# resolve data path relative to this script and ensure directory exists
data_dir = base_dir / "data"
data_dir.mkdir(parents=True, exist_ok=True)
output_file = data_dir / "historical_exchange_rates.json"

# Load existing data if present, otherwise start with empty list
try:
    with open(output_file, "r") as f:
        existing_data = json.load(f)
        if not isinstance(existing_data, list):
            existing_data = []
except FileNotFoundError:
    existing_data = []
except json.JSONDecodeError:
    existing_data = []

existing_dates = {item.get("date") for item in existing_data if isinstance(item, dict) and item.get("date")}

perth_tz = ZoneInfo("Australia/Perth")
today = date.today()

for i in range(15):
    d = today - timedelta(days=i)
    date_str = d.isoformat()  # YYYY-MM-DD
    if date_str in existing_dates:
        print(f"⏭️  Skipping {date_str} (already in {output_file})")
        continue

    endpoint = f"{BASE_URL}{date_str}"
    try:
        response = requests.get(endpoint, headers=headers, params=params_template, timeout=10)
    except requests.RequestException as e:
        print(f"❌ Request failed for {date_str}: {e}")
        continue

    if response.status_code == 200:
        try:
            data = response.json()
        except ValueError:
            print(f"❌ Invalid JSON for {date_str}")
            continue

        # Add Perth timestamp for reference
        data["fetched_at"] = datetime.now(perth_tz).isoformat()

        # Ensure the response includes the date field (API usually does)
        data.setdefault("date", date_str)

        existing_data.append(data)
        existing_dates.add(date_str)
        print(f"✅ Appended data for {date_str}")
    else:
        print(f"❌ Error fetching {date_str}: {response.status_code} - {response.text}")

# Save back to file
with open(output_file, "w") as f:
    json.dump(existing_data, f, indent=4)

print(f"✅ All done — data saved to {output_file}")
