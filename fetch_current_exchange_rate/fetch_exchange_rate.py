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
params = {
    "access_key": API_KEY,
    "base": BASE_CURRENCY,
    "symbols": SYMBOLS
    }

response = requests.get(BASE_URL, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    
    # Add timestamp for reference
    data["fetched_at"] = datetime.utcnow().isoformat() + "Z"
    
    # --- Save to JSON file ---
    output_file = "../data/latest_exchange_rates.json"
    with open(output_file, "w") as f:
        json.dump(data, f, indent=4)
    
    print(f"✅ Data successfully saved to {output_file}")
else:
    print(f"❌ Error fetching data: {response.status_code} - {response.text}")
