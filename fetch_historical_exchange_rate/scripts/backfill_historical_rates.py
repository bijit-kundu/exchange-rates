"""
One-time backfill script to retrieve 10 years of historical FX data.

The script:
- Loads EXCHANGE_API_KEY from repo-root .env.
- Splits the 10-year period into 5 chronological chunks for easier monitoring.
- Fetches AUD base rates for EUR, USD, GBP, and SGD, skipping dates already cached.
- Writes results into data/historical_exchange_rates.json so the standard ETL can load
  them into BigQuery (dim_time + fact_exchange_rate).

Usage:
    python3 scripts/backfill_historical_rates.py

After a successful backfill, run:
    python3 scripts/create_dim_currency.py
    python3 scripts/extract_transform.py
"""
from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Tuple

import requests
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

YEARS_TO_BACKFILL = int(os.getenv("BACKFILL_YEARS", "10"))
CHUNKS = int(os.getenv("BACKFILL_CHUNKS", "5"))
BASE_URL = "https://api.exchangeratesapi.io/v1/"
BASE_CURRENCY = os.getenv("BASE_CURRENCY", "AUD")
SYMBOLS = os.getenv("SYMBOLS", "EUR,USD,GBP,SGD")
REQUEST_TIMEOUT = 20

base_dir = Path(__file__).resolve().parents[1]
project_root = Path(__file__).resolve().parents[2]
data_dir = base_dir / "data"
data_dir.mkdir(parents=True, exist_ok=True)
output_file = data_dir / "historical_exchange_rates.json"

load_dotenv(project_root / ".env")

API_KEY = os.getenv("EXCHANGE_API_KEY")
if not API_KEY:
    raise SystemExit("Missing EXCHANGE_API_KEY in environment or .env")

perth_tz = ZoneInfo("Australia/Perth")


def load_existing_records(path: Path) -> Tuple[list, set]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
            if not isinstance(data, list):
                data = []
    except FileNotFoundError:
        data = []
    except json.JSONDecodeError:
        data = []
    dates = {item.get("date") for item in data if isinstance(item, dict)}
    return data, dates


def daterange(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def chunk_ranges(start: date, end: date, chunks: int) -> list[Tuple[date, date]]:
    total_days = (end - start).days + 1
    chunk_size = max(1, total_days // chunks)
    ranges = []
    chunk_start = start
    for idx in range(chunks):
        chunk_end = chunk_start + timedelta(days=chunk_size - 1)
        if idx == chunks - 1 or chunk_end > end:
            chunk_end = end
        ranges.append((chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)
        if chunk_start > end:
            break
    return ranges


def fetch_date(target_date: date, params: dict) -> dict | None:
    date_str = target_date.isoformat()
    endpoint = f"{BASE_URL}{date_str}"
    try:
        resp = requests.get(endpoint, params=params, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        print(f"[WARN] Request failed for {date_str}: {exc}")
        return None

    if resp.status_code != 200:
        print(f"[WARN] Non-200 response for {date_str}: {resp.status_code} - {resp.text}")
        return None

    try:
        payload = resp.json()
    except ValueError:
        print(f"[WARN] Invalid JSON payload for {date_str}")
        return None

    payload["fetched_at"] = datetime.now(perth_tz).isoformat()
    payload.setdefault("date", date_str)
    return payload


def main():
    existing_data, existing_dates = load_existing_records(output_file)

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=YEARS_TO_BACKFILL * 365) + timedelta(days=1)
    if start_date < date(1999, 1, 1):
        start_date = date(1999, 1, 1)

    ranges = chunk_ranges(start_date, end_date, CHUNKS)
    if not ranges:
        print("No ranges to process.")
        return

    params_template = {
        "access_key": API_KEY,
        "base": BASE_CURRENCY,
        "symbols": SYMBOLS,
    }

    total_inserted = 0
    for idx, (chunk_start, chunk_end) in enumerate(ranges, start=1):
        print(f"=== Chunk {idx}/{len(ranges)}: {chunk_start} -> {chunk_end} ===")
        chunk_inserted = 0
        for current_date in daterange(chunk_start, chunk_end):
            date_str = current_date.isoformat()
            if date_str in existing_dates:
                continue
            payload = fetch_date(current_date, params_template)
            if not payload:
                continue
            existing_data.append(payload)
            existing_dates.add(date_str)
            total_inserted += 1
            chunk_inserted += 1
            time.sleep(0.02)
            if chunk_inserted and chunk_inserted % 100 == 0:
                print(f"  ...chunk {idx}: processed {chunk_inserted} days so far (latest = {date_str})")
        print(f"Chunk {idx} complete. Inserted {chunk_inserted} new days. Running total = {total_inserted}.")
        print("Sleeping briefly before next chunk...")
        time.sleep(1)

    with output_file.open("w", encoding="utf-8") as handle:
        json.dump(existing_data, handle, indent=2, ensure_ascii=False)

    print(f"Backfill complete. Total records stored: {len(existing_data)} (added {total_inserted}).")
    print("Next steps: run create_dim_currency.py and extract_transform.py to load BigQuery.")


if __name__ == "__main__":
    main()
