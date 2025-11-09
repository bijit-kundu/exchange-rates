# exchange-rates — Portfolio

A small end-to-end project that fetches historical foreign exchange rates, enriches currency metadata, and stores cleaned data in a local SQLite data warehouse. Includes automation via GitHub Actions and optional persistence to Google Cloud Storage (GCS).  

## Highlights
- Fetch historical exchange rates for the last N days and save responses to JSON (`fetch_historical_rate/data/historical_exchange_rates.json`).
- Add Perth (Australia) timestamps to fetched records.
- Transform JSON -> tidy fact table and load into SQLite (`fetch_historical_rate/exchange_rates.db`).
- Deduplication logic when loading to the DB (avoids duplicate (exchange_date, base_currency, target_currency) rows).
- Create and seed a currency dimension (`dim_currency`) from a CSV.
- CI automation: GitHub Actions workflow to run fetch + ETL daily at midnight Perth (schedule) and manually (workflow_dispatch).
- Persistence options: workflow uploads `exchange_rates.db` as an artifact and can upload the DB to a GCS bucket using a service account.

## Repo layout
- fetch_historical_exchange_rate/
  - data/
    - historical_exchange_rates.json
    - currencies.csv
    - logs/
  - scripts/
    - fetch_historical_rate.py        # fetches historical rates (writes JSON)
    - extract_transform.py           # transforms JSON -> fact_exchange_rate and loads DB
    - create_dim_currency.py         # creates/seeds dim_currency from CSV
  - exchange_rates.db                # created by scripts (in repository)
  - sql/
    - remove_duplicates.sql
    - validation_queries.sql 
- .github/workflows/
  - daily_pipeline.yml               # GitHub Actions workflow (schedule + manual)
- README.md
- .env (local; NOT committed)

## Requirements
- Python 3.9+
- pip packages: requests, pandas, sqlalchemy, python-dotenv
- Optional (local): gcloud SDK to test GCS uploads
- GitHub: repo configured with required Secrets for workflows

## How to run locally
1. Ensure venv and requirements installed.
2. Add your API key to repo-root `.env`.
3. Run fetch:
```bash
cd fetch_historical_rate
python3 scripts/fetch_historical_rate.py
```
4. Run transform & load:
```bash
python3 scripts/extract_transform.py
```

## GitHub Actions: daily pipeline
- Location: `.github/workflows/daily_pipeline.yml`
- Schedule: cron `0 16 * * *` (16:00 UTC → 00:00 AWST / Australia/Perth)
- Manual runs: `workflow_dispatch` enabled for ad-hoc execution
- The workflow:
  - Checks out repo
  - Installs dependencies
  - Writes `.env` from `EXCHANGE_API_KEY` secret
  - Runs fetch and ETL scripts capturing logs
  - Uploads artifacts:
    - `exchange_rates_db` (exchange_rates.db)
    - `pipeline-logs` (logs directory)
  - Optionally authenticates to GCP and uploads the DB to a GCS path with timestamped filename

## GCS persistence (implemented)
- Service account JSON stored in `GCP_SA_KEY` secret
- Workflow uses `google-github-actions/auth` and `google-github-actions/setup-gcloud` to authenticate
- The DB is copied to `gs://$GCS_BUCKET/$GCS_PATH/exchange_rates-<timestamp>.db`

## Implementation Summary
- Demonstrates full-stack data engineering:
  - data ingestion (API),
  - enrichment (external APIs + caching),
  - transformation (Pandas -> normalized fact table),
  - warehousing (SQLite),
  - automation (GitHub Actions),
  - cloud persistence (GCS),
- All code is organised, path-resilient (uses Path-based resolution), and automatable.
- Workflow and scripts are reusable and extensible (e.g., add S3 instead of GCS, add more currencies, schedule frequency changes).
