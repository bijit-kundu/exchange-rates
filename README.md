# exchange-rates - Portfolio

An end-to-end project that fetches historical FX data, enriches currency metadata, and loads curated fact/dimension tables into Google BigQuery for downstream analytics (e.g., Power BI). GitHub Actions orchestrates the daily pipeline so the warehouse stays current without any local SQLite dependencies.

## Highlights
- Fetch AUD-based exchange rates (EUR, USD, GBP, SGD) for the last N days and persist API responses to JSON (`fetch_historical_exchange_rate/data/historical_exchange_rates.json`).
- Add Australia/Perth timestamps to each record for auditing.
- Transform JSON into a tidy fact table (`fact_exchange_rate`) and regenerate the `dim_time` calendar dimension directly in BigQuery. Fact rows store numeric foreign keys (`base_currency_key`, `target_currency_key`) that reference `dim_currency`.
- Maintain `dim_currency` via an on-demand script that MERGEs CSV updates into BigQuery (run when the seed list changes).
- Deduplicate fact loads by comparing `(date_key, base_currency, target_currency)` keys already stored in BigQuery.
- CI automation: GitHub Actions runs the fetch + dimension + fact steps daily at midnight Perth and on demand (`workflow_dispatch`), authenticating to GCP with a service account.
- Power BI (or any BI tool) can connect straight to BigQuery to query curated fact/dimension tables.

## Repo layout
- `fetch_historical_exchange_rate/`
  - `data/`
    - `historical_exchange_rates.json` – append-only cache of API responses.
    - `currencies.csv` – seed file for the currency dimension (supports multiple codes per line).
  - `scripts/`
    - `fetch_historical_rate.py` – fetches the most recent day(s), writes JSON, tags Perth timestamps.
    - `backfill_historical_rates.py` – one-off loader that splits 10 years of history into 5 chunks for easier retry.
    - `extract_transform.py` – builds `dim_time` and loads `fact_exchange_rate` into BigQuery.
    - `create_dim_currency.py` – ingests the CSV into a staging table, assigns/maintains numeric `currency_key` surrogates, and MERGEs into BigQuery `dim_currency`.
- `.github/workflows/daily_pipeline.yml` – scheduled + manual GitHub Actions workflow.
- `.env` – local-only file holding `EXCHANGE_API_KEY` (not committed).
- `README.md`, `requirements.txt`, etc.

## Requirements
- Python 3.11+ recommended.
- pip packages: `requests`, `pandas`, `google-cloud-bigquery`, `pyarrow`, `python-dotenv`, `zoneinfo` (Py3.9+).
- Google Cloud project with BigQuery API enabled and a dataset specified via env vars (see below).
- Service account JSON with `BigQuery Data Editor` + `BigQuery Job User` roles for CI/local runs.
- GitHub secrets: `EXCHANGE_API_KEY`, `GCP_SA_KEY`, `BQ_PROJECT`, `BQ_DATASET`, `BQ_LOCATION`.

## How to run locally
1. Create/activate a virtualenv and install requirements: `pip install -r fetch_historical_exchange_rate/requirements.txt` (or install `requests pandas google-cloud-bigquery pyarrow python-dotenv`).
2. Add your API key to `.env` at the repo root: `EXCHANGE_API_KEY=...`.
3. Export BigQuery settings for local runs (or place them in `.env`):
   ```bash
   export BQ_PROJECT=<your-project-id>
   export BQ_DATASET=<your-dataset>
   export BQ_LOCATION=<dataset-location>
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
   ```
4. Fetch recent data:
   ```bash
   cd fetch_historical_exchange_rate
   python3 scripts/fetch_historical_rate.py
   ```
5. (Optional) Run a one-time 10-year backfill:
   ```bash
   python3 scripts/backfill_historical_rates.py
   ```
6. Load dimensions/fact into BigQuery:
   ```bash
   # Run only when currencies.csv changes
   python3 scripts/create_dim_currency.py
   # Daily job
   python3 scripts/extract_transform.py
   ```
7. Verify tables in BigQuery (`bq show` or the console) and connect your BI tool to the dataset.
8. Set `FETCH_DAYS=1` (default) so the daily pipeline only appends the latest day after the backfill completes.

## GitHub Actions: daily pipeline
- Location: `.github/workflows/daily_pipeline.yml`
- Schedule: cron `0 16 * * *` (16:00 UTC → 00:00 AWST / Australia/Perth); manual trigger via `workflow_dispatch`.
- Steps:
  1. Checkout + set up Python.
  2. Install dependencies (including BigQuery client/pyarrow).
  3. Write `.env` with `EXCHANGE_API_KEY`.
  4. Authenticate to GCP via `google-github-actions/auth` using `GCP_SA_KEY`.
  5. Run `fetch_historical_rate.py` and `extract_transform.py` (both targeting BigQuery). Logs stream to the Actions UI.
- `create_dim_currency.py` is intentional manual/on-demand work: rerun it locally whenever `currencies.csv` changes to keep `currency_key` assignments stable, then re-run `extract_transform.py`.
- Outputs: no SQLite artifacts; data lands in BigQuery directly, so analytics tools can refresh immediately after the workflow finishes.

## Implementation Summary
- Demonstrates a modern data-engineering pipeline:
  - Ingestion: Python client hits the FX API, respecting existing cached dates.
  - Enrichment: adds Perth timestamps, handles malformed responses, and normalizes nested JSON.
  - Warehousing: loads curated facts/dimensions into BigQuery (numeric precision, deduping, time dimension regeneration, currency dim MERGE with surrogate keys).
  - Automation: GitHub Actions schedules daily refreshes and can be triggered manually.
  - Analytics: Power BI (or any BI tool) connects directly to BigQuery tables (`fact_exchange_rate`, `dim_currency`, `dim_time`).
- Code is path-resilient (Pathlib), cloud-ready, and easily extendable (add more currencies, switch to other cloud warehouses, adjust schedules, etc.).
