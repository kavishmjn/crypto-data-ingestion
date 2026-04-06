# Crypto Assets ELT Pipeline

**Python | PostgreSQL | dbt | Apache Airflow | Docker**

---

## Project Summary

A production-style batch ELT pipeline that ingests cryptocurrency asset data from the CoinCap API, stores raw snapshots in PostgreSQL, transforms and tests the data using dbt, and orchestrates the entire workflow with Apache Airflow running on Docker.

Built as a self-directed upskilling project to demonstrate end-to-end data engineering capability — from raw ingestion through to analytical mart models with full data quality coverage.

---

## Architecture

```
CoinCap API
    ↓
Python Ingestion Layer (fetch → validate → store)
    ↓
File-Based Batch Storage  data/<batch_id>/raw/assets.json
                          data/<batch_id>/csv/assets.csv
    ↓
PostgreSQL — raw.assets (append-only)
    ↓
dbt Staging → Analytics → Mart Models
    ↓
Apache Airflow (orchestration, scheduling, monitoring)
```

---

## Pipeline Orchestration

The pipeline is orchestrated using **Apache Airflow** running on Docker with `LocalExecutor`. A DAG runs every 3 hours with the following task sequence:

```
generate_batch_id → fetch_assets → store_raw_data → validate_raw_data → validate_data_schema → store_csv_data → load_csv_to_db → dbt_run → dbt_test
```

Key orchestration decisions:
- Each pipeline step is an independent `PythonOperator` task for granular failure visibility
- `batch_id` and file paths are passed between tasks via **XComs** — no shared state
- `fetch_assets` has dedicated retry logic (3 retries, 30s delay) as the only external dependency
- `max_active_runs=1` prevents overlapping pipeline executions
- Credentials managed via **Airflow Variables** — no secrets in code or version control
- dbt tasks use a dedicated Docker target profile to resolve host networking

---

## dbt Transformation Layer

Models are organised across three layers:

**Staging** — type casting and timestamp normalisation from raw source

**Analytics** — reusable building blocks:
- `coin_daily_price_range_analysis` — daily OHLC construction using window functions
- `coin_price_momentum` — price change between ingestion windows using LAG
- `coin_price_volatility_metric` — rolling standard deviation of returns (1d, 3d, 7d)
- `coin_trend_metrics` — rolling moving averages (1d, 3d, 7d)
- `coin_performance_metrics` — multi-period return calculations (1d, 3d, 7d, 15d, 30d)

**Mart** — consumer-facing models:
- `assets_mart` — latest price snapshot per asset (deduplicated)
- `coin_daily_market` — combined daily OHLC, trend, and volatility per asset
- `coin_top_movers` — ranked gainers and losers by percentage change

---

## Data Quality

Tests are implemented across all three model layers using dbt generic tests and `dbt_utils`:

**Generic tests** — `not_null`, `unique`, `accepted_values` on critical columns across all models

**Grain tests** — `dbt_utils.unique_combination_of_columns` on every model to enforce row-level uniqueness

**Domain sanity checks** — `dbt_utils.expression_is_true` assertions including:
- OHLC relationships: `high >= open`, `high >= close`, `low <= open`, `low <= close`, `range = high - low`
- Price sanity: `price_usd > 0`, `close >= 0`, `volatility >= 0`
- Business logic: `Gain_Lose` accepted values, `position > 0`, `change_in_price != 0`

**Singular tests** — custom SQL tests for business-critical assertions:
- `assert_assets_mart_one_row_per_asset` — ensures no duplicate assets in the latest price snapshot
- `assert_top_movers_has_both_categories` — ensures both gainers and losers are always present

**Python validation layer** — pre-database validation in three tiers:
- File level: existence, non-empty, valid JSON
- Data level: record count, volume range, structure checks, API error detection
- Schema level: critical field presence, unique IDs, null checks, type validation

---

## Engineering Decisions

| Decision | Rationale |
|---|---|
| Append-only raw layer | Preserves full historical state, enables replay and auditing |
| No transformation in Python | Clean ELT separation — all casting deferred to dbt |
| Batch directory structure | Deterministic replay, auditability, per-batch debugging |
| `execute_values` for bulk insert | Optimised micro-batch performance over row-by-row inserts |
| Schema creation separated from ingestion | Single responsibility, idempotent infrastructure setup |
| PythonOperator per pipeline step | Granular failure visibility in Airflow UI |
| XComs for inter-task data passing | Clean state management with no shared memory between tasks |
| Airflow Variables for secrets | Zero credentials in code or version control |
| Docker LocalExecutor | Reproducible local environment matching production patterns |

---

## Technology Stack

- **Python 3.11+** — ingestion, validation, orchestration
- **PostgreSQL** — raw and transformed data storage
- **psycopg2** — database connectivity and bulk loading
- **dbt (Postgres adapter)** — transformation, testing, documentation
- **dbt-utils** — extended test coverage
- **Apache Airflow 2.9** — pipeline orchestration and scheduling
- **Docker + Docker Compose** — containerised Airflow environment
- **python-dotenv** — environment-driven configuration

---

## Project Structure

```
├── ingestion/
│   ├── api_calls.py          # CoinCap API client
│   ├── store_data.py         # JSON and CSV file writers
│   └── validation.py         # Three-tier validation framework
├── database/
│   ├── connection.py         # PostgreSQL connection management
│   ├── schema_table_setup.py # Idempotent schema/table creation
│   └── load_data_in_raw_table.py  # Bulk insert via execute_values
├── crypto_db/
│   └── models/
│       ├── staging/          # Type casting from raw source
│       ├── analytics/        # Reusable analytical building blocks
│       └── mart/             # Consumer-facing models
├── airflow/
│   ├── Dockerfile            # Custom image with dbt-postgres
│   ├── docker-compose.yaml   # LocalExecutor setup
│   └── dags/
│       └── crypto_pipeline_v2.py  # Full PythonOperator DAG
├── config.py                 # Centralised configuration
└── main.py                   # Standalone pipeline runner
```

---

## Local Setup

### Prerequisites
- Docker Desktop
- PostgreSQL (local instance)
- CoinCap API key (free at coincap.io)

### Run with Airflow

```bash
# Clone the repo
git clone https://github.com/kavishmjn/crypto-data-ingestion.git
cd crypto-data-ingestion/airflow

# Set Airflow UID
echo "AIRFLOW_UID=50000" > .env

# Build and start
docker compose build
docker compose up -d

# Add credentials in Airflow UI → Admin → Variables
# DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, COINCAP_API_KEY
```

Navigate to `http://localhost:8080` (user: `admin`, password: `admin`) and trigger `crypto_pipeline_v2`.

### Run standalone

```bash
pip install -r requirements.txt
cp .env.example .env  # fill in credentials
python main.py
```

---

## Roadmap

- [ ] Add `on_failure_callback` alerting to Airflow DAG
- [ ] `BranchPythonOperator` to skip dbt if ingestion returns no new data
- [ ] dbt Snapshots (SCD Type 2) for asset history
- [ ] Containerisation of full project with Docker Compose
- [ ] CI/CD with GitHub Actions (lint, dbt compile check)

---

## Author

**Kavish Mahajan** — Data Engineer  
Python | SQL | PostgreSQL | dbt | Apache Airflow  
[github.com/kavishmjn](https://github.com/kavishmjn)
