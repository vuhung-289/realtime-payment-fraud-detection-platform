# Real-time Payment Fraud Detection Platform

> **Production-style data engineering project** — detects high-risk payments in near real-time using Kafka, Spark Structured Streaming, BigQuery, Airflow, and Streamlit.

---

## Problem Statement

Payment fraud often surfaces as patterns over short time windows: velocity spikes (rapid successive transactions), geo anomalies (activity from unfamiliar countries), or a new device paired with a large amount.

This platform continuously ingests transaction events, computes streaming risk features over time windows, scores each transaction, and raises alerts for suspicious activity — all in near real-time.

---

## Architecture

```
Payment Events
     │
     ▼
┌─────────────────────┐
│   Event Producer    │  ← Generates synthetic traffic with realistic fraud patterns
│  (Kafka Producer)   │
└────────┬────────────┘
         │ Kafka topic: payments_raw
         ▼
┌─────────────────────┐
│  Spark Streaming    │  ← Reads Kafka, validates schema, computes features, scores events
│  (PySpark 3.5.1)    │
└────────┬────────────┘
         │ foreachBatch (micro-batch)
         ▼
┌──────────────────────────────────────────────────────┐
│                    Google BigQuery                    │
│  ┌──────────────┐  ┌───────────────────────────────┐ │
│  │ raw_payments │  │ fraud_scored_transactions      │ │
│  └──────────────┘  └───────────────────────────────┘ │
│                    ┌──────────────┐                   │
│                    │ fraud_alerts │                   │
│                    └──────────────┘                   │
└──────────┬───────────────────────────────────────────┘
           │
     ┌─────┴───────┐
     │             │
     ▼             ▼
┌─────────┐  ┌──────────────────────┐
│Dashboard│  │  Airflow DAG         │
│Streamlit│  │  (Nightly Archive)   │
└─────────┘  └──────────────────────┘
                     │
                     ▼
             ┌──────────────────┐
             │  Data Lake       │
             │  (Parquet / GCS) │
             └──────────────────┘
```

### Data Flow

| Step | Component | Description |
|------|-----------|-------------|
| 1 | **Event Producer** | Publishes synthetic payment events (~20 events/s) to Kafka topic `payments_raw` |
| 2 | **Spark Streaming** | Consumes Kafka, validates/normalizes schema, computes velocity & anomaly features, applies risk rules |
| 3 | **BigQuery** | Persists all scored transactions and high-risk alerts |
| 4 | **Streamlit Dashboard** | Queries BigQuery in near real-time — fraud rate, risk distribution, top risky users/countries |
| 5 | **Airflow DAG** | Runs at 2:00 AM nightly — archives old data to Parquet and rolls up user behavior profiles |

---

## Tech Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Streaming Message Bus | Apache Kafka | (Docker) |
| Stream Processing | PySpark Structured Streaming | 3.5.1 |
| Data Warehouse | Google BigQuery | `google-cloud-bigquery` 3.25 |
| Data Lake Storage | Local Parquet / Google Cloud Storage | — |
| Orchestration | Apache Airflow | (Docker) |
| Dashboard | Streamlit + Plotly | 1.37.1 / 5.23.0 |
| Language | Python | 3.10+ |
| Local Infrastructure | Docker Compose | — |

---

## Project Structure

```text
realtime-payment-fraud-detection-platform/
├── dags/
│   └── archive_fraud_data_dag.py     # Airflow DAG: nightly archive pipeline
├── dashboard/
│   └── app.py                        # Streamlit BI dashboard
├── scripts/
│   ├── archive_old_transactions.py   # Archive script + user profile upsert
│   ├── start_local.ps1               # Automated startup script (launches all stacks)
│   └── stop_local.ps1                # Automated cleanup script (stops Docker stacks)
├── sql/
│   └── bigquery_tables.sql           # BigQuery DDL statements
├── src/
│   ├── common/
│   │   ├── settings.py               # Centralized config loaded from .env
│   │   ├── schemas.py                # Schema definitions
│   │   └── risk_rules.py             # Rule-based risk scoring engine
│   ├── producer/
│   │   └── payment_event_producer.py # Kafka event producer
│   └── streaming/
│       └── fraud_streaming_job.py    # PySpark streaming job
├── tests/
│   └── test_risk_rules.py            # Unit tests for the risk engine
├── docker-compose.yml                # Kafka + Zookeeper (local)
├── docker-compose.airflow.yml        # Airflow webserver + scheduler
├── requirements.txt
└── .env.example
```

---

## Quick Start

### Prerequisites

- Python 3.10+
- Docker Desktop (running)
- Google Cloud project with BigQuery enabled
- Service account JSON key with `BigQuery Data Editor` permissions

---

### Step 1 — Environment Setup

```bash
python -m venv .venv
.venv\Scripts\activate

pip install -r requirements.txt

copy .env.example .env
```

Fill in your `.env` file:

```env
# Google Cloud
GCP_PROJECT_ID=your-gcp-project-id
BIGQUERY_DATASET=fraud_analytics
GOOGLE_APPLICATION_CREDENTIALS=./gcp-credentials_realtime.json

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_PAYMENTS=payments_raw

# Risk Engine
ALERT_RISK_THRESHOLD=75           # Transactions with score >= 75 go to fraud_alerts

# Producer
PRODUCER_EVENTS_PER_SECOND=5

# Archival / Data Lake
ARCHIVE_DAYS_THRESHOLD=29         # Archive transactions older than 29 days
ARCHIVE_OUTPUT_DIR=data_lake      # Local directory for Parquet output
ARCHIVE_GCS_BUCKET=               # Leave empty to skip GCS upload
```

---

### Step 2 — Create BigQuery Tables

Run the DDL in the **BigQuery Console**:

```sql
sql/bigquery_tables.sql
```

---

### Step 3 — Run the Entire System

You can run the platform using the automated PowerShell scripts or step-by-step manually.

#### Option A: Automated (Recommended for Windows)

Open a PowerShell terminal and run:

```powershell
.\scripts\start_local.ps1
```

This single command will:
1. Start Zookeeper & Kafka via Docker.
2. Wait 15 seconds, then spin up the Apache Airflow stack via Docker.
3. Wait 5 seconds, then start the **Spark Streaming Job** in a new window.
4. Wait 8 seconds, then launch the **Payment Event Producer** in a new window.
5. Launch the **Streamlit Dashboard** in another window (accessible at http://localhost:8501).

To stop and clean up all Docker containers when finished, run:

```powershell
.\scripts\stop_local.ps1
```

#### Option B: Step-by-Step Manual Launch

If you prefer to start components individually or are on a non-Windows platform:

1. **Start Zookeeper & Kafka**:
   ```bash
   docker compose up -d
   ```

2. **Start Spark Streaming Job**:
   ```bash
   .venv\Scripts\activate
   python src/streaming/fraud_streaming_job.py
   ```

3. **Start Payment Event Producer**:
   ```bash
   .venv\Scripts\activate
   python src/producer/payment_event_producer.py
   ```

4. **Start Streamlit Dashboard**:
   ```bash
   .venv\Scripts\activate
   streamlit run dashboard/app.py
   ```

5. **Start Airflow Stack**:
   ```bash
   docker compose -f docker-compose.airflow.yml up -d
   ```

---

## Airflow — Nightly Archive Pipeline

The pipeline runs automatically at **2:00 AM** every night with 3 tasks:

```
check_bq_connection → archive_transactions → upsert_user_profiles
```

| Task | Description |
|------|-------------|
| `check_bq_connection` | Pings BigQuery to verify credentials and network are operational |
| `archive_transactions` | Exports transactions older than `ARCHIVE_DAYS_THRESHOLD` days to gzip-compressed Parquet; uploads to GCS if configured |
| `upsert_user_profiles` | Rolls up daily user behavior into `user_daily_profiles` table — suitable for ML feature engineering |

### Start Airflow via Docker

```bash
docker compose -f docker-compose.airflow.yml up -d
```

Access the UI at **http://localhost:8080** (admin / admin), then enable the `archive_fraud_data` DAG.

### Run Archive Manually (without Airflow)

```bash
python scripts/archive_old_transactions.py
```

---

## Risk Scoring Engine

The engine in `src/common/risk_rules.py` scores each transaction using deterministic, expert-defined rules:

| Rule | Trigger Condition | Score Added | Risk Reason Code |
|------|------------------|:---:|------------------|
| Amount spike | `amount > 3× avg_amount_30m` | **+30** | `amount_spike_vs_user_baseline` |
| High transaction amount | `amount >= 2,000` (and no amount spike) | **+20** | `high_transaction_amount` |
| High velocity transactions | `tx_count_5m >= 6` in a 5-minute window | **+25** | `high_velocity_transactions` |
| Failed payment burst | `failed_count_5m >= 3` consecutive failures | **+20** | `failed_payment_burst` |
| New country + high amount | `is_new_country == true` and `amount >= 300` | **+15** | `new_country_with_nontrivial_amount` |
| New device + high amount | `is_new_device == true` and `amount >= 300` | **+10** | `new_device_with_nontrivial_amount` |
| International payment risk | `is_international == true` and `amount >= 500` | **+10** | `international_payment_risk` |

*Note: The total raw score is capped (clamped) at **100**.*

**Output per transaction:**

```json
{
  "risk_score": 75,
  "risk_band": "high",
  "risk_reasons": [
    "amount_spike_vs_user_baseline",
    "high_velocity_transactions",
    "failed_payment_burst"
  ]
}
```

| `risk_band` | `risk_score` Range |
|-------------|-------------------|
| `low` | 0 – 39 |
| `medium` | 40 – 69 |
| `high` | 70 – 84 |
| `critical` | 85 – 100 |

---

## BigQuery Schema

| Table | Description |
|-------|-------------|
| `raw_payments` | All raw payment events ingested from Kafka |
| `fraud_scored_transactions` | Scored events with `risk_score`, `risk_band`, and `risk_reasons` |
| `fraud_alerts` | Subset of scored transactions where `risk_score >= ALERT_RISK_THRESHOLD` |
| `user_daily_profiles` | Daily roll-up per user — transaction count, avg amount, fraud alert count (used for ML) |

---

## Data Quality & Reliability

- **Schema validation**: Events are validated and normalized immediately on ingestion from Kafka.
- **Deduplication**: `dropDuplicates(["transaction_id"])` with a 10-minute watermark.
- **Event-time processing**: Watermark-based windowing correctly handles late-arriving events.
- **Fail on data loss**: Set `failOnDataLoss` to `false` for resilience against Kafka log offsets being discarded.
- **Idempotent archive**: Archive script is safe to re-run — verifies file existence before writing.
- **Retry logic**: Airflow tasks retry up to 2 times with a 15-minute delay on failure.

---

## Testing

```bash
pytest -q
```

Tests focus on the **risk scoring engine** (`tests/test_risk_rules.py`).

---

## Data Engineering Highlights

- **End-to-end streaming pipeline**: from Kafka ingestion → BigQuery serving → BI dashboard.
- **Explainable fraud scoring**: every transaction carries `risk_reasons` explaining why it was flagged.
- **Data lifecycle management**: Airflow DAG automatically archives and rolls up data to control BigQuery costs.
- **Production-minded practices**: schema contract, watermarking, deduplication, idempotency, retry logic.
- **Modular architecture**: straightforward to extend with an ML model or swap out the rule engine.
