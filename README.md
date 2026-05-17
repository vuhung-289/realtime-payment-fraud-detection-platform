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
├── docs/
│   ├── architecture.md               # Architecture notes and design decisions
│   └── runbook.md                    # Operations runbook
├── scripts/
│   ├── archive_old_transactions.py   # Archive script + user profile upsert
│   └── start_local.ps1               # PowerShell helper to start local stack
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
PRODUCER_EVENTS_PER_SECOND=20

# Archival / Data Lake
ARCHIVE_DAYS_THRESHOLD=29         # Archive transactions older than 29 days
ARCHIVE_OUTPUT_DIR=data_lake      # Local directory for Parquet output
ARCHIVE_GCS_BUCKET=               # Leave empty to skip GCS upload
```

---

### Step 2 — Create BigQuery Tables

Run the DDL in the **BigQuery Console**:

```
sql/bigquery_tables.sql
```

---

### Step 3 — Start Kafka (Docker)

```bash
docker compose up -d
```

---

### Step 4 — Run the Pipeline

Open **4 separate terminals**:

**Terminal 1 — Event Producer:**
```bash
.venv\Scripts\activate
python src/producer/payment_event_producer.py
```

**Terminal 2 — Spark Streaming Job:**
```bash
.venv\Scripts\activate
python src/streaming/fraud_streaming_job.py
```

**Terminal 3 — Streamlit Dashboard:**
```bash
.venv\Scripts\activate
streamlit run dashboard/app.py
```

> Dashboard opens automatically at **http://localhost:8501**

**Terminal 4 (optional) — Manual archive run:**
```bash
.venv\Scripts\activate
python scripts/archive_old_transactions.py
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

The engine in `src/common/risk_rules.py` scores each transaction using deterministic rules:

| Rule | Trigger Condition | Score Added |
|------|------------------|-----------| 
| Amount spike | `amount > 3× avg_amount_30m` | +40 |
| Velocity spike | `tx_count_5m > 10` within a 5-minute window | +30 |
| New country + high amount | Unfamiliar country and `amount > 1,000` | +25 |
| New device + high amount | New device and `amount > 500` | +20 |
| Failed payment burst | `failed_count_5m >= 3` consecutive failures | +35 |
| International transfer | `is_international == true` | +10 |

**Output per transaction:**

```json
{
  "risk_score": 75,
  "risk_band": "high",
  "risk_reasons": ["amount_spike", "new_country_high_amount"]
}
```

| `risk_band` | `risk_score` Range |
|-------------|-------------------|
| `low` | 0 – 24 |
| `medium` | 25 – 49 |
| `high` | 50 – 74 |
| `critical` | 75 – 100 |

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
- **Idempotent archive**: Archive script is safe to re-run — verifies file existence before writing.
- **Retry logic**: Airflow tasks retry up to 2 times with a 15-minute delay on failure.

---

## Testing

```bash
pytest -q
```

Tests focus on the **risk scoring engine** (`tests/test_risk_rules.py`).

---

## Future Improvements

- [ ] Schema Registry + Avro/Protobuf contract
- [ ] Replace heuristic country/device checks with a stateful feature store
- [ ] ML model serving integration (Vertex AI / MLflow)
- [ ] CI/CD pipeline: tests + linting + Docker image build
- [ ] Streaming ML inference to replace the rule-based engine
- [ ] Automatic GCS upload for all Parquet archive output

---

## Data Engineering Highlights

- **End-to-end streaming pipeline**: from Kafka ingestion → BigQuery serving → BI dashboard.
- **Explainable fraud scoring**: every transaction carries `risk_reasons` explaining why it was flagged.
- **Data lifecycle management**: Airflow DAG automatically archives and rolls up data to control BigQuery costs.
- **Production-minded practices**: schema contract, watermarking, deduplication, idempotency, retry logic, runbook.
- **Modular architecture**: straightforward to extend with an ML model or swap out the rule engine.
