# Runbook

## Start Services

1. `docker compose up -d`
2. Start producer: `python src/producer/payment_event_producer.py`
3. Start streaming job: `python src/streaming/fraud_streaming_job.py`
4. Start dashboard: `streamlit run dashboard/app.py`

## Common Issues

- **Kafka connection refused**
  - Check `docker compose ps`
  - Verify `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`

- **BigQuery auth error**
  - Ensure `GOOGLE_APPLICATION_CREDENTIALS` points to valid key JSON.
  - Ensure service account has BigQuery Data Editor.

- **No data on dashboard**
  - Verify producer is sending records.
  - Verify streaming job logs show `wrote_scored`.
  - Query BigQuery table directly to validate inserts.

## Incident Triage

- Spike in critical risk alerts:
  - Check for producer data skew or simulated fraud burst.
  - Validate risk threshold (`ALERT_RISK_THRESHOLD`).
- Sudden drop to zero events:
  - Validate Kafka broker health and producer process status.
- Streaming job crash:
  - Restart job and inspect checkpoint integrity under `checkpoints/`.

