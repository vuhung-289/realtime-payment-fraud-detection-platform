# Architecture Notes

## Data Flow

1. Producer emits payment events into Kafka topic `payments_raw`.
2. Spark Structured Streaming consumes topic, computes streaming features, and scores each event.
3. Scored events and high-risk alerts are persisted to BigQuery.
4. Streamlit queries BigQuery for near-real-time fraud monitoring.

## Design Choices

- **Kafka + Spark**: common stack in data engineering teams; easy to explain in interviews.
- **Rule-based scoring first**: transparent and auditable before introducing ML.
- **BigQuery serving layer**: simple and powerful for BI dashboards and ad hoc analysis.
- **Explainability**: `risk_reasons` saved for each scored transaction.

## Future Improvements

- Add Schema Registry + Avro/Protobuf contract.
- Replace heuristic new-country/device checks with historical state store.
- Add feature store and ML model serving (Vertex AI/MLflow).
- Add CI pipeline for tests + static checks + docker image build.

