import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic_payments: str = os.getenv("KAFKA_TOPIC_PAYMENTS", "payments_raw")

    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")
    bigquery_dataset: str = os.getenv("BIGQUERY_DATASET", "fraud_analytics")
    alert_risk_threshold: int = int(os.getenv("ALERT_RISK_THRESHOLD", "75"))

    producer_events_per_second: int = int(os.getenv("PRODUCER_EVENTS_PER_SECOND", "20"))

    # Archival / Data Lake settings
    # Số ngày trước khi archive (nên = partition_expiration_days - buffer)
    archive_days_threshold: int = int(os.getenv("ARCHIVE_DAYS_THRESHOLD", "29"))
    # Thư mục lưu file Parquet ở local (dùng làm bộ đệm tạm trước khi upload GCS)
    archive_output_dir: str = os.getenv("ARCHIVE_OUTPUT_DIR", "data_lake")
    # GCS bucket để lưu Parquet dài hạn. Để trống nếu chỉ lưu local.
    # Ví dụ: "my-fraud-data-lake"  (không có gs:// prefix)
    archive_gcs_bucket: str = os.getenv("ARCHIVE_GCS_BUCKET", "")

