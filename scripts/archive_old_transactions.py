"""
Archive script - Chạy hàng đêm (ví dụ: 2:00 AM) qua Task Scheduler hoặc Airflow.

Nhiệm vụ:
  1. Query BigQuery lấy dữ liệu fraud_scored_transactions cũ hơn ARCHIVE_DAYS_THRESHOLD ngày.
  2. Ghi ra file Parquet nén (gzip) vào thư mục data_lake/ phân theo ngày.
  3. Tính toán và upsert hồ sơ user tổng hợp hàng ngày vào bảng user_daily_profiles.
  4. Xác nhận file Parquet đã ghi thành công TRƯỚC KHI để BigQuery tự xóa partition.

Cách chạy thủ công:
  .venv\\Scripts\\python scripts\\archive_old_transactions.py

Cách cài lịch tự động (Windows Task Scheduler):
  - Program: C:\\...\\realtime-payment-fraud-detection-platform\\.venv\\Scripts\\python.exe
  - Arguments: scripts\\archive_old_transactions.py
  - Start in: C:\\...\\realtime-payment-fraud-detection-platform
  - Trigger: Daily at 02:00 AM
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Đảm bảo import được từ root của project
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from google.cloud import bigquery, storage

from src.common.settings import Settings


def get_bq_client(settings: Settings) -> bigquery.Client:
    return bigquery.Client(
        project=settings.gcp_project_id,
        location="asia-southeast1",
    )


def upload_to_gcs(local_path: Path, gcs_bucket: str, gcs_blob_path: str) -> None:
    """
    Upload một file local lên GCS.
    gcs_blob_path: đường dẫn trong bucket, ví dụ:
        'scored_transactions/year=2025/month=01/day=01/data.parquet'
    """
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_blob_path)
    blob.upload_from_filename(str(local_path))
    print(f"[gcs] Uploaded {local_path.name} → gs://{gcs_bucket}/{gcs_blob_path}")


def archive_scored_transactions(settings: Settings, client: bigquery.Client, cutoff_date: str) -> int:
    """
    Query toàn bộ giao dịch trước cutoff_date và ghi ra file Parquet.
    Trả về số dòng đã archive.
    """
    table = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.fraud_scored_transactions"
    query = f"""
        SELECT *
        FROM `{table}`
        WHERE DATE(event_time) < '{cutoff_date}'
        ORDER BY event_time
    """
    print(f"[archive] Querying transactions before {cutoff_date}...")
    df = client.query(query).to_dataframe()

    if df.empty:
        print("[archive] No old transactions found. Nothing to archive.")
        return 0

    # Ghi ra Parquet phân theo ngày: data_lake/year=YYYY/month=MM/day=DD/scored_transactions.parquet
    output_dir = Path(settings.archive_output_dir)
    total_written = 0

    for date_val, group_df in df.groupby(df["event_time"].dt.date):
        year = date_val.strftime("%Y")
        month = date_val.strftime("%m")
        day = date_val.strftime("%d")
        partition_dir = output_dir / "scored_transactions" / f"year={year}" / f"month={month}" / f"day={day}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_path = partition_dir / "data.parquet"
        group_df.to_parquet(output_path, index=False, compression="gzip", engine="pyarrow")
        total_written += len(group_df)
        print(f"[archive] Wrote {len(group_df):,} rows → {output_path}")

        # Upload lên GCS nếu đã cấu hình bucket
        if settings.archive_gcs_bucket:
            gcs_blob_path = f"scored_transactions/year={year}/month={month}/day={day}/data.parquet"
            upload_to_gcs(output_path, settings.archive_gcs_bucket, gcs_blob_path)

    print(f"[archive] Total archived: {total_written:,} rows from fraud_scored_transactions.")
    return total_written


def upsert_user_daily_profiles(settings: Settings, client: bigquery.Client, cutoff_date: str) -> None:
    """
    Tổng hợp hành vi user theo ngày từ dữ liệu sắp bị xóa,
    rồi INSERT vào bảng user_daily_profiles (roll-up table).
    Dùng để train ML model mà không cần truy vấn dữ liệu thô.
    """
    scored_table = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.fraud_scored_transactions"
    profiles_table = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.user_daily_profiles"

    query = f"""
        SELECT
            DATE(event_time)      AS profile_date,
            user_id,
            COUNT(*)              AS total_transactions,
            SUM(amount)           AS total_amount,
            AVG(amount)           AS avg_amount,
            MAX(risk_score)       AS max_risk_score,
            COUNTIF(risk_score >= {settings.alert_risk_threshold}) AS fraud_alert_count,
            STRING_AGG(DISTINCT country, ',' ORDER BY country) AS countries_used,
            CURRENT_TIMESTAMP()   AS created_at
        FROM `{scored_table}`
        WHERE DATE(event_time) < '{cutoff_date}'
        GROUP BY profile_date, user_id
    """
    print(f"[profile] Computing user daily profiles before {cutoff_date}...")
    df = client.query(query).to_dataframe()

    if df.empty:
        print("[profile] No data to roll up.")
        return

    # Xóa các ngày cũ trong profiles trước khi insert lại (idempotent)
    dates_to_replace = df["profile_date"].astype(str).unique().tolist()
    dates_str = ", ".join(f"'{d}'" for d in dates_to_replace)
    delete_query = f"""
        DELETE FROM `{profiles_table}`
        WHERE CAST(profile_date AS STRING) IN ({dates_str})
    """
    client.query(delete_query).result()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
    )
    job = client.load_table_from_dataframe(df, profiles_table, job_config=job_config)
    job.result()
    print(f"[profile] Upserted {len(df):,} user-day profiles into {profiles_table}.")


def main() -> None:
    settings = Settings()

    if not settings.gcp_project_id:
        print("[ERROR] GCP_PROJECT_ID chưa được cấu hình trong .env")
        sys.exit(1)

    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=settings.archive_days_threshold)).strftime("%Y-%m-%d")
    print(f"[archive] Starting nightly archive job. Cutoff date: {cutoff_date}")
    print(f"[archive] Archive output dir: {settings.archive_output_dir}")

    client = get_bq_client(settings)

    # BƯỚC 1: Archive dữ liệu cũ ra Parquet
    rows_archived = archive_scored_transactions(settings, client, cutoff_date)

    # BƯỚC 2: Chỉ tổng hợp user profiles nếu có dữ liệu cần archive
    if rows_archived > 0:
        upsert_user_daily_profiles(settings, client, cutoff_date)

    print("[archive] Nightly archive job completed successfully.")


if __name__ == "__main__":
    main()
