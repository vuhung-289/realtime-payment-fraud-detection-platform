"""
DAG: archive_fraud_data
Schedule: Mỗi đêm lúc 02:00 AM (Asia/Ho_Chi_Minh)

Pipeline 3 bước:
  1. check_bq_connection    → Kiểm tra kết nối BigQuery trước khi làm gì.
  2. archive_transactions   → Export giao dịch cũ > 29 ngày ra Parquet (Data Lake).
  3. upsert_user_profiles   → Tổng hợp hành vi user theo ngày vào bảng roll-up.

Cách deploy:
  1. Cài Airflow: pip install apache-airflow apache-airflow-providers-google
  2. Copy file này vào thư mục AIRFLOW_HOME/dags/
     (hoặc trỏ AIRFLOW__CORE__DAGS_FOLDER vào thư mục dags/ của project này)
  3. Khởi động Airflow:
       airflow db migrate
       airflow users create --username admin --password admin --role Admin --firstname A --lastname B --email a@b.com
       airflow webserver &
       airflow scheduler &
  4. Mở http://localhost:8080 → Bật DAG "archive_fraud_data" → Done.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Đảm bảo Airflow worker có thể import từ root của project
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.archive_old_transactions import (
    archive_scored_transactions,
    get_bq_client,
    upsert_user_daily_profiles,
)
from src.common.settings import Settings

# ─── Default args áp dụng cho tất cả tasks ────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,           # Không chờ run trước thành công
    "retries": 2,                       # Thử lại 2 lần nếu thất bại
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": False,          # Bật lên nếu bạn có cấu hình SMTP
}


# ─── Hàm tính cutoff_date dùng chung cho cả 2 tasks ──────────────────────────
def _get_cutoff_date() -> str:
    """Ngày cutoff = hôm nay - ARCHIVE_DAYS_THRESHOLD ngày."""
    settings = Settings()
    cutoff = datetime.now(timezone.utc) - timedelta(days=settings.archive_days_threshold)
    return cutoff.strftime("%Y-%m-%d")


# ─── Task 1: Kiểm tra kết nối BigQuery ───────────────────────────────────────
def task_check_connection(**context) -> None:
    settings = Settings()
    client = get_bq_client(settings)
    # Chạy một query đơn giản, nếu lỗi Airflow sẽ retry và cảnh báo
    result = list(client.query("SELECT 1 AS ping").result())
    assert result[0]["ping"] == 1, "BigQuery ping failed"
    print(f"[check] BigQuery connection OK. Project: {settings.gcp_project_id}")


# ─── Task 2: Archive giao dịch ra Parquet ────────────────────────────────────
def task_archive_transactions(**context) -> None:
    settings = Settings()
    client = get_bq_client(settings)
    cutoff_date = _get_cutoff_date()

    rows = archive_scored_transactions(settings, client, cutoff_date)

    # Đẩy kết quả vào XCom để task sau có thể đọc
    context["ti"].xcom_push(key="rows_archived", value=rows)
    context["ti"].xcom_push(key="cutoff_date", value=cutoff_date)
    print(f"[archive] Done. rows_archived={rows}, cutoff_date={cutoff_date}")


# ─── Task 3: Upsert user daily profiles ──────────────────────────────────────
def task_upsert_profiles(**context) -> None:
    rows_archived = context["ti"].xcom_pull(key="rows_archived", task_ids="archive_transactions")
    cutoff_date = context["ti"].xcom_pull(key="cutoff_date", task_ids="archive_transactions")

    if not rows_archived:
        print("[profile] No rows archived in previous task. Skipping profile upsert.")
        return

    settings = Settings()
    client = get_bq_client(settings)
    upsert_user_daily_profiles(settings, client, cutoff_date)
    print(f"[profile] Done. Profiles upserted up to {cutoff_date}.")


# ─── Định nghĩa DAG ──────────────────────────────────────────────────────────
with DAG(
    dag_id="archive_fraud_data",
    description="Hàng đêm archive giao dịch cũ ra Data Lake và tổng hợp user profiles.",
    default_args=default_args,
    schedule_interval="0 2 * * *",     # 02:00 AM mỗi ngày (cron)
    start_date=datetime(2025, 1, 1),
    catchup=False,                     # Không chạy bù các ngày đã qua
    max_active_runs=1,                 # Không cho chạy song song 2 lần cùng lúc
    tags=["fraud-detection", "archive", "bigquery", "data-lake"],
) as dag:

    check_connection = PythonOperator(
        task_id="check_bq_connection",
        python_callable=task_check_connection,
        doc_md="Ping BigQuery để xác nhận credentials và network còn hoạt động.",
    )

    archive_transactions = PythonOperator(
        task_id="archive_transactions",
        python_callable=task_archive_transactions,
        doc_md="Export giao dịch cũ hơn ARCHIVE_DAYS_THRESHOLD ngày ra file Parquet nén.",
    )

    upsert_profiles = PythonOperator(
        task_id="upsert_user_profiles",
        python_callable=task_upsert_profiles,
        doc_md="Tổng hợp hành vi user theo ngày vào bảng user_daily_profiles (roll-up table).",
    )

    # Pipeline: check → archive → profile upsert
    check_connection >> archive_transactions >> upsert_profiles
