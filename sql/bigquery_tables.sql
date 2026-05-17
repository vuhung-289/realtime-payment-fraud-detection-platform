-- ============================================================
-- LIFECYCLE MANAGEMENT STRATEGY
-- ============================================================
-- fraud_scored_transactions : giữ 30 ngày (hot tier), dữ liệu cũ
--                             được archive ra Data Lake (GCS/local Parquet)
-- fraud_alerts              : giữ vĩnh viễn (< 0.1% volume, high value)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS `payment-fraud-detection-496015.fraud_analytics`;

-- ------------------------------------------------------------
-- BẢNG 1: Toàn bộ giao dịch đã chấm điểm
-- Tự động xóa partition sau 30 ngày để kiểm soát chi phí.
-- Script archive sẽ backup dữ liệu ra Parquet trước khi xóa.
-- LƯU Ý: DROP TABLE sẽ xóa dữ liệu hiện tại. Nếu cần giữ lại,
--         hãy export trước: bq extract hoặc dùng archive script.
-- ------------------------------------------------------------
DROP TABLE IF EXISTS `payment-fraud-detection-496015.fraud_analytics.fraud_scored_transactions`;

CREATE TABLE `payment-fraud-detection-496015.fraud_analytics.fraud_scored_transactions` (
  transaction_id STRING,
  event_time TIMESTAMP,
  user_id STRING,
  device_id STRING,
  country STRING,
  merchant_id STRING,
  amount FLOAT64,
  currency STRING,
  payment_status STRING,
  is_international BOOL,
  risk_score INT64,
  risk_band STRING,
  risk_reasons STRING
)
PARTITION BY DATE(event_time)
CLUSTER BY risk_band, country, user_id
OPTIONS (partition_expiration_days = 30);

-- ------------------------------------------------------------
-- BẢNG 2: Chỉ chứa giao dịch gian lận (risk_score >= threshold)
-- KHÔNG xóa - giữ vĩnh viễn cho mục đích điều tra và train AI.
-- Chiếm < 0.1% tổng volume nên chi phí lưu trữ không đáng kể.
-- ------------------------------------------------------------
DROP TABLE IF EXISTS `payment-fraud-detection-496015.fraud_analytics.fraud_alerts`;

CREATE TABLE `payment-fraud-detection-496015.fraud_analytics.fraud_alerts` (
  transaction_id STRING,
  event_time TIMESTAMP,
  user_id STRING,
  device_id STRING,
  country STRING,
  merchant_id STRING,
  amount FLOAT64,
  currency STRING,
  payment_status STRING,
  is_international BOOL,
  risk_score INT64,
  risk_band STRING,
  risk_reasons STRING
)
PARTITION BY DATE(event_time)
CLUSTER BY risk_score, country, user_id;


-- ------------------------------------------------------------
-- BẢNG 3 (Tuỳ chọn): Hồ sơ user tổng hợp hàng ngày (roll-up)
-- Dùng để train ML model mà không cần truy vấn dữ liệu thô.
-- Được populate bởi script archive hàng đêm.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `payment-fraud-detection-496015.fraud_analytics.user_daily_profiles` (
  profile_date DATE,
  user_id STRING,
  total_transactions INT64,
  total_amount FLOAT64,
  avg_amount FLOAT64,
  max_risk_score INT64,
  fraud_alert_count INT64,
  countries_used STRING,
  created_at TIMESTAMP
)
PARTITION BY profile_date
CLUSTER BY user_id;

