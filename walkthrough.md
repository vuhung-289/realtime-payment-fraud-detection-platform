# Hướng dẫn Project Fraud Detection từ con số 0

## Phần 1: Lý thuyết cốt lõi

### Bài toán: Phát hiện gian lận thanh toán

Hãy tưởng tượng bạn là ngân hàng. Mỗi giây có hàng nghìn giao dịch chuyển tiền. Bạn cần phát hiện **ngay lập tức** giao dịch nào đáng ngờ (fraud), ví dụ:
- Ai đó chuyển **$3000** trong khi bình thường chỉ chuyển **$50**
- **6 giao dịch** trong vòng 5 phút (quá nhanh)
- Chuyển tiền từ **nước lạ** chưa từng dùng
- Nhiều giao dịch **thất bại** liên tiếp (thử nhiều thẻ)

### Các công nghệ chính (giải thích đơn giản)

| Công nghệ | Ví dụ thực tế | Vai trò |
|---|---|---|
| **Kafka** | Băng chuyền trong nhà máy - hàng đặt lên băng chuyền, ai cần thì lấy | Nhận và truyền tải event giao dịch liên tục |
| **Spark Streaming** | Công nhân đứng cuối băng chuyền, kiểm tra từng sản phẩm | Đọc event từ Kafka, tính toán và chấm điểm rủi ro |
| **BigQuery** | Kho lưu trữ khổng lồ của Google | Lưu kết quả đã chấm điểm để phân tích sau |
| **Streamlit** | Màn hình giám sát trong phòng điều khiển | Dashboard hiển thị biểu đồ, cảnh báo |
| **Docker** | Máy ảo nhẹ, cài sẵn phần mềm | Chạy Kafka trên máy local mà không cần cài phức tạp |

### Luồng dữ liệu

```
Giao dịch sinh ra → Kafka (hàng đợi) → Spark (chấm điểm) → BigQuery (lưu trữ) → Dashboard (hiển thị)
```

---

## Phần 2: Thứ tự tạo file/folder (từng bước)

### Bước 0 — Khởi tạo project

Tạo folder gốc và các file quản lý cơ bản:

```
realtime-payment-fraud-detection-platform/
├── .gitignore          ← Bỏ qua file không cần commit (venv, .env, cache)
├── .env.example        ← Mẫu biến môi trường để người khác biết cần config gì
├── requirements.txt    ← Danh sách thư viện Python cần cài
├── README.md           ← Giới thiệu project
└── docker-compose.yml  ← Cấu hình Docker để chạy Kafka
```

**Tại sao tạo trước?** Đây là "xương sống" của mọi project. Giống như xây nhà phải đổ móng trước.

---

#### 📄 [.env.example](file:///c:/Users/hungv/realtime-payment-fraud-detection-platform/.env.example)

```properties
KAFKA_BOOTSTRAP_SERVERS=localhost:9092    # Địa chỉ Kafka
KAFKA_TOPIC_PAYMENTS=payments_raw         # Tên "kênh" trên Kafka
GCP_PROJECT_ID=your-gcp-project-id       # Project Google Cloud
BIGQUERY_DATASET=fraud_analytics          # Tên dataset BigQuery
GOOGLE_APPLICATION_CREDENTIALS=...        # File key xác thực GCP
PRODUCER_EVENTS_PER_SECOND=20            # Tốc độ sinh event
ALERT_RISK_THRESHOLD=75                  # Ngưỡng cảnh báo (0-100)
```

> [!NOTE]
> File `.env.example` là **mẫu**. Khi chạy thật, bạn copy thành `.env` và điền giá trị thật vào.

---

#### 📄 [docker-compose.yml](file:///c:/Users/hungv/realtime-payment-fraud-detection-platform/docker-compose.yml)

```yaml
services:
  zookeeper:    # Quản lý Kafka cluster
    image: confluentinc/cp-zookeeper:7.5.3
    ports: ["2181:2181"]

  kafka:        # Message broker chính
    image: confluentinc/cp-kafka:7.5.3
    depends_on: [zookeeper]
    ports: ["9092:9092"]   # Port để app kết nối
```

**Giải thích:**
- **Zookeeper** = "quản lý" của Kafka, theo dõi ai đang kết nối, topic nào tồn tại
- **Kafka** = "băng chuyền" chính, nhận và phân phối message
- Chỉ cần gõ `docker compose up -d` là có cả hệ thống messaging chạy

---

#### 📄 [requirements.txt](file:///c:/Users/hungv/realtime-payment-fraud-detection-platform/requirements.txt)

```
pyspark==3.5.1              # Xử lý dữ liệu streaming
kafka-python==2.0.2         # Kết nối Python → Kafka
pandas==2.2.2               # Xử lý dữ liệu dạng bảng
python-dotenv==1.0.1        # Đọc file .env
google-cloud-bigquery==3.25.0        # Ghi dữ liệu lên BigQuery
google-cloud-bigquery-storage==2.26.0
streamlit==1.37.1           # Tạo dashboard web
plotly==5.23.0              # Vẽ biểu đồ đẹp
pytest==8.3.2               # Chạy test
```

---

### Bước 1 — Cấu hình & module dùng chung (`config/`, `src/common/`)

```
src/
├── __init__.py             ← Đánh dấu đây là Python package
└── common/
    ├── settings.py         ← Đọc biến môi trường, tập trung config
    ├── schemas.py          ← Định nghĩa cấu trúc dữ liệu (schema)
    └── risk_rules.py       ← Logic chấm điểm rủi ro (CORE!)
```

**Tại sao tạo trước?** Các module khác (producer, streaming, dashboard) đều **phụ thuộc** vào phần này. Giống như phải định nghĩa "quy tắc chơi" trước khi bắt đầu chơi.

---

#### 📄 [settings.py](file:///c:/Users/hungv/realtime-payment-fraud-detection-platform/src/common/settings.py) — Trung tâm cấu hình

```python
@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic_payments: str = os.getenv("KAFKA_TOPIC_PAYMENTS", "payments_raw")
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")
    ...
```

**Giải thích:** Thay vì viết `"localhost:9092"` rải rác khắp code, ta tập trung vào **1 chỗ**. Muốn đổi config thì chỉ sửa file `.env`, không cần sửa code.

---

#### 📄 [schemas.py](file:///c:/Users/hungv/realtime-payment-fraud-detection-platform/src/common/schemas.py) — Hợp đồng dữ liệu

```python
PAYMENT_EVENT_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),  # Bắt buộc
    StructField("event_time", TimestampType(), True),
    StructField("user_id", StringType(), False),         # Bắt buộc
    StructField("amount", DoubleType(), True),
    ...
])
```

**Giải thích:** Đây là "hợp đồng" mô tả 1 giao dịch gồm những trường gì, kiểu dữ liệu gì. Giống như form đăng ký — bạn phải điền đúng ô, đúng loại (số/chữ).

---

#### 📄 [risk_rules.py](file:///c:/Users/hungv/realtime-payment-fraud-detection-platform/src/common/risk_rules.py) — ❤️ Trái tim của project

```python
def compute_risk_score(amount, avg_amount_30m, tx_count_5m, ...) -> (score, band, reasons):
```

Hàm này nhận thông tin giao dịch và trả về **3 thứ**:

| Output | Ví dụ | Ý nghĩa |
|---|---|---|
| `risk_score` | `85` | Điểm rủi ro từ 0-100 |
| `risk_band` | `"critical"` | Mức: low / medium / high / critical |
| `risk_reasons` | `["amount_spike", "high_velocity"]` | Lý do tại sao bị đánh dấu |

**6 quy tắc chấm điểm:**

| Rule | Điều kiện | Điểm cộng |
|---|---|---|
| Amount spike | Giao dịch > 3x trung bình 30 phút | +30 |
| High amount | Giao dịch ≥ $2000 | +20 |
| High velocity | ≥ 6 giao dịch trong 5 phút | +25 |
| Failed burst | ≥ 3 lần thất bại trong 5 phút | +20 |
| New country | Nước lạ + amount ≥ $300 | +15 |
| New device | Device lạ + amount ≥ $300 | +10 |
| International | Giao dịch quốc tế + amount ≥ $500 | +10 |

> [!TIP]
> Tổng tối đa có thể = 30+25+20+15+10+10 = **110** → clamp về **100**.
> Score ≥ 85 = critical, ≥ 70 = high, ≥ 40 = medium, < 40 = low.

---

### Bước 2 — BigQuery tables (`sql/`)

```
sql/
└── bigquery_tables.sql    ← DDL tạo bảng trên BigQuery
```

**Tại sao bước này?** Trước khi streaming job ghi dữ liệu, bảng đích phải tồn tại.

#### 📄 [bigquery_tables.sql](file:///c:/Users/hungv/realtime-payment-fraud-detection-platform/sql/bigquery_tables.sql)

Tạo 2 bảng:
- **`fraud_scored_transactions`** — Mọi giao dịch đã chấm điểm (partition theo ngày, cluster theo risk_band)
- **`fraud_alerts`** — Chỉ giao dịch nguy hiểm (risk_score ≥ 75)

---

### Bước 3 — Producer (`src/producer/`)

```
src/
└── producer/
    └── payment_event_producer.py  ← Sinh dữ liệu giao dịch giả lập
```

**Tại sao bước này?** Cần có **nguồn dữ liệu** trước khi xây phần xử lý. Trong thực tế, đây là app thanh toán thật. Ở đây ta **giả lập** nó.

#### 📄 [payment_event_producer.py](file:///c:/Users/hungv/realtime-payment-fraud-detection-platform/src/producer/payment_event_producer.py)

**Cách hoạt động:**
1. Tạo 1 giao dịch giả → JSON
2. Gửi lên Kafka topic `payments_raw`
3. Sleep → lặp lại (20 event/giây)

**Dữ liệu giả lập thông minh:**
- **92% giao dịch bình thường**: amount $5-$250, nước VN/SG/US, 92% success
- **8% pattern gian lận**: amount $500-$3500, nước JP/TH/ID, 40% failed

```python
if is_fraud_pattern:  # 8% cơ hội
    amount = random.uniform(500, 3500)    # Số tiền lớn bất thường
    country = random.choice(["JP","TH","ID"])  # Nước lạ
    status = 60% success / 40% failed     # Nhiều lần thất bại
```

---

### Bước 4 — Streaming Job (`src/streaming/`)

```
src/
└── streaming/
    └── fraud_streaming_job.py  ← Đọc Kafka → xử lý → ghi BigQuery
```

**Tại sao bước này?** Đây là "bộ não" xử lý chính. Phải có producer (bước 3) chạy trước để có dữ liệu.

#### 📄 [fraud_streaming_job.py](file:///c:/Users/hungv/realtime-payment-fraud-detection-platform/src/streaming/fraud_streaming_job.py)

**Pipeline xử lý (theo thứ tự):**

```
1. Đọc từ Kafka ─→ raw bytes
2. Parse JSON    ─→ cấu trúc có trường
3. Cast types    ─→ amount thành double, event_time thành timestamp
4. Validate      ─→ lọc bỏ record thiếu transaction_id/user_id
5. Dedup         ─→ loại giao dịch trùng transaction_id
6. Tính features ─→ avg_amount, tx_count, failed_count per user
7. Chấm điểm    ─→ gọi compute_risk_score()
8. Ghi BigQuery  ─→ scored_transactions + alerts (nếu score ≥ 75)
```

**Khái niệm quan trọng — `foreachBatch`:**
Spark gom event thành **micro-batch** (ví dụ mỗi 5 giây), xử lý cả batch cùng lúc thay vì từng event → nhanh hơn.

---

### Bước 5 — Dashboard (`dashboard/`)

```
dashboard/
└── app.py    ← Streamlit web app hiển thị kết quả
```

**Tại sao cuối cùng?** Dashboard **đọc** dữ liệu từ BigQuery — cần có dữ liệu đã được ghi (bước 3+4) thì mới có gì hiển thị.

#### 📄 [app.py](file:///c:/Users/hungv/realtime-payment-fraud-detection-platform/dashboard/app.py)

Hiển thị 4 thứ:
1. **KPI cards**: Tổng giao dịch, số high-risk, tỷ lệ fraud, avg score
2. **Biểu đồ histogram**: Phân bố risk band (low/medium/high/critical)
3. **Biểu đồ bar**: Top quốc gia có risk score cao nhất
4. **Bảng dữ liệu**: 100 giao dịch high-risk gần nhất

> [!NOTE]
> Dashboard cache dữ liệu 60 giây (`@st.cache_data(ttl=60)`), tự refresh để gần real-time.

---

### Bước 6 — Tests (`tests/`)

```
tests/
└── test_risk_rules.py    ← Unit test cho logic chấm điểm
```

#### 📄 [test_risk_rules.py](file:///c:/Users/hungv/realtime-payment-fraud-detection-platform/tests/test_risk_rules.py)

2 test cases:
- **`test_high_risk_transaction`**: Giao dịch $3000, velocity cao, failed nhiều → assert score ≥ 85, band = "critical"
- **`test_low_risk_transaction`**: Giao dịch $30 bình thường → assert score < 40, band = "low"

---

### Bước 7 — Docs & Scripts (`docs/`, `scripts/`)

```
docs/
├── architecture.md    ← Giải thích kiến trúc và design choices
└── runbook.md         ← Hướng dẫn vận hành + xử lý sự cố

scripts/
└── start_local.ps1    ← Script PowerShell chạy tất cả 1 lệnh
```

---

## Phần 3: Tóm tắt thứ tự tạo

```
Bước 0: Khởi tạo project
  ├── .gitignore, .env.example, requirements.txt, README.md
  └── docker-compose.yml

Bước 1: Module dùng chung (dependencies trước)
  ├── src/__init__.py
  └── src/common/ → settings.py → schemas.py → risk_rules.py

Bước 2: Tạo bảng BigQuery
  └── sql/bigquery_tables.sql

Bước 3: Producer (nguồn dữ liệu)
  └── src/producer/payment_event_producer.py

Bước 4: Streaming Job (bộ xử lý chính)
  └── src/streaming/fraud_streaming_job.py

Bước 5: Dashboard (hiển thị kết quả)
  └── dashboard/app.py

Bước 6: Tests
  └── tests/test_risk_rules.py

Bước 7: Documentation & Scripts
  ├── docs/architecture.md, docs/runbook.md
  └── scripts/start_local.ps1
```

> [!IMPORTANT]
> **Nguyên tắc chung**: Luôn tạo **dependency trước** → **phần phụ thuộc sau**.
> settings/schemas/rules là nền tảng → producer cần settings → streaming cần cả settings + rules → dashboard cần BigQuery có dữ liệu.
