# PRD — Real-time E-commerce Analytics Platform

<!--
  METADATA (for AI Agent parsing)
  ================================
  document_type    : Product Requirements Document
  version          : 3.0
  project_name     : realtime-ecommerce-analytics
  last_updated     : 2026-03-07
  status           : Draft
  language         : Vietnamese (technical terms in English)
  total_modules    : 9
  module_ids       : [F1, F2, F3, F4, F5, F6, F7, F8, F9]
  new_in_v3        : [F5_Airflow, F6_dbt] — added orchestration and transformation layers
-->

---

## QUICK REFERENCE

> AI Agent: Đọc section này trước để nắm toàn bộ hệ thống trong 1 lượt.

```
PROJECT GOAL    : Stream user behavior events (e-commerce) → process real-time → live dashboard
PRIMARY STACK   : Python · Kafka · PySpark · PostgreSQL · Redis · dbt · Airflow · FastAPI · React
RUNTIME MODEL   : docker compose up  →  make seed  →  make simulate  →  everything runs
TOTAL MODULES   : 9 modules, mỗi module = 1 thư mục độc lập

DATA FLOW (tóm tắt):
  [Simulator] → Kafka → [PySpark Streaming] → PostgreSQL (raw)
                                             → Redis (real-time cache)
  [Airflow]   → schedule [dbt] → PostgreSQL (transformed/mart tables)
  [FastAPI]   → query PostgreSQL + Redis → WebSocket → [React Dashboard]

KEY BOUNDARIES:
  - PySpark   : xử lý STREAMING, ghi raw data + real-time aggregates
  - dbt       : xử lý BATCH transformation, tạo mart/reporting tables
  - Airflow   : ORCHESTRATE toàn bộ batch pipeline (seed → dbt run → cleanup)
  - FastAPI   : chỉ READ, không write data
  - Simulator : nguồn dữ liệu duy nhất, chạy độc lập
```

---

## 1. TỔNG QUAN DỰ ÁN

### 1.1 Mục tiêu
Xây dựng hệ thống xử lý **streaming big data** mô phỏng hành vi người dùng trên sàn thương mại điện tử. Hệ thống gồm hai pipeline song song:

- **Streaming pipeline** (real-time): Simulator → Kafka → PySpark → PostgreSQL/Redis → Dashboard
- **Batch pipeline** (scheduled): Airflow orchestrate dbt transformation → PostgreSQL mart tables → Dashboard

### 1.2 Phạm vi
- Simulate users, products, và behavioral events ở quy mô lớn (configurable)
- Real-time streaming processing với PySpark Structured Streaming
- Batch data transformation với dbt, điều phối bởi Airflow
- Live analytics dashboard cập nhật qua WebSocket
- Không bao gồm: payment thật, auth thật, production deployment

### 1.3 Nguyên tắc thiết kế

| Nguyên tắc | Áp dụng cụ thể |
|---|---|
| **Module isolation** | Mỗi module = 1 thư mục, 1 Dockerfile, 1 requirements.txt riêng |
| **Single responsibility** | PySpark chỉ stream, dbt chỉ transform, Airflow chỉ orchestrate |
| **Config over code** | Mọi tham số qua `.env` hoặc `config.yaml`, không hardcode |
| **Runnable locally** | `docker compose up` chạy toàn bộ stack, không cần cloud |
| **Observable** | Structured JSON log ở mọi service, Grafana dashboard built-in |

---

## 2. KIẾN TRÚC TỔNG THỂ

### 2.1 Sơ đồ luồng dữ liệu

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                        STREAMING PIPELINE (real-time)                       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ┌─────────────┐     ┌──────────────────┐     ┌────────────────────────┐    ║
║  │  SIMULATOR  │────▶│  KAFKA BROKER    │────▶│  PYSPARK STREAMING     │    ║
║  │  F1         │     │  F2              │     │  F3                    │    ║
║  │             │     │  · raw-events    │     │  · parse & validate    │    ║
║  │  generates: │     │  · agg-metrics   │     │  · window aggregation  │    ║
║  │  · users    │     │  · dead-letter   │     │  · 1m / 5m / 1h        │    ║
║  │  · products │     └──────────────────┘     └──────────┬─────────────┘    ║
║  │  · events   │                                         │                  ║
║  └─────┬───────┘                              ┌──────────┴──────────┐       ║
║        │ seed (one-time)                      ▼                     ▼       ║
║        ▼                             ┌──────────────┐    ┌──────────────┐   ║
║  ┌───────────────┐                   │  PostgreSQL  │    │    REDIS     │   ║
║  │  PostgreSQL   │                   │  · raw_events│    │  · counters  │   ║
║  │  · users      │                   │  · agg_metrics    │  · top-N     │   ║
║  │  · products   │                   └──────┬───────┘    └──────┬───────┘   ║
║  └───────────────┘                         │                    │           ║
╚════════════════════════════════════════════ │ ═══════════════════│═══════════╝
                                             │                    │ pub/sub
╔════════════════════════════════════════════ │ ═══════════════════╡═══════════╗
║                        BATCH PIPELINE (scheduled)               │           ║
╠══════════════════════════════════════════════════════════════════╪═══════════╣
║                                             │                   │           ║
║  ┌─────────────────────────────────────┐    │                   │           ║
║  │  AIRFLOW  F5                        │    │                   │           ║
║  │                                     │    │                   │           ║
║  │  DAG: daily_batch_pipeline          │    │                   │           ║
║  │  ┌─────────┐  ┌─────────────────┐   │    │                   │           ║
║  │  │ sensor  │─▶│  dbt run (F6)   │   │    │                   │           ║
║  │  │ (check  │  │  · staging      │   │    │                   │           ║
║  │  │  data   │  │  · mart         │   │    │                   │           ║
║  │  │  ready) │  │  · dbt test     │   │    │                   │           ║
║  │  └─────────┘  └────────┬────────┘   │    │                   │           ║
║  │                        │            │    ▼                   │           ║
║  └────────────────────────│────────────┘  ┌─────────────────────┴─────────┐ ║
║                           └─────────────▶ │      PostgreSQL               │ ║
║                                           │  · stg_*  (staging views)     │ ║
║                                           │  · mart_* (reporting tables)  │ ║
║                                           └──────────────────┬────────────┘ ║
╚══════════════════════════════════════════════════════════════ │ ════════════╝
                                                               │
╔══════════════════════════════════════════════════════════════ │ ════════════╗
║                        SERVING LAYER                          │            ║
╠══════════════════════════════════════════════════════════════ │ ════════════╣
║                                                               ▼            ║
║  ┌───────────────────────┐         ┌─────────────────────────────────────┐ ║
║  │    REDIS (F4)         │────────▶│   API  F7  (FastAPI)                │ ║
║  │  pub/sub notify       │         │   · REST endpoints                  │ ║
║  └───────────────────────┘         │   · WebSocket push                  │ ║
║                                    └───────────────┬─────────────────────┘ ║
║                                                    │                       ║
║                                                    ▼                       ║
║                                    ┌─────────────────────────────────────┐ ║
║                                    │   DASHBOARD  F8  (React)            │ ║
║                                    │   · real-time charts (WebSocket)    │ ║
║                                    │   · batch reports (REST poll)       │ ║
║                                    └─────────────────────────────────────┘ ║
╚═════════════════════════════════════════════════════════════════════════════╝
```

### 2.2 Stack công nghệ

| Module | Technology | Version target | Vai trò |
|---|---|---|---|
| Simulator | Python + Faker | Python 3.12 | Sinh dữ liệu giả lập |
| Message Broker | Apache Kafka (KRaft) | 3.7+ | Event streaming backbone |
| Stream Processor | PySpark Structured Streaming | Spark 3.5 | Real-time aggregation |
| Database | PostgreSQL | 16 | Lưu trữ chính |
| Cache | Redis | 7 | Real-time counter + pub/sub |
| Orchestrator | Apache Airflow | 2.9 | Điều phối batch pipeline |
| Transformer | dbt-core + dbt-postgres | dbt 1.8 | SQL transformation + testing |
| API | FastAPI | 0.111 | REST + WebSocket |
| Dashboard | React + Recharts | React 18 | Live analytics UI |
| Infra | Docker Compose | v2 | Local stack management |
| Monitoring | Prometheus + Grafana | latest | Metrics + alerting |

### 2.3 Phân loại pipeline

```
STREAMING PIPELINE          BATCH PIPELINE
──────────────────          ──────────────
Trigger  : continuous       Trigger  : Airflow schedule (hourly/daily)
Latency  : < 5 seconds      Latency  : minutes to hours (acceptable)
Tools    : PySpark           Tools    : dbt (SQL models)
Output   : raw_events        Output   : mart_* reporting tables
           aggregated_metrics           stg_* staging views
Use case : live dashboard    Use case : daily reports, historical trends
```

---

## 3. MODULE SPECIFICATIONS

> Cấu trúc mỗi module:
> - **ID** — tên ngắn
> - **Mô tả** — một câu mục đích
> - **Dependencies** — module nào phải chạy trước
> - **Outputs** — module này tạo ra gì
> - **Sub-functions** — breakdown chi tiết tối đa level 4

---

### F1 — SIMULATOR MODULE

```
id           : F1
purpose      : Sinh toàn bộ dữ liệu giả lập — users, products, events
depends_on   : [F4.storage phải khởi động trước để seed vào DB]
outputs      :
  - PostgreSQL tables: users, products (seeded)
  - Kafka topic: raw-events (continuous stream)
directory    : /simulator
entrypoint   : python main.py --mode [seed|simulate|both]
```

#### F1.1 — User Generator
```
purpose   : Tạo user profiles realistic, seed vào bảng [users]
output    : INSERT records vào PostgreSQL.users
```

- **F1.1.1 — Thông tin định danh**
  - `user_id` : UUID v4
  - `username` : Faker, unique
  - `email` : `{username}@{domain}.com`
  - `full_name` : họ tên đầy đủ
  - `gender` : male / female / other (weighted: 48% / 48% / 4%)
  - `date_of_birth` : random, tuổi 18–65
  - `avatar_url` : placeholder URL

- **F1.1.2 — Thông tin địa lý**
  - `country` : VN 40% · TH 20% · ID 15% · PH 10% · MY 10% · SG 5%
  - `city` : city hợp lệ theo country
  - `timezone` : timezone tương ứng với country

- **F1.1.3 — Thông tin thiết bị**
  - `preferred_device` : mobile 65% · desktop 25% · tablet 10%
  - `os` : iOS / Android / Windows / macOS (phù hợp với device)
  - `preferred_language` : vi / en / th / id (phù hợp với country)

- **F1.1.4 — Behavioral profile** _(dùng để weight event generation)_
  - `user_segment` : new_user · casual · regular · power_user · vip
  - `purchase_frequency` : low / medium / high
  - `price_sensitivity` : budget / mid_range / premium
  - `avg_session_duration_min` : 5–120 phút (tùy segment)
  - `registered_at` : timestamp đăng ký
  - `is_active` : boolean — chỉ active users mới sinh event

#### F1.2 — Product Generator
```
purpose   : Tạo product catalog đầy đủ, seed vào bảng [products]
output    : INSERT records vào PostgreSQL.products
```

- **F1.2.1 — Thông tin cơ bản**
  - `product_id` : UUID v4
  - `sku` : `{CAT}-{random_6digits}` (e.g. `ELC-482931`)
  - `product_name` : tên theo category (e.g. "Samsung Galaxy A55")
  - `brand` : brand hợp lệ theo category
  - `description` : 1–3 câu (Faker)
  - `image_url` : placeholder URL

- **F1.2.2 — Phân loại danh mục**
  - `category_name` : Electronics · Fashion · Beauty · Home & Living · Sports · Food · Books · Toys
  - `subcategory` : subcategory thuộc category (e.g. Electronics → Smartphones / Laptops / Tablets)
  - `tags` : list string (e.g. `["bestseller", "new_arrival", "on_sale"]`)

- **F1.2.3 — Giá & tồn kho**
  - `original_price` : giá gốc theo khoảng của category
  - `sale_price` : bằng hoặc thấp hơn original 10–40%
  - `discount_percent` : tính từ 2 giá trên
  - `stock_quantity` : 0–10,000
  - `currency` : VND / USD / THB / IDR (theo country distribution)

- **F1.2.4 — Metadata & scoring**
  - `rating_avg` : 1.0–5.0
  - `rating_count` : số lượng đánh giá
  - `is_available` : false nếu `stock_quantity = 0`
  - `popularity_score` : 0–100, **dùng làm weight khi pick product trong event generation**
  - `created_at` : timestamp

#### F1.3 — Event Generator
```
purpose   : Sinh behavioral events liên tục, realistic, publish lên Kafka
output    : JSON messages → Kafka topic [raw-events]
```

- **F1.3.1 — Event types và tỷ lệ**

  | event_type | Mô tả | Default weight |
  |---|---|---|
  | `page_view` | Xem trang chủ / category / search | 30% |
  | `product_view` | Xem chi tiết sản phẩm | 25% |
  | `click` | Click sản phẩm trong listing | 20% |
  | `add_to_cart` | Thêm vào giỏ | 12% |
  | `remove_from_cart` | Xóa khỏi giỏ | 3% |
  | `wishlist_add` | Thêm vào wishlist | 5% |
  | `purchase` | Hoàn tất đặt hàng | 4% |
  | `review_submit` | Gửi đánh giá | 1% |

- **F1.3.2 — Event JSON schema**
  ```json
  {
    "event_id"      : "uuid-v4",
    "event_type"    : "purchase",
    "event_version" : "1.0",
    "timestamp"     : "2026-03-07T10:23:45.123Z",
    "session_id"    : "uuid-v4",
    "user" : {
      "user_id"   : "uuid-v4",
      "segment"   : "regular",
      "country"   : "VN",
      "city"      : "Ho Chi Minh City",
      "device"    : "mobile",
      "os"        : "Android",
      "language"  : "vi"
    },
    "product" : {
      "product_id"  : "uuid-v4",
      "sku"         : "ELC-123456",
      "name"        : "Samsung Galaxy A55",
      "category"    : "Electronics",
      "subcategory" : "Smartphones",
      "brand"       : "Samsung",
      "sale_price"  : 8990000,
      "currency"    : "VND"
    },
    "event_data" : {
      "quantity"          : 1,
      "referrer"          : "search",
      "search_keyword"    : "samsung 5g",
      "position_in_list"  : 3,
      "time_on_page_sec"  : 45,
      "payment_method"    : "e-wallet",
      "order_id"          : "ORD-20260307-00123"
    },
    "metadata" : {
      "ip_address"  : "192.168.x.x",
      "user_agent"  : "Mozilla/5.0 ...",
      "app_version" : "3.2.1",
      "screen_size" : "390x844"
    }
  }
  ```

- **F1.3.3 — Behavior simulation logic**
  - User funnel có xác suất: `page_view → product_view → click → add_to_cart → purchase`
  - `power_user` và `vip` có purchase rate cao hơn 3× so với `new_user`
  - Product được pick theo `popularity_score` (weighted random, không uniform)
  - Session lifecycle: `session_id` mới sau 30 phút idle
  - Peak hour simulation (configurable): event rate ×3 lúc 12:00–13:00 và 20:00–22:00

- **F1.3.4 — Load configuration** _(qua `config.yaml` hoặc env vars)_
  - `NUM_ACTIVE_USERS` : số user đồng thời (default: 100)
  - `TARGET_EVENTS_PER_SEC` : throughput mục tiêu (default: 500)
  - `RUN_MODE` : `continuous` | `timed:{seconds}`
  - `PEAK_HOUR_ENABLED` : true / false

#### F1.4 — Kafka Producer
```
purpose   : Serialize và publish events lên Kafka đảm bảo reliability
output    : messages trong Kafka topic [raw-events]
```

- **F1.4.1** — Serialize event dict → JSON bytes
- **F1.4.2** — Partition key = `user_id` → đảm bảo ordering per user
- **F1.4.3** — Batch publish: max 100 messages hoặc timeout 50ms
- **F1.4.4** — Retry 3 lần với exponential backoff; sau đó log error và drop

#### F1.5 — Data Seeder
```
purpose   : Seed users và products vào PostgreSQL trước khi simulate
output    : populated tables [users] và [products]
run_once  : true (idempotent — chạy lại không duplicate)
```

- **F1.5.1** — `seed_users.py` : generate + batch INSERT vào `users`
- **F1.5.2** — `seed_products.py` : generate + batch INSERT vào `products`
- **F1.5.3** — `seed_all.py` : chạy tuần tự F1.5.1 → F1.5.2, kiểm tra idempotency
- **F1.5.4** — Configurable: `NUM_USERS=10000`, `NUM_PRODUCTS=5000` qua env vars

---

### F2 — KAFKA BROKER MODULE

```
id           : F2
purpose      : Message backbone — nhận events từ Simulator, phân phối cho consumers
depends_on   : [] (infra, khởi động đầu tiên)
outputs      :
  - topic [raw-events]       : consumed by PySpark (F3)
  - topic [aggregated-metrics]: output từ PySpark
  - topic [dead-letter-queue] : malformed events
directory    : /infra/kafka
```

#### F2.1 — Topic configuration

| Topic | Partitions | Retention | Producer | Consumer |
|---|---|---|---|---|
| `raw-events` | 6 | 24h | Simulator (F1) | PySpark (F3) |
| `aggregated-metrics` | 3 | 48h | PySpark (F3) | API / downstream |
| `dead-letter-queue` | 1 | 72h | PySpark (F3) | manual inspection |

#### F2.2 — Infrastructure setup
- **F2.2.1** — Kafka KRaft mode (no Zookeeper) chạy qua Docker
- **F2.2.2** — AKHQ UI expose tại `localhost:8080` — xem topics, consumer lag, message browser
- **F2.2.3** — `init-topics.sh` tự động tạo topics khi container start (idempotent)

#### F2.3 — Consumer groups

| Group ID | Module | Topic consumed |
|---|---|---|
| `pyspark-streaming` | F3 PySpark | `raw-events` |
| `raw-ingester` | F3 PySpark | `raw-events` |

- **F2.3.1** — Monitor consumer lag qua AKHQ; log WARNING khi lag > 10,000

---

### F3 — STREAMING MODULE (PySpark)

```
id           : F3
purpose      : Xử lý real-time event stream, tính windowed aggregations, ghi ra storage
depends_on   : [F2.Kafka phải có data, F4.PostgreSQL phải sẵn sàng, F4.Redis phải sẵn sàng]
outputs      :
  - PostgreSQL table: raw_events (append)
  - PostgreSQL table: aggregated_metrics (upsert per window)
  - Redis: real-time counters + sorted sets
  - Redis pub/sub: channel [metrics-update] trigger
  - Kafka topic: aggregated-metrics
directory    : /streaming
entrypoint   : python main.py
```

#### F3.1 — Spark session setup
- **F3.1.1** — SparkSession với package `spark-sql-kafka-0-10_2.12`
- **F3.1.2** — `spark.sql.streaming.checkpointLocation` → `/tmp/spark-checkpoints/{job_name}`
- **F3.1.3** — Watermark 2 phút cho tất cả windowed queries (xử lý late data)
- **F3.1.4** — Output mode: `update` cho aggregation sinks · `append` cho raw event sink

#### F3.2 — Stream ingestion
- **F3.2.1** — Read Kafka `raw-events` → Structured Streaming DataFrame
- **F3.2.2** — Parse JSON → StructType schema (defined in `schemas/event_schema.py`)
- **F3.2.3** — Flatten nested fields: `user.*` · `product.*` · `event_data.*` → flat columns
- **F3.2.4** — Validate: null `event_id` / null `user_id` / invalid `event_type` → route → DLQ

#### F3.3 — Windowed aggregations

- **F3.3.1 — Tumbling window 1 phút** _(file: `jobs/aggregate_1m.py`)_
  - Event count per `event_type` per minute
  - Revenue: `SUM(sale_price × quantity)` WHERE `event_type = 'purchase'`
  - Active users: `COUNT(DISTINCT user_id)` per minute

- **F3.3.2 — Sliding window 5 phút, slide 1 phút** _(file: `jobs/aggregate_5m.py`)_
  - Conversion rate: `purchase_count / click_count`
  - Add-to-cart rate: `add_to_cart_count / product_view_count`
  - Avg `time_on_page_sec` per category

- **F3.3.3 — Tumbling window 1 giờ** _(file: `jobs/aggregate_1h.py`)_
  - Revenue per category
  - Top 10 products by purchase count
  - User segment distribution

#### F3.4 — Sinks

- **F3.4.1 — PostgreSQL sink** _(file: `sinks/postgres_sink.py`)_
  - `foreachBatch` → INSERT `raw_events`
  - `foreachBatch` → UPSERT `aggregated_metrics` ON CONFLICT (window_start, window_type, metric_name)
  - Trigger: `processingTime = "30 seconds"`

- **F3.4.2 — Redis sink** _(file: `sinks/redis_sink.py`)_
  - `HSET metric:summary` → cập nhật counters
  - `ZADD top:products:click` và `top:products:purchase` → sorted sets
  - `PUBLISH metrics-update` → notify API layer

- **F3.4.3 — Kafka sink** _(file: `sinks/kafka_sink.py`)_
  - Ghi aggregated results → topic `aggregated-metrics`

#### F3.5 — Error handling
- **F3.5.1** — Malformed events → `PRODUCE` vào Kafka `dead-letter-queue` kèm field `error_reason`
- **F3.5.2** — Container `restart: always` trong Docker Compose → auto-recovery
- **F3.5.3** — Checkpoint đảm bảo exactly-once semantics sau restart

---

### F4 — STORAGE MODULE

```
id           : F4
purpose      : Định nghĩa schema, migration, và data model cho PostgreSQL và Redis
depends_on   : [] (infra, khởi động cùng với F2)
outputs      :
  - PostgreSQL schemas: users, products, raw_events, aggregated_metrics
  - PostgreSQL schemas (dbt): stg_*, mart_* (tạo bởi F6.dbt)
  - Redis key conventions (documented)
directory    : /storage
```

#### F4.1 — PostgreSQL: raw tables

- **F4.1.1 — Bảng `users`** _(seed data từ F1.5)_
  ```sql
  user_id UUID PRIMARY KEY,
  username VARCHAR UNIQUE, email VARCHAR,
  full_name VARCHAR, gender VARCHAR, date_of_birth DATE,
  country VARCHAR, city VARCHAR, timezone VARCHAR,
  preferred_device VARCHAR, os VARCHAR, preferred_language VARCHAR,
  user_segment VARCHAR, purchase_frequency VARCHAR, price_sensitivity VARCHAR,
  registered_at TIMESTAMPTZ, is_active BOOLEAN, created_at TIMESTAMPTZ
  ```

- **F4.1.2 — Bảng `products`** _(seed data từ F1.5)_
  ```sql
  product_id UUID PRIMARY KEY,
  sku VARCHAR UNIQUE, product_name VARCHAR, brand VARCHAR,
  description TEXT, image_url VARCHAR,
  category_name VARCHAR, subcategory VARCHAR, tags JSONB,
  original_price NUMERIC, sale_price NUMERIC, discount_percent NUMERIC,
  stock_quantity INT, currency VARCHAR,
  rating_avg NUMERIC, rating_count INT,
  is_available BOOLEAN, popularity_score INT,
  created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ
  ```

- **F4.1.3 — Bảng `raw_events`** _(streaming sink từ F3)_
  ```sql
  event_id UUID PRIMARY KEY, event_type VARCHAR, event_version VARCHAR,
  timestamp TIMESTAMPTZ, session_id UUID,
  user_id UUID, user_segment VARCHAR, country VARCHAR, device VARCHAR,
  product_id UUID, category_name VARCHAR, subcategory VARCHAR,
  sale_price NUMERIC, currency VARCHAR, quantity INT,
  referrer VARCHAR, search_keyword VARCHAR,
  position_in_list INT, time_on_page_sec INT,
  order_id VARCHAR, ingested_at TIMESTAMPTZ
  ```

- **F4.1.4 — Bảng `aggregated_metrics`** _(streaming sink từ F3)_
  ```sql
  id BIGSERIAL PRIMARY KEY,
  window_start TIMESTAMPTZ, window_end TIMESTAMPTZ,
  window_type VARCHAR,        -- '1m' | '5m' | '1h'
  metric_name VARCHAR,        -- 'click_count' | 'revenue' | 'conversion_rate' | ...
  metric_value NUMERIC,
  dimension_key VARCHAR,      -- 'event_type' | 'category' | 'country' | null
  dimension_value VARCHAR,    -- 'purchase' | 'Electronics' | 'VN' | null
  computed_at TIMESTAMPTZ,
  UNIQUE (window_start, window_type, metric_name, dimension_key, dimension_value)
  ```

#### F4.2 — PostgreSQL: dbt-managed tables _(tạo bởi F6)_

```
NOTE: Các bảng này KHÔNG có trong migrations/
      Chúng được tạo và quản lý hoàn toàn bởi dbt (F6)

stg_events          : cleaned, typed events từ raw_events
stg_users           : cleaned users
stg_products        : cleaned products

mart_daily_summary  : daily KPIs (revenue, orders, users, conversion)
mart_product_perf   : product performance theo ngày/tuần
mart_user_segments  : phân tích theo user segment
mart_funnel         : funnel analysis (view → click → cart → purchase)
mart_geo_revenue    : revenue breakdown theo country/city
```

#### F4.3 — Index strategy

- **F4.3.1** — `raw_events(timestamp, event_type)` — range scan cho time queries
- **F4.3.2** — `raw_events(user_id, timestamp)` — user-level queries
- **F4.3.3** — `aggregated_metrics(window_start, window_type, metric_name)` — dashboard queries
- **F4.3.4** — `products(category_name, popularity_score DESC)` — product lookup

#### F4.4 — Migration management

- **F4.4.1** — Files: `storage/migrations/001_*.sql` ... `005_*.sql` (raw tables only, không có dbt tables)
- **F4.4.2** — `run_migrations.py` : đọc tuần tự, track bằng bảng `schema_migrations`, idempotent
- **F4.4.3** — Chạy tự động khi container `storage-init` start trong Docker Compose

#### F4.5 — Redis data model

| Key pattern | Type | TTL | Nội dung |
|---|---|---|---|
| `metric:summary` | HASH | none | fields: `click_1m`, `cart_1m`, `purchase_1m`, `revenue_1m` |
| `top:products:click` | ZSET | none | score = click_count |
| `top:products:purchase` | ZSET | none | score = purchase_count |
| `session:{session_id}` | STRING | 1800s | marker để detect active sessions |
| channel: `metrics-update` | PUB/SUB | — | trigger API → WebSocket push |

---

### F5 — AIRFLOW MODULE (Orchestration)

```
id           : F5
purpose      : Điều phối toàn bộ batch pipeline — scheduling, dependency management, monitoring
depends_on   : [F4.PostgreSQL, F6.dbt phải được cài đặt]
outputs      :
  - Trigger dbt runs theo schedule
  - Monitor và alert khi pipeline fail
  - Cung cấp audit log cho mọi pipeline run
directory    : /airflow
ui_url       : http://localhost:8088
```

#### F5.1 — DAG: `daily_batch_pipeline`
```
schedule     : "0 1 * * *"  (1:00 AM hàng ngày)
purpose      : Chạy full dbt pipeline sau khi đủ data của ngày hôm trước
max_active_runs : 1
```

- **F5.1.1 — Task: `check_data_availability`**
  - Sensor kiểm tra `raw_events` có records của ngày hôm qua
  - Timeout: 2 giờ; poke interval: 5 phút
  - Fail pipeline nếu không có data sau timeout

- **F5.1.2 — Task: `run_dbt_staging`**
  - Chạy `dbt run --select staging`
  - Phụ thuộc: `check_data_availability` pass

- **F5.1.3 — Task: `run_dbt_marts`**
  - Chạy `dbt run --select marts`
  - Phụ thuộc: `run_dbt_staging` success

- **F5.1.4 — Task: `run_dbt_tests`**
  - Chạy `dbt test`
  - Phụ thuộc: `run_dbt_marts` success
  - Fail task (không fail DAG) nếu test warn; fail DAG nếu test error

#### F5.2 — DAG: `hourly_refresh`
```
schedule     : "0 * * * *"  (mỗi đầu giờ)
purpose      : Refresh một số mart tables cần độ trễ thấp hơn (hourly granularity)
```

- **F5.2.1 — Task: `run_dbt_hourly_models`**
  - Chạy `dbt run --select tag:hourly`
  - Chỉ chạy các models được tag `hourly` trong dbt

- **F5.2.2 — Task: `notify_api_cache_refresh`**
  - Sau khi dbt xong → gọi `POST /internal/cache/invalidate` trên API
  - API sẽ xóa cache cũ và query lại từ mart tables

#### F5.3 — DAG: `seed_and_init` _(manual trigger, chạy 1 lần)_
```
schedule     : None  (manual trigger only)
purpose      : Bootstrap — chạy seeder khi setup lần đầu
```

- **F5.3.1 — Task: `run_migrations`** : chạy `storage/run_migrations.py`
- **F5.3.2 — Task: `seed_users`** : chạy `simulator/seeder/seed_users.py`
- **F5.3.3 — Task: `seed_products`** : chạy `simulator/seeder/seed_products.py`
- **F5.3.4 — Task: `run_dbt_compile`** : chạy `dbt compile` để verify models

#### F5.4 — Airflow infrastructure config
- **F5.4.1** — Executor: `LocalExecutor` (đủ cho single-node dev)
- **F5.4.2** — Metadata DB: dùng chung PostgreSQL instance, schema `airflow`
- **F5.4.3** — DAG files mount từ `./airflow/dags/` vào container
- **F5.4.4** — Connections cấu hình qua Airflow UI hoặc env vars:
  - `AIRFLOW_CONN_POSTGRES_DEFAULT` : PostgreSQL connection string
  - `AIRFLOW_CONN_DBT_DEFAULT` : path đến dbt project

---

### F6 — DBT MODULE (Transformation)

```
id           : F6
purpose      : Transform raw data thành analytics-ready mart tables bằng SQL models
depends_on   : [F4.raw tables phải có data, F5.Airflow gọi dbt run]
outputs      :
  - PostgreSQL views: stg_* (staging layer)
  - PostgreSQL tables: mart_* (mart layer, materialized)
directory    : /dbt
dbt_project  : /dbt/dbt_project.yml
profiles     : /dbt/profiles.yml  (kết nối PostgreSQL)
```

#### F6.1 — Staging layer _(materialized as: view)_
```
convention : stg_{source}_{entity}
purpose    : Clean, rename, type-cast raw data — KHÔNG có business logic
```

- **F6.1.1 — `stg_events`**
  - Source: `raw_events`
  - Transforms: rename columns → snake_case · cast timestamps · filter out test events
  - Tests: `not_null(event_id)` · `not_null(user_id)` · `accepted_values(event_type)`

- **F6.1.2 — `stg_users`**
  - Source: `users`
  - Transforms: standardize `country` codes · normalize `user_segment` values
  - Tests: `unique(user_id)` · `not_null(user_id)`

- **F6.1.3 — `stg_products`**
  - Source: `products`
  - Transforms: parse `tags` JSONB → array · compute `is_on_sale` boolean
  - Tests: `unique(product_id)` · `not_null(product_id)`

#### F6.2 — Mart layer _(materialized as: table, incremental where applicable)_
```
convention : mart_{domain}_{subject}
purpose    : Business-level aggregations cho dashboard và reporting
```

- **F6.2.1 — `mart_daily_summary`** _(incremental by date)_
  ```
  Columns  : date, total_events, total_purchases, total_revenue,
             unique_users, conversion_rate, avg_order_value
  Granularity : 1 row per day
  Tags     : daily
  ```

- **F6.2.2 — `mart_product_performance`** _(incremental by date)_
  ```
  Columns  : date, product_id, product_name, category,
             view_count, click_count, cart_count, purchase_count,
             revenue, conversion_rate
  Granularity : 1 row per product per day
  Tags     : daily
  ```

- **F6.2.3 — `mart_user_segments`** _(incremental by date)_
  ```
  Columns  : date, user_segment, user_count, event_count,
             purchase_count, revenue, avg_session_duration
  Granularity : 1 row per segment per day
  Tags     : daily
  ```

- **F6.2.4 — `mart_funnel_analysis`** _(incremental by date)_
  ```
  Columns  : date, category, page_view_count, product_view_count,
             click_count, add_to_cart_count, purchase_count,
             view_to_click_rate, click_to_cart_rate, cart_to_purchase_rate
  Granularity : 1 row per category per day
  Tags     : daily
  ```

- **F6.2.5 — `mart_geo_revenue`** _(incremental by date)_
  ```
  Columns  : date, country, city, event_count, purchase_count, revenue
  Granularity : 1 row per country/city per day
  Tags     : daily
  ```

- **F6.2.6 — `mart_hourly_kpi`** _(incremental by hour)_
  ```
  Columns  : hour_start, total_events, purchases, revenue, active_users
  Granularity : 1 row per hour
  Tags     : hourly  ← được Airflow F5.2 trigger hàng giờ
  ```

#### F6.3 — dbt project config
- **F6.3.1** — `dbt_project.yml` : định nghĩa models, seeds, tests, vars
- **F6.3.2** — `profiles.yml` : PostgreSQL connection (đọc từ env vars, không hardcode)
- **F6.3.3** — `schema.yml` per model : descriptions + column-level tests
- **F6.3.4** — `sources.yml` : khai báo raw PostgreSQL tables là sources

#### F6.4 — dbt testing strategy
- **F6.4.1** — Generic tests: `not_null`, `unique`, `accepted_values`, `relationships`
- **F6.4.2** — Custom test: `assert_revenue_non_negative` — revenue không âm
- **F6.4.3** — Custom test: `assert_conversion_rate_valid` — conversion rate ∈ [0, 1]
- **F6.4.4** — Severity: `error` cho primary key tests · `warn` cho business rule tests

---

### F7 — API MODULE

```
id           : F7
purpose      : Serve data tới Dashboard qua REST (batch/historical) và WebSocket (real-time)
depends_on   : [F4.PostgreSQL, F4.Redis]
outputs      :
  - REST API endpoints (historical + batch data từ PostgreSQL mart tables)
  - WebSocket push (real-time updates từ Redis pub/sub)
directory    : /api
port         : 8000
rule         : READ ONLY — API không write bất kỳ data nào
```

#### F7.1 — REST endpoints

| Method | Path | Data source | Mô tả |
|---|---|---|---|
| GET | `/api/v1/metrics/summary` | Redis HASH | KPIs hiện tại (real-time) |
| GET | `/api/v1/metrics/timeseries` | PostgreSQL `aggregated_metrics` | Chuỗi thời gian cho charts |
| GET | `/api/v1/metrics/top-products` | Redis ZSET | Top products real-time |
| GET | `/api/v1/metrics/breakdown` | PostgreSQL `aggregated_metrics` | Phân bổ theo dimension |
| GET | `/api/v1/reports/daily` | PostgreSQL `mart_daily_summary` | Báo cáo ngày từ dbt |
| GET | `/api/v1/reports/funnel` | PostgreSQL `mart_funnel_analysis` | Funnel analysis từ dbt |
| GET | `/api/v1/reports/products` | PostgreSQL `mart_product_performance` | Product performance từ dbt |
| GET | `/api/v1/health` | all services | Health check |
| POST | `/internal/cache/invalidate` | — | Trigger bởi Airflow (F5.2.2) |

- **F7.1.1** — Query params chuẩn: `from`, `to` (ISO 8601) · `window_type` · `limit` · `dimension`
- **F7.1.2** — Response format: `{ data: [...], meta: { count, from, to, generated_at } }`

#### F7.2 — WebSocket
- **F7.2.1** — `WS /ws/live` : subscribe Redis pub/sub channel `metrics-update`, forward tới clients
- **F7.2.2** — Client gửi JSON `{ "subscribe": ["summary", "top-products"] }` để filter channels
- **F7.2.3** — Heartbeat ping mỗi 30 giây · auto-reconnect sau 5 giây nếu disconnect

#### F7.3 — Middleware & config
- **F7.3.1** — CORS: `localhost:3000` (dev) + production domain
- **F7.3.2** — Rate limiting: 200 req/min per IP (REST) · unlimited (WebSocket)
- **F7.3.3** — Config via `.env`: `DATABASE_URL` · `REDIS_URL` · `KAFKA_BOOTSTRAP_SERVERS`

---

### F8 — DASHBOARD MODULE

```
id           : F8
purpose      : Single-page app hiển thị live analytics, không cần refresh
depends_on   : [F7.API phải running]
outputs      : UI tại http://localhost:3000
data_sources :
  - WebSocket /ws/live  → real-time panels (KPI cards, live charts)
  - REST /api/v1/reports → batch panels (daily reports, funnel, product perf)
directory    : /dashboard
```

#### F8.1 — Real-time panels _(cập nhật qua WebSocket)_

- **F8.1.1 — KPI Overview**
  - 4 cards: Total Events · Active Users (5m) · Revenue (today) · Conversion Rate (5m)
  - Mũi tên + màu xanh/đỏ so với window trước
  - Sparkline mini-chart trend 30 phút

- **F8.1.2 — Live Event Stream Charts**
  - Line chart: event volume rolling 30 phút, 3 series (click / cart / purchase)
  - Stacked bar: event breakdown theo type, window hiện tại
  - Area chart: revenue theo giờ trong ngày

- **F8.1.3 — Top Products Table (real-time)**
  - Top 10 products, toggle: by click / by purchase / by revenue
  - Rank change column: `▲2` / `▼1`, animate highlight khi thay đổi
  - Cập nhật mỗi 30 giây

#### F8.2 — Batch report panels _(cập nhật qua REST poll)_

- **F8.2.1 — Daily Summary Report**
  - Source: `mart_daily_summary` (dbt output)
  - Bar chart: revenue 7 ngày gần nhất
  - Table: daily KPIs với WoW comparison

- **F8.2.2 — Funnel Analysis**
  - Source: `mart_funnel_analysis` (dbt output)
  - Funnel chart: view → click → cart → purchase per category
  - Dropdown chọn category

- **F8.2.3 — Dimension Breakdown**
  - Source: `aggregated_metrics` (real-time agg)
  - Pie/donut: phân bổ theo country / device
  - Bar: phân bổ theo user segment

#### F8.3 — System Status Bar
- **F8.3.1** — WebSocket status: Connected / Reconnecting / Disconnected (màu xanh/vàng/đỏ)
- **F8.3.2** — Events/sec từ Simulator (cập nhật real-time)
- **F8.3.3** — Kafka consumer lag badge: OK / Warning / Critical
- **F8.3.4** — Last dbt run: timestamp + status (success / failed) từ Airflow

---

### F9 — OBSERVABILITY MODULE

```
id           : F9
purpose      : Monitor, log, và alert cho toàn bộ hệ thống
depends_on   : [] (chạy song song, không block modules khác)
outputs      :
  - Structured logs từ tất cả services (stdout → docker logs)
  - Prometheus metrics từ tất cả Python services
  - Grafana dashboards pre-built
directory    : /observability
grafana_url  : http://localhost:3001
```

#### F9.1 — Structured logging
- **F9.1.1** — Tất cả Python services dùng `structlog`, output JSON
- **F9.1.2** — Log fields chuẩn: `service` · `level` · `timestamp` · `message` · `trace_id`
- **F9.1.3** — `LOG_LEVEL` env var: `DEBUG` (dev) · `INFO` (prod)
- **F9.1.4** — `docker compose logs -f {service}` để xem logs per service

#### F9.2 — Metrics (Prometheus + Grafana)
- **F9.2.1** — `/metrics` endpoint tại: simulator · streaming · api
- **F9.2.2** — Grafana pre-built dashboards:
  - **Kafka**: topic throughput, consumer lag per group, partition distribution
  - **PySpark**: batch processing time, records per batch, checkpoint latency
  - **PostgreSQL**: query latency, connection pool, table sizes
  - **Redis**: memory usage, pub/sub message rate, key counts
  - **Airflow**: DAG run duration, task success/fail rate
  - **dbt**: model run time, test pass/fail counts
- **F9.2.3** — Alert rules:
  - Kafka consumer lag > 10,000 → WARNING
  - PySpark batch time > 60s → WARNING
  - Airflow DAG fail → CRITICAL
  - dbt test error → CRITICAL

#### F9.3 — Developer experience
- **F9.3.1** — `docker compose up` : khởi động toàn bộ stack
- **F9.3.2** — `make seed` : chạy F1.5 Data Seeder
- **F9.3.3** — `make simulate` : chạy F1 Event Generator
- **F9.3.4** — `make dbt-run` : trigger dbt run manually (bypass Airflow)
- **F9.3.5** — `make reset` : drop all data, re-run migrations, fresh start
- **F9.3.6** — `make logs SERVICE=streaming` : tail logs cho service cụ thể

---

## 4. CẤU TRÚC THƯ MỤC

```
realtime-ecommerce-analytics/
│
├── docker-compose.yml          # Toàn bộ stack: 10+ services
├── Makefile                    # seed · simulate · dbt-run · reset · logs
├── .env.example                # Template — copy thành .env trước khi chạy
├── README.md
│
│
├── simulator/                  # ══ F1: SIMULATOR ══════════════════════════
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── config.yaml             # NUM_USERS, EVENTS_PER_SEC, RUN_MODE, ...
│   ├── main.py                 # Entrypoint: --mode seed|simulate|both
│   │
│   ├── generators/
│   │   ├── user_generator.py   # F1.1 — user profiles
│   │   ├── product_generator.py# F1.2 — product catalog
│   │   └── event_generator.py  # F1.3 — behavioral events
│   │
│   ├── producer/
│   │   └── kafka_producer.py   # F1.4 — publish to Kafka
│   │
│   └── seeder/
│       ├── seed_users.py       # F1.5.1
│       ├── seed_products.py    # F1.5.2
│       └── seed_all.py         # F1.5.3 — idempotent entrypoint
│
│
├── streaming/                  # ══ F3: PYSPARK STREAMING ═══════════════════
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                 # SparkSession init, submit jobs
│   │
│   ├── jobs/
│   │   ├── ingest_stream.py    # F3.2 — read Kafka, parse JSON
│   │   ├── aggregate_1m.py     # F3.3.1 — tumbling 1m
│   │   ├── aggregate_5m.py     # F3.3.2 — sliding 5m
│   │   └── aggregate_1h.py     # F3.3.3 — tumbling 1h
│   │
│   ├── sinks/
│   │   ├── postgres_sink.py    # F3.4.1
│   │   ├── redis_sink.py       # F3.4.2
│   │   └── kafka_sink.py       # F3.4.3
│   │
│   └── schemas/
│       └── event_schema.py     # F3.2.2 — StructType definition
│
│
├── storage/                    # ══ F4: STORAGE ════════════════════════════
│   ├── migrations/
│   │   ├── 001_create_users.sql
│   │   ├── 002_create_products.sql
│   │   ├── 003_create_raw_events.sql
│   │   ├── 004_create_aggregated_metrics.sql
│   │   └── 005_create_indexes.sql
│   ├── run_migrations.py       # F4.4.2 — idempotent migration runner
│   └── redis_schema.md         # F4.5 — Redis key conventions (docs)
│
│
├── airflow/                    # ══ F5: AIRFLOW ORCHESTRATION ═══════════════
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       ├── daily_batch_pipeline.py   # F5.1 — daily dbt run
│       ├── hourly_refresh.py         # F5.2 — hourly mart refresh
│       └── seed_and_init.py          # F5.3 — one-time bootstrap DAG
│
│
├── dbt/                        # ══ F6: DBT TRANSFORMATION ══════════════════
│   ├── dbt_project.yml
│   ├── profiles.yml
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_events.sql      # F6.1.1
│   │   │   ├── stg_users.sql       # F6.1.2
│   │   │   ├── stg_products.sql    # F6.1.3
│   │   │   └── schema.yml          # tests + descriptions
│   │   │
│   │   └── marts/
│   │       ├── mart_daily_summary.sql        # F6.2.1
│   │       ├── mart_product_performance.sql  # F6.2.2
│   │       ├── mart_user_segments.sql        # F6.2.3
│   │       ├── mart_funnel_analysis.sql      # F6.2.4
│   │       ├── mart_geo_revenue.sql          # F6.2.5
│   │       ├── mart_hourly_kpi.sql           # F6.2.6
│   │       └── schema.yml                    # tests + descriptions
│   │
│   └── sources.yml             # F6.3.4 — khai báo raw tables là dbt sources
│
│
├── api/                        # ══ F7: API ════════════════════════════════
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                 # FastAPI app
│   ├── config.py               # env var loading
│   ├── routes/
│   │   ├── metrics.py          # F7.1 — REST endpoints
│   │   └── websocket.py        # F7.2 — WebSocket
│   └── services/
│       ├── postgres_service.py
│       └── redis_service.py
│
│
├── dashboard/                  # ══ F8: DASHBOARD ══════════════════════════
│   ├── Dockerfile
│   ├── package.json
│   ├── vite.config.js
│   └── src/
│       ├── App.jsx
│       ├── components/
│       │   ├── KPICard.jsx                 # F8.1.1
│       │   ├── LiveEventChart.jsx          # F8.1.2
│       │   ├── TopProductsTable.jsx        # F8.1.3
│       │   ├── DailyReport.jsx             # F8.2.1
│       │   ├── FunnelChart.jsx             # F8.2.2
│       │   ├── DimensionBreakdown.jsx      # F8.2.3
│       │   └── SystemStatusBar.jsx         # F8.3
│       ├── hooks/
│       │   ├── useWebSocket.js
│       │   └── useMetrics.js
│       └── services/
│           └── api.js
│
│
└── observability/              # ══ F9: OBSERVABILITY ══════════════════════
    ├── prometheus.yml
    └── grafana/
        ├── datasources.yml
        └── dashboards/
            ├── kafka.json
            ├── pyspark.json
            ├── postgres.json
            ├── airflow.json
            └── ecommerce_overview.json
```

---

## 5. DATA FLOW CHI TIẾT

### 5.1 Bootstrap (chạy 1 lần)

```
TRIGGER : make seed
          hoặc Airflow DAG [seed_and_init] manual trigger

STEPS:
  1. run_migrations.py
     → Kiểm tra bảng schema_migrations
     → Chạy tuần tự 001..005 SQL files (skip nếu đã chạy)
     → Output: PostgreSQL tables [users, products, raw_events, aggregated_metrics]

  2. seed_all.py
     → seed_users.py   : generate 10,000 users → batch INSERT [users]
     → seed_products.py: generate 5,000 products → batch INSERT [products]
     → Kiểm tra idempotency: skip nếu table đã có data
```

### 5.2 Streaming pipeline (chạy liên tục)

```
TRIGGER : make simulate

STEP 1 — Event generation (Simulator):
  event_generator.py
  → Load users + products từ PostgreSQL vào in-memory cache
  → Pick user theo is_active=true (random)
  → Pick product theo popularity_score (weighted random)
  → Sinh event JSON theo behavior logic + tỷ lệ event
  → kafka_producer.py → batch publish → Kafka [raw-events]
     partition_key = user_id

STEP 2 — Stream processing (PySpark):
  main.py → SparkSession
  → Read Kafka [raw-events] → DataFrame
  → ingest_stream.py: parse JSON, validate schema
      malformed → Kafka [dead-letter-queue]
  → Fork 3 aggregation jobs (parallel):
      aggregate_1m.py → tumbling window 1 phút
      aggregate_5m.py → sliding window 5 phút / slide 1 phút
      aggregate_1h.py → tumbling window 1 giờ
  → foreachBatch (mỗi 30s):
      postgres_sink.py  → INSERT [raw_events]
                        → UPSERT [aggregated_metrics]
      redis_sink.py     → HSET metric:summary
                        → ZADD top:products:*
                        → PUBLISH metrics-update
      kafka_sink.py     → PRODUCE [aggregated-metrics]

STEP 3 — API serving:
  FastAPI
  → Redis subscriber lắng nghe channel [metrics-update]
  → Khi có message → push WebSocket tới tất cả connected clients
  → Client Dashboard nhận → cập nhật real-time panels
  → Fallback: Dashboard poll REST /api/v1/metrics/summary mỗi 10 giây
```

### 5.3 Batch pipeline (scheduled)

```
TRIGGER : Airflow scheduler → 1:00 AM daily

DAG: daily_batch_pipeline
  Task 1: check_data_availability
  → SQL: SELECT COUNT(*) FROM raw_events WHERE DATE(timestamp) = yesterday
  → Sensor: retry mỗi 5 phút, timeout 2 giờ
  → PASS → continue

  Task 2: run_dbt_staging
  → dbt run --select staging
  → Tạo/refresh: stg_events · stg_users · stg_products (views)

  Task 3: run_dbt_marts
  → dbt run --select marts
  → Tạo/refresh incremental tables:
      mart_daily_summary · mart_product_performance
      mart_user_segments · mart_funnel_analysis
      mart_geo_revenue

  Task 4: run_dbt_tests
  → dbt test
  → WARN nếu business rule fail
  → FAIL DAG nếu primary key / not_null test fail

TRIGGER : Airflow scheduler → hourly (:00 mỗi giờ)

DAG: hourly_refresh
  Task 1: run_dbt_hourly_models
  → dbt run --select tag:hourly
  → Refresh: mart_hourly_kpi

  Task 2: notify_api_cache_refresh
  → POST http://api:8000/internal/cache/invalidate
  → API invalidate cache → query fresh từ mart tables
```

---

## 6. METRICS CATALOGUE

| Metric | Loại | Nguồn data | Granularity | Cập nhật |
|---|---|---|---|---|
| `events_per_second` | real-time | Redis HASH | continuous | liên tục |
| `click_count` | real-time | Redis / PostgreSQL agg | 1m / 5m | 30 giây |
| `add_to_cart_count` | real-time | Redis / PostgreSQL agg | 1m / 5m | 30 giây |
| `purchase_count` | real-time | Redis / PostgreSQL agg | 1m / 5m | 30 giây |
| `revenue_realtime` | real-time | PostgreSQL agg | 1m / 1h | 30 giây |
| `conversion_rate` | real-time | PostgreSQL agg | 5m sliding | 1 phút |
| `active_users` | real-time | Redis ZSET | 5m | 30 giây |
| `top_10_products` | real-time | Redis ZSET | rolling | 30 giây |
| `consumer_lag` | infra | Kafka metrics | real-time | 10 giây |
| `daily_revenue` | batch | `mart_daily_summary` | 1 ngày | hourly |
| `funnel_rates` | batch | `mart_funnel_analysis` | 1 ngày | hourly |
| `product_perf` | batch | `mart_product_performance` | 1 ngày | hourly |
| `segment_breakdown` | batch | `mart_user_segments` | 1 ngày | daily |
| `geo_revenue` | batch | `mart_geo_revenue` | 1 ngày | daily |

---

## 7. DEPENDENCY MAP

```
Startup order (docker compose healthchecks enforce this):

  Layer 0 (infra):     PostgreSQL · Redis · Kafka
        │
  Layer 1 (init):      storage-init (run_migrations) · kafka-init (create topics)
        │
  Layer 2 (processing):PySpark Streaming · Airflow
        │
  Layer 3 (serving):   FastAPI
        │
  Layer 4 (ui):        React Dashboard

Module dependencies:
  F1 (Simulator)   → needs: F2 (Kafka), F4 (PostgreSQL) for seed
  F2 (Kafka)       → needs: nothing
  F3 (PySpark)     → needs: F2 (Kafka), F4 (PostgreSQL), F4 (Redis)
  F4 (Storage)     → needs: nothing (is infra)
  F5 (Airflow)     → needs: F4 (PostgreSQL), F6 (dbt installed)
  F6 (dbt)         → needs: F4 (PostgreSQL with raw tables populated by F3)
  F7 (API)         → needs: F4 (PostgreSQL), F4 (Redis)
  F8 (Dashboard)   → needs: F7 (API)
  F9 (Observability)→ needs: all services running (scrapes metrics)
```

---

## 8. PHÂN CHIA GIAI ĐOẠN

| Phase | Modules | Nội dung | Ước tính |
|---|---|---|---|
| **1 — Infra** | F2, F4 | Docker Compose · Kafka KRaft · PostgreSQL · Redis · migrations | 2–3 ngày |
| **2 — Simulator** | F1 | User/Product generators · Seeder · Event generator · Kafka producer | 3–4 ngày |
| **3 — Streaming** | F3 | PySpark setup · 3 aggregation jobs · 3 sinks | 4–5 ngày |
| **4 — dbt** | F6 | Staging models · Mart models · Tests · Sources | 3–4 ngày |
| **5 — Airflow** | F5 | 3 DAGs · Connections · Scheduling | 2–3 ngày |
| **6 — API** | F7 | REST endpoints · WebSocket · cache invalidation | 2–3 ngày |
| **7 — Dashboard** | F8 | Real-time panels · Batch report panels · Status bar | 3–4 ngày |
| **8 — Observability** | F9 | Prometheus · Grafana dashboards · Alerts | 2 ngày |
| **Tổng** | | | **~3.5–4.5 tuần** |

---

## 9. DEFINITION OF DONE

```
INFRASTRUCTURE:
  ✓ docker compose up khởi động toàn bộ stack (10+ services) không lỗi
  ✓ Không có hardcoded credentials trong source code (tất cả qua .env)

SIMULATOR:
  ✓ make seed tạo đủ 10,000 users + 5,000 products trong PostgreSQL
  ✓ make simulate generate ổn định ≥ 500 events/sec trong 10 phút liên tục

STREAMING:
  ✓ PySpark xử lý event stream, Kafka consumer lag < 10,000 messages
  ✓ Malformed events route vào DLQ, không crash pipeline

DBT + AIRFLOW:
  ✓ dbt run --select staging && dbt run --select marts chạy không lỗi
  ✓ dbt test pass tất cả (0 error, warn có thể có)
  ✓ Airflow DAG daily_batch_pipeline chạy thành công end-to-end

API + DASHBOARD:
  ✓ Dashboard cập nhật real-time panels trong < 5 giây sau khi event xảy ra
  ✓ Batch report panels hiển thị data từ mart tables sau dbt run

OBSERVABILITY:
  ✓ Grafana dashboard hiển thị metrics từ tất cả services
  ✓ README đủ để người mới clone repo và chạy demo trong 15 phút
```
