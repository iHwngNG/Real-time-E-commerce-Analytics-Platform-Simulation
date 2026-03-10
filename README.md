# Real-time E-commerce Analytics Platform Simulation

![Build Status](https://img.shields.io/badge/status-active-success.svg)
![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![React](https://img.shields.io/badge/frontend-React%20%2B%20Vite-61dafb.svg)
![Streaming](https://img.shields.io/badge/engine-Spark%20Streaming-orange.svg)

A high-performance, real-time e-commerce analytics platform designed to process millions of user behavioral events (Click, Add-to-cart, Purchase) and visualize KPIs on a dynamic dashboard.

⚠️ *The project is developed with the assistance of AI Agents* ⚠️

---

## 🏗 System Architecture

The platform follows a **Lambda-inspired Architecture** optimized for low-latency processing and long-term storage:

1.  **Data Generation Layer (F1):** A Python-based simulator that mimics real-world user behavior, including session lifecycles, weighted product picking, and peak-hour traffic spikes.
2.  **Message Ingestion Layer (F2):** **Apache Kafka** (KRaft mode) acts as the central nervous system, buffering high-velocity event streams into specialized topics.
3.  **Real-time Processing Layer (F3):** **Apache Spark Structured Streaming** performs micro-batch computations, managing stateful aggregations across 1-minute, 5-minute, and 1-hour windows.
4.  **Storage Layer (F4):**
    *   **Hot Layer (Redis):** Stores high-frequency metrics for sub-second dashboard updates.
    *   **Cold Layer (PostgreSQL):** Stores raw events (partitioned by time) and historical aggregations for deep technical drills.
5.  **Orchestration Layer (F5):** **Apache Airflow** manages batch processing pipelines and scheduled DBT transformations.
6.  **Transformation Layer (F6):** **DBT (Data Build Tool)** creates clean data marts for business intelligence from raw ingested data.
7.  **Serving Layer (F7):** A **FastAPI** backend provides REST endpoints and WebSockets for real-time data push.
8.  **Visualization Layer (F8):** A modern **React** dashboard (Vite-powered) featuring glassmorphism design and live-updating charts.

---

## 🛠 Technology Stack

*   **Streaming Engine:** Apache Spark 3.5.1 (PySpark)
*   **Message Broker:** Apache Kafka 3.7.1
*   **Databases:**
    *   PostgreSQL 16 (Relational/Cold Storage/Time-series Partitioning)
    *   Redis 7 (Cache/Hot Storage/Metrics)
*   **Backend:** FastAPI (Python 3.11)
*   **Frontend:** React 18, Vite, Recharts, Lucide Icons
*   **Data Orchestration:** Apache Airflow 2.9, DBT 1.8
*   **Infrastructure:** Docker & Docker Compose

---

## 🔄 Project Workflow

The data flows through the system in an automated loop:

1.  **Simulate:** The `Simulator` generates JSON events and publishes them to the `raw-events` Kafka topic.
2.  **Ingest:** `Spark Streaming` reads the Kafka topic, flattens the nested JSON, and validates the schema.
3.  **Process:** Spark calculates KPIs (Revenue, Conversion Rates, Active Users) using consolidated aggregators to optimize RAM usage.
4.  **Sink:** 
    *   Cleaned raw data is appended to partitioned PostgreSQL tables.
    *   Aggregated KPIs are UPSERTED into PostgreSQL and HSET into Redis.
    *   Notifications are SHUT via Redis Pub/Sub to the API.
5.  **Serve:** The `API` listens to Redis, fetches the latest metrics, and pushes them via `WebSocket` to the frontend.
6.  **Visualize:** The `Dashboard` receives real-time updates and refreshes charts/KPI cards instantly without page reloads.

---

## 📂 Project Structure

```text
├── simulator/          # User behavior simulation & Kafka producer
├── streaming/          # Spark Structured Streaming jobs & sinks
├── api/                # FastAPI backend (REST & WebSockets)
├── dashboard/          # React frontend (Vite)
├── storage/            # PostgreSQL migrations & DB schemas
├── airflow/            # Workflow orchestration (DAGs)
├── dbt/                # Data transformation models
├── observability/      # Prometheus & Grafana configurations
├── docs/               # Architecture PRD, logs, and manuals
├── docker-compose.yml  # Full-stack container definitions
└── Makefile            # "Direct-access" command center
```

---

## 🚀 How to Run (using Makefile)

We provide a `Makefile` to simplify complex Docker commands. Follow these steps in order:

### 1. Initialize Infrastructure
Spin up the core databases, Kafka, and monitoring tools:
```bash
make up
```

### 2. Prepare Data (One-time)
Seed the database with a pool of users and products:
```bash
make seed
```

### 3. Start Streaming Engine
Launch the Spark processor to start listening to Kafka:
```bash
make stream
```

### 4. Start Simulation
Start generating real-time traffic:
```bash
make simulate
# or use 'make both' to seed and simulate in one go
```

### 5. Access the Dashboard
Open your browser at: [http://localhost:3000](http://localhost:3000)

---
⚠️ *The project is still in development and may have some issues.* ⚠️
