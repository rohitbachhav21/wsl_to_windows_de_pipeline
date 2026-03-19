# wsl_to_windows_de_pipeline

# Real-Time Pizza Sales Data Pipeline

## 📌 Overview
This project implements an end-to-end real-time data pipeline that streams data from MySQL to PostgreSQL using Kafka and PySpark. The pipeline processes transactional data, performs transformations and joins, and stores analytics-ready data for reporting.

---

## 🏗️ Architecture

MySQL (OLTP Database)
        ↓
Kafka (Streaming Layer)
        ↓
PySpark (Processing & Transformations)
        ↓
PostgreSQL (Data Warehouse / Analytics)

---

## ⚙️ Tech Stack

- Python
- Apache Kafka
- PySpark (Structured Streaming)
- MySQL
- PostgreSQL
- SQL
- WSL (Linux environment)

---

## 📂 Data Sources

### Dynamic Tables (Streaming)
- orders
- order_details

### Static Tables (Batch Load)
- pizzas
- pizza_types

---

## 🔄 Pipeline Workflow

1. **MySQL → Kafka**
   - Python producer reads incremental data from MySQL
   - Sends JSON messages to Kafka topics

2. **Kafka → Spark**
   - Spark consumes streaming data from Kafka topics
   - Parses JSON data into structured format

3. **Data Processing**
   - Joins streaming data with static tables
   - Performs transformations:
     - Date & timestamp conversion
     - Revenue calculation

4. **Spark → PostgreSQL**
   - Writes cleaned and enriched data into:
     - `pizza_orders_stream` (fact table)
     - `pizza_sales_stream` (analytics table)

---

## 📊 Key Features

- Real-time streaming pipeline
- Incremental data ingestion using Kafka
- Schema parsing and transformation in PySpark
- Join with dimension tables for enrichment
- Revenue calculation for analytics
- Idempotent writes using UPSERT logic
- Checkpointing for fault tolerance

---

## 📈 Sample Analytics

- Revenue per pizza type
- Category-wise sales performance
- Order trends over time

