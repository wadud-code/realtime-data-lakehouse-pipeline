

# Real-Time Event-Driven Data Lakehouse Pipeline

An end-to-end, production-grade data engineering platform built using the **Medallion Architecture** (Bronze → Silver → Gold). Demonstrates automated cloud infrastructure provisioning, real-time message streaming, event-driven batch processing, and analytics engineering with automated data quality contracts.

---

## 🏗️️ Architecture Diagram


[ Data Source / Simulator ]
│
├── (JSON Events) ──► [ Apache Kafka ] ──► [ PySpark Streaming ] ──► [ S3 Silver Lake (Parquet) ]
│                                                                          │
└── (Raw File Upload) ──► [ AWS S3 Bronze ] ──► [ SQS Queue ] ──► [ Airflow ]  │
▼
[ DuckDB Analytics Engine ]
│
(Data Quality Contracts)
▼
[ S3 Gold Star Schema ]
├── fct_orders
├── dim_customers
└── fct_daily_sales_summary


---

## 🛠️ Tech Stack & Tools

* **Infrastructure as Code (IaC):** Terraform, LocalStack (S3, SQS, IAM)
* **Containerization:** Docker, Docker Compose
* **Batch Orchestration:** Apache Airflow, Python
* **Streaming Engine:** Apache Kafka, PySpark Structured Streaming
* **Storage & Formats:** Amazon S3, Apache Parquet
* **Analytics Engineering:** DuckDB, SQL (Kimball Star Schema Modeling)
* **Data Quality & Governance:** Automated Python & SQL Data Quality Contracts

---

## 💡 Key Pipeline Features

1. **Infrastructure Provisioning:** Automated AWS S3 bucket, SQS queue, and IAM least-privilege policy setup via Terraform.
2. **Event-Driven File Ingestion:** Automated S3 bucket notifications emitting events to AWS SQS queues, triggering Airflow DAG processing upon file delivery.
3. **Real-Time Stream Processing:** Continuous JSON payload ingestion via Apache Kafka brokers, processed in micro-batches using PySpark Structured Streaming with active metadata checkpoints to ensure exactly-once semantics.
4. **Gold Layer Dimensional Modeling:** Transformed raw Silver Parquet files into a Star Schema model (`fct_orders`, `dim_customers`) using DuckDB for high-performance analytical queries.
5. **Data Quality Contracts:** Automated pipeline assertions validating primary key uniqueness, non-null customer keys, and positive numeric order thresholds prior to writing Gold layers to S3.

---

## 🚀 Quickstart & Execution

### 1. Spin up Infrastructure
```bash
docker compose up -d
cd terraform && terraform apply -auto-approve

python scripts/produce_orders.py
python scripts/consume_spark_stream.py

python scripts/build_gold_layer.py```


PROJECTS & EXPERIENCE

End-to-End Real-Time Data Lakehouse Pipeline | Python, PySpark, Kafka, Terraform, DuckDB, S3
• Architected a production-grade Medallion Data Lakehouse (Bronze/Silver/Gold) processing real-time streaming order events and event-driven batch files.
• Provisioned cloud storage infrastructure (AWS S3, SQS queues, IAM policies) using Terraform and LocalStack to ensure reproducible, IaC-driven deployments.
• Ingested real-time continuous message streams using Apache Kafka and PySpark Structured Streaming, writing partitioned Parquet datasets with active Spark metadata checkpoints for fault-tolerant, exactly-once processing.
• Modeled a Gold Layer Kimball Star Schema (Fact & Dimension tables) using DuckDB, executing automated Data Quality Contracts (uniqueness, non-null, and metric assertions) before persisting analytical models to S3.


