# GoalFlow Analytics Pipeline

A production-quality Data Engineering portfolio project demonstrating a real-time football analytics pipeline.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-E23B36?logo=apache-kafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?logo=apache-spark&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?logo=apache-airflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)

## Architecture

```mermaid
graph LR
  A[Football API] --> B[Kafka Producer]
  B --> C[Kafka: matches-raw]
  B --> D[Kafka: standings-raw]
  C --> E[PySpark Processor]
  D --> E
  E --> F[(PostgreSQL)]
  F --> G[HTML Report]
  H[Airflow DAG] --> B
  H --> E
  H --> G
```

## Prerequisites
- Docker Desktop 4.x
- Make

## Quick Start
1. `cp .env.example .env` and add your API key.
2. `make up`
3. Open http://localhost:8080 for Airflow (admin/admin).

## Services Overview

| Service | Port | Description |
| --- | --- | --- |
| PostgreSQL | 5432 | Relational storage for transformed data |
| Airflow | 8080 | Workflow orchestration DAG runs |
| Kafka | 9092 | Message broker for raw JSON payloads |

---

**Author: Mohamed Chaari**
Data Engineering Student, ISIMS Sfax
