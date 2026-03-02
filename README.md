# BEES Data Engineering – Breweries Case
# 📌 Overview
This project implements a complete data pipeline following the Medallion Architecture (Bronze, Silver, Gold) pattern.
The pipeline consumes data from the Open Brewery DB API, processes it through transformation layers, and produces an aggregated analytical dataset.
The entire workflow is orchestrated using Apache Airflow, ensuring scheduling, retries, and execution control.

# 🏗 Architecture
The solution follows the Medallion Architecture:
API → Bronze → Silver → Gold
Each layer has a specific responsibility and increasing data refinement.

# 🥉 Bronze Layer – Raw Ingestion
Purpose: 
   Store raw API data without transformation.
Characteristics:
   Data fetched directly from API
   Stored in native JSON format
   Organized by ingestion date
   Immutable storage (append-only)
Example Structure:
   data/bronze/
      2026-02-27/
         breweries_raw.json
      2026-02-28/
         breweries_raw.json
Design Decisions:
   JSON preserves raw payload
   Date partitioning enables historical tracking
   Supports replay and reprocessing

# 🥈 Silver Layer – Curated & Structured Data
Purpose:
   Transform raw data into optimized analytical format.
   Transformations Applied
   Schema standardization
   Column selection
   Type enforcement
   Deduplication by id
   Partitioning by country and state
   Conversion to Parquet format
   Storage Format
Columnar format: Parquet
Partitioned by:
   country
   state
Example Structure:
   data/silver/
      country=United States/
         state=Missouri/
            part-xxxx.parquet
Why Parquet?
   Columnar storage
   Efficient compression
   Optimized for analytical queries
   Industry standard for data lakes
   Idempotency
The Silver layer ensures:
   Reprocessing does not create duplicates
   Only unique id values are persisted
   This guarantees pipeline reliability.

# 🥇 Gold Layer – Analytical Aggregation
Purpose:
   Provide business-ready aggregated dataset.
   Aggregation Logic
Aggregated view with:
   country
   state
   brewery_type
   total_breweries
Example:
   country	state	brewery_type	total_breweries
   Business Value
   Enables analytics per location
   Provides summarized metrics
   Reduces query complexity for consumers

# 🔄 Orchestration
The pipeline is orchestrated using Apache Airflow.
DAG Structure
bronze_task >> silver_task >> gold_task
Features Implemented
Daily schedule (@daily)
Task dependency management
Automatic retries (2 retries with delay)
Fail-fast mechanism
Centralized logging
Manual trigger capability

# Why Airflow?
Industry-standard orchestrator
Clear DAG visualization
Built-in retry & scheduling mechanisms
Scalable architecture

# 🐳 Containerization
The solution runs using Docker:
   Airflow runs inside container
   DAGs and source code mounted via volumes
   Local environment isolated and reproducible
Benefits:
   Environment consistency
   Easy setup
   Reproducibility

# 📊 Monitoring & Alerting Strategy (Design Proposal)
Although not fully implemented, the following monitoring strategy is proposed:

1. Pipeline Monitoring
Airflow task status tracking
Retry alerts
SLA monitoring
2. Data Quality Checks
Row count validation (Bronze vs Silver)
Distinct ID validation
Schema validation
Null percentage checks
3. Alerting
Potential implementation:
Email notifications via Airflow
Slack webhook integration
Metrics export to Prometheus + Grafana

# 🧪 Testing Strategy
Test cases should validate:
   Silver deduplication logic
   Schema consistency
   Gold aggregation correctness
   Idempotency behavior
Recommended framework: pytest
