# Bank Transaction ETL Pipeline

## Project Overview
This project implements an end-to-end batch data engineering pipeline to process financial transaction data using Apache Spark. The pipeline ingests raw transaction data from the PaySim dataset, performs data quality validation, transforms the data into partitioned Parquet format, and generates daily aggregated summaries for analytics.

## Tech Stack
- Apache Spark (PySpark)
- PostgreSQL
- Apache Airflow
- Docker
- GitHub Actions CI

## Architecture

PaySim CSV  
↓  
PySpark Ingestion  
↓  
Raw Transactions (PostgreSQL)  
↓  
Quality Validation  
↓  
Clean Transactions / Rejected Transactions  
↓  
Spark Transformation  
↓  
Partitioned Parquet Storage  
↓  
Daily Aggregation  
↓  
PostgreSQL Analytics Table  

## Pipeline Stages

### Ingestion
Loads raw PaySim dataset into PostgreSQL.

### Quality Check
Detects invalid transactions:
- NULL amount
- Negative amount

Invalid rows are moved to `rejected_transactions`.

### Transformation
Converts cleaned transactions to partitioned Parquet files.

### Aggregation
Creates `daily_account_summary` for downstream analytics.

## Orchestration
Apache Airflow orchestrates the entire pipeline using DAG tasks:
- ingest_csv
- quality_check
- transform
- aggregate

## Dataset
PaySim Financial Dataset  
https://www.kaggle.com/datasets/ealaxi/paysim1

Place dataset in:
