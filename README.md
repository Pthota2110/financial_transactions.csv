# financial-etl-pipeline
End-to-end Financial ETL Pipeline using AWS, Python, Glue, Redshift, and Airflow

# End-to-End Financial ETL Pipeline

Author: **Pavan Thota**  
Domain: **Financial Services / Banking**

## 📌 Overview
This project implements a complete end-to-end ETL pipeline for processing financial transaction data. 
The pipeline ingests raw transaction files, performs data cleansing and validation, and loads analytics-ready data into a data warehouse.

## 🏗️ Architecture
![Architecture](architecture/architecture.png)

**Flow:**
Financial CSV Data → AWS S3 (Raw) → AWS Glue (PySpark ETL) → AWS S3 (Curated – Parquet) → Amazon Redshift → Analytics

## 🛠️ Tech Stack
- Python
- AWS S3
- AWS Glue (PySpark)
- Amazon Redshift
- Apache Airflow
- SQL

## ⚙️ ETL Pipeline Steps
1. Ingest raw financial transaction data into S3
2. Clean and deduplicate transactions using PySpark
3. Apply business rules and data quality checks
4. Store curated data in Parquet format
5. Load data into Amazon Redshift
6. Automate the pipeline using Airflow

## ✅ Data Quality Checks
- Duplicate transaction detection
- Null value validation
- High-value transaction identification (fraud monitoring)

## 📊 Use Cases
- Fraud detection and monitoring
- Financial reporting
- Regulatory compliance analytics

## 📂 Repository Structure

## 🚀 How to Run (Conceptual)
- Upload raw data to S3
- Trigger Glue ETL job
- Run Airflow DAG
- Query curated data from Redshift
