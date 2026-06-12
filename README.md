# toll-data-etl-airflow
End-to-end ETL pipeline built with Apache Airflow to extract, transform, and load toll transaction data from CSV, TSV, and fixed-width files into a single cleaned dataset. Demonstrates modular workflow orchestration and production-style data processing using Python and Airflow DAGs.
# 🚦 Toll Data ETL Pipeline with Apache Airflow

## 📌 Overview
This project builds an end-to-end ETL (Extract, Transform, Load) pipeline using Apache Airflow. It processes toll transaction data from multiple formats (CSV, TSV, and fixed-width files) into a single cleaned dataset.

The workflow is fully orchestrated using Airflow DAGs and demonstrates modular, production-style data engineering practices.

---

## ⚙️ Tech Stack
- Apache Airflow
- Python
- Pandas
- CSV / TSV / Fixed-width parsing
- Wget & Tarfile
- LocalExecutor

---

## 🏗️ Pipeline Workflow

The DAG performs the following steps:

1. Download compressed dataset
2. Extract tar file
3. Extract CSV data (vehicle data)
4. Extract TSV data (toll plaza data)
5. Extract fixed-width data (payment data)
6. Consolidate all datasets
7. Transform data (clean + standardize fields)

---

## 📁 Project Structure