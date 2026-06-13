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

```text
python_etl_project/
│
├── dags/
│   └── ETL_toll_data.py
├── airflow.db (ignored)
├── airflow_env/
└── README.md
```

---

## 🚀 How to Run

### 1. Create virtual environment

```bash
python3 -m venv airflow_env
source airflow_env/bin/activate
```

### 2. Install dependencies

```bash
pip install apache-airflow pandas wget
```

### 3. Initialize Airflow

```bash
airflow db migrate
```

### 4. Start Airflow

```bash
airflow standalone
```

### 5. Add DAG

Make sure your DAG file is in:

```bash
$AIRFLOW_HOME/dags/
```

### 6. Trigger DAG

Open Airflow UI → enable DAG → run `ETL_toll_data`

---

## 📊 Output

A single consolidated dataset containing cleaned toll transaction records ready for analysis.

---

## 🧠 Learning Outcomes

- Building ETL pipelines with Airflow
- Working with multiple file formats
- DAG orchestration and task dependencies
- Data transformation using Python

---

## 📌 Author

**Carlos Amaya**  
Aspiring Data Engineer  