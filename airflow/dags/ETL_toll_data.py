from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import wget, os, tarfile, csv
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date:': datetime.today(),
    'email':['amayatibana@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# ------------------------------
# DAG Definition Create DAG
# ------------------------------

dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description="Apache Airflow Project Assignment",
    schedule='@daily',
    catchup=False
)

# ------------------------------
# Define the path of the dataset and the function to download the dataset
source_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/airflow/assignment/tolldata.tgz'
staging = "/Users/amayatibana/Projects/python_etl_project/staging"

def download_dataset():
    wget.download(source_url, out=staging)

#unzip the dataset
def untar_dataset():
    tgz_path = os.path.join(staging, 'tolldata.tgz')
    with tarfile.open(tgz_path) as tar:
        tar.extractall(path=staging)

# Extract the data from the csv file and write to a new csv file
def extract_data_from_csv():
    input_file = os.path.join(staging, 'vehicle-data.csv')
    output_file = os.path.join(staging, 'csv_data.csv')

    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            writer.writerow([row[0], row[1], row[2], row[3]])

# ------------------------------
def extract_data_from_tsv():
    input_file = os.path.join(staging, 'tollplaza-data.tsv')
    output_file = os.path.join(staging, 'tsv_data.csv')

    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile, delimiter='\t')
        writer = csv.writer(outfile)

        for row in reader:
            writer.writerow([row[0], row[1], row[2], row[3], row[4], row[5]])

# ------------------------------

def extract_data_from_fixed_width():
    input_file = os.path.join(staging, 'payment-data.txt')
    output_file = os.path.join(staging, 'fixed_width_data.csv')

    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)

        for line in infile:
            fields = line.strip().split()

            writer.writerrow([fields[-2], fields[-1]])

# ------------------------------

def consolidate_data():
    csv_data = os.path.join(staging, 'csv_data.csv')
    tsv_data = os.path.join(staging, 'tsv_data.csv')
    fixed_data = os.path.join(staging, 'fixed_width_data.csv')
    
    consolidated = pd.concat([csv_data, tsv_data, fixed_data], axis =1)

    consolidated.columns = [
        "Rowid", "Timestamp", "Anonymized Vehicle number", "Vehicle type",
        "Number of axles", "Tollplaza id", "Tollplaza code",
        "Type of Payment code", "Vehicle Code"
    ]

    consolidated.to_csv(os.path.join(staging, 'consolidated_data.csv'), index=False)

# ------------------------------
def transform_data():
    input_file = os.path.join(staging, "extracted_data.csv")
    output_file = os.path.join(staging, "transformed_data.csv")

    df = pd.read_csv(input_file)
    df["Vehicule type"] = df["Vehicle type"].str.upper()
    df.to_csv(output_file, index=False)

# ------------------------------
# Define DAG Tasks



download_dataset = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag
)

untar_dataset = PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    dag=dag
)

extract_csv = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_data_from_csv,
    dag=dag
)

extract_tsv = PythonOperator(
    task_id='extract_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag
)

extract_fixed_width = PythonOperator(
    task_id='extract_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag
)

consolidate_data = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

# ------------------------------
# Define Task Dependencies
download_dataset >> untar_dataset >> [extract_csv, extract_tsv, extract_fixed_width] >> consolidate_data >> transform_data