import os
from datetime import datetime
import sqlite3
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from kaggle.api.kaggle_api_extended import KaggleApi


def extract_data():
    dataset = "rohanrao/nifty50-stock-market-data"
    download_path = "/opt/airflow/dags/etl_pipeline/data"

    os.makedirs(download_path, exist_ok=True)

    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(
        dataset,
        path=download_path,
        unzip=True
    )

    return "Extraction complete"


def transform_data():
    # Source path
    data_path = "/opt/airflow/dags/etl_pipeline/data"

    # Destination path for cleaned CSV
    output_path = "/opt/airflow/data/cleaned.csv"

    # Make sure destination directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Get all CSV files
    csv_files = [f for f in os.listdir(data_path) if f.endswith(".csv")]

    if not csv_files:
        raise ValueError("No CSV files found for transformation")

    df_list = []

    for file in csv_files:
        file_path = os.path.join(data_path, file)
        print(f"Reading file: {file}")

        df = pd.read_csv(file_path)

        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)

        df_list.append(df)

    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.columns = combined_df.columns.str.lower().str.strip()

    # Save cleaned CSV to /opt/airflow/data/cleaned.csv
    combined_df.to_csv(output_path, index=False)

    print(f"Transformation complete. Saved to {output_path}")

    return "Transformation complete"


def load_data():
    file_path = "/opt/airflow/data/cleaned.csv"
    db_path = "/opt/airflow/data/stocks.db"

    os.makedirs("/opt/airflow/data", exist_ok=True)

    conn = sqlite3.connect(db_path)

    chunk_size = 5000
    first_chunk = True

    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        if first_chunk:
            chunk.to_sql("stocks", conn, if_exists="replace", index=False)
            first_chunk = False
        else:
            chunk.to_sql("stocks", conn, if_exists="append", index=False)

    conn.close()

    return "Load complete"


with DAG(
    dag_id="etl_kaggle_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task
