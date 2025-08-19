from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Import your ETL logic from files
from etl_extract import extract_latest_csv
from etl_transform import transform_csv
from etl_load import load_to_postgres

# DAG config
default_args = {
    "owner": "bikas",
    "retries": 1,
}

with DAG(
    dag_id="retail_sales_etl",
    schedule="@daily",  # ✅ this works in Airflow 3.x
    start_date=datetime(2025, 8, 1),
    catchup=False,
    default_args=default_args,
    tags=["retail", "etl"],
) as dag:


    def extract_task(**context):
        """Extract: picks latest CSV and archives it"""
        path = extract_latest_csv()
        return path  # push to XCom

    def transform_task(**context):
        """Transform: reads raw CSV and writes cleaned + agg parquet"""
        input_path = context["ti"].xcom_pull(task_ids="extract")
        cleaned_path, agg_path = transform_csv(input_path)
        return {"cleaned": cleaned_path, "agg": agg_path}

    def load_task(**context):
        """Load: inserts cleaned & upserts aggregates"""
        result = context["ti"].xcom_pull(task_ids="transform")
        load_to_postgres(result["cleaned"], result["agg"])

    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract_task
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable=transform_task
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable=load_task
    )

    t1 >> t2 >> t3
