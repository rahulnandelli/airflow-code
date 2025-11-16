# /home/airflow/airflow-code/dags/test_netflix_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator


# DAG definition
with DAG(
    dag_id="test_netflix_dag",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["test"]
) as dag:
    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")
    start_task >> end_task

