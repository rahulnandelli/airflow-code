from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="netflix_data_analytics",
    description="ETL pipeline with DBT, Snowflake, and Airflow",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["netflix", "analytics"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    load_data = BashOperator(
        task_id="load_data_from_s3_to_snowflake",
        bash_command="python3 /home/airflow/airflow-code/dags/source_load/data_load.py",
    )

    run_stage_model = BashOperator(
        task_id="run_stage_model",
        bash_command="cd /home/airflow/dbt-code && dbt run --selector stage",
    )

    run_fact_dim_models = BashOperator(
        task_id="run_fact_dim_models",
        bash_command="cd /home/airflow/dbt-code && dbt run --selector fact_dim",
    )

    run_test_cases = BashOperator(
        task_id="run_test_cases",
        bash_command="cd /home/airflow/dbt-code && dbt test",
    )

    start >> load_data >> run_stage_model >> run_fact_dim_models >> run_test_cases >> end
