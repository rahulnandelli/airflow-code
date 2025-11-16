from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

import sys
sys.path.append('/home/airflow/airflow-code/dags')
from source_load.data_load import run_script


# ---------------------------------------------------------------------
# Default DAG Arguments
# ---------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 5, 12),
}

# ---------------------------------------------------------------------
# DAG Definition (Airflow 3.x syntax)
# ---------------------------------------------------------------------
with DAG(
    dag_id='Netflix_Data_Analytics',
    description='ETL DAG to process Netflix data from S3 and load into Snowflake',
    default_args=default_args,
    schedule=None,        # Manual trigger (correct for Airflow 3.1+)
    catchup=False,
    tags=['netflix', 'snowflake', 's3']
) as dag:

    # -----------------------------------------------------------------
    # Task Definitions
    # -----------------------------------------------------------------

    # 1️⃣ S3 Sensor - Wait for credits.csv
    credits_sensor = S3KeySensor(
        task_id='credits_rawfile_sensor',
        bucket_key='raw_files/credits.csv',
        bucket_name='nelix-analycs-data',
        aws_conn_id=None,
	poke_interval = 20,
	timeout = 1800,
	mode = 'reschedule',
    )

    # 2️⃣ S3 Sensor - Wait for titles.csv
    titles_sensor = S3KeySensor(
        task_id='titles_rawfile_sensor',
        bucket_key='raw_files/titles.csv',
        bucket_name='nelix-analycs-data',
        aws_conn_id=None,
	poke_interval = 20,
	timeout = 1800,
	mode = 'reschedule',
    )

    # 3️⃣ PythonOperator - Load data into Snowflake
    load_data_snowflake = PythonOperator(
        task_id='Load_Data_Snowflake',
        python_callable=run_script
    )

    # 4️⃣ BashOperator - Run dbt staging models (runs in sequence)
    run_staging_models = BashOperator(
    task_id='run_staging_models',
    bash_command='''
        cd /home/airflow/dbt-code && \
        /home/airflow/dbt-env-3-10/bin/dbt run \
        --models tag:DIMENSION \
        --profiles-dir /home/airflow/.dbt \
        --profile netflix --target dev
    '''
)
    run_fact_dim_models = BashOperator(
    task_id='run_fact_dim_models',
    bash_command='''
        cd /home/airflow/dbt-code && \
        /home/airflow/dbt-env-3-10/bin/dbt run \
        --select tag:FACT tag:DIMENSION \
        --profiles-dir /home/airflow/.dbt \
        --profile netflix --target dev
    ''',
    dag=dag
)
    run_test_cases = BashOperator(
    task_id='run_test_cases',
    bash_command='''
        cd /home/airflow/dbt-code && \
        /home/airflow/dbt-env-3-10/bin/dbt test \
        --profiles-dir /home/airflow/.dbt \
        --profile netflix --target dev
    '''
)



    # 5️⃣ Start & End tasks
    start_task = EmptyOperator(task_id='start_task')
    end_task = EmptyOperator(task_id='end_task')

    # -----------------------------------------------------------------
    # DAG Dependencies (Final confirmed flow)
    # -----------------------------------------------------------------

    # Main sequence:
    # start → credits_sensor → titles_sensor → Load_Data_Snowflake → run_staging_models → end
    start_task >> credits_sensor >> titles_sensor >> load_data_snowflake >> run_staging_models >> run_fact_dim_models >> run_test_cases  >> end_task



