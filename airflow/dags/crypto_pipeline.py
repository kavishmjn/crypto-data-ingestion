from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'kavish',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crypto_pipeline',
    description='Crypto ELT Pipeline - Ingest, Transform, Test',
    default_args=default_args,
    start_date=datetime(2026, 3, 23),
    schedule_interval='0 */6 * * *',
    catchup=False,
) as dag:

    ingest = BashOperator(
        task_id='ingest_crypto',
        bash_command='cd /opt/crypto_project && python main.py',
        env={
            'DB_HOST': Variable.get('DB_HOST'),
            'DB_NAME': Variable.get('DB_NAME'),
            'DB_USER': Variable.get('DB_USER'),
            'DB_PASSWORD': Variable.get('DB_PASSWORD'),
            'DB_PORT': Variable.get('DB_PORT'),
            'COINCAP_API_KEY': Variable.get('COINCAP_API_KEY'),
        }
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/crypto_project/crypto_db && dbt run --target docker --profiles-dir /home/airflow/.dbt',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/crypto_project/crypto_db && dbt test --target docker --profiles-dir /home/airflow/.dbt',
    )

    ingest >> dbt_run >> dbt_test