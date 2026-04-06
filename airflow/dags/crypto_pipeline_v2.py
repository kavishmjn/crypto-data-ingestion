from airflow import DAG
from config import data_folder
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from ingestion.api_calls import fetch_assets
from ingestion.store_data import store_raw_data, store_csv_data 
from ingestion.validation import file_validation , data_level_validation , schema_level_validation
from database.load_data_in_raw_table import load_data
from datetime import datetime, timedelta, timezone
import os 

#task for storing raw data in json file
def store_raw_data_fxn(**context):
    ti = context['ti']
    batch_id = ti.xcom_pull(task_ids = 'define_batch_id')
    data = ti.xcom_pull(task_ids = 'fetch_assets')
    file_loc = f'{data_folder}/{batch_id}/raw'
    os.makedirs(file_loc, exist_ok=True)
    file_name = f'{file_loc}/assets.json'
    return store_raw_data(data,file_name)

# task for validating the raw data file
def validate_raw_data_fxn(**context):
    ti = context['ti']
    raw_file_path = ti.xcom_pull(task_ids = 'store_raw_data')
    return file_validation(raw_file_path)


#task for validating data and schema level validation 
def validate_data_schema_fxn(**context):
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids = 'validate_raw_data')
    data_level_validation(raw_data)
    schema_level_validation(raw_data)

#task for converting json to csv and storing the csv
def store_csv_data_fxn(**context):
    ti = context['ti']
    batch_id = ti.xcom_pull(task_ids = 'define_batch_id')
    raw_file_path = ti.xcom_pull(task_ids = 'store_raw_data')
    csv_file_loc = f'{data_folder}/{batch_id}/csv'
    os.makedirs(csv_file_loc, exist_ok=True)
    csv_file_path = f'{csv_file_loc}/assets.csv'
    return store_csv_data(batch_id,raw_file_path,csv_file_path)

#task for loading csv data into raw table in postgres
def load_csv_to_db_fxn(**context):
    # gathering all the necessary info from previous taks
    ti = context['ti']
    csv_file_path = ti.xcom_pull(task_ids = 'store_csv_data')
    
    #schema and table
    schema = 'raw'
    table = 'assets'
    #fix how to call the load data fxn with 
    DB_CONFIG = {
    "host": Variable.get("DB_HOST"),
    "database": Variable.get("DB_NAME"),
    "user": Variable.get("DB_USER"),
    "password": Variable.get("DB_PASSWORD"),
    "port": Variable.get("DB_PORT")
    }
    load_data(csv_file=csv_file_path, schema_name='raw', table_name='assets', db_config=DB_CONFIG)
   

with DAG (
    'crypto_pipeline_v2',
    start_date=datetime(2024, 6, 1, tzinfo=timezone.utc),
    schedule_interval='0 */3 * * *',#run every 3 hours
    catchup=False,
    max_active_runs=1
) as dag:
    fetch_assets_task = PythonOperator(
        task_id='fetch_assets',
        python_callable=fetch_assets,
        retries=3,
        retry_delay=timedelta(seconds=30))
    define_batch_id = PythonOperator(
        task_id = 'define_batch_id',
        python_callable = lambda: datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    )
    store_raw_data_task = PythonOperator(
        task_id = 'store_raw_data',
        python_callable = store_raw_data_fxn
    )
    validate_raw_data_task = PythonOperator(
        task_id = 'validate_raw_data',
        python_callable = validate_raw_data_fxn
    )
    validate_data_schema_task = PythonOperator(
        task_id = 'validate_data_schema',
        python_callable = validate_data_schema_fxn
    )
    store_csv_data_task = PythonOperator(
        task_id = 'store_csv_data',
        python_callable = store_csv_data_fxn
    )
    load_csv_to_db_task = PythonOperator(
        task_id = 'load_csv_to_db',
        python_callable = load_csv_to_db_fxn
    )
    dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/crypto_db && dbt run --target docker --profiles-dir /home/airflow/.dbt 2>&1'
    )
    define_batch_id >> fetch_assets_task >> store_raw_data_task >> validate_raw_data_task >> validate_data_schema_task >> store_csv_data_task >> load_csv_to_db_task >> dbt_run