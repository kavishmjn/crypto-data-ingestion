from airflow import DAG
from config import data_folder
from airflow.operators.python import PythonOperator
from ingestion.api_calls import fetch_assets
from ingestion.store_data import store_raw_data, store_csv_data 
from ingestion.validation import file_validation , data_level_validation , schema_level_validation
from datetime import datetime, timezone
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


with DAG (
    'crypto_pipeline_v2',
    start_date=datetime(2024, 6, 1, tzinfo=timezone.utc),
    schedule_interval='0 */3 * * *',#run every 3 hours
    catchup=False
) as dag:
    fetch_assets_task = PythonOperator(
        task_id='fetch_assets',
        python_callable=fetch_assets)
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
    define_batch_id >> fetch_assets_task >> store_raw_data_task >> validate_raw_data_task >> validate_data_schema_task >> store_csv_data_task   