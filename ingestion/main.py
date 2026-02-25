from config import logs,data_folder
import logging
from api_calls import fetch_assets
from store_data import store_raw_data,store_csv_data
import os
from datetime import datetime, timezone
from pathlib import Path
from validation import file_validation,data_level_validation,schema_level_validation

#-------Logging Configuration------
os.makedirs(logs, exist_ok=True)
os.makedirs(data_folder, exist_ok=True)
log_file = os.path.join(logs, f"log_{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')}.log")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


#------Raw Data Ingestion--------

def ingestion(batchid: str) -> Path:
    logger.info("Starting raw data ingestion process")
    try:
        data = fetch_assets()
        if not data:
            raise ValueError("No data fetched from API")
        batchfolder = os.path.join(data_folder,batchid)
        filename = f'assets.json'
        raw_folder = os.path.join(batchfolder,'raw')
        file_path = os.path.join(raw_folder,filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        raw_data_file =store_raw_data(data,file_path)
        logger.info("Data ingestion process completed successfully")
        return Path(raw_data_file)
    except Exception as e:
        logger.error(f"Data ingestion process failed: {e}")
        raise

#------------Raw Data Validation-------
def validation(batchid:str,raw_file:str|Path)->None:
    logger.info(f"Starting raw data validation process for batch {batchid}")
    try:
        raw_data = file_validation(raw_file)
        data_level_validation(raw_data)
        schema_level_validation(raw_data)
        logger.info(f'Validation successful for batch {batchid}')
    except:
        raise ValueError(f'Validation failed for batch {batchid}')

#-----------CSV Creation----------------
def  staging_csv_creation(batchid:str,raw_file:str|Path)->Path:
    logger.info(f'Create staging files for the batch {batchid}')
    batch_folder = os.path.join(data_folder,batchid)
    file_name = 'assets.csv'
    csv_folder = os.path.join(batch_folder,'CSV')
    os.makedirs(csv_folder,exist_ok=True)
    
    csv_file_path = os.path.join(csv_folder,file_name)
    csv_file = store_csv_data(batchid,raw_file,csv_file_path)
    logger.info(f'CSV file created : {csv_file}')
    return csv_file


if __name__ == "__main__":
    batchid = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
    raw_file = ingestion(batchid)
    validation(batchid,raw_file)
    staging_csv_creation(batchid,raw_file)

