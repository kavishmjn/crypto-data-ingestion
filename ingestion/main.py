from config import logs,data_folder
import logging
from api_calls import fetch_assets
from store_data import store_raw_data
import os
from datetime import datetime, timezone

#-------Logging Configuration------
os.makedirs(logs, exist_ok=True)
os.makedirs(data_folder, exist_ok=True)
log_file = os.path.join(logs, f"log_{datetime.now(timezone.utc).strftime('%Y-%m-%d %H-%M-%S')}.log")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


#------Process Flow--------

def main(batchid: str) -> None:
    logger.info("Starting data ingestion process")
    try:
        data = fetch_assets()
        if not data:
            raise ValueError("No data fetched from API")
        filelocation = os.path.join(data_folder,batchid)
        filename = f'assets.json'
        file_path = os.path.join(filelocation,'raw')
        file_path = os.path.join(file_path,filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        store_raw_data(data,file_path)
        logger.info("Data ingestion process completed successfully")
    except Exception as e:
        logger.error(f"Data ingestion process failed: {e}")
        raise

if __name__ == "__main__":
    batchid = datetime.now(timezone.utc).strftime('%Y-%m-%d %H-%M-%S')
    main(batchid)
