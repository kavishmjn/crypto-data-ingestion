from datetime import datetime


import json
import os
import logging

from pydantic import FilePath

#-------LOGGER-------
logger = logging.getLogger(__name__)

#-------STORE_RAW_DATA-------
def store_raw_data(data: dict, filename: FilePath) -> None:
        try:
            with open(filename,'w',encoding='utf-8') as f:
                json.dump(data,f,indent=4)
            logger.info(f"Data stored successfully at {filename}")
        except IOError as e:
            logger.error(f"Failed to write data to file: {e}")
        except OSError as e:
            logger.error(f"OS error occurred while writing data: {e}")
        except Exception as e:
            logger.error(f"Unexpected error occurred while writing data: {e}")  