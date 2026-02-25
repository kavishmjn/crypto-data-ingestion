import json
from pathlib import Path
from config import field_mapping
import logging
import csv

#-------LOGGER-------
logger = logging.getLogger(__name__)

#-------STORE_RAW_DATA-------
def store_raw_data(data: dict, filename: str|Path) -> Path:
        try:
            with open(filename,'w',encoding='utf-8') as f:
                json.dump(data,f,indent=4)
            logger.info(f"Data stored successfully at {filename}")
            return Path(filename)  
        except Exception:
            logger.exception(f"Failed to write data to file: {filename}")
            raise 


#------------CREATE CSV FILES-----------
def store_csv_data(batchid:str,rawfilepath:str|Path,csvfilePath:str|Path) ->Path:
        try:
            with open(rawfilepath,'r',encoding='utf-8') as f:
                file_content = json.load(f)
            data_pull_timestamp = file_content.get('timestamp','')
            file_data = file_content.get('data')
            
            if not  file_data:
                 raise ValueError("No Data found in JSON")
            
            csv_field_names = list(field_mapping.values())
            csv_field_names += ['data_pull_timestamp','ingestion_timestamp']


            csv_file_content = [] #variable for creating list of dict that we write in stage csv file
            
            for row in file_data:
                #iterate over each row and create a new row using field mapping and values
                new_row = {}
                for source_field,target_field in field_mapping.items():
                    new_row[target_field] = row.get(source_field)
                new_row["data_pull_timestamp"] = data_pull_timestamp
                new_row["ingestion_timestamp"] = batchid
                csv_file_content.append(new_row)

            with open(csvfilePath, "w", newline='', encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile,fieldnames = csv_field_names)
                writer.writeheader()
                writer.writerows(csv_file_content)
            
            return csvfilePath
        except Exception as e:
             raise ValueError(f'Error while creating csv: {e}')
              
              