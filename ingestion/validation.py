import logging
from pathlib import Path
import json
from config import critical_fields,numeric_fields,ignore_fields

#------SETTING UP LOGGER-----
logger = logging.getLogger(__name__)


#-------FILE LEVEL VALIDATION------
def file_exist_check(file: str | Path) -> None:
    #check file exists or not
    path = Path(file)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if not path.is_file():
        raise ValueError(f"Path is not a file: {path}")
    

def empty_file_check(file:str|Path)->None:
    #check the file is not empty
    path = Path(file)
    size = path.stat().st_size
    if size == 0:
        logger.error(f'File is empty : {path}')
        raise ValueError(f'File is empty : {path}')
    logger.info(f'File is not empty : {size} bytes')
    

def load_json(file_path:str|Path)->dict:
    #load json file and check file is readable or not
    try:
        path = Path(file_path)
        with open(path,'r',encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f'File loaded successfully : {path}')
        return data
    except json.JSONDecodeError as e:
        logger.error(f'Error decoding JSON file : {path} - {str(e)}')
        raise ValueError(f'Error decoding JSON file : {path} - {str(e)}')
    except Exception as e:
        logger.error(f'Error loading file : {path} - {str(e)}')
        raise ValueError(f'Error loading file : {path} - {str(e)}')
    
def data_structure_check(data:dict)->list:
   #check if the json structure is correct
   #check whether 'data' key exists and its type is list
    if not isinstance(data, dict):
        #logger.error(f"Expected json root to be dictionary, got {type(data)}")
        raise ValueError(f'Expected json root to be dictionary, got {type(data)}')
    if 'data' not in data:
        #logger.error(f'DATA key missing')
        raise ValueError('"data" key missing in json')
    records = data['data']
    if not isinstance(records,list):
        #logger.error(f'Expected list, got {type(records)} for Data key')
        raise ValueError(f'data must be list, got {type(records)}')
    logger.info(f'JSON structure is valid')
    return records

#---------DATA LEVEL CHECKS-----------
def record_count_check(data:list)->None:
    #check for data exist not zero
    record_count = len(data)
    if record_count==0:
        raise ValueError('No records in the dataset')
    logger.info(f'{record_count} records found in data')

def data_volume_check(data:list,threshold:int=50)->None:
    #Check the data volume is in valid range
    record_count = len(data)
    min_expected = int(0.7 * threshold)
    max_expected = int(1.3 * threshold)
    if record_count < min_expected or record_count > max_expected:
        logger.warning(f"Volume outside range: {record_count} ")  
    else:
        logger.info("Volume within expected range")

def record_json_structure_check(data:list)->None:
    #check if the json structure is correct (list of dicts) and no invalid record and api failure payload
    if not isinstance(data, list):
        #logger.error(f"Expected list, got {type(data)}")
        raise ValueError("Invalid JSON structure")
    #check for dict type in first 10 records
    invalid_record = 0
    for i,row in enumerate(data[:10]):
        if not isinstance(row,dict):
            invalid_record+=1
            logger.warning(f'Invalid Record at {i} index of {type(row)}')
    if invalid_record>0:
        raise ValueError("Invalid record structure")
   
    #check for error message in data
    first_record = data[0]
    if isinstance(first_record, dict) and ("error" in first_record or "message" in first_record):
        raise ValueError(f'API Error payloaf:{first_record}')
    
    logger.info('Data Record Structure Validated')



#------SCHEMA VALIDATION-----
def critical_fields_check(data:list)->None:
    #check critical fields present in all records
    invalid_records = 0
    for i,row in enumerate(data):
        missing_fields = set(critical_fields)-row.keys()
        if missing_fields:
            invalid_records+=1
            logger.warning(f'Record [{i}] has missing critical keys: {missing_fields}')
    if invalid_records>0:
        raise ValueError(f'{invalid_records} records have missing required fields')
    logger.info('Crtitcal fields present in all records')

def unique_id_check(data:list)->None:
    #check all ids are unique
    ids = set()
    duplicate_id = 0
    for i,row in enumerate(data):
        row_id = row['id']
        if row_id in ids:
            duplicate_id+=1
            logger.warning(f'Duplicate ID {row_id} found at index {i}')
        else:
            ids.add(row_id)
    if duplicate_id > 0:
        raise ValueError('Duplicate IDs exist in the batch')
    logger.info("No Duplicate ID found")

def null_value_checks(data:list)->None:
    #check all critical fields are present, not null or empty string
    invalid_records = 0
    for i,row in enumerate(data):
        for field in critical_fields:
            field_value = row.get(field)
            if field_value is None or field_value.strip() == '' :
                logger.warning(f'{field} has null/empty value in record {i}')
                invalid_records +=1
    if invalid_records>0:
        raise ValueError('Critical records are null/empty')
    logger.info('No Critical field is empty')

def type_check(data:list)->None:
    #Check data types of all fields
    invalid_records = 0
    for i,row in enumerate(data):
        for field,value in row.items():
            if field in ignore_fields:
                continue
            elif field not in numeric_fields:
                if not isinstance(value,str):
                    invalid_records += 1
                    logger.warning(f'invalid {field} value {value} at {i}')
            
            else:
                if value not in (None,'') and value.strip()!='':
                    try:
                        float(value)
                    except (TypeError, ValueError):
                        invalid_records += 1
                        logger.warning(f"Invalid {field} at {i}: {value}")
    if invalid_records > 0:
        raise ValueError(
            f"{invalid_records} records failed type validation"
        )

    logger.info("Type validation passed")

#------------FILE VALIDATION MASTER-----------------
def file_validation(path:str|Path)->list:
    #master fxn to file validation
    file_exist_check(path)
    empty_file_check(path)
    file_content = load_json(path)
    raw_data = data_structure_check(file_content)
    return raw_data

def data_level_validation(data:list)->None:
    #master fxn for data level validation
    record_count_check(data) 
    data_volume_check(data)
    record_json_structure_check(data)

def schema_level_validation(data:list)->None:
    #master fxn for schema levle validation
    critical_fields_check(data)
    unique_id_check(data)
    null_value_checks(data)
    type_check(data)


