from database.connection import get_connection
import logging
from config import csv_raw_mapping
#----SETUP LOGGING
logger = logging.getLogger(__name__)


def create_schema(schema_name: str) -> None:
    """
    Creates a schema if it does not already exist.
    """
    query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
            conn.commit()
        print(f"Schema '{schema_name}' ensured.")

    except Exception as e:
        raise RuntimeError(f"Failed to create schema '{schema_name}': {e}")


def create_assets_table(schema_name: str) -> None:
    """
    Creates staging.assets table if not exists.
    """
    cols = [f"{key.upper()} {' '.join(v.upper() for v in values)}" for key, values in csv_raw_mapping.items()]
    query = f"CREATE TABLE IF NOT EXISTS {schema_name}.ASSETS ({','.join(cols)})"

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
        conn.commit()
        print(f"Table '{schema_name}.assets' ensured.")
    except Exception as e:
        raise RuntimeError(f"Failed to create table '{schema_name}.assets': {e}")