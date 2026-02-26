import psycopg2
from psycopg2.extensions import connection
from typing import Optional, Dict
from config import DB_CONFIG
import logging

#----set up logger
logger = logging.getLogger(__name__)

def get_connection(config: Optional [Dict] = None) -> connection:
    """
    Creates and returns a PostgreSQL database connection.
    """
    db_config = config or DB_CONFIG
    try:
        logger.info("Trying to connect to the Database")
        conn = psycopg2.connect(**db_config)
        logger.info("Connection to Database created")
        return conn
    except Exception as e:
        raise RuntimeError(f"Failed to establish database connection: {e}")