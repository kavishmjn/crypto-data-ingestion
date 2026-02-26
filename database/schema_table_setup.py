from database.connection import get_connection
import logging

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
        print(f"Schema '{schema_name}' ensured.")
    except Exception as e:
        raise RuntimeError(f"Failed to create schema '{schema_name}': {e}")


def create_assets_table(schema_name: str) -> None:
    """
    Creates staging.assets table if not exists.
    """
    query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.assets (
        asset_id VARCHAR(50) PRIMARY KEY,
        symbol VARCHAR(20),
        name VARCHAR(100),
        price_usd NUMERIC,
        market_cap_usd NUMERIC,
        volume_usd_24hr NUMERIC,
        supply NUMERIC,
        max_supply NUMERIC,
        rank INTEGER,
        last_updated TIMESTAMP,
        batch_id VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
        print(f"Table '{schema_name}.assets' ensured.")
    except Exception as e:
        raise RuntimeError(f"Failed to create table '{schema_name}.assets': {e}")