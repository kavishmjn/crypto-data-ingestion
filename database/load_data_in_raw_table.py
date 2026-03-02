from database.connection import get_connection
from psycopg2.extras import execute_values
import logging
from pathlib import Path
import csv
from typing import Union
from datetime import datetime,timezone
logger = logging.getLogger(__name__)


def load_data(csv_file: Union[str, Path], schema_name: str, table_name: str = "assets") -> None:
    """
    Loads CSV data into a Postgres table using execute_values.
    Append-only ingestion.
    """
    file_path = Path(csv_file)
    insert_query = f"""
        INSERT INTO {schema_name}.{table_name} (
        asset_id, symbol, name,
        price_usd, market_cap_usd,
        volume_usd_24hr, supply, max_supply,
        rank, change_percent_24hr, vwap_24hr,
        data_pull_timestamp, ingestion_timestamp) VALUES %s"""
    timestamp =  datetime.now(timezone.utc).strftime('%Y-%m-%d %H-%M-%S')
    try:
        with get_connection() as conn:
            with conn.cursor() as curr:
                with open(file_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    records = [(row["asset_id"],row["symbol"],row["name"],row["price_usd"],row["market_cap_usd"],
                         row["volume_usd_24hr"],row["supply"],row["max_supply"],row["rank"],
                         row["change_percent_24hr"],row["vwap_24hr"],row["data_pull_timestamp"],timestamp)
                        for row in reader]
                execute_values(curr, insert_query, records)
            conn.commit()
        logger.info(f"{len(records)} rows inserted into {schema_name}.{table_name}")
    except Exception as e:
        logger.error("Failed to load data", exc_info=True)
        raise RuntimeError(f"Data load failed: {e}")