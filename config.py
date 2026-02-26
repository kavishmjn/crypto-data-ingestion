import os
from dotenv import load_dotenv

load_dotenv()

api_url = 'https://rest.coincap.io/v3/assets'
api_key = os.getenv("COINCAP_API_KEY")
logs = 'logs'
data_folder = 'data' 
timeout = os.getenv('TIMEOUT', 10)
per_page = os.getenv('PER_PAGE',50)

#SCHEMA VARIABLES
critical_fields = ["id", "symbol", "priceUsd"]
ignore_fields = ['explorer','tokens']
field_mapping = {
    "id": "asset_id",
    "symbol": "symbol",
    "name": "name",
    "rank": "rank",
    "supply": "supply",
    "maxSupply": "max_supply",
    "marketCapUsd": "market_cap_usd",
    "volumeUsd24Hr": "volume_usd_24hr",
    "priceUsd": "price_usd",
    "changePercent24Hr": "change_percent_24hr",
    "vwap24Hr": "vwap_24hr"
}
numeric_fields = [
    "rank","supply","maxSupply","marketCapUsd",
    "volumeUsd24Hr","priceUsd",
    "changePercent24Hr", "vwap24Hr"]# to check convertibilty later

#DB Vars
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
}