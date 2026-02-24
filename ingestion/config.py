import os

api_url = 'https://rest.coincap.io/v3/assets'
api_key = os.getenv("COINCAP_API_KEY")
logs = 'logs'
data_folder = 'data' 
timeout = os.getenv('TIMEOUT', 10)
per_page = os.getenv('PER_PAGE',50)
