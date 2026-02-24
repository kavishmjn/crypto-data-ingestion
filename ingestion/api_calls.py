from config import api_url, api_key, timeout, per_page
import requests
import logging 

#---------LOGGER-------
logger = logging.getLogger(__name__)

#--------API_CALLS-----

def fetch_assets(limit: int = per_page)->dict:
    headers =  {"Authorization":f"Bearer {api_key}"}
    params = {'limit':limit}
    try:
        response = requests.get(api_url,headers = headers,params=params,timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise     
