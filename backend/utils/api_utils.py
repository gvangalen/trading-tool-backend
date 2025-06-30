import requests
import os
import logging
import traceback
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

# ✅ Instellingen
API_BASE_URL = os.getenv("API_BASE_URL", "http://market_dashboard-api:5002/api")
HEADERS = {"Content-Type": "application/json"}
TIMEOUT = 10

# ✅ Retry wrapper
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(endpoint, method="POST", payload=None):
    url = urljoin(API_BASE_URL, endpoint)
    try:
        response = requests.request(method, url, json=payload, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        logging.info(f"✅ API-call succesvol: {url}")
        return response.json()
    except Exception as e:
        logging.error(f"❌ Fout bij API-call naar {url}: {e}")
        logging.error(traceback.format_exc())
        raise

# ✅ Hulpfunctie voor logging
def log_retry_failure(task_name):
    logging.error(f"❌ Alle retries mislukt voor {task_name}")
