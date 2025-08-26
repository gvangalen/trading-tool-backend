import requests
import os
import logging
import traceback
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

# ✅ Basisinstellingen
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5002/api")
HEADERS = {"Content-Type": "application/json"}
TIMEOUT = 10  # seconden

logger = logging.getLogger(__name__)

# ✅ Robuuste retry wrapper met exponential backoff
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=20), reraise=True)
def safe_request(endpoint: str, method: str = "POST", payload: dict = None):
    """
    Veilige API-aanroep met retries en foutafhandeling.
    """
    url = urljoin(API_BASE_URL, endpoint)
    try:
        response = requests.request(
            method=method.upper(),
            url=url,
            json=payload,
            headers=HEADERS,
            timeout=TIMEOUT
        )
        response.raise_for_status()
        logger.info(f"✅ API-call succesvol: {method.upper()} {url}")
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Fout bij API-call: {method.upper()} {url}: {e}")
        logger.debug(traceback.format_exc())
        raise

# ✅ Fallback logger bij volledig mislukte retry
def log_retry_failure(task_name: str):
    logger.error(f"❌ Alle retries mislukt voor taak: {task_name}")
