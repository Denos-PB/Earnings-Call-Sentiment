from dotenv import load_dotenv
import os
import logging
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception,
    before_sleep_log
)
import requests
import time
import functools
from pathlib import Path

load_dotenv()

api_key = os.getenv("FMP_API_KEY")

if not api_key:
    raise ValueError("FMP_API_KEY not found. Did you create a .env file?")

Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level = logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/fetch.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

BASE_URL = "https://financialmodelingprep.com/api/v3"

def should_retry(exception) -> bool:
    if isinstance(exception, requests.exceptions.ConnectionError):
        logger.warning("Connection error — retrying")
        return True
    
    if isinstance(exception, requests.exceptions.Timeout):
        logger.warning("Timeout — retrying")
        return True
    
    if isinstance(exception, requests.exceptions.HTTPError):
        
        if exception.response is None:
            logger.error("HTTPError with no response object")
            return False
        
        status_code = exception.response.status_code

        RETRYABLE_CODES = {429,500,502,503}
        PERMANENT_CODES = {400,401,403,404,422}

        if status_code in RETRYABLE_CODES:
            if status_code == 429:
                retry_after = exception.response.headers.get("Retry-After")
                if retry_after:
                    logger.warning(f"Rate limited — server says wait {retry_after}s")
                    time.sleep(int(retry_after))
            else:
                logger.warning(f"Retryable error {status_code}")
            return True
        
        if status_code in PERMANENT_CODES:
            logger.error(f"Permanent error {status_code} — skipping")
            return False
        
        logger.error(f"Unexpected status code {status_code} — not retrying")
        return False
    
    logger.error(f"Unexpected exception type: {type(exception)}")
    return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception(should_retry),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def call_api(url: str, params: dict, api_key: str) -> dict:
    """
    Makes a GET request to the given URL with params.
    Retries on transient failures per should_retry logic.
    
    Raises:
        RetryError: if all retry attempts are exhausted
        HTTPError: immediately on permanent HTTP failures
    """
    params = {**params, "apikey": api_key}
    response  = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

fmp_get = functools.partial(call_api, api_key=api_key)

def fetch_transcript(ticker: str, year: int, quarter:int) -> dict:
    """
    Fetches a single earnings call transcript.
    Returns raw dict — no transformation here.
    Raises on failure — let the caller handle errors.
    """
    logger.info(f"Fetching transcript: {ticker} Q{quarter} {year}")

    url = f"{BASE_URL}/earning_call_transcript/{ticker}"
    params={"year":year,"quarter":quarter}

    raw = fmp_get(url=url, params=params)

    if not isinstance(raw, list):
        raise ValueError(f"Expected list from FMP, got {type(raw)} for {ticker} Q{quarter} {year}")
    
    if not raw:
        raise ValueError(f"No transcript found for {ticker} Q{quarter} {year}")
    
    transcript = raw[0]


    word_count = len(transcript.get("content", "").split())
    logger.info(f"Fetched {ticker} Q{quarter} {year} — {word_count} words")
    
    return transcript