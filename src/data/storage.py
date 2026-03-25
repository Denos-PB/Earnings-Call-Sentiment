import logging
from pathlib import Path
import orjson

logging.basicConfig(
    level = logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/storage.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def get_or_fetch(path: Path, fetch_func, *args):
    if path.exists:
        logger.info(f"Data already exists: {path}")
        return orjson.loads(path.read_bytes())
    
    logger.info(f"Data miss: fetching from API")
    data = fetch_func(*args)

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(orjson.dumps(data))

    return data
