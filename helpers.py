import logging
import os
import warnings

import ccxt.async_support as async_ccxt
from dotenv import load_dotenv

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
LOG = logging.getLogger("screening-service")
LOG.setLevel(logging.INFO)

load_dotenv()

warnings.filterwarnings("ignore")
HOST = "localhost"
BASE_WS = f"ws://{HOST}:"
WS_PORT = 8768


def get_api_keys(exchange: str, websocket: bool = False) -> dict:
    key = os.environ[f"{exchange}_api_key"]
    secret = os.environ[f"{exchange}_api_secret"]
    if key and secret:
        if websocket:
            return dict(key_id=key, key_secret=secret)
        else:
            return dict(apiKey=key, secret=secret)


def get_exchange_object(exchange: str) -> async_ccxt.Exchange:
    exchange_class = getattr(async_ccxt, exchange)
    keys = get_api_keys(exchange)
    return exchange_class(keys) if keys else exchange_class()
