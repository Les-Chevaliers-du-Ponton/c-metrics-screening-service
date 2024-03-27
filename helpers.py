import logging
import os
import warnings
from datetime import datetime as dt

import ccxt
from dotenv import load_dotenv
from redis import Redis

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
LOG = logging.getLogger("screening-service")
LOG.setLevel(logging.INFO)

load_dotenv()

warnings.filterwarnings("ignore")

REDIS_CON = Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    decode_responses=True,
)


def get_api_keys(exchange: str, websocket: bool = False) -> dict:
    key = os.environ[f"{exchange}_api_key"]
    secret = os.environ[f"{exchange}_api_secret"]
    if key and secret:
        if websocket:
            return dict(key_id=key, key_secret=secret)
        else:
            return dict(apiKey=key, secret=secret)


def get_exchange_object(exchange: str) -> ccxt.Exchange:
    exchange_class = getattr(ccxt, exchange)
    keys = get_api_keys(exchange)
    return exchange_class(keys) if keys else exchange_class()


def get_available_redis_streams() -> list:
    i = 0
    all_streams = list()
    while True:
        i, streams = REDIS_CON.scan(i, _type="STREAM", match="{real-time}*")
        all_streams += streams
        if i == 0:
            return all_streams


def write_fractal_refresh_tmstmp():
    REDIS_CON.xadd(
        "{fractal_refresh_tmstmp}",
        {"last": dt.now().isoformat()},
        maxlen=1,
        approximate=True,
    )
