import logging
import os
import warnings

import ccxt.async_support as async_ccxt
from dotenv import load_dotenv
from redis.asyncio import Redis

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


def get_exchange_object(exchange: str) -> async_ccxt.Exchange:
    exchange_class = getattr(async_ccxt, exchange)
    keys = get_api_keys(exchange)
    return exchange_class(keys) if keys else exchange_class()


async def get_available_redis_streams() -> list:
    i = 0
    all_streams = list()
    while True:
        i, streams = await REDIS_CON.scan(i, _type="STREAM", match="{real-time}*")
        all_streams += streams
        if i == 0:
            return all_streams
