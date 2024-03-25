import asyncio
from helpers import REDIS_CON


async def test_redis():
    streams = {"screening": "$"}
    data = await REDIS_CON.xread(streams=streams, block=0)
    data = data[0][1]
    _, latest_record = data[len(data) - 1]
    print(latest_record)


asyncio.run(test_redis())
