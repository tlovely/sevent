import json
import asyncio

import aiohttp

from toolz import curry

from sevent.publish.exceptions import PublishError
from sevent.types import Message, WebHook

TYPE = WebHook
_timeout = aiohttp.ClientTimeout(total=10)


def prepare(message: Message) -> bytes:
    data = json.dumps(message.__dict__, indent=2)
    return data.encode()


@curry
async def publish(uri: str, headers: dict, message: Message) -> None:
    headers_ = {**headers, "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession(timeout=_timeout) as session:
            await session.post(
                uri, 
                headers=headers_, 
                data=prepare(message),
            )
    except (
        aiohttp.ClientConnectorError, 
        aiohttp.ServerDisconnectedError, 
        aiohttp.ClientOSError,
        asyncio.exceptions.TimeoutError,
    ):
        raise PublishError()
