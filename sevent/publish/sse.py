import json

from typing import Coroutine

from toolz import curry

from sevent.publish.exceptions import PublishError
from sevent.types import Message, SSE

TYPE = SSE


def prepare(message: Message) -> bytes:
    data = json.dumps(message.__dict__, indent=2)
    message = "\n".join((
        f"id: {message.id}",
        f"event: {message.topic}",
        *(f"data: {p}" for p in data.split("\n")),
    ))
    return f"{message}\n\n".encode()


@curry
async def publish(write: Coroutine, message: Message) -> None:
    try:
        await write(prepare(message))
    except ConnectionResetError:
        raise PublishError()
