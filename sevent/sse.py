import json

from typing import Coroutine

from toolz import curry


def create_message(id_: str, timestamp: int, topic: str, message: dict) -> bytes:
	data = json.dumps({
		"id": id_,
		"topic": topic,
		"timestamp": timestamp,
		"payload": message,
	})
    message = "\n".join((
        f"id: {id_}",
        f"event: {topic}",
        *(f"data: {p}" for p in data.split("\n")),
    ))
    return f"{message}\n\n".encode()


@curry
async def publish(write: Coroutine, id_: str, timestamp: int, topic: str, message: dict, hello) -> None:
	print(write, id_, timestamp, topic, message, hello)
	await write(create_message(id_, timestamp, topic, message))