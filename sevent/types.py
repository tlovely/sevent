from typing import Literal
from dataclasses import dataclass


@dataclass
class Message:
	id: str
	timestamp: int
	topic: str
	data: dict


SSE: Literal = "sse"
WebHook: Literal = "webhook"
SubType = Literal[SSE, WebHook]