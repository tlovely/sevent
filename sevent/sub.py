from typing import Generator, Coroutine, Optional
from dataclasses import dataclass

from sevent.utils import get_id
from sevent.types import Message, SubType


class Sub:
	def __init__(self, type: SubType, topic: str, send: Coroutine) -> None:
		self.id = get_id()
		self.type = type
		self.topic = topic
		self.index = tuple(topic.split("/"))
		self._send = send

	async def send(self, message: Message) -> None:
		await self._send(message)

	def topic_match(self, topic: str) -> bool:
		parts = topic.split('/')
		return len(parts) == len(self.topic_parts) and all(
			(p1 == p2 or "*" == p2)  
			for p1, p2 in zip(parts, self.topic_parts)
		)
	

class Subs:
	def __init__(self) -> None:
		self._subs = {}
		self._index = {
			"s": set(),
			"c": {}
		}

	def add(self, type: SubType, topic: str, send: Coroutine) -> Sub:
		sub = Sub(type, topic, send)
		node = self._index
		for part in sub.index:
			if part not in node["c"]:
				node["c"][part] = {"s": set(), "c": {}}
			node = node["c"][part]
		node["s"].add(sub.id)
		self._subs[sub.id] = sub
		return sub

	def delete(self, sub_id: str) -> None:
		sub = self._subs[sub_id]
		node = self._index
		node_chain = []
		for part in sub.index:
			node_chain.append((part, node))
			node = node["c"][part]
		node["s"].remove(sub.id)
		for c, node in node_chain[::-1]:
			if not node["c"][c]["s"] and not node["c"][c]["c"]:
				del node["c"][c]
			else:
				break
		del self._subs[sub.id]

	def list(self, topic: Optional[str] = None) -> Generator[Sub, None, None]:
		index = tuple(topic.split("/"))
		nodes = [self._index]
		for i, part in enumerate(index):
			nodes_next = []
			for node in nodes:
				if part in node["c"]:
					nodes_next.append(node["c"][part])
				if "*" in node["c"]:
					nodes_next.append(node["c"]["*"])
			if not nodes_next:
				return
			nodes = nodes_next
		for node in nodes:
			yield from (self._subs[i] for i in node["s"])

	def get(self, sub_id: str) -> Optional[Sub]:
		return self._subs.get(sub_id)
