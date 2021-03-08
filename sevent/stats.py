import time
import json

from contextlib import contextmanager

from sevent.utils import get_time

SCALE_LIMIT_SUB_DUR_AVG = 20
SCALE_LIMIT_PUB_DUR_AVG = 40


class Stats:
	def __init__(self):
		self._start = get_time()
		self._pub_count = 0
		self._pub_started = 0
		self._pub_completed = 0
		# limit these values to 100 or so topics
		self._pub_by_topic = {}
		self._pub_dur_avg = None
		self._sub_count = 0
		self._sub_active = 0
		self._sub_dur_avg = None
		self._sub_by_topic = {}
		self._sub_active_by_topic = {}

	def to_dict(self):
		return {
			"runtime": get_time() - self._start,
			"publish": {
				"count": self._pub_count,
				"count_by_topic": self._pub_by_topic,
				"in_progress": self._pub_started - self._pub_completed,
				"latency": self._pub_dur_avg,
			},
			"subscribe": {
				"count": self._sub_count,
				"count_by_topic": self._sub_by_topic,
				"count_active": self._sub_active,
				"count_active_by_topic": self._sub_active_by_topic,
				"duration_average": self._sub_dur_avg,
			}
		}

	def to_json(self):
		return json.dumps(self.to_dict(), indent=2)
	
	@contextmanager
	def subscribe(self, topic):
		start = get_time()
		if topic not in self._sub_by_topic:
			self._sub_by_topic[topic] = 0
		self._sub_by_topic[topic] += 1
		if topic not in self._sub_active_by_topic:
			self._sub_active_by_topic[topic] = 0
		self._sub_active_by_topic[topic] += 1
		self._sub_count += 1
		self._sub_active += 1
		try:
			yield self
		finally:
			self._sub_active -= 1
			self._sub_active_by_topic[topic] -= 1
			if not self._sub_active_by_topic[topic]:
				del self._sub_active_by_topic[topic]
			dur = get_time() - start
			weight = (
				self._sub_count 
				if self._sub_count < SCALE_LIMIT_SUB_DUR_AVG
				else SCALE_LIMIT_SUB_DUR_AVG
			)
			if self._sub_dur_avg is None:
				self._sub_dur_avg = int(dur)
			else:
				self._sub_dur_avg = int(
					self._sub_dur_avg +
					(dur - self._sub_dur_avg) /
					weight
				)

	@contextmanager
	def publish(self, topic):
		self._pub_count += 1
		if topic not in self._pub_by_topic:
			self._pub_by_topic[topic] = 0
		self._pub_by_topic[topic] += 1
		yield self
	
	@contextmanager
	def publish_send(self):
		start = get_time()
		self._pub_started += 1
		try:
			yield self
		finally:
			self._pub_completed += 1
			dur = get_time() - start
			weight = (
				self._sub_count 
				if self._sub_count < SCALE_LIMIT_PUB_DUR_AVG
				else SCALE_LIMIT_PUB_DUR_AVG
			)
			if self._pub_dur_avg is None:
				self._pub_dur_avg = int(dur)
			else:
				self._pub_dur_avg = int(
					self._pub_dur_avg +
					(dur - self._pub_dur_avg) /
					weight
				)