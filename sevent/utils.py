import uuid
import time


def get_id() -> str:
	return uuid.uuid4().hex


def get_time() -> int:
	return int(time.time()*1000)
