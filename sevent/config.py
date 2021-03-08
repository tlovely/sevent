import json
import os

SEVENT_WEBHOOKS = {}
SEVENT_SECRET = None

_g = globals()

if os.path.exists('config.json'):
	for key, value in json.load(open('config.json')).items():
		if key in _g:
			_g[key] = value