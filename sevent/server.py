import asyncio
import json

from uuid import uuid4 as uuid
from typing import Coroutine

from aiohttp import web
from toolz import curry

from sevent import config
from sevent.publish import sse, webhook
from sevent.publish.exceptions import PublishError
from sevent.sub import Sub, Subs
from sevent.types import Message, WebHook
from sevent.utils import get_id, get_time
from sevent.stats import Stats

app = web.Application()
subs = Subs()
stats = Stats()


async def _setup_webhooks(app):
    for wh in config.SEVENT_WEBHOOKS:
        subs.add(
            webhook.TYPE,
            wh["topic"],
            webhook.publish(wh["uri"], wh.get("headers", {}))
        )


async def _dispatch_one(sub: Sub, message: Message) -> None:
    try:
        await sub.send(message)
    except PublishError:
        if sub.type != WebHook:
            subs.delete(sub.id)


async def _dispatch(message: Message) -> None:
    with stats.publish_send():
        tasks = []
        for sub in subs.list(message.topic):
            tasks.append(_dispatch_one(sub, message))
        await asyncio.gather(*tasks)


async def publish(request):
    topic_ = request.match_info["topic"]
    with stats.publish(topic_):
        id_ = get_id()
        message = Message(
            id_,
            get_time(),
            topic_,
            json.loads((await request.content.read()).decode("utf-8"))
        )
        # TODO: keep track of pending tasks
        asyncio.create_task(_dispatch(message))
        return web.json_response({"id": id_})


async def subscribe(request):
    topic_ = request.match_info["topic"]
    with stats.subscribe(topic_):
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        sub = subs.add(sse.TYPE, topic_, sse.publish(response.write))
        while subs.get(sub.id):
            await asyncio.sleep(5)
        return response


async def get_stats(request):
    return web.json_response(stats.to_dict())


app.add_routes([
    web.post('/t/{topic:.*}', publish),
    web.get('/t/{topic:.*}', subscribe),
    # support wild card to drill into specific stats
    web.get('/c/stats', get_stats),
])

app.on_startup.append(_setup_webhooks)
