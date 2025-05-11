import asyncio
import json
import os
from typing import Callable
import re
import logging

logger = logging.getLogger(__name__)

class LocalMQTTClient:
    on_message: Callable
    on_disconnect: Callable
    on_connect: Callable

    _subscriptions: list[re.Pattern]
    _input_file: str
    _output_file: str
    _completed: asyncio.Event

    def __init__(self, input_file: str, output_file: str):
        self._input_file = input_file
        self._output_file = output_file
        self._subscriptions = []
        self._completed = asyncio.Event()
        with open(self._output_file, 'wt') as f:
            pass

    def subscribe(self, topic: str, qos: int = 0):
        topic_pattern = topic.replace('+', '[^/]+').replace('#', '.+')
        topic_regex = re.compile(f'^{topic_pattern}$')
        self._subscriptions.append(topic_regex)

    def publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False):
        msg = {
            'topic': topic,
            'payload': payload
        }

        with open(self._output_file, 'at') as f:
            f.write(json.dumps(msg) + '\n')

    async def connect(self, *args, **kwargs):
        if self.on_connect is not None:
            self.on_connect(self)

        with open(self._input_file) as f:
            for line in f:
                msg = json.loads(line)
                for topic_regex in self._subscriptions:
                    if topic_regex.match(msg['topic']):
                        self.on_message(None, msg['topic'], msg['payload'].encode('utf-8'), 0, {})
        self._completed.set()
        if self.on_disconnect is not None:
            await self.on_disconnect(None, None)

    async def disconnect(self):
        await self._completed.wait()