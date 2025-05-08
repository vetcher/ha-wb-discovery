import json
import os
from typing import Callable
import re

class LocalMQTTClient:
    on_message: Callable = None
    on_disconnect: Callable = None
    on_connect: Callable = None

    _subscriptions = []

    def __init__(self, input_file: str, output_file: str):
        self.input_file = input_file
        self.output_file = output_file

    def subscribe(self, topic: str, qos: int = 0):
        topic_pattern = topic.replace('+', '[^/]+').replace('#', '.+')
        topic_regex = re.compile(f'^{topic_pattern}$')
        self._subscriptions.append(topic_regex)

    def publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False):
        msg = {
            'topic': topic,
            'payload': payload
        }

        mode = 'a' if os.path.exists(self.output_file) else 'w'
        with open(self.output_file, mode) as f:
            f.write(json.dumps(msg) + '\n')

    async def connect(self, *args, **kwargs):
        if self.on_connect is not None:
            self.on_connect(self)
        # Process existing messages from file
        if os.path.exists(self.input_file):
            with open(self.input_file, 'r') as f:
                for line in f:
                    msg = json.loads(line)
                    for topic_regex in self._subscriptions:
                        if topic_regex.match(msg['topic']):
                            self.on_message(None, msg['topic'], msg['payload'].encode('utf-8'), 0, {})
        if self.on_disconnect is not None:
            await self.on_disconnect(None, None)

    async def disconnect(self):
        pass