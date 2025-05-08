import logging
from typing import Callable, Union
import re

from gmqtt import Client
from ha_wb_discovery.mqtt_conn.local_mqtt import LocalMQTTClient

logger = logging.getLogger(__name__)

class Subscription:
    re_matcher: re.Pattern = None
    callback: Callable = None

    def __init__(self, pattern: str, callback: Callable):
        self.callback = callback
        pattern = pattern.replace('+', '[^/]+').replace('#', '.+')
        self.re_matcher = re.compile(pattern)

def default_404(topic: str, payload: bytes):
    if logger.isEnabledFor(logging.DEBUG):
        pl = payload.decode('utf-8')
        logger.debug(f'no handler matched for topic={topic} payload={pl}')

class MQTTRouter:
    _mqtt: Client = None
    _subscriptions: list[Subscription] = []
    on_404: Callable = default_404

    def __init__(self, cl: Union[Client, LocalMQTTClient]):
        cl.on_message = self._on_message
        self._mqtt = cl

    def subscribe(self, topic: str, callback: Callable[[str, bytes], None], qos: int = 0):
        self._subscriptions.append(Subscription(topic, callback))
        self._mqtt.subscribe(topic, qos=qos)

    def publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False):
        self._mqtt.publish(topic, payload, qos=qos, retain=retain)

    def _on_message(self, client: Client, topic: str, payload: bytes, qos: int, properties):
        if logger.isEnabledFor(logging.DEBUG):
            pl = payload.decode('utf-8')
            logger.debug(f'Received message topic={topic} payload={pl}')

        for sub in self._subscriptions:
            if sub.re_matcher.match(topic):
                sub.callback(topic, payload)
                return
        self.on_404(topic, payload)