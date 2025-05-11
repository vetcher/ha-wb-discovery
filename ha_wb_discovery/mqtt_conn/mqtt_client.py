import logging
from typing import Callable, Union
import re

from gmqtt import Client
from ha_wb_discovery.mqtt_conn.local_mqtt import LocalMQTTClient

logger = logging.getLogger(__name__)

class Subscription:
    re_matcher: re.Pattern
    callback: Callable

    def __init__(self, pattern: str, callback: Callable):
        self.callback = callback
        pattern = pattern.replace('+', '[^/]+').replace('#', '.+')
        self.re_matcher = re.compile(pattern)

def default_404(topic: str, payload: bytes):
    if logger.isEnabledFor(logging.DEBUG):
        pl = payload.decode('utf-8')
        logger.warning(f'no handler matched for topic={topic} payload={pl}')

class MQTTRouter:
    _client_name: str = ''
    _mqtt: Client | LocalMQTTClient
    _subscriptions: list[Subscription]
    on_404: Callable = default_404

    def __init__(self, cl: Client | LocalMQTTClient, client_name: str):
        self._client_name = client_name
        cl.on_message = self._on_message
        self._mqtt = cl
        self._subscriptions = []

    def subscribe(self, topic: str, callback: Callable[[str, bytes], None], qos: int = 0):
        self._subscriptions.append(Subscription(topic, callback))
        self._mqtt.subscribe(topic, qos=qos)
        logger.info(f"[{self._client_name}] subscribed to topic={topic} with qos={qos}")

    def publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False):
        self._mqtt.publish(topic, payload, qos=qos, retain=retain)
        logger.debug(f"[{self._client_name}] published to topic={topic} payload={payload} with qos={qos}")

    def _on_message(self, client: Client, topic: str, payload: bytes, qos: int, properties):
        if logger.isEnabledFor(logging.DEBUG):
            pl = payload.decode('utf-8')
            logger.debug(f"[{self._client_name}] received message topic={topic} payload={pl}")

        for sub in self._subscriptions:
            if sub.re_matcher.match(topic):
                sub.callback(topic, payload)
                return
        self.on_404(topic, payload)