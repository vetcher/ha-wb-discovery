import asyncio
import logging
import re
import json
from typing import Callable

from ha_wb_discovery.wirenboard_registry import WirenBoardDeviceRegistry, WirenDevice, WirenControl
from ha_wb_discovery.mqtt_conn.mqtt_client import MQTTRouter
from ha_wb_discovery.mappers import WirenControlType, WIREN_UNITS_DICT

logger = logging.getLogger(__name__)


class WirenConnector():
    hass = None
    _publish_delay_sec = 1  # Delay before publishing to ensure that we got all meta topics
    _subscribe_qos = 1
    _control_state_publish_qos = 1
    _control_state_publish_retain = False

    def __init__(self, broker_host, broker_port, username, password, client_id, topic_prefix):
        super().__init__(broker_host, broker_port, username, password, client_id)

        self._topic_prefix = topic_prefix

        self._device_meta_topic_re = re.compile(self._topic_prefix + r"/devices/([^/]*)/meta/([^/]*)")
        self._control_meta_topic_re = re.compile(self._topic_prefix + r"/devices/([^/]*)/controls/([^/]*)/meta/([^/]*)")
        self._control_state_topic_re = re.compile(self._topic_prefix + r"/devices/([^/]*)/controls/([^/]*)$")
        self._unknown_types = []

    @staticmethod
    def _on_device_meta_change(device_id, meta_name, meta_value):
        device = WirenBoardDeviceRegistry().get_device(device_id)
        if meta_name == 'name':
            device.name = meta_value
        logger.debug(f'DEVICE: {device_id} / {meta_name} ==> {meta_value}')

    def _on_control_meta_change(self, device_id, control_id, meta_name, meta_value):
        device = WirenBoardDeviceRegistry().get_device(device_id)
        control = device.get_control(control_id)

        # print(f'CONTROL: {device_id} / {control_id} / {meta_name} ==> {meta_value}')
        if meta_name == 'error':
            # publish availability separately. do not publish all device
            if control.apply_error(False if not meta_value else True):
                self.hass.publish_availability(device, control)
        else:
            has_changes = False

            if control.error is None:
                # We assume that there is no error by default
                control.error = False
                has_changes = True

            if meta_name == 'order':
                return  # Ignore
            elif meta_name == 'type':
                try:
                    has_changes |= control.apply_type(WirenControlType(meta_value))
                    if control.type in WIREN_UNITS_DICT:
                        has_changes |= control.apply_units(WIREN_UNITS_DICT[control.type])
                except ValueError:
                    if not meta_value in self._unknown_types:
                        logger.warning(f'Unknown type for wirenboard control: {meta_value}')
                        self._unknown_types.append(meta_value)
            elif meta_name == 'readonly':
                has_changes |= control.apply_read_only(True if meta_value == '1' else False)
            elif meta_name == 'units':
                has_changes |= control.apply_units(meta_value)
            elif meta_name == 'max':
                has_changes |= control.apply_max(int(meta_value) if meta_value else None)
            if has_changes:
                self.hass.publish_config(device, control)

    def _on_connect(self, client):
        client.subscribe(self._topic_prefix + '/devices/+/meta/+', qos=self._subscribe_qos)
        client.subscribe(self._topic_prefix + '/devices/+/controls/+/meta/+', qos=self._subscribe_qos)
        client.subscribe(self._topic_prefix + '/devices/+/controls/+', qos=self._subscribe_qos)

    def _on_message(self, client, topic, payload, qos, properties):
        logger.debug(f'RECV MSG: {topic} {payload}')
        # print(json.dumps({"topic": topic, "payload": payload.decode("utf-8")}))
        payload = payload.decode("utf-8")
        device_topic_match = self._device_meta_topic_re.match(topic)
        control_meta_topic_match = self._control_meta_topic_re.match(topic)
        control_state_topic_match = self._control_state_topic_re.match(topic)
        if device_topic_match:
            self._on_device_meta_change(device_topic_match.group(1), device_topic_match.group(2), payload)
        elif control_meta_topic_match:
            self._on_control_meta_change(control_meta_topic_match.group(1), control_meta_topic_match.group(2), control_meta_topic_match.group(3), payload)
        elif control_state_topic_match:
            device = WirenBoardDeviceRegistry().get_device(control_state_topic_match.group(1))
            control = device.get_control(control_state_topic_match.group(2))
            control.state = payload
            self.hass.publish_state(device, control)

    def set_control_state(self, device: WirenDevice, control: WirenControl, payload):
        target_topic = f"{self._topic_prefix}/devices/{device.id}/controls/{control.id}/on"
        self._publish(target_topic, payload, qos=self._control_state_publish_qos, retain=self._control_state_publish_retain)

class Wirenboard:
    _subscribe_qos = 1
    _device_meta_topic_re = re.compile(r"/devices/([^/]*)/meta/([^/]*)")
    _control_meta_topic_re = re.compile(r"/devices/([^/]*)/controls/([^/]*)/meta/([^/]*)")
    _control_state_topic_re = re.compile(r"/devices/([^/]*)/controls/([^/]*)$")

    _router: MQTTRouter = None
    _device_registry: WirenBoardDeviceRegistry = None

    on_publish_ha_device_config: Callable[[WirenDevice], None] = None
    on_publish_ha_control_config: Callable[[WirenDevice, WirenControl], None] = None
    on_publish_ha_control_state_config: Callable[[WirenDevice, WirenControl], None] = None

    def __init__(self, router: MQTTRouter, registry: WirenBoardDeviceRegistry):
        self._router = router
        self._device_registry = registry

    def on_connect(self, *args, **kwargs):
        self._router.subscribe('/devices/+/meta/+', self._device_meta_handler, qos=self._subscribe_qos)
        self._router.subscribe('/devices/+/controls/+/meta/+', self._control_meta_handler, qos=self._subscribe_qos)
        self._router.subscribe('/devices/+/controls/+', self._control_state_handler, qos=self._subscribe_qos)

    def _device_meta_handler(self, topic: str, payload: bytes):
        match = self._device_meta_topic_re.match(topic)
        device_id, meta_name, meta_value = match.group(1), match.group(2), payload.decode('utf-8')
        device = self._device_registry.get_device(device_id)
        if meta_name == 'name':
            device.name = meta_value
        logger.debug(f'DEVICE META: {device_id} / {meta_name} ==> {meta_value}')

    def _control_meta_handler(self, topic: str, payload: bytes):
        match = self._control_meta_topic_re.match(topic)
        device_id, control_id, meta_name, meta_value = match.group(1), match.group(2), match.group(3), payload.decode('utf-8')
        logger.debug(f'CONTROL META: {device_id} / {control_id} / {meta_name} ==> {meta_value}')

        # Обработка специальных контролов.
        # В mqtt в wb системная информация зарегана под устройством system.
        # Вытаскиваем из system максимум информации, при этом не регаем его как отдельный контрол.
        # Конкретно тут скипаем регу.
        if device_id == 'system' and self.is_known_system_control(control_id):
            return

        device = self._device_registry.get_device(device_id)
        control = device.get_control(control_id)

        if meta_name == 'error':
            # publish availability separately. do not publish all device
            if control.apply_error(False if not meta_value else True):
                self.hass.publish_availability(device, control)
        else:
            has_changes = False

            if control.error is None:
                # We assume that there is no error by default
                control.error = False
                has_changes = True

            if meta_name == 'order':
                return  # Ignore
            elif meta_name == 'type':
                try:
                    has_changes |= control.apply_type(WirenControlType(meta_value))
                    if control.type in WIREN_UNITS_DICT:
                        has_changes |= control.apply_units(WIREN_UNITS_DICT[control.type])
                except ValueError:
                    if not meta_value in self._unknown_types:
                        logger.warning(f'Unknown type for wirenboard control: {meta_value}')
                        self._unknown_types.append(meta_value)
            elif meta_name == 'readonly':
                has_changes |= control.apply_read_only(True if meta_value == '1' else False)
            elif meta_name == 'units':
                has_changes |= control.apply_units(meta_value)
            elif meta_name == 'max':
                has_changes |= control.apply_max(int(meta_value) if meta_value else None)
            if has_changes:
                self.on_publish_ha_control_config(device, control)

    def _control_state_handler(self, topic: str, payload: bytes):
        match = self._control_state_topic_re.match(topic)
        device_id, control_id, control_state = match.group(1), match.group(2), payload.decode('utf-8')

        # Обработка специальных контролов.
        # В mqtt в wb системная информация зарегана под устройством system.
        # Вытаскиваем из system максимум информации, при этом не регаем его как отдельный контрол.
        # Конкретно тут скипаем регу.
        if device_id == 'system':
            if self.process_system_control(device_id, control_id, control_state):
                return
        normilized_control_id = control_id.lower().replace(" ", "_")
        if normilized_control_id == 'serial':
            device = self._device_registry.get_device(device_id)
            device.serial_number = control_state
            self.on_publish_ha_device_config(device)
            return
        device = self._device_registry.get_device(device_id)
        control = device.get_control(control_id)
        control.state = control_state
        self.on_publish_ha_control_state_config(device, control)

    def is_known_system_control(self, control_id: str) -> bool:
        return control_id.lower().replace(" ", "_") in _known_system_controls

    def process_system_control(self, device_id: str, control_id: str, value: str) -> bool:
        if not self.is_known_system_control(control_id):
            return False

        device = self._device_registry.get_device(device_id)
        normalized_control_id = control_id.lower().replace(" ", "_")
        if normalized_control_id == 'hw_revision':
            device.hw_version = value
            device.model = value
        elif normalized_control_id == 'short_sn':
            device.serial_number = value
        elif normalized_control_id == 'release_name':
            device.sw_version = value
        else:
            return False
        self.on_publish_ha_device_config(device)
        return True

    def on_control_set_state(self, device_id: str, control_id: str, control_state: str):
        self._router.publish(f"/devices/{device_id}/controls/{control_id}/on", control_state)

_known_system_controls = ['hw_revision', 'short_sn', 'release_name']