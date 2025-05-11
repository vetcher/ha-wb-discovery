import asyncio
import json
import logging
import re
import time
from typing import Callable, Coroutine

import ha_wb_discovery.mappers as mappers
from ha_wb_discovery.mqtt_conn.mqtt_client import MQTTRouter
from ha_wb_discovery.wirenboard_registry import WirenControl, WirenDevice, WirenBoardDeviceRegistry

logger = logging.getLogger(__name__)

class CombinedDevice:
    device_id: str
    new_device_id: str
    new_name: str

    def __init__(self, device_id: str, new_device_id: str, new_name: str):
        self.device_id = device_id
        self.new_device_id = new_device_id
        self.new_name = new_name

_default_combined_devices: list[CombinedDevice] = [
    CombinedDevice('wb_adc', 'wirenboard', 'Wiren Board'),
    CombinedDevice('wbrules', 'wirenboard', 'Wiren Board'),
    CombinedDevice('wb_gpio', 'wirenboard', 'Wiren Board'),
    CombinedDevice('power_status', 'wirenboard', 'Wiren Board'),
    CombinedDevice('network', 'wirenboard', 'Wiren Board'),
    CombinedDevice('system', 'wirenboard', 'Wiren Board'),
    CombinedDevice('hwmon', 'wirenboard', 'Wiren Board'),
    CombinedDevice('buzzer', 'wirenboard', 'Wiren Board'),
    CombinedDevice('alarms', 'wirenboard', 'Wiren Board'),
    CombinedDevice('metrics', 'wirenboard', 'Wiren Board'),
]

class HomeAssistantDiscoveryCustomizer:
    _ignored_device_ids: set[str]
    _ignored_device_control_ids: set[str]
    _splitted_device_ids: set[str]
    _combined_devices: dict[str, CombinedDevice]

    def __init__(self,
                 ignored_device_ids: list[str] = [],
                 ignored_device_control_ids: list[str] = [],
                 splitted_device_ids: list[str] = [],
                 combined_devices: list[dict] = [],
                 enable_default_combined_devices: bool = True,
        ):
        self._ignored_device_ids = set(ignored_device_ids)
        self._ignored_device_control_ids = set(ignored_device_control_ids)
        self._splitted_device_ids = set(splitted_device_ids)
        self._combined_devices = {e['device_id']: CombinedDevice(**e) for e in combined_devices}
        if enable_default_combined_devices:
            self._combined_devices.update({e.device_id: e for e in _default_combined_devices})

    def is_ignored_device(self, device_id: str) -> bool:
        return device_id in self._ignored_device_ids

    # Используем entity_id, а не device_id + control_id, потому что в вызывающем коде идентификатор уже прошел преобразование.
    def is_ignored_control(self, entity_id: str) -> bool:
        return entity_id in self._ignored_device_control_ids

    def is_splitted_device(self, device_id: str) -> bool:
        return device_id in self._splitted_device_ids

    def get_combined_device_id(self, device_id: str) -> CombinedDevice | None:
        return self._combined_devices.get(device_id)

class HomeAssistant:
    # components
    _router: MQTTRouter
    _registry: WirenBoardDeviceRegistry
    _ha_customizer: HomeAssistantDiscoveryCustomizer
    _async_tasks: dict[str, asyncio.Task]

    # internal states
    _ratelimiter: dict[str, float]
    _ratelimit_intervals: dict[str, int]
    _first_published_configs: dict[str, bool]

    # configs
    _config_publish_delay: int
    _config_first_publish_delay: int
    _subscribe_qos: int
    _availability_qos: int
    _availability_retain: bool
    _config_qos: int
    _config_retain: bool
    _state_qos: int
    _state_retain: bool

    _control_command_topic_re = re.compile(r"/devices/([^/]*)/controls/([^/]*)/on$")

    on_control_set_state: Callable[[str, str, str], None]

    def __init__(self,
                 router: MQTTRouter,
                 registry: WirenBoardDeviceRegistry,
                 customizer: HomeAssistantDiscoveryCustomizer,
                 config_first_publish_delay: int = 1,
                 config_publish_delay: int = 0,
                 subscribe_qos: int = 1,
                 availability_qos: int = 1,
                 availability_retain: bool = True,
                 config_qos: int = 1,
                 config_retain: bool = True,
                 state_qos: int = 1,
                 state_retain: bool = True,
        ):
        self._router = router
        self._registry = registry
        self._ha_customizer = customizer
        self._config_first_publish_delay = config_first_publish_delay
        self._config_publish_delay = config_publish_delay
        self._subscribe_qos = subscribe_qos
        self._availability_qos = availability_qos
        self._availability_retain = availability_retain
        self._config_qos = config_qos
        self._config_retain = config_retain
        self._state_qos = state_qos
        self._state_retain = state_retain
        self._async_tasks = {}
        self._ratelimiter = {}
        self._ratelimit_intervals = {}
        self._first_published_configs = {}

    def _run_task(self, task_id: str, task: Coroutine):
        loop = asyncio.get_event_loop()
        if task_id in self._async_tasks:
            self._async_tasks[task_id].cancel()
        self._async_tasks[task_id] = loop.create_task(task)

    def on_connect(self, *args, **kwargs):
        logger.warning(f"connected to MQTT")
        self._router.subscribe(f"hass/status", self._ha_status_topic_handler, qos=self._subscribe_qos)
        self._router.subscribe(f"/devices/+/controls/+/on", self._control_set_state_topic_handler, qos=self._subscribe_qos)
        self._publish_all_devices()

    def _publish_all_devices(self):
        async def do_publish_all_devices():
            for device in self._registry.devices().values():
                self.publish_device_config(device)
        self._run_task("publish_all_devices", do_publish_all_devices())

    def publish_device_config(self, device: WirenDevice):
        async def do_publish_device_config():
            await asyncio.sleep(self._config_publish_delay)
            self._publish_device_config(device)

        self._run_task(f"{device.device_id}_device_config", do_publish_device_config())

    def _publish_device_config(self, device: WirenDevice):
        for control in device.controls.values():
            self.publish_control_config(device, control)

    def publish_control_config(self, device: WirenDevice, control: WirenControl):
        if self._ha_customizer.is_ignored_device(prepare_ha_identifier(device.device_id)):
            return
        if self._ha_customizer.is_ignored_control(format_entity_id(device.device_id, control.id)):
            return
        async def do_publish_control_config():
            if control.id not in self._first_published_configs:
                try:
                    # Wait for 1 second to ensure that all data is gathered from all wb topics
                    await asyncio.sleep(self._config_first_publish_delay)
                    # Next time do not wait
                    self._first_published_configs[control.id] = True
                except asyncio.CancelledError:
                    return
            self._publish_control_config(device, control)
            self._publish_availability_sync(device, control)
            self._publish_control_state_sync(device, control)
        self._run_task(f"{device.device_id}_{control.id}_config", do_publish_control_config())

    def _publish_control_config(self, device: WirenDevice, control: WirenControl):
        # Итоговый идентификатор девайса, под которым девайс или контрол будет зарегистрирован в Home Assistant
        # Ниже эти параметры будут переопределены в соответствии с конфигом кастомизации
        device_unique_id = prepare_ha_identifier(device.device_id)
        device_name = device.name

        # Entity в Home Assistant, control в WirenBoard
        entity_unique_id = format_entity_id(device.device_id, control.id)
        entity_name = f"{device.device_id} {control.id}".replace("_", " ").title()
        object_id = prepare_ha_identifier(control.id)

        if self._ha_customizer.is_ignored_device(device_unique_id):
            return
        if self._ha_customizer.is_ignored_control(entity_unique_id):
            return

        if self._ha_customizer.is_splitted_device(device_unique_id):
            device_unique_id = entity_unique_id
            device_name = f"{device_name} {control.id}".replace("_", " ").title()

        combined_device = self._ha_customizer.get_combined_device_id(device_unique_id)
        if combined_device:
            device_unique_id = combined_device.new_device_id
            device_name = combined_device.new_name

        d_payload = {
            'name': device_name,
            'identifiers': device_unique_id
        }
        if device.manufactorer:
            d_payload['manufacturer'] = device.manufactorer
        if device.model:
            d_payload['model'] = device.model
        if device.hw_version:
            d_payload['hw_version'] = device.hw_version
        if device.serial_number:
            d_payload['serial_number'] = device.serial_number
        if device.sw_version:
            d_payload['sw_version'] = device.sw_version

        payload = {
            'device': d_payload,
            'name': entity_name,
            'unique_id': entity_unique_id
        }

        payload['availability_topic'] = self._get_availability_topic(device, control);
        payload['payload_available'] = "1"
        payload['payload_not_available'] = "0"

        component = self._enrich_with_component(payload, device, control)
        if not component:
            return

        node_id = device_unique_id

        # https://www.home-assistant.io/integrations/mqtt/#discovery-messages
        topic = 'homeassistant' + '/' + component.value + '/' + node_id + '/' + object_id + '/config'
        logger.info(f"publish config of {control} to '{topic}'")

        async def publish_config():
            self._router.publish(topic, json.dumps(payload), qos=self._config_qos, retain=self._config_retain)

        self._run_task(f"publish_{topic}", publish_config())

    def _get_control_topic(self, device: WirenDevice, control: WirenControl):
        return f"/devices/{device.device_id}/controls/{control.id}"

    def _get_availability_topic(self, device: WirenDevice, control: WirenControl):
        return f"{self._get_control_topic(device, control)}/availability"

    def _enrich_with_component(self, payload: dict, device: WirenDevice, control: WirenControl) -> mappers.HassControlType | None:
        hass_entity_type = mappers.wiren_to_hass_type(control)
        if hass_entity_type is None:
            return None

        control_topic = self._get_control_topic(device, control)
        # if inverse:
        #     _payload_on = '0'
        #     _payload_off = '1'
        # else:
        #     _payload_on = '1'
        #     _payload_off = '0'

        _payload_on = '1'
        _payload_off = '0'

        if hass_entity_type == mappers.HassControlType.switch:
            payload.update({
                'payload_on': _payload_on,
                'payload_off': _payload_off,
                'state_on': _payload_on,
                'state_off': _payload_off,
                'state_topic': f"{control_topic}",
                'command_topic': f"{control_topic}/on",
            })
        elif hass_entity_type == mappers.HassControlType.binary_sensor:
            payload.update({
                'payload_on': _payload_on,
                'payload_off': _payload_off,
                'state_topic': f"{control_topic}",
            })
        elif hass_entity_type == mappers.HassControlType.sensor:
            payload.update({
                'state_topic': f"{control_topic}",
            })
            if control.type == mappers.WirenControlType.temperature:
                payload['device_class'] = 'temperature'
            if control.units:
                payload['unit_of_measurement'] = control.units
        elif hass_entity_type == mappers.HassControlType.button:
            payload.update({
                'command_topic': f"{control_topic}/on",
            })
        else:
            logger.warning(f"No algorithm for hass type '{control.type.name}', hass: '{hass_entity_type}', {device}")
            return None

        return hass_entity_type

    def publish_availability(self, device: WirenDevice, control: WirenControl):
        self._publish_availability_sync(device, control)

    def _publish_availability_sync(self, device: WirenDevice, control: WirenControl):
        if self._ha_customizer.is_ignored_device(prepare_ha_identifier(device.device_id)):
            return
        if self._ha_customizer.is_ignored_control(format_entity_id(device.device_id, control.id)):
            return
        topic = self._get_availability_topic(device, control)
        payload = '1' if not control.error else '0'
        logger.info(f"[{device.debug_id}/{control.debug_id}] availability: {'online' if control.state else 'offline'}")
        self._router.publish(topic, payload, qos=self._availability_qos, retain=self._availability_retain)

    def publish_control_state(self, device: WirenDevice, control: WirenControl):
        if self._ratelimiter.get(control.id, 0) + self._ratelimit_intervals.get(control.id, 0) > time.time():
            return
        self._run_task(f"publish_state_{control.id}", self._publish_control_state(device, control))

    async def _publish_control_state(self, device: WirenDevice, control: WirenControl):
        self._publish_control_state_sync(device, control)

    def _publish_control_state_sync(self, device: WirenDevice, control: WirenControl):
        if self._ha_customizer.is_ignored_device(prepare_ha_identifier(device.device_id)):
            return
        if self._ha_customizer.is_ignored_control(format_entity_id(device.device_id, control.id)):
            return
        target_topic = self._get_control_topic(device, control)
        if control.state is None:
            logger.debug(f"[{control}] state is None, skip publishing")
            return
        self._router.publish(target_topic, control.state, qos=self._state_qos, retain=self._state_retain)
        self._ratelimiter[control.id] = time.time()

    def _ha_status_topic_handler(self, topic: str, payload: bytes):
        if payload == b'online':
            logger.info('Home assistant changed status to online. Pushing all devices')
            self._publish_all_devices()
        elif payload == b'offline':
            logger.info('Home assistant changed status to offline')

    def _control_set_state_topic_handler(self, topic: str, payload: bytes):
        match = self._control_command_topic_re.match(topic)
        if not match:
            logger.warning(f'not matched topic={topic} re={self._control_command_topic_re}')
            return
        device_id, control_id, control_state = match.group(1), match.group(2), payload.decode('utf-8')
        self.on_control_set_state(device_id, control_id, control_state)

def prepare_ha_identifier(name: str) -> str:
    return name.lower().replace(" ", "_").replace("-", "_")

def format_entity_id(device_id: str, control_id: str) -> str:
    return prepare_ha_identifier(f"{device_id}_{control_id}")
