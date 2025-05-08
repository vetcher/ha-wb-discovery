import asyncio
import json
import logging
import re
import time
from typing import Callable

import ha_wb_discovery.mappers as mappers
from ha_wb_discovery.mqtt_conn.mqtt_client import MQTTRouter
from ha_wb_discovery.wirenboard_registry import WirenControl, WirenDevice, WirenBoardDeviceRegistry

logger = logging.getLogger(__name__)


class HomeAssistantConnector():
    wiren = None

    def __init__(self, broker_host, broker_port, username, password, client_id,
                 topic_prefix,
                 entity_prefix,
                 discovery_topic,
                 status_topic,
                 status_payload_online,
                 status_payload_offline,
                 debounce,
                 subscribe_qos,
                 availability_qos,
                 availability_retain,
                 availability_publish_delay,
                 state_qos,
                 state_retain,
                 config_qos,
                 config_retain,
                 config_publish_delay,
                 inverse,
                 split_devices,
                 split_entities,
                 ignore_availability
                 ):
        super().__init__(broker_host, broker_port, username, password, client_id)

        self._topic_prefix = topic_prefix
        self._entity_prefix = entity_prefix
        self._discovery_prefix = discovery_topic
        self._status_topic = status_topic
        self._status_payload_online = status_payload_online
        self._status_payload_offline = status_payload_offline
        self._debounce = debounce
        self._subscribe_qos = subscribe_qos
        self._availability_retain = availability_retain
        self._availability_qos = availability_qos
        self._availability_publish_delay = availability_publish_delay
        self._state_qos = state_qos
        self._state_retain = state_retain
        self._config_qos = config_qos
        self._config_retain = config_retain
        self._config_publish_delay = config_publish_delay # Delay (sec) before publishing to ensure that we got all meta topics
        self._inverse = inverse
        self._split_devices = split_devices
        self._split_entities = split_entities
        self._ignore_availability = ignore_availability

        self._control_set_topic_re = re.compile(self._topic_prefix + r"devices/([^/]*)/controls/([^/]*)/on$")
        self._component_types = {}
        self._debounce_last_published = {}
        self._async_tasks = {}

    def _on_connect(self, client):
        client.subscribe(self._status_topic, qos=self._subscribe_qos)
        client.subscribe(f"{self._topic_prefix}devices/+/controls/+/on", qos=self._subscribe_qos)
        self._publish_all_controls()

    def _on_message(self, client, topic, payload, qos, properties):
        # print(f'RECV MSG: {topic}', payload)
        payload = payload.decode("utf-8")
        if topic == self._status_topic:
            if payload == self._status_payload_online:
                logger.info('Home assistant changed status to online. Pushing all devices')
                self._publish_all_controls()
            elif payload == self._status_payload_offline:
                logger.info('Home assistant changed status to offline')
            else:
                logger.error(f'Invalid payload for status topic ({topic} -> {payload})')
        else:
            # Сообщения в controls/*/on перекладываем в wirenboard
            control_set_state_topic_match = self._control_set_topic_re.match(topic)
            if control_set_state_topic_match:
                device = WirenBoardDeviceRegistry().get_device(control_set_state_topic_match.group(1))
                control = device.get_control(control_set_state_topic_match.group(2))
                self.wiren.set_control_state(device, control, payload)

    def _publish_all_controls(self):
        for device in WirenBoardDeviceRegistry().devices.values():
            for control in device.controls.values():
                self.publish_config(device, control)

    def _get_control_topic(self, device: WirenDevice, control: WirenControl):
        return f"{self._topic_prefix}devices/{device.id}/controls/{control.id}"

    def _get_availability_topic(self, device: WirenDevice, control: WirenControl):
        return f"{self._get_control_topic(device, control)}/availability"

    def _run_task(self, task_id, task):
        loop = asyncio.get_event_loop()
        if task_id in self._async_tasks:
            self._async_tasks[task_id].cancel()
        self._async_tasks[task_id] = loop.create_task(task)

    def publish_state(self, device, control):
        if control.id in self._component_types:
            component = self._component_types[control.id]
            if component in self._debounce:
                debounce_interval = self._debounce[component]
                if control.id in self._debounce_last_published:
                    interval = (time.time() - self._debounce_last_published[control.id]) * 1000
                    if interval < debounce_interval:
                        return
        self._publish_state_sync(device, control)

    def _publish_state_sync(self, device, control):
        target_topic = f"{self._topic_prefix}devices/{device.id}/controls/{control.id}"
        self._publish(target_topic, control.state, qos=self._state_qos, retain=self._state_retain)
        logger.debug(f"[{device.debug_id}/{control.debug_id}] state: {control.state}")
        self._debounce_last_published[control.id] = time.time()

    def publish_availability(self, device: WirenDevice, control: WirenControl):
        if self._ignore_availability:
            return

        async def publish_availability():
            await asyncio.sleep(self._availability_publish_delay)
            self._publish_availability_sync(device, control)

        self._run_task(f"{device.id}_{control.id}_availability", publish_availability())

    def _publish_availability_sync(self, device: WirenDevice, control: WirenControl):
        if self._ignore_availability:
            return

        topic = self._get_availability_topic(device, control)
        payload = '1' if not control.error else '0'
        logger.info(f"[{device.debug_id}/{control.debug_id}] availability: {'online' if control.state else 'offline'}")
        self._publish(topic, payload, qos=self._availability_qos, retain=self._availability_retain)

    def publish_config(self, device: WirenDevice, control: WirenControl):
        async def do_publish_config():
            await asyncio.sleep(self._config_publish_delay)
            self._publish_config_sync(device, control)

            # Publish availability and state every time after publishing config
            self._publish_availability_sync(device, control)
            self._publish_state_sync(device, control)

        self._run_task(f"{device.id}_{control.id}_config", do_publish_config())

    def _publish_config_sync(self, device: WirenDevice, control: WirenControl):
        """
        Publish discovery topic to the HA
        """

        if self._entity_prefix:
            entity_id_prefix = self._entity_prefix.lower().replace(" ", "_").replace("-", "_") + '_'
        else:
            entity_id_prefix = ''

        if WirenBoardDeviceRegistry().is_local_device(device):
            device_unique_id = entity_id_prefix + 'wirenboard'
            device_name = self._entity_prefix + ' Wirenboard'
        else:
            device_unique_id = entity_id_prefix + device.id
            device_name = self._entity_prefix + ' ' + device.name

        device_unique_id = device_unique_id.lower().replace(" ", "_").replace("-", "_")

        entity_unique_id = f"{entity_id_prefix}{device.id}_{control.id}".lower().replace(" ", "_").replace("-", "_")
        object_id = f"{control.id}".lower().replace(" ", "_").replace("-", "_")
        entity_name = f"{self._entity_prefix} {device.id} {control.id}".replace("_", " ").title()

        if device_unique_id in self._split_devices or entity_unique_id in self._split_entities:
            device_unique_id = entity_unique_id
            device_name = entity_name

        node_id = device_unique_id

        # common payload
        payload = {
            'device': {
                'name': device_name,
                'identifiers': device_unique_id
            },
            'name': entity_name,
            'unique_id': entity_unique_id
        }

        if not self._ignore_availability:
            payload['availability_topic'] = self._get_availability_topic(device, control);
            payload['payload_available'] = "1"
            payload['payload_not_available'] = "0"

        inverse = entity_unique_id in self._inverse

        control_topic = self._get_control_topic(device, control)
        component = mappers.apply_payload_for_component(payload, device, control, control_topic, inverse=inverse)
        self._component_types[control.id] = component

        if not component:
            return

        # Topic path: <discovery_topic>/<component>/[<node_id>/]<object_id>/config
        topic = self._discovery_prefix + '/' + component + '/' + node_id + '/' + object_id + '/config'
        logger.info(f"[{device.debug_id}/{control.debug_id}] publish config to '{topic}'")
        self._publish(topic, json.dumps(payload), qos=self._config_qos, retain=self._config_retain)

class HomeAssistant:
    _router: MQTTRouter = None
    _registry: WirenBoardDeviceRegistry = None
    _split_devices: list[str] = []
    _split_entities: list[str] = []
    _config_publish_delay: int = 0
    _async_tasks: dict[str, asyncio.Task] = {}
    _ratelimiter: dict[str, float] = {}
    _ratelimit_intervals: dict[str, int] = {}
    _first_published_configs: dict[str, bool] = {}
    _subscribe_qos: int = 1

    _control_state_topic_re = re.compile(r"/devices/([^/]*)/controls/([^/]*)/on$")

    on_control_set_state: Callable[[str, str, str], None] = None

    def __init__(self, router: MQTTRouter, registry: WirenBoardDeviceRegistry, split_devices: list[str], split_entities: list[str]):
        self._router = router
        self._registry = registry
        self._split_devices = split_devices
        self._split_entities = split_entities
        self._async_tasks = {}

    def _run_task(self, task_id: str, task: Callable):
        loop = asyncio.get_event_loop()
        if task_id in self._async_tasks:
            self._async_tasks[task_id].cancel()
        self._async_tasks[task_id] = loop.create_task(task)

    def on_connect(self, *args, **kwargs):
        self._router.subscribe(f"hass/status", self._ha_status_topic_handler, qos=self._subscribe_qos)
        self._router.subscribe(f"/devices/+/controls/+/on", self._control_set_state_topic_handler, qos=self._subscribe_qos)
        self._run_task("publish_all_devices", self._publish_all_devices())

    async def _publish_all_devices(self):
        for device in self._registry.devices().values():
            self.publish_device_config(device)

    def publish_device_config(self, device: WirenDevice):
        async def do_publish_device_config():
            await asyncio.sleep(self._config_publish_delay)
            self._publish_device_config(device)

        self._run_task(f"{device.device_id}_device_config", do_publish_device_config())

    def _publish_device_config(self, device: WirenDevice):
        for control in device.controls.values():
            self.publish_control_config(device, control)

    def publish_control_config(self, device: WirenDevice, control: WirenControl):
        async def do_publish_control_config():
            if control.id not in self._first_published_configs:
                try:
                    # Wait for 1 second to ensure that all data is gathered from all wb topics
                    await asyncio.sleep(1)
                    # Next time do not wait
                    self._first_published_configs[control.id] = True
                except asyncio.CancelledError:
                    return
            self._publish_control_config(device, control)
            self._publish_availability_sync(device, control)
            self._publish_control_state_sync(device, control)
        self._run_task(f"{device.device_id}_{control.id}_config", do_publish_control_config())

    def _publish_control_config(self, device: WirenDevice, control: WirenControl):
        device_name = device.name
        device_unique_id = prepare_ha_identifier(device.device_id)
        entity_name = f"{device.device_id} {control.id}".replace("_", " ").title()
        entity_unique_id = prepare_ha_identifier(f"{device.device_id}_{control.id}")
        object_id = prepare_ha_identifier(control.id)

        if device_unique_id in self._split_devices or entity_unique_id in self._split_entities:
            device_unique_id = entity_unique_id
            device_name = entity_name

        payload = {
            'device': {
                'name': device_name,
                'identifiers': device_unique_id
            },
            'name': entity_name,
            'unique_id': entity_unique_id
        }
        if device.manufactorer:
            payload['device']['manufacturer'] = device.manufactorer
        if device.model:
            payload['device']['model'] = device.model
        if device.hw_version:
            payload['device']['hw_version'] = device.hw_version
        if device.serial_number:
            payload['device']['serial_number'] = device.serial_number
        if device.sw_version:
            payload['device']['sw_version'] = device.sw_version

        payload['availability_topic'] = self._get_availability_topic(device, control);
        payload['payload_available'] = "1"
        payload['payload_not_available'] = "0"

        component = self._enrich_with_component(payload, device, control)
        if not component:
            return

        node_id = device_unique_id

        topic = 'homeassistant' + '/' + component.value + '/' + node_id + '/' + object_id + '/config'
        logger.info(f"publish config of {device} to '{topic}'")

        async def publish_config():
            self._router.publish(topic, json.dumps(payload))#qos=self._config_qos, retain=self._config_retain)

        self._run_task(f"publish_{topic}", publish_config())

    def _get_control_topic(self, device: WirenDevice, control: WirenControl):
        return f"/devices/{device.device_id}/controls/{control.id}"

    def _get_availability_topic(self, device: WirenDevice, control: WirenControl):
        return f"{self._get_control_topic(device, control)}/availability"

    def _enrich_with_component(self, payload: dict, device: WirenDevice, control: WirenControl) -> mappers.HassControlType:
        control_topic = self._get_control_topic(device, control)
        hass_entity_type = mappers.wiren_to_hass_type1(control)
        if hass_entity_type is None:
            return None

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

    def _publish_availability_sync(self, device: WirenDevice, control: WirenControl):
        # if self._ignore_availability:
        #     return

        topic = self._get_availability_topic(device, control)
        payload = '1' if not control.error else '0'
        logger.info(f"[{device.debug_id}/{control.debug_id}] availability: {'online' if control.state else 'offline'}")
        self._router.publish(topic, payload), #qos=self._availability_qos, retain=self._availability_retain)

    def publish_control_state(self, device: WirenDevice, control: WirenControl):
        if self._ratelimiter.get(control.id, 0) + self._ratelimit_intervals.get(control.id, 0) > time.time():
            return
        self._run_task(f"publish_state_{control.id}", self._publish_control_state(device, control))

    async def _publish_control_state(self, device: WirenDevice, control: WirenControl):
        self._publish_control_state_sync(device, control)

    def _publish_control_state_sync(self, device: WirenDevice, control: WirenControl):
        target_topic = self._get_control_topic(device, control)
        self._router.publish(target_topic, control.state)#, qos=self._state_qos, retain=self._state_retain)
        logger.debug(f"[{device.debug_id}/{control.debug_id}] state: {control.state}")
        self._ratelimiter[control.id] = time.time()

    def _ha_status_topic_handler(self, topic: str, payload: bytes):
        if payload == b'online':
            logger.info('Home assistant changed status to online. Pushing all devices')
            self._publish_all_controls()
        elif payload == b'offline':
            logger.info('Home assistant changed status to offline')

    def _control_set_state_topic_handler(self, topic: str, payload: bytes):
        match = self._control_state_topic_re.match(topic)
        device_id, control_id, control_state = match.group(1), match.group(2), payload.decode('utf-8')
        self.on_control_set_state(device_id, control_id, control_state)

def prepare_ha_identifier(name: str) -> str:
    return name.lower().replace(" ", "_").replace("-", "_")
