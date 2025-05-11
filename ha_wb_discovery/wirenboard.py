import logging
import re
from typing import Protocol

from ha_wb_discovery.wirenboard_registry import WirenBoardDeviceRegistry, WirenDevice, WirenControl
from ha_wb_discovery.mqtt_conn.mqtt_client import MQTTRouter
from ha_wb_discovery.mappers import WirenControlType, WIREN_UNITS_DICT

logger = logging.getLogger(__name__)

class IHomeAssistant(Protocol):
    def publish_device_config(self, device: WirenDevice) -> None:
        ...

    def publish_control_config(self, device: WirenDevice, control: WirenControl) -> None:
        ...

    def publish_control_state(self, device: WirenDevice, control: WirenControl) -> None:
        ...

    def publish_availability(self, device: WirenDevice, control: WirenControl) -> None:
        ...

class Wirenboard:
    _device_meta_topic_re = re.compile(r"/devices/([^/]*)/meta/([^/]*)")
    _control_meta_topic_re = re.compile(r"/devices/([^/]*)/controls/([^/]*)/meta/([^/]*)")
    _control_state_topic_re = re.compile(r"/devices/([^/]*)/controls/([^/]*)$")

    _router: MQTTRouter
    _device_registry: WirenBoardDeviceRegistry
    __hass: IHomeAssistant
    _unknown_types: list[str]

    _subscribe_qos: int
    _publish_qos: int
    _publish_retain: bool

    def __init__(self,
                 router: MQTTRouter,
                 registry: WirenBoardDeviceRegistry,
                 hass: IHomeAssistant | None = None,
                 subscribe_qos: int = 1,
                 publish_qos: int = 1,
                 publish_retain: bool = False):
        self._router = router
        self._device_registry = registry
        self._subscribe_qos = subscribe_qos
        self._publish_qos = publish_qos
        self._publish_retain = publish_retain
        if hass is not None:
            self.hass = hass

    @property
    def hass(self) -> IHomeAssistant:
        if self.__hass is None:
            raise RuntimeError("Home Assistant interface is not set up. Call set_hass() first.")
        return self.__hass

    @hass.setter
    def hass(self, value: IHomeAssistant) -> None:
        """Set the Home Assistant interface after initialization.

        Args:
            value: An implementation of IHomeAssistant interface
        """
        self.__hass = value

    def on_connect(self, *args, **kwargs):
        logger.warning(f"connected to MQTT")
        self._router.subscribe('/devices/+/meta/+', self._device_meta_handler, qos=self._subscribe_qos)
        self._router.subscribe('/devices/+/controls/+/meta/+', self._control_meta_handler, qos=self._subscribe_qos)
        self._router.subscribe('/devices/+/controls/+', self._control_state_handler, qos=self._subscribe_qos)

    def _device_meta_handler(self, topic: str, payload: bytes):
        match = self._device_meta_topic_re.match(topic)
        if match is None:
            logger.warning(f'not matched topic={topic} re={self._device_meta_topic_re}')
            return
        device_id, meta_name, meta_value = match.group(1), match.group(2), payload.decode('utf-8')
        device = self._device_registry.get_device(device_id)
        if meta_name == 'name':
            device.name = meta_value
        logger.debug(f'DEVICE META: {device_id} / {meta_name} ==> {meta_value}')

    def _control_meta_handler(self, topic: str, payload: bytes):
        match = self._control_meta_topic_re.match(topic)
        if match is None:
            logger.warning(f'not matched topic={topic} re={self._control_meta_topic_re}')
            return
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
                        logger.warning(f'unknown type for wirenboard control: {meta_value}')
                        self._unknown_types.append(meta_value)
            elif meta_name == 'readonly':
                has_changes |= control.apply_read_only(True if meta_value == '1' else False)
            elif meta_name == 'units':
                has_changes |= control.apply_units(meta_value)
            elif meta_name == 'max':
                has_changes |= control.apply_max(int(meta_value) if meta_value else None)
            if has_changes:
                self.hass.publish_control_config(device, control)

    def _control_state_handler(self, topic: str, payload: bytes):
        match = self._control_state_topic_re.match(topic)
        if match is None:
            logger.warning(f'not matched topic={topic} re={self._control_state_topic_re}')
            return
        device_id, control_id, control_state = match.group(1), match.group(2), payload.decode('utf-8')

        # Обработка специальных контролов.
        # В mqtt в wb системная информация зарегана под устройством system.
        # Вытаскиваем из system максимум информации, при этом не регаем его как отдельный контрол.
        # Конкретно тут пытаемся обогатить данными существующие девайсы.
        if device_id == 'system':
            if self.process_system_control(device_id, control_id, control_state):
                return
        normilized_control_id = control_id.lower().replace(" ", "_")
        if normilized_control_id == 'serial':
            device = self._device_registry.get_device(device_id)
            device.serial_number = control_state
            self.hass.publish_device_config(device)
            return
        device = self._device_registry.get_device(device_id)
        control = device.get_control(control_id)
        control.state = control_state
        self.hass.publish_control_state(device, control)

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
        self.hass.publish_device_config(device)
        return True

    def on_control_set_state(self, device_id: str, control_id: str, control_state: str):
        self._router.publish(f"/devices/{device_id}/controls/{control_id}/on", control_state, qos=self._publish_qos, retain=self._publish_retain)

_known_system_controls = ['hw_revision', 'short_sn', 'release_name']