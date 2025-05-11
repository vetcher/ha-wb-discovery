import logging
from typing import Callable, Protocol

from ha_wb_discovery.mappers import WirenControlType

logger = logging.getLogger(__name__)

class WirenControl:
    id: str
    type: WirenControlType | None = None
    read_only: bool | None
    error: bool | None
    units: str | None
    max: float | None
    state: str | None
    device_id: str

    def __init__(self, device_id: str, control_id: str):
        self.id = control_id
        self.device_id = device_id
        self.read_only = None
        self.error = None
        self.units = None
        self.max = None
        self.state = None

    @property
    def debug_id(self):
        return self.id.lower().replace(" ", "_").replace("-", "_")

    def apply_type(self, t: WirenControlType):
        if self.type == t:
            return False
        else:
            self.type = t
            return True

    def apply_read_only(self, read_only: bool):
        if self.read_only == read_only:
            return False
        else:
            self.read_only = read_only
            return True

    def apply_error(self, error: bool):
        if self.error == error:
            return False
        else:
            self.error = error
            return True

    def apply_units(self, units: str):
        if self.units == units:
            return False
        else:
            self.units = units
            return True

    def apply_max(self, max: int | None):
        if self.max == max:
            return False
        else:
            self.max = max
            return True

    def __str__(self) -> str:
        return f'Control [{self.id}] type: {self.type}, units: {self.units}, read_only: {self.read_only}, error: {self.error}, max: {self.max}, state: {self.state}'

class WirenDevice:
    device_id: str
    _name: str
    manufactorer: str | None = None
    model: str | None = None
    hw_version: str | None = None
    sw_version: str | None = None
    serial_number: str | None = None
    _controls: dict[str, WirenControl]

    def __init__(self, device_id):
        self.device_id = device_id
        self.manufactorer = 'Wiren Board'
        self._controls = {}

    @property
    def debug_id(self):
        return self.device_id.lower().replace(" ", "_").replace("-", "_")

    @property
    def controls(self):
        return self._controls

    def get_control(self, control_id) -> WirenControl:
        if control_id not in self._controls.keys():
            self._controls[control_id] = WirenControl(self.device_id, control_id)
            logger.debug(f'{self}: new control: {control_id}')
        return self._controls[control_id]

    def __str__(self) -> str:
        return f'Device [{self.device_id}] {self.name}'

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = 'Wiren Board ' + name

class WirenBoardDeviceRegistry:
    _wb_devices: dict[str, WirenDevice]

    def __init__(self):
        self._wb_devices = {}

    def devices(self):
        return self._wb_devices

    def get_device(self, device_id: str) -> WirenDevice:
        if self._wb_devices.get(device_id) is None:
            self._wb_devices[device_id] = WirenDevice(device_id)
            logger.debug(f'New device: {device_id}')

        return self._wb_devices[device_id]
