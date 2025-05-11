import logging
from enum import unique, Enum

logger = logging.getLogger(__name__)


@unique
class WirenControlType(Enum):
    """
    Wirenboard controls types
    Based on https://github.com/wirenboard/homeui/blob/master/conventions.md
    """
    # generic types
    switch = "switch"
    alarm = "alarm"
    pushbutton = "pushbutton"
    range = "range"
    rgb = "rgb"
    text = "text"
    value = "value"

    # special types
    temperature = "temperature"
    rel_humidity = "rel_humidity"
    atmospheric_pressure = "atmospheric_pressure"
    rainfall = "rainfall"
    wind_speed = "wind_speed"
    power = "power"
    power_consumption = "power_consumption"
    voltage = "voltage"
    water_flow = "water_flow"
    water_consumption = "water_consumption"
    resistance = "resistance"
    concentration = "concentration"
    heat_power = "heat_power"
    heat_energy = "heat_energy"

    # custom types
    current = "current"


WIREN_UNITS_DICT = {
    WirenControlType.temperature: '°C',
    WirenControlType.rel_humidity: '%',
    WirenControlType.atmospheric_pressure: 'millibar',
    WirenControlType.rainfall: 'mm per hour',
    WirenControlType.wind_speed: 'm/s',
    WirenControlType.power: 'watt',
    WirenControlType.power_consumption: 'kWh',
    WirenControlType.voltage: 'V',
    WirenControlType.water_flow: 'm³/hour',
    WirenControlType.water_consumption: 'm³',
    WirenControlType.resistance: 'Ohm',
    WirenControlType.concentration: 'ppm',
    WirenControlType.heat_power: 'Gcal/hour',
    WirenControlType.heat_energy: 'Gcal',
    WirenControlType.current: 'A',
}

@unique
class HassControlType(Enum):
    binary_sensor = "binary_sensor"
    sensor = "sensor"
    switch = "switch"
    button = "button"

_WIREN_TO_HASS_MAPPER: dict[WirenControlType, HassControlType | None] = {
    WirenControlType.switch: None,  # see wirenboard_to_hass_type()
    WirenControlType.alarm: HassControlType.binary_sensor   ,
    WirenControlType.pushbutton: HassControlType.button,
    WirenControlType.range: None,  # see wirenboard_to_hass_type()
    # WirenControlType.rgb: 'light', #TODO: add
    WirenControlType.text: HassControlType.sensor,
    WirenControlType.value: HassControlType.sensor,

    WirenControlType.temperature: HassControlType.sensor,
    WirenControlType.rel_humidity: HassControlType.sensor,
    WirenControlType.atmospheric_pressure: HassControlType.sensor,
    WirenControlType.rainfall: HassControlType.sensor,
    WirenControlType.wind_speed: HassControlType.sensor,
    WirenControlType.power: HassControlType.sensor,
    WirenControlType.power_consumption: HassControlType.sensor,
    WirenControlType.voltage: HassControlType.sensor,
    WirenControlType.water_flow: HassControlType.sensor,
    WirenControlType.water_consumption: HassControlType.sensor,
    WirenControlType.resistance: HassControlType.sensor,
    WirenControlType.concentration: HassControlType.sensor,
    WirenControlType.heat_power: HassControlType.sensor,
    WirenControlType.heat_energy: HassControlType.sensor,

    WirenControlType.current: HassControlType.sensor,
}

def wiren_to_hass_type(control) -> HassControlType | None:
    if control.type == None:
        # We doesnt read yet mqtt topic .../type, so skip converting until async loop completes.
        # On next read this function would be called again and type would not be empty.
        return None
    if control.type == WirenControlType.switch:
        return HassControlType.binary_sensor if control.read_only else HassControlType.switch
    elif control.type == WirenControlType.range:
        # return 'sensor' if control.read_only else 'light'
        # return 'sensor' if control.read_only else 'cover'
        return HassControlType.sensor if control.read_only else None
    elif control.type in _WIREN_TO_HASS_MAPPER:
        return _WIREN_TO_HASS_MAPPER[control.type]
    return None
