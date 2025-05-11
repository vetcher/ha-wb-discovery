# Config parameters

```python
def config_schema_builder(program_args: dict) -> Schema:
    return Schema(
        {
            # Logger level for this addon
            Optional("general.loglevel", default=ConfigLogLevel.INFO): DEBUG | INFO | WARNING | WARN | ERROR | FATAL,
            # Logger level for both MQTT clients: Home Assistant and Wiren Board
            Optional("mqtt.loglevel", default=ConfigLogLevel.ERROR): DEBUG | INFO | WARNING | WARN | ERROR | FATAL,
            # Wiren Board part configuration
            Required("wirenboard"): {
                # Wiren Board MQTT broker host
                Required("broker_host"): str,
                # Wiren Board MQTT broker port
                Optional("broker_port", default=1883): int,
                # Wiren Board MQTT broker username. Pass empty if mqtt without authentication.
                Optional("username"): str,
                # Wiren Board MQTT broker password. Pass empty if mqtt without authentication.
                Optional("password"): str,
                # MQTT client ID, required by MQTT protocol.
                # By default used same client ID for both MQTT clients.
                Required("mqtt_client_id", default="ha-wb-discovery"): str,
                # Wiren Board MQTT subscribe QoS. For more details check MQTT spec.
                Optional("subscribe_qos", default=1): Range(min=0, max=2, msg="Invalid QoS: must be 0, 1 or 2"),
                # Wiren Board MQTT publish QoS. For more details check MQTT spec.
                Optional("publish_qos", default=1): Range(min=0, max=2, msg="Invalid QoS: must be 0, 1 or 2"),
                # Wiren Board MQTT publish retain flag. For more details check MQTT spec.
                Optional("publish_retain", default=False): bool,
            },
            # Home Assistant part configuration
            Required("homeassistant", default={}): {
                # Home Assistant MQTT broker host.
                # When runned as Home Assistant addon, it will be read from Home Assistant supervisor API. So keep it empty.
                Required("broker_host", default = program_args.get("ha_mqtt_host", '')): str,
                # Home Assistant MQTT broker port.
                # When runned as Home Assistant addon, it will be read from Home Assistant supervisor API. So keep it empty.
                Optional("broker_port", default = program_args.get("ha_mqtt_port", 1883)): int,
                # Home Assistant MQTT broker username.
                # When runned as Home Assistant addon, it will be read from Home Assistant supervisor API. So keep it empty.
                # Supervisor will provide exclusive credentials for this addon.
                Optional("username", default = program_args.get("ha_mqtt_username", '')): str,
                # Home Assistant MQTT broker password.
                # When runned as Home Assistant addon, it will be read from Home Assistant supervisor API. So keep it empty.
                # Supervisor will provide exclusive credentials for this addon.
                Optional("password", default = program_args.get("ha_mqtt_password", '')): str,
                # MQTT client ID, required by MQTT protocol.
                # By default used same client ID for both MQTT clients.
                Required("mqtt_client_id", default="ha-wb-discovery"): str,
                # Delay in seconds before first config publish.
                # Parameter is used when device discovered in first time after addon start.
                # To let addon consume all MQTT messages from wirenboard broker and prepare fully filled entities/controls/devices.
                # Also to reduce pushing too much data to Home Assistant after start, delay is used.
                Optional("config_first_publish_delay", default=1): Range(min=0),
                # Delay in seconds before publish device config (and all its controls) to Home Assistant.
                Optional("config_publish_delay", default=0): Range(min=0),
                # Home Assistant MQTT subscribe QoS. For more details check MQTT spec.
                Optional("subscribe_qos", default=1): Range(min=0, max=2, msg="Invalid QoS: must be 0, 1 or 2"),
                # QoS for pushing availability messages to Home Assistant.
                # For more details about QoS check MQTT spec.
                # For more details about availability messages check Home Assistant documentation.
                Optional("availability_qos", default=1): Range(min=0, max=2, msg="Invalid QoS: must be 0, 1 or 2"),
                # Retain flag for pushing availability messages to Home Assistant.
                # For more details about retain flag check MQTT spec.
                # For more details about availability messages check Home Assistant documentation.
                Optional("availability_retain", default=True): bool,
                # QoS for pushing config messages to Home Assistant.
                # For more details about QoS check MQTT spec.
                # For more details about config messages check Home Assistant documentation.
                Optional("config_qos", default=1): Range(min=0, max=2, msg="Invalid QoS: must be 0, 1 or 2"),
                # Retain flag for pushing config messages to Home Assistant.
                # For more details about retain flag check MQTT spec.
                # For more details about config messages check Home Assistant documentation.
                Optional("config_retain", default=True): bool,
                # QoS for pushing state messages to Home Assistant.
                # For more details about QoS check MQTT spec.
                # For more details about state messages check Home Assistant documentation.
                Optional("state_qos", default=1): Range(min=0, max=2, msg="Invalid QoS: must be 0, 1 or 2"),
                # Retain flag for pushing state messages to Home Assistant.
                # For more details about retain flag check MQTT spec.
                # For more details about state messages check Home Assistant documentation.
                Optional("state_retain", default=True): bool,
            },
            # Home Assistant ignored devices configuration.
            #
            # Device ID should be provided in lower case with `-` replaced with `_`.
            # Device ID in Wiren Board MQTT `wb-mr3_16` should be provided as `wb_mr3_16`.
            Optional("homeassistant.ignored_device_ids", default=[]): [str],
            # Home Assistant ignored controls configuration.
            #
            # Device ID should be provided in lower case with `-` replaced with `_`.
            # Device ID in Wiren Board MQTT `wb-mr3_16` control `k1` should be provided as `wb_mr3_16_k1`.
            Optional("homeassistant.ignored_device_control_ids", default=[]): [str],
            # List of device ids in Wiren Board that should be splitted into multiple devices in Home Assistant.
            # After splitting each control in device will be registered as separate device in Home Assistant.
            #
            # Format of new device_id: `device_id`_`control_id`.
            # For example, `wb_mr3_16` with controls `k1`, `k2`, `k3` will be splitted to `wb_mr3_16_k1`, `wb_mr3_16_k2`, `wb_mr3_16_k3` devices.
            #
            # Device ID should be provided in lower case with `-` replaced with `_`.
            # Device ID in Wiren Board MQTT `wb-mr3_16` should be provided as `wb_mr3_16`.
            #
            # This parameter can be used with combined devices.
            Optional("homeassistant.splitted_device_ids", default=[]): [str],
            # List of combined devices that should be combined to one in Home Assistant.
            # Also parameters can be used for remapping.
            #
            # Combining devices does not changes availability, command or state topics.
            #
            # Device ID should be provided in lower case with `-` replaced with `_`.
            # Device ID in Wiren Board MQTT `wb-mr3_16` should be provided as `wb_mr3_16`.
            # new_device_id should be valid unique Home Assistant device id.
            #
            # This parameter can be used with splitted devices.
            # Use splitted device ID as device_id to map control to new device.
            Optional("homeassistant.combined_devices", default=[]): [
                {
                    # Device ID in Wiren Board MQTT.
                    Required("device_id"): str,
                    # New device ID in Home Assistant.
                    Required("new_device_id"): str,
                    # New displayed device name in Home Assistant.
                    Required("new_name"): str,
                }
            ],
            # Enable default combined devices in Home Assistant.
            # Default combined devices:
            # - `wb_adc` -> `wirenboard`
            # - `wbrules` -> `wirenboard`
            # - `wb_gpio` -> `wirenboard`
            # - `power_status` -> `wirenboard`
            # - `network` -> `wirenboard`
            # - `system` -> `wirenboard`
            # - `hwmon` -> `wirenboard`
            # - `buzzer` -> `wirenboard`
            # - `alarms` -> `wirenboard`
            # - `metrics` -> `wirenboard`
            Optional("homeassistant.enable_default_combined_devices", default=True): bool,
        }
)
```

# Config examples

```yaml
wirenboard:
  broker_host: localhost
```

[tests/testdata/complex/options.json](https://github.com/vetcher/ha-wb-discovery/blob/main/tests/testdata/complex/options.json)

```yaml
homeassistant.combined_devices:
- device_id: wb_mr3_16_k1
  new_device_id: light_switch_1
  new_name: Light Switch 1 WB-MR3 16
homeassistant.ignored_device_control_ids:
- system_reboot
homeassistant.ignored_device_ids:
- buzzer
- knx
homeassistant.splitted_device_ids:
- wb_mr3_16
wirenboard:
  broker_host: localhost
  broker_port: 1883
```

[tests/testdata/ha-input/options.json](https://github.com/vetcher/ha-wb-discovery/blob/main/tests/testdata/ha-input/options.json)

```yaml
homeassistant.combined_devices:
- device_id: wb_mr3_16_k1
  new_device_id: light_switch_1
  new_name: Light Switch 1 WB-MR3 16
homeassistant.ignored_device_control_ids:
- system_Reboot
homeassistant.ignored_device_ids:
- buzzer
- knx
homeassistant.splitted_device_ids:
- wb_mr3_16
wirenboard:
  broker_host: localhost
  broker_port: 1883
```

# Troubleshooting

### Error: Service not enabled

```
{"services":[{"slug":"mqtt","available":false,"providers":["core_mosquitto"]},{"slug":"mysql","available":false,"providers":[]}]}
[22:40:52] ERROR: Got unexpected response from the API: Service not enabled
[22:40:52] ERROR: Failed to get services from Supervisor API
```

How to fix:

1.  Check MQTT (mosquitto brocker) addon installed. If not - install it.
2.  Restart Home Assistant. MQTT may be installed, but not registered as available mqtt service provider.
