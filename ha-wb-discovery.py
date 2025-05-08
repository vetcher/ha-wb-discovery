import asyncio
import json
import optparse
import logging
import os
import signal
from enum import Enum
from sys import argv
import yaml
from voluptuous import Required, Schema, MultipleInvalid, Optional, Coerce

from ha_wb_discovery.homeassistant import HomeAssistantConnector
from ha_wb_discovery.wirenboard import WirenConnector
from gmqtt.client import Client as MQTTClient
from ha_wb_discovery.app import App
from ha_wb_discovery.mqtt_conn.mqtt_client import MQTTRouter

logging.getLogger().setLevel(logging.INFO)  # root

logger = logging.getLogger(__name__)


class ConfigLogLevel(Enum):
    FATAL = "FATAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"


LOGLEVEL_MAPPER = {
    ConfigLogLevel.FATAL: logging.FATAL,
    ConfigLogLevel.ERROR: logging.ERROR,
    ConfigLogLevel.WARNING: logging.WARNING,
    ConfigLogLevel.INFO: logging.INFO,
    ConfigLogLevel.DEBUG: logging.DEBUG,
}


def main(conf):
    logging.basicConfig(
        level=LOGLEVEL_MAPPER[conf["general.loglevel"]],
        format="[%(asctime)s] %(levelname)s:%(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("gmqtt").setLevel(LOGLEVEL_MAPPER[conf["mqtt.loglevel"]])

    wb_conf = conf["wirenboard"]
    ha_conf = conf["homeassistant"] if "homeassistant" in conf else {}

    logger.info("Starting")
    wb_mqtt_client = MQTTClient(client_id=conf["mqtt.client_id"])
    if wb_conf.get('username') and wb_conf.get('password'):
        wb_mqtt_client.set_auth_credentials(
            wb_conf['username'],
            wb_conf['password']
        )
    ha_mqtt_client = MQTTClient(client_id=conf["mqtt.client_id"])
    if ha_conf.get('username') and ha_conf.get('password'):
        ha_mqtt_client.set_auth_credentials(
            ha_conf['username'],
            ha_conf['password']
        )
    app = App(ha_conf, wb_conf, ha_mqtt_client, wb_mqtt_client, [], [])

    loop = asyncio.get_event_loop()

    def stop_app():
        loop.create_task(app.stop())

    loop.add_signal_handler(signal.SIGINT, stop_app)
    loop.add_signal_handler(signal.SIGTERM, stop_app)

    loop.run_until_complete(app.run())


if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option(
        "-c",
        "--config",
        default="/etc/ha-wb-discovery.yaml",
        dest="config_file",
        help="Path to config file",
    )
    parser.add_option(
        "--addon",
        action="store_true",
        default=False,
        dest="is_addon",
        help="Is running as HA addon. When running as addon, config file is read from /data/options.json and --config option is ignored",
    )
    parser.add_option(
        "--ha-mqtt.host", default="", dest="ha_mqtt_host", help="HA MQTT host"
    )
    parser.add_option(
        "--ha-mqtt.port", type=int, default=1883, dest="ha_mqtt_port", help="HA MQTT port"
    )
    parser.add_option(
        "--ha-mqtt.username",
        default="",
        dest="ha_mqtt_username",
        help="HA MQTT username",
    )
    parser.add_option(
        "--ha-mqtt.password",
        default="",
        dest="ha_mqtt_password",
        help="HA MQTT password",
    )
    opts, args = parser.parse_args()
    config_file = opts.config_file
    if opts.is_addon:
        config_file = "/data/options.json"
    logger.info(f"Initializing with config file: {config_file}")
    if not config_file:
        parser.print_help()
        exit(1)

    try:
        with open(config_file) as f:
            config_file_content = f.read()
    except OSError as e:
        logger.error(f'Could not open config file "{config_file}: {e}"')
        exit(1)

    config = None
    if config_file.endswith(".json"):
        config = json.loads(config_file_content)
    elif config_file.endswith(".yaml") or config_file.endswith(".yml"):
        config = yaml.load(config_file_content, Loader=yaml.FullLoader)
    else:
        logger.error(f'Unsupported config file extension: "{config_file}"')
        exit(1)
    if not config:
        logger.error(f'Empty config "{config_file}"')
        exit(1)

    config_schema = Schema(
        {
            Optional("general.loglevel", default=ConfigLogLevel.INFO): Coerce(
                ConfigLogLevel
            ),
            Optional("mqtt.loglevel", default=ConfigLogLevel.ERROR): Coerce(
                ConfigLogLevel
            ),
            Required("mqtt.client_id", default="ha-wb-discovery"): str,
            Required("wirenboard"): {
                Required("broker_host"): str,
                Optional("broker_port", default=1883): int,
                Optional("username"): str,
                Optional("password"): str,
            },
            Required("homeassistant", default={}): {
                # broker_host вообще Required, но пока оставим Optional
                # потому что целевое место запуска - HA аддон.
                Required("broker_host", default=opts.ha_mqtt_host): str,
                Optional("broker_port", default=opts.ha_mqtt_port): int,
                Optional("username", default=opts.ha_mqtt_username): str,
                Optional("password", default=opts.ha_mqtt_password): str,
                Optional("topic_prefix", default=""): str,
                Optional("entity_prefix", default=""): str,
                Optional("discovery_topic", default="homeassistant"): str,
                Optional("status_topic", default="hass/status"): str,
                Optional("status_payload_online", default="online"): str,
                Optional("status_payload_offline", default="offline"): str,
                Optional("debounce", default={}): {
                    Optional("sensor", default=1000): int
                },
                Optional("subscribe_qos", default=0): int,
                Optional("publish_availability", default={}): {
                    Optional("qos", default=0): int,
                    Optional("retain", default=True): bool,
                    Optional("publish_delay", default=1.0): float,
                },
                Optional("publish_state", default={}): {
                    Optional("qos", default=0): int,
                    Optional("retain", default=True): bool,
                },
                Optional("publish_config", default={}): {
                    Optional("qos", default=0): int,
                    Optional("retain", default=False): bool,
                    Optional("publish_delay", default=1.0): float,
                },
                Optional("inverse", default=[]): [str],
                Optional("split_devices", default=[]): [str],
                Optional("split_entities", default=[]): [str],
                Optional("ignore_availability", default=False): bool,
            },
        }
    )

    try:
        config = config_schema(config)
    except MultipleInvalid as e:
        logger.error(f"Config validation error: {e}")
        exit(1)

    main(config)
