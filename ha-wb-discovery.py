import asyncio
import json
import optparse
import logging
import signal
import yaml
from voluptuous import MultipleInvalid

from ha_wb_discovery.config import config_schema_builder, LOGLEVEL_MAPPER
from ha_wb_discovery.homeassistant import HomeAssistantDiscoveryCustomizer
from gmqtt.client import Client as MQTTClient
from ha_wb_discovery.app import App

logging.getLogger().setLevel(logging.INFO)  # root

logger = logging.getLogger(__name__)

def main(cfg):
    logging.basicConfig(
        level=LOGLEVEL_MAPPER[cfg["general.loglevel"]],
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("gmqtt").setLevel(LOGLEVEL_MAPPER[cfg["mqtt.loglevel"]])

    wb_cfg = cfg["wirenboard"]
    ha_cfg = cfg["homeassistant"] if "homeassistant" in cfg else {}

    logger.info("Starting")
    wb_mqtt_client = MQTTClient(client_id=wb_cfg["mqtt_client_id"])
    if wb_cfg.get('username') and wb_cfg.get('password'):
        wb_mqtt_client.set_auth_credentials(
            wb_cfg["username"],
            wb_cfg["password"]
        )
    ha_mqtt_client = MQTTClient(client_id=ha_cfg["mqtt_client_id"])
    if ha_cfg.get("username") and ha_cfg.get("password"):
        ha_mqtt_client.set_auth_credentials(
            ha_cfg["username"],
            ha_cfg["password"]
        )
    ha_customizer = HomeAssistantDiscoveryCustomizer(
        splitted_device_ids=cfg["homeassistant.splitted_device_ids"],
        combined_devices=cfg["homeassistant.combined_devices"],
        ignored_device_ids=cfg["homeassistant.ignored_device_ids"],
        ignored_device_control_ids=cfg["homeassistant.ignored_device_control_ids"],
        enable_default_combined_devices=cfg["homeassistant.enable_default_combined_devices"],
    )
    app = App(ha_cfg, wb_cfg, ha_mqtt_client, wb_mqtt_client, ha_customizer)

    loop = asyncio.get_event_loop()

    def stop_app():
        loop.create_task(app.stop())

    loop.add_signal_handler(signal.SIGINT, stop_app)
    loop.add_signal_handler(signal.SIGTERM, stop_app)

    loop.run_until_complete(app.run())

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option("-c", "--config", default="/etc/ha-wb-discovery.yaml", dest="config_file", help="Path to config file")
    parser.add_option("--ha_mqtt_host", default="", dest="ha_mqtt_host", help="HA MQTT host")
    parser.add_option("--ha_mqtt_port", type=int, default=1883, dest="ha_mqtt_port", help="HA MQTT port")
    parser.add_option("--ha_mqtt_username", default="", dest="ha_mqtt_username", help="HA MQTT username")
    parser.add_option("--ha_mqtt_password", default="", dest="ha_mqtt_password", help="HA MQTT password")
    opts, args = parser.parse_args()

    config_file = opts.config_file
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

    try:
        config = config_schema_builder(vars(opts))(config)
    except MultipleInvalid as e:
        logger.error(f"Config validation error: {e}")
        exit(1)

    main(config)