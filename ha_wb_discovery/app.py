import asyncio
import logging
from typing import Union
from ha_wb_discovery.homeassistant import HomeAssistant, HomeAssistantDiscoveryCustomizer
from ha_wb_discovery.mqtt_conn.local_mqtt import LocalMQTTClient
from gmqtt import Client as MQTTClient
from ha_wb_discovery.mqtt_conn.mqtt_client import MQTTRouter
from ha_wb_discovery.wirenboard import Wirenboard
from ha_wb_discovery.wirenboard_registry import WirenBoardDeviceRegistry

logger = logging.getLogger(__name__)

class App:
    _ha_mqtt_router: MQTTRouter
    _wb_mqtt_router: MQTTRouter
    _ha_mqtt_client: Union[LocalMQTTClient, MQTTClient]
    _wb_mqtt_client: Union[LocalMQTTClient, MQTTClient]
    _wb: Wirenboard
    _ha: HomeAssistant
    _ha_config: dict
    _wb_config: dict
    _stoper: asyncio.Event

    def __init__(self,
                ha_config: dict,
                wb_config: dict,
                ha_mqtt_client: Union[LocalMQTTClient, MQTTClient],
                wb_mqtt_client: Union[LocalMQTTClient, MQTTClient],
                ha_customizer: HomeAssistantDiscoveryCustomizer,
                ):
        self._stoper = asyncio.Event()
        assert 'broker_host' in ha_config
        assert 'broker_port' in ha_config
        assert 'broker_host' in wb_config
        assert 'broker_port' in wb_config
        self._ha_config = ha_config
        self._wb_config = wb_config
        self._ha_mqtt_client = ha_mqtt_client
        self._wb_mqtt_client = wb_mqtt_client
        self._ha_mqtt_router = MQTTRouter(self._ha_mqtt_client, 'homeassistant')
        self._wb_mqtt_router = MQTTRouter(self._wb_mqtt_client, 'wirenboard')
        device_registry = WirenBoardDeviceRegistry()
        self._ha = HomeAssistant(
            self._ha_mqtt_router,
            device_registry,
            ha_customizer,
            ha_config.get('config_first_publish_delay', 1),
            ha_config.get('config_publish_delay', 0),
            ha_config.get('subscribe_qos', 1),
            ha_config.get('availability_qos', 1),
            ha_config.get('availability_retain', True),
            ha_config.get('config_qos', 1),
            ha_config.get('config_retain', True),
            ha_config.get('state_qos', 1),
            ha_config.get('state_retain', True),
        )
        self._wb = Wirenboard(
            self._wb_mqtt_router,
            device_registry,
            None, # setup later
            wb_config.get('subscribe_qos', 1),
            wb_config.get('publish_qos', 1),
            wb_config.get('publish_retain', False),
        )
        self._wb.hass = self._ha
        self._ha_mqtt_client.on_connect = self._ha.on_connect
        self._wb_mqtt_client.on_connect = self._wb.on_connect
        self._ha.on_control_set_state = self._wb.on_control_set_state

    async def run(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._connect_mqtt(
                name="wirenboard",
                client=self._wb_mqtt_client,
                host=self._wb_config['broker_host'],
                port=self._wb_config['broker_port'],
            ))
            tg.create_task(self._connect_mqtt(
                name="homeassistant",
                client=self._ha_mqtt_client,
                host=self._ha_config['broker_host'],
                port=self._ha_config['broker_port'],
            ))
        await self._stoper.wait()
        while True:
            pending = asyncio.all_tasks()
            pending.remove(asyncio.current_task())
            if len(pending) == 0:
                break
            try:
                await asyncio.gather(*pending)
            except asyncio.CancelledError:
                pass

    async def _connect_mqtt(self, name: str, client: Union[LocalMQTTClient, MQTTClient], host: str, port: int):
        # infinite loop of reconnections
        trynum = 0
        while True:
            try:
                await client.connect(host, port)
                logger.info(f"[{name}] connected to MQTT")
                break
            except ConnectionRefusedError as e:
                # backoff
                trynum = min(trynum + 6, 30)
                logger.error(f"[{name}] error connecting to MQTT: {e}; next try in {trynum} seconds")
                await asyncio.sleep(trynum)
            except Exception as e:
                logger.error(f"[{name}] MQTT: error connecting: {e}")
                raise

    async def stop(self):
        logger.info("Stopping app")
        await self._wb_mqtt_client.disconnect()
        await self._ha_mqtt_client.disconnect()
        self._stoper.set()