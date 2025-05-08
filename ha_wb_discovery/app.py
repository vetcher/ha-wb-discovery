import asyncio
import logging
from typing import Union
from ha_wb_discovery.homeassistant import HomeAssistant
from ha_wb_discovery.mqtt_conn.local_mqtt import LocalMQTTClient
from gmqtt import Client as MQTTClient
from ha_wb_discovery.mqtt_conn.mqtt_client import MQTTRouter
from ha_wb_discovery.wirenboard import Wirenboard
from ha_wb_discovery.wirenboard_registry import WirenBoardDeviceRegistry

logger = logging.getLogger(__name__)

class App:
    _ha_mqtt_router: MQTTRouter = None
    _wb_mqtt_router: MQTTRouter = None
    _ha_mqtt_client: Union[LocalMQTTClient, MQTTClient] = None
    _wb_mqtt_client: Union[LocalMQTTClient, MQTTClient] = None
    _wb: Wirenboard = None
    _ha: HomeAssistant = None
    _ha_config: dict = None
    _wb_config: dict = None
    _stoper: asyncio.Event = None

    def __init__(self,
                ha_config: dict,
                wb_config: dict,
                ha_mqtt_client: Union[LocalMQTTClient, MQTTClient],
                wb_mqtt_client: Union[LocalMQTTClient, MQTTClient],
                split_devices: list[str],
                split_entities: list[str]):
        self._stoper = asyncio.Event()
        assert 'broker_host' in ha_config
        assert 'broker_port' in ha_config
        assert 'broker_host' in wb_config
        assert 'broker_port' in wb_config
        self._ha_config = ha_config
        self._wb_config = wb_config
        self._ha_mqtt_client = ha_mqtt_client
        self._wb_mqtt_client = wb_mqtt_client
        self._ha_mqtt_router = MQTTRouter(ha_mqtt_client)
        self._wb_mqtt_router = MQTTRouter(wb_mqtt_client)
        device_registry = WirenBoardDeviceRegistry()
        self._wb = Wirenboard(self._wb_mqtt_router, device_registry)
        self._ha = HomeAssistant(self._ha_mqtt_router, device_registry, split_devices, split_entities)
        wb_mqtt_client.on_connect = self._wb.on_connect
        ha_mqtt_client.on_connect = self._ha.on_connect
        self._wb.on_publish_ha_control_config = self._ha.publish_control_config
        self._wb.on_publish_ha_device_config = self._ha.publish_device_config
        self._wb.on_publish_ha_control_state_config = self._ha.publish_control_state
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
            await asyncio.gather(*pending)

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