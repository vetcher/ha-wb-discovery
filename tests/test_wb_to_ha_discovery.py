import asyncio
from itertools import zip_longest
import json
import os
import sys
import pytest
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ha_wb_discovery.mqtt_conn.local_mqtt import LocalMQTTClient
from ha_wb_discovery.homeassistant import HomeAssistantDiscoveryCustomizer
from ha_wb_discovery.app import App
from ha_wb_discovery.config import config_schema_builder

logging.basicConfig(level=logging.DEBUG)

def get_test_dirs():
    test_dir = os.path.join(os.path.dirname(__file__), 'testdata')
    return [os.path.join(test_dir, d) for d in os.listdir(test_dir)]

@pytest.mark.parametrize('wb_input_dir', get_test_dirs())
def test_wb_ha_discovery_matrix_files(wb_input_dir):
    wb_input_file = os.path.join(wb_input_dir, 'wb.input.txt')
    ha_input_file = wb_input_file.replace('wb.input.txt', 'ha.input.txt')
    wb_output_file = wb_input_file.replace('wb.input.txt', 'wb.output.txt')
    ha_output_file = wb_input_file.replace('wb.input.txt', 'ha.output.txt')
    wb_golden_file = wb_input_file.replace('wb.input.txt', 'wb.golden.txt')
    ha_golden_file = wb_input_file.replace('wb.input.txt', 'ha.golden.txt')
    options_file = wb_input_file.replace('wb.input.txt', 'options.json')

    assert os.path.exists(ha_input_file)
    assert os.path.exists(wb_golden_file)
    assert os.path.exists(ha_golden_file)

    options = {
        "homeassistant": {'broker_host': 'localhost', 'broker_port': 1883, 'config_first_publish_delay': 0},
        "wirenboard": {'broker_host': 'localhost', 'broker_port': 1883},
    }
    if os.path.exists(options_file):
        with open(options_file, 'r') as f:
            options = json.load(f)
    cfg = config_schema_builder({})(options)

    if os.path.exists(wb_output_file):
        os.remove(wb_output_file)
    if os.path.exists(ha_output_file):
        os.remove(ha_output_file)

    # Create new MQTT client for each test file
    wb_mqtt_client = LocalMQTTClient(wb_input_file, wb_output_file)
    ha_mqtt_client = LocalMQTTClient(ha_input_file, ha_output_file)
    app = App(
        cfg["homeassistant"],
        cfg["wirenboard"],
        ha_mqtt_client, wb_mqtt_client,
        HomeAssistantDiscoveryCustomizer(
            ignored_device_ids=cfg.get("homeassistant.ignored_device_ids", []),
            ignored_device_control_ids=cfg.get("homeassistant.ignored_device_control_ids", []),
            splitted_device_ids=cfg.get("homeassistant.splitted_device_ids", []),
            combined_devices=cfg.get("homeassistant.combined_devices", []),
            enable_default_combined_devices=cfg.get("homeassistant.enable_default_combined_devices", True),
        ),
    )

    completed = 0
    async def on_disconnect(a, b):
        nonlocal completed
        completed += 1
        if completed == 2:
            await app.stop()

    wb_mqtt_client.on_disconnect = on_disconnect
    ha_mqtt_client.on_disconnect = on_disconnect

    # Run the main test with this client
    asyncio.run(wb_ha_discovery_matrix_test(app))

    def compare_output_with_golden(output_file, golden_file):
        with open(output_file) as out_f, open(golden_file) as gold_f:
            output_content = out_f.readlines()
            golden_content = gold_f.readlines()

            printed_lines = 0
            diff_lines = 0
            if len(output_content) != len(golden_content):
                print(f"Output len {output_file}: {len(output_content)}")
                print(f"Golden len {golden_file}: {len(golden_content)}")
            for i, (out_line, gold_line) in enumerate(zip_longest(output_content, golden_content, fillvalue='')):
                if out_line != gold_line:
                    if printed_lines < 10:
                        print(f"Line {i+1}:")
                        print(f"Output: {out_line.rstrip()}")
                        print(f"Golden: {gold_line.rstrip()}")
                        printed_lines += 1
                    diff_lines += 1
            if diff_lines > printed_lines:
                print(f"and {diff_lines - printed_lines} more lines")
            if diff_lines > 0:
                pytest.fail(f"{output_file} does not match {golden_file} file, diff lines: {diff_lines}")

    compare_output_with_golden(wb_output_file, wb_golden_file)
    if os.path.exists(wb_output_file):
        os.remove(wb_output_file)

    compare_output_with_golden(ha_output_file, ha_golden_file)
    if os.path.exists(ha_output_file):
        os.remove(ha_output_file)

async def wb_ha_discovery_matrix_test(app: App):
    await app.run()
