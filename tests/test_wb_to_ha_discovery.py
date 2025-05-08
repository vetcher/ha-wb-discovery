import asyncio
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from ha_wb_discovery.mqtt_conn.local_mqtt import LocalMQTTClient
from ha_wb_discovery.mqtt_conn.mqtt_client import MQTTRouter
import glob
from ha_wb_discovery.app import App
import logging

logging.basicConfig(level=logging.DEBUG)

def get_test_files():
    test_dir = os.path.join(os.path.dirname(__file__), 'testdata')
    return glob.glob(os.path.join(test_dir, '*.wb.input.txt'))

@pytest.mark.parametrize('wb_input_file', get_test_files())
def test_wb_ha_discovery_matrix_files(wb_input_file):
    ha_input_file = wb_input_file.replace('.wb.input.txt', '.ha.input.txt')
    wb_output_file = wb_input_file.replace('.wb.input.txt', '.wb.output.txt')
    ha_output_file = wb_input_file.replace('.wb.input.txt', '.ha.output.txt')
    wb_golden_file = wb_input_file.replace('.wb.input.txt', '.wb.golden.txt')
    ha_golden_file = wb_input_file.replace('.wb.input.txt', '.ha.golden.txt')

    assert os.path.exists(ha_input_file)
    assert os.path.exists(wb_golden_file)
    assert os.path.exists(ha_golden_file)

    if os.path.exists(wb_output_file):
        os.remove(wb_output_file)
    if os.path.exists(ha_output_file):
        os.remove(ha_output_file)

    # Create new MQTT client for each test file
    wb_mqtt_client = LocalMQTTClient(wb_input_file, wb_output_file)
    ha_mqtt_client = LocalMQTTClient(ha_input_file, ha_output_file)
    app = App(
        {'broker_host': 'localhost', 'broker_port': 1883},
        {'broker_host': 'localhost', 'broker_port': 1883},
        ha_mqtt_client, wb_mqtt_client,
        [], []
    )

    async def on_disconnect(a, b):
        print('on_disconnect')
        await app.stop()
    wb_mqtt_client.on_disconnect = on_disconnect
    ha_mqtt_client.on_disconnect = on_disconnect

    # Run the main test with this client
    wb_ha_discovery_matrix_test(app)

    def compare_output_with_golden(output_file, golden_file):
        if os.path.exists(output_file) and os.path.exists(golden_file):
            with open(output_file, 'r') as out_f, open(golden_file, 'r') as gold_f:
                output_content = out_f.readlines()
                output_content.sort()
                golden_content = gold_f.readlines()
                golden_content.sort()

                if output_content != golden_content:
                    print(f"\nDifferences found in {os.path.basename(output_file)}:")
                    for i, (out_line, gold_line) in enumerate(zip(output_content, golden_content)):
                        if out_line != gold_line:
                            print(f"Line {i+1}:")
                            print(f"Output: {out_line.rstrip()}")
                            print(f"Golden: {gold_line.rstrip()}")
                            print()
                    pytest.fail("Output does not match golden file")

    compare_output_with_golden(wb_output_file, wb_golden_file)
    compare_output_with_golden(ha_output_file, ha_golden_file)

    # Clean up output file after test
    if os.path.exists(wb_output_file):
        os.remove(wb_output_file)
    if os.path.exists(ha_output_file):
        os.remove(ha_output_file)

def wb_ha_discovery_matrix_test(app: App):
    asyncio.run(app.run())
