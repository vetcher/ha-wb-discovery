#!/usr/bin/with-contenv bashio

# debug log for discovering available services
bashio::log.info $(bashio::api.supervisor GET "/services")

MQTT_HOST=$(bashio::services mqtt "host")
MQTT_PORT=$(bashio::services mqtt "port")
MQTT_USER=$(bashio::services mqtt "username")
MQTT_PASSWORD=$(bashio::services mqtt "password")

python3 ha-wb-discovery.py --addon \
    --ha-mqtt.host=$MQTT_HOST \
    --ha-mqtt.port=$MQTT_PORT \
    --ha-mqtt.username=$MQTT_USER \
    --ha-mqtt.password=$MQTT_PASSWORD