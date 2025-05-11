#!/usr/bin/with-contenv bashio

SERVICES=$(bashio::api.supervisor GET "/services")
# Check if MQTT service is available
if echo "$SERVICES" | jq -e '.services[] | select(.slug == "mqtt" and .available == false)' > /dev/null; then
    bashio::log.error "MQTT service is not available"
    bashio::log.error "$SERVICES"
    bashio::log.error "Possible solutions: install MQTT addon (mosquitto broker) or restart Home Assistant"
    bashio::log.error "To setup MQTT follow https://www.home-assistant.io/integrations/mqtt/"
    exit 1
fi

MQTT_HOST=$(bashio::services mqtt "host")
MQTT_PORT=$(bashio::services mqtt "port")
MQTT_USER=$(bashio::services mqtt "username")
MQTT_PASSWORD=$(bashio::services mqtt "password")

python3 ha-wb-discovery.py \
    --config /data/options.json \
    --ha_mqtt_host=$MQTT_HOST \
    --ha_mqtt_port=$MQTT_PORT \
    --ha_mqtt_username=$MQTT_USER \
    --ha_mqtt_password=$MQTT_PASSWORD