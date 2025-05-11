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
