# MQTT test

Small test project to get acquainted with golang mqtt library.

Try it like this:

    ./build.sh
    bin/mqtt-test

Few settings are recognized:

- `MQTT_BROKER` - mqtt broker address, by default `tcp://127.0.0.1:1883`
- `MQTT_CLIENT_ID` - client id for mqtt, defaults to `thermal-gateway`
- `POLL_INTERVAL_SEC` - interval over which batch sensors data are sent (`30` by default)
- `POLL_BATCH_SIZE` - batch size for sensor data (`5` by default)
- `LOG_LEVEL` - set to `warn`, `info` (default) or `debug`
