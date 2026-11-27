# MQTT.js against MqttWire's server

[MQTT.js](https://github.com/mqttjs/MQTT.js) is the second independent MQTT 5 implementation that
checks MqttWire's server (design 6.13 item 7); MQTTnet, in `tests/MqttWire.Interop.Tests`, is the
first. The tests connect over WebSockets to `tests/MqttWire.TestHost`, a small host with an
exact-topic fan-out handler, which `host.mjs` starts as a process of its own on a free port.

They cover QoS 0, 1 and 2 in both directions, the CONNACK properties, a refused user name
(`0x86`), a takeover (`0x8E`), a publish over the server's limit (`0x97`, and the connection
stays open), a payload of 200,000 bytes, two hundred QoS 2 publishes in a row, and the host
stopping (`0x8B`).

## Running them

Node 24 or later and the .NET 11 SDK are needed, and the .NET 10 runtime with ASP.NET Core to run
the host on .NET 10. From the repository's root:

```
dotnet build tests/MqttWire.TestHost -c Release
cd tests/interop-js
npm ci
npm test
```

The host must be built first: `host.mjs` runs it with `--no-build`. The build is for both targets,
and the tests run against one of them: the `net11.0` host unless the environment variable
`MQTTWIRE_TEST_FRAMEWORK` says `net10.0`. CI runs the same steps once for each target, in the
`interop-js` job of `.github/workflows/ci.yml`.

## The pinned version

| Package | Version |
|---|---|
| `mqtt` (MQTT.js) | 5.16.1 |

The version is exact in `package.json`, and `package-lock.json` pins everything below it.
Raising it is a deliberate change (design 6.15): change the version, run `npm install`, run the
tests, and update this table.
