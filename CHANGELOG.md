# Changelog

What changed in each release of the packages `MqttWire` and `MqttWire.Server`. The two always
carry the same version. Versions follow [semantic versioning](https://semver.org); until 1.0
the public API may change between previews, and such a change is listed here as breaking.

## 0.1.0-preview.1 (2026-11-23)

The first release. .NET 10 only.

### MqttWire.Server

- **A connection server for ASP.NET Core, over WebSockets.** It is not a broker: it handles the
  protocol for every connection, and the host decides who connects, what a subscription means
  and where each message goes, through the six methods of `IMqttWireHandler` and a state slot on
  each connection.
- **Registration and mapping:** `AddMqttWireServer<THandler>` and `MapMqttWire`. The four
  options of `MqttWireServerOptions` are checked when the host starts.
- **QoS 0, 1 and 2 in both directions,** as complete exchanges inside one connection.
- **Sending never waits:** `MqttWireConnection.TrySend` returns at once and copies neither the
  topic nor the payload, so one payload goes to any number of connections.
- **Every connection is bounded** in bytes and in time: two outgoing limits that disconnect a
  client that falls behind, a limit on what a client may publish, and fixed deadlines for the
  CONNECT, the host's answer, a partial packet, the keep-alive, a write, an acknowledgement and
  the close.
- **A bounded stop:** when the host stops, every connection is closed with `0x8B` and the
  server waits until all have ended.
- **One log line for the end of every connection,** with its reason.
- **An idle connection holds no buffer, no queue, no timer and no QoS record.** Measured, it
  costs about 600 bytes on top of a bare ASP.NET Core WebSocket
  ([the report](docs/bench/2026-11-idle-memory.md)).

### MqttWire

- **`MqttWireClient`:** an MQTT 5 client over WebSockets. One instance is one connection, at any
  QoS; it never reconnects, never sends anything twice and keeps no session. It works in Blazor
  WebAssembly.
- **`MqttWireReconnectingClient`,** opt-in: a new client for every attempt, with a growing
  delay, fresh options before each attempt, and the subscriptions made through it made again.
- The package has no dependencies.

### Both

- **The part of MQTT 5 that is implemented** is written down packet by packet and property by
  property in [docs/mqtt-support.md](docs/mqtt-support.md), with what is not: routing between
  clients, stored sessions, retained messages, Will messages, topic aliases, shared
  subscriptions, subscription identifiers, enhanced authentication, MQTT 3.x and raw TCP.
- **Tested against** MQTTnet 5.2.0.1603 and MQTT.js 5.16.1, at QoS 0, 1 and 2 in both
  directions.
- **`MqttWire.Server` depends on exactly its own version of `MqttWire`:** it uses the codec
  inside that package.
- A sample, [`samples/GroupChat`](samples/GroupChat): a group chat with a topic per group, on a
  server and a Blazor WebAssembly client.
