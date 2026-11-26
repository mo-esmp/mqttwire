# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.x (latest pre-release) | Yes |
| Earlier pre-releases | No |

`MqttWire` and `MqttWire.Server` always ship together at one version. A fix is released in a new
pre-release of both; it is not backported to an earlier one.

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly:

1. **Do not** open a public issue, discussion or pull request.
2. Use [GitHub's private vulnerability reporting](https://github.com/mo-esmp/mqttwire/security/advisories/new).
3. Include a description of the vulnerability, the version and the side it is on (the client,
   the reconnecting client or the server), steps to reproduce (the bytes a peer sends are the
   most useful), and its potential impact.

We will acknowledge receipt within 48 hours and aim to release a fix within 7 days for critical
issues. Once a fix is released, the advisory is published with credit to the reporter, unless
they ask not to be named.

## Security Model

MqttWire is a connection server, not a broker. It handles the MQTT 5 protocol for each
connection and leaves every decision about trust to the host:

- **The host authenticates and authorizes.** Every connection passes through
  `IMqttWireHandler.OnConnectAsync`, which sees the user name, the password, the user properties
  and the `HttpContext` of the WebSocket request. Every subscription and every publish passes
  through the handler before anything happens. The result types fail closed: `default` is a
  refusal with `0x87` (not authorized), and a handler method that throws closes its connection
  with `0x83`.
- **Malformed input is never an exception in the host.** A packet that does not parse is
  answered with a reason code and the end of the connection.
- **Every connection is bounded in bytes and in time.** Packet sizes, the payload of a publish,
  the messages and bytes that may wait for a client, and the acknowledgements waiting to be
  written all have limits, and a client that exceeds one is disconnected. Fixed deadlines end a
  connection that does not send its CONNECT, does not finish a packet, falls silent, stops
  reading or stops acknowledging. The client bounds the packets it accepts in the same way. The
  limits and deadlines are listed in
  [docs/mqtt-support.md](docs/mqtt-support.md), sections 6 and 7.
- **Nothing is stored.** There are no sessions, retained messages or queued messages for a
  client that is away: what a connection holds ends with it.
- **No dependencies.** The packages depend on nothing beyond .NET and ASP.NET Core.

What MqttWire leaves to the host, and is not a vulnerability in MqttWire:

- **Transport security.** MqttWire runs over the WebSocket that ASP.NET Core accepts and adds no
  encryption of its own. The password in a CONNECT is sent as it is: serve the endpoint over
  HTTPS (`wss://`) only.
- **Limits across connections.** MqttWire limits each connection, not how many there are or how
  fast they arrive. Use Kestrel's limits (`MaxConcurrentUpgradedConnections`), ASP.NET Core rate
  limiting or a proxy in front.
- **The origin of browser requests.** MqttWire does not check the `Origin` header. A host that
  authenticates with cookies should check it in `OnConnectAsync`, through the request's
  `HttpContext`.
- **What the handler does** with topics, payloads and the identities it accepts.
