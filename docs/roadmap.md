# Roadmap

Last updated: 2026-11-20

Which release delivers what, for GrainWire and for MqttWire, the MQTT library it will run on.
It records decisions that have been made; it has no dates, and the version numbers after the
first preview are proposals. The designs behind it:

- [MqttWire](specs/2026-11-19-mqttwire-design.md)
- [GrainWire on MqttWire](specs/2026-11-19-grainwire-on-mqttwire-design.md)

Both designs were approved by the owner on 2026-11-19. Nothing below has been built yet, except
GrainWire 0.1.0-preview.1. Step 0 of the order of work below is done.

## MqttWire

MqttWire lives in this repository; its design moved here from GrainWire on 2026-11-20.

| Release | What it delivers |
|---|---|
| **0.1.0-preview.1** | The codec (internal). The client: one connection per instance, any QoS. The opt-in reconnecting client. The connection server, driven by the host through a six-method handler and a state slot per connection. QoS 0, 1 and 2 inside a connection, in both directions. The two outgoing limits, the deadlines, and a bounded shutdown. Shared topics through the host, with the `GroupChat` sample. Interop tests with MQTTnet and MQTT.js. The benchmark, and the document that lists the part of MQTT 5 that is implemented. .NET 10 only. |
| **Later, each needing its own design** | A routing helper: a topic table and fan-out on top of the handler. Retained messages, through the host or that helper. Metrics. Limits across connections. |
| **Only if a measurement asks for it** | Own WebSocket framing, if what Kestrel and .NET's WebSocket cost per connection turns out to be most of the total. |
| **After Unity 7** | A .NET Standard 2.1 build of the client. |
| **1.0** | The public API is frozen. Not before GrainWire has run on MqttWire and the memory target is met. |

Not planned: a raw TCP endpoint, MQTT 3.x, stored sessions, Will messages, topic aliases, shared
subscriptions, subscription identifiers, enhanced authentication.

## GrainWire

| Release | What it delivers |
|---|---|
| **0.1.0-preview.1** (built, not published) | What exists today, on MQTTnet: rooms on Orleans grains, the gateway, the TypeScript and .NET clients, the chess sample. See [its changelog](https://github.com/mo-esmp/grainwire/blob/main/CHANGELOG.md). |
| **0.1.0-preview.2** | The gateway and the .NET client on MqttWire; no MQTTnet in the packages. New options: `MaxOutgoingBytes` and `MaxQoS` on the gateway; `ConfigureWebSocket`, `KeepAlive`, `CommandQoS` and `InboxQoS` on the .NET client; `commandQos` and `inboxQos` on the TypeScript client. The changes in behaviour listed in section 5.8 of the design. New benchmark results, with the memory an idle connection costs. |
| **Later, each needing its own design** | Message retention: stored room history, so that a client that comes back gets the messages it missed and not only the current state. Passing the WebSocket request's `HttpContext` to authenticators. An automated browser test of the Blazor client. Metrics. |
| **After Unity 7** | A client that works in Unity. |

## Open decisions (the owner's)

- **Whether 0.1.0-preview.1 is ever published on MQTTnet.** The design assumes that nothing has
  been published, so the move to MqttWire needs no deprecation step. The first public preview
  could already be the one on MqttWire.
- **What 1.0 requires** for each package.
- **Dates.**

## Order of work for the next releases

From section 7 of [GrainWire on MqttWire](specs/2026-11-19-grainwire-on-mqttwire-design.md). The
two repositories are built together, a part at a time.

| Step | Where | What |
|---|---|---|
| 0 | GrainWire | Measure what an idle connection costs today and what the floor is; report to the owner. **Done** on 2026-11-20: the floor is 35% of today, and the owner set the GrainWire target to floor + 3 KB ([the report](https://github.com/mo-esmp/grainwire/blob/main/docs/step-0-idle-memory.md)). |
| 1 | MqttWire | The repository, the codec and its tests. Planned in [docs/plans/2026-11-20-step-1-codec-plan.md](plans/2026-11-20-step-1-codec-plan.md), tracked in #1. |
| 2 | MqttWire | The client and the reconnecting client. |
| 3 | GrainWire | `GrainWire.Client` moves to MqttWire's client. |
| 4 | MqttWire, then GrainWire | MqttWire's server, sample and benchmark; then the gateway becomes its host. |
| 5 | GrainWire | Measure again and write up. |
