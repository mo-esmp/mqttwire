# Roadmap

Last updated: 2026-11-27

Which release delivers what, for GrainWire and for MqttWire, the MQTT library it will run on.
It records decisions that have been made; it has no dates, and the version numbers after the
first preview are proposals. The designs behind it:

- [MqttWire](specs/2026-11-19-mqttwire-design.md)
- [GrainWire on MqttWire](specs/2026-11-19-grainwire-on-mqttwire-design.md)

Both designs were approved by the owner on 2026-11-19. Of the work below, GrainWire 0.1.0-preview.1
exists, and steps 0 to 4 of the order of work are done: MqttWire is finished for its first
preview, and `MqttWire` and `MqttWire.Server` 0.1.0-preview.2 are on nuget.org since 2026-11-26.
Step 5, the first of GrainWire's, is next. On 2026-11-21 the
owner decided that MqttWire is finished first, and GrainWire moves to it afterwards.

## MqttWire

MqttWire lives in this repository; its design moved here from GrainWire on 2026-11-20.

| Release | What it delivers |
|---|---|
| **0.1.0-preview.1** (tagged `v0.1.0-preview.1` on 2026-11-25; never published on nuget.org; [what changed](../CHANGELOG.md#010-preview1-2026-11-23)) | The codec (internal). The client: one connection per instance, any QoS. The opt-in reconnecting client. The connection server, driven by the host through a six-method handler and a state slot per connection. QoS 0, 1 and 2 inside a connection, in both directions. The two outgoing limits, the deadlines, and a bounded shutdown. Shared topics through the host, with the `GroupChat` sample. Interop tests with MQTTnet and MQTT.js. The benchmark, and the document that lists the part of MQTT 5 that is implemented. .NET 10 only. |
| **0.1.0-preview.2** (published on nuget.org on 2026-11-26, by CI from the tag `v0.1.0-preview.2` through Trusted Publishing; [what changed](../CHANGELOG.md#010-preview2-2026-11-26)) | The first release on nuget.org: everything in 0.1.0-preview.1, and a client that writes the acknowledgements it owes together in one WebSocket message (#12) and whose sends no longer hold up its reading or allocate while they wait for another write (#13). |
| **Later, each needing its own design** | A routing helper: a topic table and fan-out on top of the handler (#36). Retained messages, through the host or that helper (#37). The properties of a PUBLISH, passed to the host and the client (#38). Metrics (#39). Limits across connections (#40). |
| **Only if a measurement asks for it** | Own WebSocket framing, if what Kestrel and .NET's WebSocket cost per connection turns out to be most of the total (#41). |
| **After Unity 7** | A .NET Standard 2.1 build of the client (#42). |
| **1.0** | The public API is frozen. Not before GrainWire has run on MqttWire and the memory target is met. |

Not planned: a raw TCP endpoint, MQTT 3.x, Will messages, topic aliases, Native AOT and trimming
annotations (the owner, 2026-11-27; #29). Also not planned, but each with an issue that holds
the case for it, should the owner change that decision: stored sessions (#43), shared
subscriptions as a server feature (#44), subscription identifiers (#45) and enhanced
authentication (#46).

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

From section 7 of [GrainWire on MqttWire](specs/2026-11-19-grainwire-on-mqttwire-design.md), in
the order the owner set on 2026-11-21: **MqttWire is finished first**, its server, sample,
benchmark and preview package, and GrainWire moves to it afterwards. The design built the two
repositories together, a part at a time, so that GrainWire's tests would exercise MqttWire
early; MqttWire's own interop tests and sample now carry that until step 5. Steps 3 to 7 are
numbered in the new order; the MqttWire steps keep the numbers of section 7 of the MqttWire
design.

| Step | Where | What |
|---|---|---|
| 0 | GrainWire | Measure what an idle connection costs today and what the floor is; report to the owner. **Done** on 2026-11-20: the floor is 35% of today, and the owner set the GrainWire target to floor + 3 KB ([the report](https://github.com/mo-esmp/grainwire/blob/main/docs/step-0-idle-memory.md)). |
| 1 | MqttWire | The repository, the codec and its tests. Planned in [docs/plans/2026-11-20-step-1-codec-plan.md](plans/2026-11-20-step-1-codec-plan.md), tracked in #1. **Done**: merged in #2. |
| 2 | MqttWire | The client and the reconnecting client. Planned in [docs/plans/2026-11-20-step-2-client-plan.md](plans/2026-11-20-step-2-client-plan.md), tracked in #3; the local package `0.1.0-dev.2` was the hand-over to GrainWire's move, now step 5. **Done**: merged in #4. |
| 3 | MqttWire | The server, its tests, the interops with MQTTnet and MQTT.js, the `GroupChat` sample and the benchmark. Planned in [docs/plans/2026-11-22-step-3-server-plan.md](plans/2026-11-22-step-3-server-plan.md), tracked in #6. **Done** in two pull requests: the server, its tests and the .NET interops in #9; MQTT.js, the sample and the benchmark in #10. The benchmark measured 519 to 624 bytes above the floor for an idle connection; on 2026-11-23 the owner decided that the target of floor + 1 KB is judged on MqttWire's own state, which meets it before and after activity ([the report](bench/2026-11-idle-memory.md)). |
| 4 | MqttWire | The preview package `0.1.0-preview.1` of both packages, with [docs/mqtt-support.md](mqtt-support.md). Tracked in #7, which was its plan. **Done** in #11: both packages packed, restored from a clean feed into a project outside the repository, and the `GroupChat` sample run on them. The tag `v0.1.0-preview.1` was never published; the first release on nuget.org is `0.1.0-preview.2` (#30, on 2026-11-26), which CI publishes from its tag. |
| 5 | GrainWire | `GrainWire.Client` moves to MqttWire's client, restored as the package `MqttWire` `0.1.0-preview.2` from nuget.org. Tracked in [mo-esmp/grainwire#8](https://github.com/mo-esmp/grainwire/issues/8). **Next.** |
| 6 | GrainWire | The gateway becomes MqttWire's host, `ScriptedGateway` is rebuilt on MqttWire's server, and MQTTnet's server packages leave GrainWire. |
| 7 | GrainWire | Measure again and write up. |
