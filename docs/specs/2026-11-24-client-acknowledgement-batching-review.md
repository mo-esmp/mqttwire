# Design review — client acknowledgement batching

**24 November 2026 · Recommendation: request changes before implementation.**

The proposal lets the client process available MQTT packets and send several acknowledgement packets in a single WebSocket write. The burst measurements justify pursuing this approach. Three correctness issues need design decisions first: when a read can suspend, how long an acknowledgement can wait under continuous input, and when a delivery releases a Receive Maximum slot. This review examines the proposed behavior against the existing client. The scenarios below are acceptance tests for the future implementation. [Design under review](../specs/2026-11-24-client-acknowledgement-batching-design.md) · [Client connection specification, §6.9](../specs/2026-11-19-mqttwire-design.md)

## 1. P1 — Flush before either input operation suspends

**Failure case.** The client has a PUBACK buffered. The link’s zero-byte WebSocket receive sees the start of the next message, so `WaitForDataAsync` completes synchronously. The peer delays the payload. The subsequent `ReadAsync` can now suspend with the PUBACK still buffered. A peer waiting for that acknowledgement can reach its deadline even though the client was ready to send it. The link explicitly documents this behavior. [WebSocketLink](../../src/MqttWire/Link/WebSocketLink.cs) · [Client read loop](../../src/MqttWire/MqttWireClient.ReadLoop.cs)

**Design change.** Apply the section 5 flush rule to both the readiness wait and the payload read. When either operation is pending, flush outstanding replies before awaiting it. Consume each started operation exactly once and keep any receive memory valid until its read finishes, including when the flush fails and the connection closes.

**Acceptance test.** Use an in-memory link that reports readiness immediately and holds the next payload read open. After delivering a QoS 1 PUBLISH, observe its PUBACK while that read is still pending. Then release the read and verify that the next packet is handled once. Control the sequence with explicit signals so the test needs no timeout to establish ordering.

## 2. P1 — Bound packets processed while a reply is pending

**Failure case.** One QoS 1 PUBLISH creates a pending PUBACK. A stream of immediately available QoS 0 messages follows. Each message invokes the callback, yet none adds a reply. The batch stays at one, and the input never runs dry. Section 5 promises a delay of at most fifteen more packets, but its sixteen-reply threshold cannot enforce that bound. A slow handler makes the delay visible sooner.

**Design change.** Keep the reply-count cap for buffer capacity. To preserve the proposed fifteen-packet bound, also count every subsequent MQTT packet handled while the oldest reply is pending. Flush before processing a sixteenth subsequent packet, even if every intervening packet was QoS 0. State the guarantee in packets: an individual synchronous callback or a stalled transport write can still take arbitrarily long.

**Acceptance test.** Supply one QoS 1 PUBLISH followed by at least sixteen QoS 0 messages in one ready buffer. Record callback and write order. Require the first PUBACK to be written before the sixteenth subsequent callback. A test containing only QoS 1 messages would exercise batch capacity and miss this starvation case.

## 3. P1 — Preserve Receive Maximum until the release packet is sent

**Failure case.** The client advertises Receive Maximum 1,024. Suppose 1,023 distinct inbound QoS 2 exchanges await PUBREL. A QoS 1 PUBLISH takes the last slot and queues its PUBACK. If another new QoS 1 PUBLISH arrives before the batch is written, the current check still sees only 1,023 QoS 2 identifiers. It can admit one delivery too many. A similar gap occurs when handling PUBREL removes a QoS 2 identifier while its PUBCOMP is still buffered. [Receive limit](../../src/MqttWire/Client/ClientLimits.cs) · [Inbound publish and PUBREL handling](../../src/MqttWire/MqttWireClient.Receive.cs) · [MQTT 5.0, §§3.3.4 and 4.9](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)

**Design change.** Specify slot ownership separately from reply encoding. A new QoS 1 delivery holds its slot through the PUBACK write. A successful QoS 2 exchange holds one slot through PUBREC and releases it on the PUBCOMP write. A duplicate packet or a reply for an unknown identifier must not release an extra slot. Apply the limit before invoking the application callback for a new delivery.

**Acceptance tests.**

- With 1,023 QoS 2 exchanges outstanding, supply two distinct QoS 1 publishes together. Keep the first PUBACK buffered and require the second publish to trigger DISCONNECT with reason `0x93` before its callback.
- With all 1,024 slots occupied by QoS 2, deliver a matching PUBREL and a new PUBLISH before the queued PUBCOMP is written. Verify the same limit. In a separate case, let the PUBCOMP write complete first and verify that exactly one new delivery can enter.

## 4. P2 — Include outbound QoS 2 PUBREL in the contract

**Coverage gap.** Section 8 pairs a fresh incoming QoS 2 PUBLISH with its own PUBREL in one batch. A conforming broker sends that PUBREL after it receives the client’s PUBREC. Meanwhile the client’s outgoing publish path also calls `RecordReply`: an incoming PUBREC for a client-originated QoS 2 publish can produce an outgoing PUBREL. MQTT requires PUBREL packets to preserve the order of their corresponding PUBRECs. [Outgoing acknowledgement handling](../../src/MqttWire/MqttWireClient.Publish.cs) · [MQTT 5.0, §4.6](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)

**Design change and acceptance tests.** List PUBREL explicitly among the replies covered by section 5. For a legal mixed batch, first establish an inbound exchange A whose PUBREC the broker has received. Then deliver a fresh QoS 2 PUBLISH B followed by PUBREL A. Expect one write containing PUBREC B followed by PUBCOMP A. Separately, start two client-originated QoS 2 publishes and deliver both PUBRECs together. Check the identifiers and order of both outgoing PUBRELs, then verify each publish completes only on its matching PUBCOMP.

## 5. P2 — Distinguish the measured speedup from its proposed cause

**Evidence.** The original BenchmarkDotNet run measured a plain QoS 1 publish at 86.198 µs for MqttWire and 93.669 µs for MQTTnet. The self-echo took 108.264 µs and 100.700 µs respectively. An interleaved follow-up probe reversed the self-echo ordering to 104.2 µs and 112.2 µs. The later design spike found faster bursts, but changed both the number of writes and when they occurred. It also used a 256-byte threshold, roughly sixty short replies, while the proposal specifies sixteen. Those measurements leave the contribution of each change open. [QoS 1 benchmark report](2026-11-qos1-latency.md) · [Design spike, §§4 and 6](../specs/2026-11-24-client-acknowledgement-batching-design.md)

**Verification.** Compare the unchanged client with the proposed sixteen-reply implementation using BenchmarkDotNet and `[MemoryDiagnoser]`. Measure self-echo with blocking and awaiting callers, plus the fifty-message burst. Record actual writes per burst alongside latency and allocation: four writes is achievable only if enough input remains ready between flushes. If the report attributes the original gap to a specific write, trace echo receipt, the start and completion of the client’s PUBACK write, and dispatch of the broker’s PUBACK for the original publish. Correlate by packet identifier **and direction**, since the two acknowledgements belong to separate exchanges.
