# GrainWire on MqttWire (design)

Date: 2026-11-19
Status: final design (v6), approved by the owner on 2026-11-19
Companion: [MqttWire](2026-11-19-mqttwire-design.md), the library this builds on.
Builds on: [the GrainWire design](https://github.com/mo-esmp/grainwire/blob/main/docs/specs/2026-11-15-grainwire-design.md), [ADR 0002](https://github.com/mo-esmp/grainwire/blob/main/docs/adr/0002-implementation-notes.md), [the wire protocol](https://github.com/mo-esmp/grainwire/blob/main/docs/protocol.md)
History: v1 to v5 were one document, ["GrainWire: its own minimal MQTT 5 implementation"](2026-11-19-minimal-mqtt-design.md).
v6 splits it in two, because the owner decided that the MQTT implementation is a package of its
own. That document keeps the review history.

## 1. Intent

GrainWire uses MQTTnet 5 for both ends of its connections: the broker inside the gateway and
the MQTT client inside `GrainWire.Client`. This work replaces MQTTnet in both packages with
**MqttWire**, the owner's own lean MQTT 5 connection server and client.

GrainWire becomes the first **host** of MqttWire: its gateway is the handler that decides who
may connect, what may be subscribed and published, and what is sent to whom. Everything about
the MQTT protocol itself, connections, QoS, limits, deadlines, is MqttWire's and is described
in its design.

The reasons are those of the earlier versions: only what is used, memory per connection, and
control over the APIs.

## 2. Goals

1. `GrainWire.Server` and `GrainWire.Client` have **no MQTTnet dependency**. They depend on
   `MqttWire.Server` and `MqttWire`.
2. **The wire stays standard MQTT 5 over WebSockets.** Everything in protocol.md still holds,
   with the changes of section 5.8. The TypeScript client stays on MQTT.js.
3. **An idle connection is cheap.** GrainWire's own state for an idle, joined connection is
   small and holds no queue, no worker and no timer. The target is in section 6.
4. **Latency and throughput are not worse** than the results of 2026-11-15 in
   [benchmarks.md](https://github.com/mo-esmp/grainwire/blob/main/docs/benchmarks.md) (section 7).
5. **Behaviour seen by apps and clients does not change**, apart from section 5.8. The existing
   tests are the definition of that behaviour.

## 3. Non-goals

- Anything that is MqttWire's (see its section 3): routing, stored sessions, retained
  messages, Wills, a TCP endpoint.
- Changing the JSON envelope, the room protocol, or how a gateway renders and orders events.
- Using shared topics in GrainWire. A room keeps delivering to each member's own inbox: each
  copy carries that player's `seq`, and membership is decided by the room, not by
  subscriptions.
- Reducing allocations per delivered message.
- Message retention (stored room history). The owner wants it later; it is a room-side feature
  that would use the existing `seq` and `sys/sync` exchange.
- Metrics. An automated browser test of the Blazor client.

## 4. Constraints

- .NET 10, C# 14, warnings are errors.
- `GrainWire.Client` must not reference Orleans or `GrainWire.Abstractions`, and must keep
  working in Blazor WebAssembly (`samples/Chess/Chess.BlazorClient` is the check).
- Hosting stays as it is: `AddGrainWireGateway()` and `MapGrainWire()` keep their signatures.
- MQTTnet's **client** stays in GrainWire's tests and benchmark (`TestClient`, `BenchClient`),
  so the gateway keeps being checked by an independent implementation and the benchmark keeps
  its load generator. MQTTnet's server packages leave the repository (5.7).

## 5. Design

### 5.1 How GrainWire refers to MqttWire

`GrainWire.Server` references the package `MqttWire.Server`; `GrainWire.Client` references
`MqttWire`. GrainWire pins an exact version while both are previews.

Until MqttWire's packages are published, GrainWire restores them from a local folder feed that
MqttWire's build fills (`dotnet pack`), named in GrainWire's `nuget.config`. No source of
MqttWire is copied into GrainWire and there is no git submodule.

### 5.2 The gateway as MqttWire's handler

`Gateway` implements MqttWire's handler interface. The policy code, its messages and its reason
codes stay as they are today; they move from MQTTnet's seven events into six methods.

| Handler method | What the gateway does | Today |
|---|---|---|
| `OnConnectAsync` | Checks the client id pattern (`0x85`); calls `IConnectionAuthenticator` with the client id, user name, token, remote end point and "is secure" (`0x86` with its reason, `0x88` if it throws); refuses a player id that is empty or too long. On success it returns its per-connection state (5.3) and the CONNACK user property `gw-player`. | `OnValidatingConnection` |
| `OnAdmit` | **The player's connection slot**, in MqttWire's atomic admission step: see below. | `OnValidatingConnection` + `OnClientConnected` |
| `OnSubscribe` | Grants the exact own inbox at the QoS asked for and remembers that QoS in the connection's state; refuses every other filter with `0x87` and today's reason string. | `OnSubscription` |
| `OnUnsubscribe` | The own inbox: delivery to this connection stops. | (new) |
| `OnPublish` | Today's policy, unchanged: retained refused, topic must be a room command or system topic, rate limit, pending-request limit. An accepted message is **copied** (the payload is valid only during the call) and goes to the connection's request queue (5.3). | `OnPublish` |
| `OnClosed` | Today's clean-up, once: forget the connection, give back the player's slot unless the reason is "taken over", let the accepted requests finish, then detach from every room. | `OnClientDisconnected` |

The size check of today's `OnPublish` goes away: MqttWire refuses a publish over
`MaxPublishPayloadBytes` before the handler sees it, with the same reason code and the same
text.

**The player's slot, in `OnAdmit`.** MqttWire calls it with the new connection and the
connection that currently has the same client id, if there is one:

- **No such connection:** the player needs a free slot (`MaxConnectionsPerPlayer`). Taking it
  either succeeds or refuses with `0x97` and today's reason string.
- **It belongs to the same player:** accept. The new connection **inherits the slot**; no
  second one is needed, so a tab that reconnects while its old connection is still open is not
  refused when its player is at the limit. MqttWire closes the old connection with "session
  taken over", and its `OnClosed` does not give the slot back.
- **It belongs to another player:** refuse with `0x85` and "The client id is in use."

Because the step is atomic and `OnClosed` comes exactly once for every admitted connection, the
count cannot drift: every Active connection holds exactly one slot, and a slot is given back
exactly once. This replaces the two-step check of ADR 0002 difference 10.

Slots and takeover are per gateway process, as limits are today. A reconnect to another gateway
is settled by generations at the room, unchanged. Detaches sent to rooms name the old
connection's generation, so they cannot detach its replacement.

### 5.3 GrainWire's state for a connection

`GatewayConnection` stays GrainWire's own object and is what MqttWire carries in the
connection's state slot. It holds: the player, the claims, the generation, the rooms joined,
the rate limiter's two numbers, the QoS granted to the inbox, and the request queue.

**Requests, with no worker while idle.** Today every connection has a bounded channel and a
worker task parked on it. The rule that must survive is: **a connection's requests are handled
one at a time, in the order they arrived, and at most `MaxPendingRequests` wait.** The
mechanism becomes "only while there is work": a request that arrives when nothing is running
starts the handler; requests that arrive meanwhile are appended to a list created on demand;
the handler takes them in order and stops when the list is empty.

The clean-up rule survives exactly as the code has it today:

- After `OnClosed`, **no new request is accepted** from the connection.
- **Every request accepted before that is still handled**, in order. A command that got its
  acknowledgement just before the tab closed is applied.
- The detach from the connection's rooms happens only after the last of them has finished.
- A join that completes after the close is undone again (today: the `connection.Closed` check
  at the end of `JoinAsync`).

**Smaller state.** The rooms a connection has joined (at most `MaxRoomsPerConnection`) become a
small list created on the first join. The gateway's own table of connections by client id can
go: MqttWire's server already finds a connection by client id. These, and the per-room tables
(`_directory`), are tuned by what the measurement of section 6 shows.

**The required tests** for the request queue are those v5 asked for: a deterministic test that
forces "the handler has just found the list empty" against "another request arrives", in both
orders; and a stress test with a close at a random moment that asserts every accepted request
ran, none started after the last accepted one, and the detach came last.

### 5.4 Delivery

- `PublishAsync(clientId, payload)` finds the connection through MqttWire's server and calls
  `TrySend` with the inbox topic, the payload and the QoS granted to that connection's inbox.
  It returns at once; a room's delivery loop is never held up by one member. The
  `OutboundFilter` test hook stays.
- **Replies and snapshots are addressed by client id and looked up when they are sent**, as
  today. A reply to a request of a connection that has since been replaced reaches the
  replacement, if it has subscribed; otherwise it is dropped, and the client's re-sent request
  gets the recorded outcome.
- A connection that has not subscribed to its inbox gets nothing, as today.
- **The inbox topic.** `TrySend` takes an encoded topic. Whether the gateway keeps it in the
  connection's state (some dozens of bytes per connection) or builds it per message (an
  allocation per message, as today) is decided by the measurement.
- **A message that MqttWire says is too large** (over `MaxOutgoingBytes`): the gateway logs an
  error that names the room, the message's size and the option. MqttWire closes the connection.
- The delivery code (`PublishBatchAsync`, `EventTemplate`, `Envelopes`, `BatchSequencer`) is
  otherwise not changed. `Wire.GatewaySender` and the "message injected by the gateway itself"
  check are no longer needed.

### 5.5 Options, registration, stopping

- `GrainWireGatewayOptions` keeps its properties and gains two. The gateway passes them on:

  | GrainWire option | MqttWire option |
  |---|---|
  | `Path` | the mapped path |
  | `MaxPayloadBytes` | `MaxPublishPayloadBytes` |
  | `MaxOutgoingQueue` | `MaxOutgoingMessages` |
  | `MaxOutgoingBytes` (**new**, default 1 MiB) | `MaxOutgoingBytes` |
  | `MaxQoS` (**new**, 1 or 2, default 2) | `MaxQoS` |

  The remaining options (connections per player, rooms per connection, command rate, pending
  requests, room creations, lease interval) are GrainWire's own and stay in the gateway.
- `AddGrainWireGateway` registers MqttWire's server with the gateway as its handler, in place
  of MQTTnet and the Connections layer. `MapGrainWire` maps MqttWire's endpoint at
  `options.Path`.
- `DisconnectAsync` closes a connection through MqttWire. `tests/GrainWire.TestHost`'s
  `/control/disconnect` endpoint uses it.
- **When the host stops,** MqttWire closes every connection (`0x8B`, server shutting down) and
  calls `OnClosed` for each. The gateway then lets the requests that were already accepted
  finish, **for at most 5 s in total**. After that it stops waiting: their answers are dropped,
  and what the rooms were not told is repaired by the rooms' leases, exactly as when a gateway
  dies. A room handler that never returns must not keep the process from stopping.

### 5.6 The client

`GrainWireClient` uses `MqttWireClient` where it uses MQTTnet's `IMqttClient` today, one new
instance per connection attempt, as today.

| Today | After |
|---|---|
| `ConnectAsync` and its result code, reason string, user properties | `ConnectAsync`; the same three things from MqttWire's result |
| `SubscribeAsync(inbox, QoS 0)` | `SubscribeAsync(inbox, InboxQoS)`; the client works at what was granted |
| `PublishAsync`, not awaited, errors swallowed | `PublishAsync` at `CommandQoS`, not awaited; a publish that fails at once is treated as not sent, and the existing retry timer re-sends the request |
| `ApplicationMessageReceivedAsync` | `OnMessage`; `Receive` parses the payload before it returns, as the payload is valid only during the call |
| `DisconnectedAsync` | `OnClosed`, once |

What `GrainWireClient` does itself is unchanged: rooms, ordering, sync, retries with the same
command id, reconnecting with back-off. It does **not** use MqttWire's opt-in reconnecting
client: after a reconnect GrainWire has to re-join rooms and re-send requests with their
original command ids, which is more than re-subscribing, and it already does that today.

Public options of `GrainWireClientOptions`:

- `ConfigureMqtt` (`Action<MqttClientOptionsBuilder>`) is **removed**.
- **New** `ConfigureWebSocket` (`Action<ClientWebSocketOptions>`), `KeepAlive` (default 15 s),
  `CommandQoS` (1 or 2, default 1) and `InboxQoS` (0, 1 or 2, default 0), with a small public
  enum for the three levels. They are passed to MqttWire's client and checked when the client
  is created.
- The TypeScript client gains `commandQos` and `inboxQos` with the same meaning and defaults,
  passed to MQTT.js.

The defaults are today's behaviour. A higher QoS costs more packets and adds no guarantee
across reconnects, which come from `cmd`, `seq` and sync; the documentation says so plainly.

### 5.7 Tests

MqttWire has its own tests for the protocol, including interop with MQTTnet and MQTT.js at
every QoS. GrainWire's tests are about GrainWire.

1. **Existing tests, unchanged in what they assert** (except 5.9):
   - the gateway with MQTTnet's client: `GatewayTests`, `FailoverTests`, `RoomPersistenceTests`
     through `TestClient`;
   - the gateway with MQTT.js: `clients/ts` integration tests;
   - the .NET client with the gateway: `ClientIntegrationTests`.
2. **`ScriptedGateway` is rebuilt on MqttWire's server.** Today it is an MQTTnet broker on raw
   TCP, and `ClientProtocolTests` points the client at it through `ConfigureMqtt`, which goes
   away. A scripted gateway is exactly a small MqttWire host: it records what the client
   publishes and sends what the test tells it to. The tests keep their assertions. With that,
   `MQTTnet.Server` and `MQTTnet.AspNetCore` leave `Directory.Packages.props`; only `MQTTnet`
   (the client) stays, for tests and the benchmark.
3. **New gateway tests:** the three cases of `OnAdmit` (5.2), including a player at the limit
   reconnecting with a client id it has open; the request queue's two tests (5.3); one member
   of a room stops reading while the others keep receiving in time; the gateway stopping while
   a room handler does not return.
4. **QoS:** a subset of the room tests is run again with `CommandQoS` 2 and `InboxQoS` 2, with
   MQTTnet's client, MQTT.js and the .NET client.
5. **The chess sample, at the end of steps 4 and 5 of section 7:** the Playwright test
   `samples/Chess/Chess.WebClient/e2e/chess.spec.ts`, and the Blazor client run by hand once
   (two tabs play a few moves, one reloads and gets the position back).

If an existing test other than those of 5.9 cannot pass without changing what it asserts, that
is a behaviour change: it is brought to the owner, not adjusted.

### 5.8 Changes in behaviour

These are what an app, a client or an operator of GrainWire can notice. Most come from
MqttWire's rules; they are listed here because protocol.md and GrainWire's documentation
promise the old behaviour.

| # | Today | After | Why |
|---|---|---|---|
| 1 | A Will is treated like any other publish from that client. | A CONNECT that carries a Will is **refused** (`0x87`). | The owner's decision, and what the original design asked for. |
| 2 | Every message for a connection is queued; more than `MaxOutgoingQueue` at once disconnects it, even if it is reading normally. | Only deliveries that are waiting count; a hard byte limit applies beside the count. | A client is no longer dropped by a burst that fits within the limits, and a slow one is bounded in bytes. |
| 3 | A slow consumer loses a message at the limit and is then disconnected (ADR 0002, difference 4). | It is disconnected at the limit, or when one write of at most 64 KB has not completed after 15 s. | What the original design asked for, plus a bound in time. |
| 4 | Connections per player are checked twice (ADR 0002, difference 10). A reconnect with a client id that is still open needs a second slot. | Checked once, atomically. A takeover by the same player inherits the slot. | The owner's decision. |
| 5 | A second connection with a client id takes over whoever it belongs to. | A client id that is open for another player is refused (`0x85`). | A client id names an inbox; another player must not be able to cut it off. |
| 6 | A client that asks for no keep-alive gets none. | It is told 60 s. | A connection that vanished without closing is no longer held until the operating system notices. |
| 7 | WebSocket ping frames are sent by the server. | They are not; MQTT's keep-alive is the only one. | One timer less per connection. |
| 8 | A client's "maximum packet size" is whatever MQTTnet makes of it. | Less than 4 KiB is refused; a message that does not fit closes the connection with `0x95`. | A silently dropped snapshot would leave the client asking for it for ever. |
| 9 | Refusals always carry a reason string, of any length. | It is cut to 256 bytes, and left out where the client forbids it ("request problem information" 0). | MQTT 5 forbids it there; and every control packet has a known largest size. |
| 10 | Enhanced authentication and subscription identifiers are whatever MQTTnet makes of them. | Refused (`0x8C`, `0xA1`). CONNACK announces "no subscription identifiers" and "receive maximum 256", and "maximum QoS 1" when the gateway is capped. | Not supported; said clearly. |
| 11 | Every inbox message is delivered at QoS 0, whatever QoS the client subscribed with. | It is delivered at the QoS the client subscribed with, capped by `MaxQoS`. | The owner's decision. Both GrainWire clients subscribe at QoS 0 by default. |
| 12 | A command published at QoS 2 is whatever MQTTnet makes of it. | It is accepted through the standard QoS 2 exchange. | The owner's decision. |
| 13 | A message of any size is sent to a connection. | A message larger than `MaxOutgoingBytes` (1 MiB by default) is not sent; the connection is closed with `0x95` and an error is logged. | A hard bound: a room whose snapshot outgrows it fails visibly. |
| 14 | When the host stops, MQTTnet's server stops with it. | Every connection is closed with `0x8B` (server shutting down), and the gateway stops within a bound. | The shutdown is now GrainWire's and MqttWire's own. |

### 5.9 Existing tests that change, with the owner's decisions

| Test | Today | After |
|---|---|---|
| `FailoverTests.A_client_that_cannot_keep_up_is_disconnected_and_cleaned_up_and_can_come_back` | `MaxOutgoingQueue = 2` and a burst of 5,000 events disconnect two clients that are reading normally. | The slow client is one that has **stopped reading**; it is disconnected and cleaned up and can come back. A second assertion is new: a client that keeps reading survives a burst **that fits within the limits the test configures**. |
| `GatewayTests.A_will_message_cannot_reach_another_client` | A client connects with a Will addressed to another inbox, is aborted, and the other inbox stays empty. | The CONNECT with a Will is refused with `0x87`, and the other inbox stays empty. |
| `ClientProtocolTests` | Reach `ScriptedGateway` over TCP through `ConfigureMqtt`. | Reach the rebuilt `ScriptedGateway` over WebSockets (5.7). Their assertions do not change. |

No existing test reuses a client id for a different player, so row 5 of 5.8 changes none.

### 5.10 Public API changes

| Package | Change |
|---|---|
| `GrainWire.Client` | `ConfigureMqtt` removed; `ConfigureWebSocket`, `KeepAlive`, `CommandQoS`, `InboxQoS` added (5.6). `ConnectException.ReasonCode` keeps its meaning. |
| `GrainWire.Server` | `GrainWireGatewayOptions.MaxOutgoingBytes` and `MaxQoS` added. `MaxOutgoingQueue` keeps its name and default; its documentation says what it counts now. The gateway's options are checked at start-up, and a gateway with an invalid value does not start. |
| `GrainWire.Server` | `ConnectionAuthContext` is unchanged. (MqttWire also offers the `HttpContext` of the WebSocket request; passing it on to authenticators is left for later.) |
| TypeScript client | `commandQos` and `inboxQos` added. |

The packages are a preview and not published, so the removal needs no deprecation step.

### 5.11 Documentation

- A new ADR (`docs/adr/0003-mqttwire.md`): why MqttWire, what was measured in step 0, the
  differences of 5.8, the pinned MqttWire version, and the tag of the last MQTTnet-based commit
  so that the "before" side of the benchmark can be run again.
- `protocol.md`: the Will row; the client-id rule (5.8, 5); the two QoS rows, with the plain
  statement that a higher QoS adds no guarantee across reconnects; the advice to leave "maximum
  packet size" unset; and a pointer to MqttWire's list of the part of MQTT 5 it implements.
- `docs/security.md`: the Will line; the byte limit, that it is a hard bound, and how to choose
  it (about four times the largest snapshot an app expects); a line on Kestrel's
  `MaxConcurrentUpgradedConnections`.
- `README.md`, `CHANGELOG.md`, `docs/hosting.md`, `docs/api-reference.md`, both client READMEs,
  the package descriptions: MqttWire in place of MQTTnet; the new options.
- `docs/benchmarks.md`: new results next to those of 2026-11-15, and the idle measurements.

## 6. Measurement

**Step 0, before anything is built, in `bench/GrainWire.Bench`:**

- An `idle` workload. It reports the gateways' growth per connection in four states:
  1. connected and subscribed;
  2. joined (rooms of two; one run with five rooms per connection);
  3. **active, then idle:** every connection has sent commands and received a burst, then
     everything is quiet again;
  4. **after churn:** every connection has disconnected; the live managed bytes must come back
     to about where they started. Then all reconnect, and state 2 is measured again.
- It runs at two connection counts, *N* and at least 4 *N*; connections carry three claims
  each. **"Per connection" is a slope:** (bytes at the larger count − bytes at the smaller) ÷
  (the difference in connections).
- **Three figures are reported, and only the first decides:** live managed bytes after a full
  collection, as a slope; what the buffer pool keeps after activity, which must not grow with
  connections; working set, for information.
- Everything that exists because of a connection is charged to it, including its share of the
  gateway's per-room tables. The target is stated for one room of two per pair of connections.
- Run it on today's code. Take one heap dump of a gateway in state 2 and attribute the bytes to
  MQTTnet, the Connections layer, Kestrel/ASP.NET Core and GrainWire's own objects.
- Measure **the floor**: the same idle connections against a bare ASP.NET Core WebSocket
  endpoint with sub-protocol `mqtt`, keep-alive off, waiting in a zero-byte read. One extra run
  with TLS in Kestrel is reported, not decisive.
- **Report to the owner** before anything else is built: today's figures and their split, the
  floor, and the targets. The proposed targets are **floor + 1 KB for MqttWire alone** and
  **floor + 2 KB for a joined connection in GrainWire**, in states 2 and 3 alike; both are
  provisional until the attribution shows whether they are realistic. **After step 0 the owner
  set the GrainWire target to floor + 3 KB** (2026-11-20; see
  [the step 0 report](https://github.com/mo-esmp/grainwire/blob/main/docs/step-0-idle-memory.md)); the MqttWire target stays floor + 1 KB. **If the floor alone is
  more than about 70% of today's figure, the approach cannot deliver much, and the owner
  decides** whether to continue, or to look at own WebSocket framing in MqttWire.

**At the end** (step 5 of section 7):

- The `idle` workload and both workloads of benchmarks.md, old and new code on the same machine
  on the same day, three runs each. The old code is measured against itself first, to learn how
  much the figures move with no change at all. The machine is a virtual machine, so small
  differences are not read as results.
- For each run: the gateways' CPU time, bytes allocated, number of collections and total pause
  time, beside the latencies. For the big room also the largest number of deliveries and bytes
  that were waiting for one connection; if that comes near the defaults (1,000 messages,
  1 MiB), the defaults are raised before they are published.
- The "many small rooms" workload at 2 and 4 times its command rate, old and new.
- **The price of QoS**, new code only, reported and not part of success: the big room with
  every inbox at QoS 1 and at QoS 2; and state 3 after activity at QoS 2, which must come back
  to the same idle figure as at QoS 0.

## 7. Order of work

The two repositories are built together, a part at a time. Each step ends with every test
green in the repository it touches.

| Step | Where | What |
|---|---|---|
| 0 | GrainWire | Measure (section 6) and report to the owner. Depends on nothing else in either design and can start on approval. |
| 1 | MqttWire | The repository, the codec and its tests. |
| 2 | MqttWire | The client, its tests, interop with MQTTnet's broker. A first preview package in the local feed. |
| 3 | GrainWire | `GrainWire.Client` moves to MqttWire's client (5.6). The gateway is still MQTTnet. `ScriptedGateway` is an MQTTnet broker on raw TCP, which the new client cannot reach; until it is rebuilt in step 4, the builder either serves that MQTTnet broker over WebSockets for `ClientProtocolTests`, or holds those tests for step 4, whichever is smaller. The other client tests run against the real gateway and pass in this step. |
| 4 | MqttWire, then GrainWire | MqttWire's server, tests, interops, benchmark and sample. Then the gateway becomes its handler (5.2 to 5.5), `ScriptedGateway` is rebuilt (5.7), and MQTTnet's server packages leave GrainWire. |
| 5 | GrainWire | Measure again (section 6) and write up (5.11). |

The last commit that still uses MQTTnet gets a git tag when the owner makes it.

## 8. Success

- Goals 1, 2 and 5 hold: no MQTTnet in the two packages; all existing tests pass with their
  assertions unchanged, except those of 5.9.
- The idle workload shows the targets of step 0 met in states 2 and 3, as the slope of live
  managed bytes; state 4 shows the live managed bytes coming back; what the buffer pool keeps
  does not grow with the number of connections.
- For both workloads of benchmarks.md: no failed commands, all deliveries made, and the
  **p50 and p95** of "command to event" (both workloads) and of "request to reply" (many small
  rooms) are not worse than the old code's, comparing medians of three runs. "Not worse" means
  within 10%, or within the spread the old code shows against itself, whichever is larger.
- At 2 and 4 times the load, the new code does not fail where the old code succeeds.
- p99, max, and the big room's "request to reply" are reported but do not decide success. CPU,
  allocation and collection figures are reported; a clear regression is brought to the owner.

## 9. Decided by the owner

- Replace MQTTnet in both the gateway and the .NET client, for the reasons of section 1.
- The MQTT implementation is **MqttWire**, a package of its own in its own repository, a
  connection server that the host drives through one handler and a state slot.
- Memory per connection is the number to design for.
- `MaxOutgoingQueue` counts only deliveries that are waiting; the slow-consumer test is
  rewritten around a client that stops reading.
- A CONNECT with a Will is refused.
- `MaxOutgoingBytes` beside the count: a hard bound, 1 MiB by default, adjustable.
- A takeover by the same player inherits the slot; a client id open for another player is
  refused.
- CPU, allocation and collection figures are recorded, and the benchmark also runs at 2 and 4
  times the load. No search for the maximum.
- QoS 2, and QoS 1 for deliveries, inside a connection, in both directions, chosen by the
  connecting client. No stored sessions.
- Message retention is wanted later, for messaging apps, and is not built now.
- Unity and .NET Standard 2.1 wait for Unity 7.
- After step 0 (2026-11-20): the target for a joined connection in GrainWire is **floor + 3 KB**,
  in states 2 and 3 alike, not the floor + 2 KB that was proposed (section 6).

Put to the owner as assumptions and not objected to: the wire stays standard MQTT 5; the
TypeScript client stays on MQTT.js; MQTTnet stays in the tests and the benchmark.

## 10. Proposed by Claude (owner to check)

1. GrainWire takes MqttWire as a package, from a local folder feed until it is published (5.1).
2. `ScriptedGateway` is rebuilt on MqttWire's server, and MQTTnet's server packages leave the
   repository (5.7). The independent check of MqttWire's client against MQTTnet's broker then
   lives in MqttWire's own tests.
3. `ConfigureMqtt` is replaced by `ConfigureWebSocket` and `KeepAlive`; the app chooses QoS
   through `CommandQoS` and `InboxQoS`, on both clients (5.6).
4. GrainWire keeps delivering to each member's own inbox and does not use shared topics (3).
5. The gateway's request handling changes from a channel and a parked worker to work that
   exists only while there are requests (5.3). This goes beyond replacing MQTTnet, but it is
   part of what an idle connection costs today.
6. When the gateway stops, accepted requests get 5 s in total to finish (5.5).
7. The target "floor + 1 KB" for MqttWire alone, and the 70% rule (section 6). GrainWire's
   target is the owner's decision now (section 9).
8. "Not worse" is judged on p50 and p95 only (section 8).
9. The steps of section 7 alternate between the two repositories; step 3 leaves the builder a
   choice about `ClientProtocolTests`.
10. `ConnectionAuthContext` does not get the `HttpContext` in this work (5.10).

## 11. Left to the builder

- The layout of `GatewayConnection` and of the gateway's per-room tables, guided by the heap
  dump of step 0, including whether the encoded inbox topic is kept per connection.
- Whether the gateway keeps its own table of connections or uses MqttWire's.
- Whether the floor endpoint of step 0 stays in the benchmark project or is thrown away once
  MqttWire's own benchmark has it.
- The connection counts of the `idle` workload, within what the test machine can drive.

## 12. History and what was checked

[The v5 design](2026-11-19-minimal-mqtt-design.md) keeps the full record: the builder check,
the external review of v1, the two external reviews of v4, the fact checks against the MQTT 5
standard and the .NET 10 source, and what could not be reached. v6 changes where things live,
not the rules: everything about the protocol moved to MqttWire's design, and what stays
GrainWire's is here. The changes of substance in v6 are the owner's four decisions listed first
in section 9 of MqttWire's design.
