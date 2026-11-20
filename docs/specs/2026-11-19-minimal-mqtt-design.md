# GrainWire: its own minimal MQTT 5 implementation (design)

Date: 2026-11-19
Status: **superseded by v6**, which is two documents: [MqttWire](2026-11-19-mqttwire-design.md), the MQTT implementation as a package of its own, and [GrainWire on MqttWire](2026-11-19-grainwire-on-mqttwire-design.md). This file is v5, kept as the record: the reviews refer to it, section 13 lists what was checked against the MQTT standard and the .NET source, and section 14 is the review history. Do not build from it.
Builds on: [the GrainWire design](https://github.com/mo-esmp/grainwire/blob/main/docs/specs/2026-11-15-grainwire-design.md), [ADR 0002](https://github.com/mo-esmp/grainwire/blob/main/docs/adr/0002-implementation-notes.md), [the wire protocol](https://github.com/mo-esmp/grainwire/blob/main/docs/protocol.md)
Reviewed in: [the review of v1](2026-11-19-minimal-mqtt-design-review.md) and two reviews of v4 that are not kept in the repository (section 14 says what was taken from each)

## 1. Intent

GrainWire uses MQTTnet 5 for both ends of its connections: the broker inside the gateway
(`GrainWire.Server`) and the MQTT client inside `GrainWire.Client`. MQTTnet is a general-purpose
library. GrainWire uses a small, fixed part of MQTT 5 and pays, per connection, for the rest.

This work replaces MQTTnet in both packages with an implementation written for GrainWire: only
the packets and rules that [protocol.md](https://github.com/mo-esmp/grainwire/blob/main/docs/protocol.md) needs, designed so that a connection
which is doing nothing holds as little memory as possible.

Why the owner wants it:

1. **Only what is used.** No features, objects or settings that GrainWire never exercises.
2. **Performance.** The owner chose **memory per connection** as the number to design for. A
   gateway for turn-based games holds mostly idle connections, so memory per connection is what
   sets how many players one gateway can hold.
3. **Control over the APIs.** No third-party type in GrainWire's public surface, and no
   behaviour that has to be worked around because the library does not offer it (ADR 0002,
   differences 1 and 4).

## 2. Goals

1. `GrainWire.Server` and `GrainWire.Client` have **no MQTTnet dependency**. `GrainWire.Client`
   has no package dependencies at all.
2. **The wire stays standard MQTT 5 over WebSockets.** Everything in protocol.md still holds,
   with the changes of 7.11. MQTT.js (the TypeScript client) and any third-party MQTT 5 client
   that follows protocol.md keep working unchanged.
3. **An idle connection costs GrainWire almost nothing.** For a connection that is connected,
   subscribed and joined to a room, with nothing in flight, GrainWire itself holds: no payload
   or packet buffer, no backing storage for a queue, no timer, no cancellation source, no
   waiting request worker, and no record for QoS 1 or 2. What remains is the connection object, the parked read, and what
   Kestrel and .NET's WebSocket hold, which is the floor that step 0 measures. The target
   number is set there (section 8).
4. **Latency and throughput are not worse** than the results of 2026-11-15 in
   [benchmarks.md](https://github.com/mo-esmp/grainwire/blob/main/docs/benchmarks.md), measured again on the same machine (section 9).
5. **Behaviour seen by apps and clients does not change**, apart from the items listed in
   section 7.11. The existing tests are the definition of app-visible behaviour. They are not
   the definition of MQTT correctness: that is section 7.5 and the codec's own tests.

## 3. Non-goals

- **.NET Standard 2.1 or Unity.** Postponed by the owner until Unity 7. Everything stays on
  `net10.0`.
- **Own WebSocket framing** (approach B in section 6). The design keeps it possible later.
- A raw TCP MQTT endpoint, MQTT 3.x, retained messages, Will messages, topic aliases, shared
  subscriptions, subscription identifiers, enhanced authentication (AUTH), WebSocket
  compression.
- **Sessions that survive a reconnect.** QoS 1 and 2 are supported inside one connection (7.4).
  Nothing is stored for a client that is away, and nothing is sent a second time. What makes a
  command run once and a client catch up across reconnects stays what it is today: the `cmd`
  id, and `seq` with sync.
- **Message retention.** The owner wants it later, for messaging apps; it is not built now.
  Section 7.14 says what this design does so that it stays possible.
- A public, general-purpose MQTT API. Every MQTT type stays `internal`.
- Replacing MQTT.js in the TypeScript client.
- Changing the JSON envelope, the room protocol, or how a gateway renders and orders events.
- Reducing allocations per delivered message (for example rendering an event straight into the
  send buffer). It can follow once the idle cost is done, if a measurement asks for it.
- Operational metrics (queued bytes, write age, disconnect reasons as counters). The packages
  have no metrics today; adding them is a feature of its own.
- Limits across connections (how many unauthenticated connections or authentications at once).
  None exists today. The security guide gets a line pointing at Kestrel's own limit on upgraded
  connections.
- An automated browser test of the Blazor client.

## 4. Anti-goals (failures even if it technically works)

- It grows into a general MQTT library: options and packet types "for completeness".
- A client that has not authenticated can make the server hold more than a small, fixed amount
  of memory, or hold it for longer than the connect and authentication deadlines. (The
  deadlines bound the socket and the gateway's own memory. They cannot stop an app's
  authenticator that ignores cancellation; 7.4 says what is released regardless.)
- An acknowledgement or a ping response waits behind a delivery that is itself waiting for the
  client.
- A connection that has stopped reading can hold memory without a bound in bytes and in time.
- One slow recipient delays delivery to the other members of its room.
- A message is silently never sent because the "send only when there is work" logic lost a
  wake-up. The client's 15-second sync would hide such a bug, so it must be caught by tests.
- A buffer is used after it went back to the pool, or never goes back.
- Memory is saved per connection by moving the cost somewhere unmeasured (native memory, a
  shared structure that grows with connections, memory retained after activity).

## 5. Constraints

- .NET 10, C# 14, warnings are errors (as the rest of the repo).
- The server sits on **ASP.NET Core's WebSocket** (`HttpContext.WebSockets`), the owner's choice
  (approach A). No use of the ASP.NET Core "Connections" layer.
- The client sits on `System.Net.WebSockets.ClientWebSocket` and **must keep working in Blazor
  WebAssembly** (single-threaded, no blocking waits). `samples/Chess/Chess.BlazorClient` is the
  check.
- `GrainWire.Client` must not reference Orleans or `GrainWire.Abstractions` (it does not today).
- **MQTTnet stays as a dependency of the tests and the benchmark only.** `TestClient`,
  `ScriptedGateway` and `BenchClient` keep using it. Together with MQTT.js they are two
  independent MQTT implementations that check the new one, and the benchmark keeps the same
  load generator, so before and after are comparable. `MQTTnet.AspNetCore` is no longer needed
  anywhere.
- Hosting stays as it is: `AddGrainWireGateway()` and `MapGrainWire()` keep their signatures.

## 6. Approaches considered

The choice that decides how low the memory per connection can go is the layer the server's MQTT
code sits on.

| | Approach | Trade-off |
|---|---|---|
| **A (chosen)** | ASP.NET Core's WebSocket, own MQTT on top. An idle connection waits with a zero-byte read and holds no GrainWire buffer. | The framework keeps handling WebSocket framing, ping/pong, the close handshake and HTTP/2. The floor is what Kestrel and its WebSocket object cost per connection, which is not known yet. |
| B | Own WebSocket framing as well, on the raw upgraded connection. | Removes the framework's WebSocket object and one copy per message. Adds a second protocol to own and to defend against untrusted input, and HTTP/2 WebSockets would need separate work. The saving over A is expected to be small. |
| C | Stay on the ASP.NET Core "Connections" layer that MQTTnet.AspNetCore uses today. | Least new I/O code, but keeps a pipe pair and transport loops per connection. Works against the goal. |

The owner picked **A**. B stays possible because the MQTT code never calls WebSocket APIs
directly (7.3); step 0 measures A's floor, which is the fact that would bring B back.

Before that, the owner chose the performance goal: **memory per connection** over "delivery
latency and throughput" and over "just not worse".

## 7. Design

### 7.1 Parts

| Part | Where | What |
|---|---|---|
| Codec | `src/GrainWire.Mqtt/` (source files only, no project) | Reads and writes MQTT 5 packets. Pure functions over spans. Compiled into both packages. |
| Server connection | `src/GrainWire.Server/Internal/` | One object per connection: the MQTT state machine, the read loop, the send path, and the gateway's own state for that connection. |
| Gateway | `Gateway.cs` (existing) | Keeps its job (policy, routing to rooms, delivery, leases). Loses its MQTTnet hooks and talks to connections directly. |
| Client connection | `src/GrainWire.Client/Internal/` | One MQTT connection over a transport: connect, subscribe, publish, receive, keep-alive. |
| `GrainWireClient` | existing | Unchanged logic. Uses the client connection where it uses `IMqttClient` today. |

### 7.2 The codec

Shared source, compiled into `GrainWire.Server` and `GrainWire.Client` with a `<Compile
Include>` link. All types are `internal`, in the namespace `GrainWire.Mqtt`.

Rules:

- **It never allocates and never owns a buffer.** Decoding takes a `ReadOnlySpan<byte>` (or
  `ReadOnlySequence<byte>` if the builder finds it needs one) and returns views into it;
  strings are turned into `string` only by the caller, when it needs one. Encoding writes into
  a span or an `IBufferWriter<byte>` the caller supplies.
- **Decoding is incremental at packet level.** "Is there a complete packet at the start of this
  buffer, and how long is it?" is a separate, cheap question from "decode it", because packets
  arrive split across WebSocket frames.
- **Every property is validated, then honoured, ignored or refused as 7.5 says.** A property
  that 7.5 marks "ignored" is parsed for length and skipped; a packet that carries it is valid
  and must be accepted.
- **Malformed input is an error result, not an exception.** The cases: a variable-length integer
  longer than four bytes, a length that runs past the packet, invalid UTF-8 in a string, reserved
  flag bits that are wrong for the packet type, a property that may appear once appearing twice,
  a property that is not allowed in that packet type, an unknown property id, a property value
  outside its range (7.5), a packet type 0.

Packets:

| Packet | Server reads | Server writes | Client reads | Client writes |
|---|---|---|---|---|
| CONNECT | yes | | | yes |
| CONNACK | | yes | yes | |
| PUBLISH | yes (QoS 0, 1, 2) | yes (QoS 0, 1, 2) | yes | yes (QoS 1, 2) |
| PUBACK | yes | yes | yes | yes |
| PUBREC, PUBREL, PUBCOMP | yes | yes | yes | yes |
| SUBSCRIBE / SUBACK | reads SUBSCRIBE | writes SUBACK | reads SUBACK | writes SUBSCRIBE |
| UNSUBSCRIBE / UNSUBACK | reads UNSUBSCRIBE | writes UNSUBACK | | |
| PINGREQ / PINGRESP | reads PINGREQ | writes PINGRESP | reads PINGRESP | writes PINGREQ |
| DISCONNECT | yes | yes | yes | yes |
| AUTH | recognised only to refuse it | | | |

The codec tests live in a small test project of their own that links the same source files
(`tests/GrainWire.Mqtt.Tests`). The existing test project sees the internals of both packages,
so the codec's types would be ambiguous there.

### 7.3 The link below the MQTT code

Neither connection calls `WebSocket` directly. Each uses a small internal abstraction with five
operations: wait until data is available, read into a buffer, write a buffer, close with a
status, abort. One implementation wraps a `WebSocket`.

This is what keeps approach B possible, and it lets tests drive a connection over an in-memory
link, byte by byte, without sockets, and decide exactly when a write completes. How it is
expressed (an abstract base class, an interface, a generic parameter) is the builder's choice,
with one condition on the server: it must not add a noticeable object per connection. Its operations return `ValueTask`, so
an operation that completes at once allocates nothing.

Rules for the WebSocket implementation, both sides:

- Sub-protocol `mqtt`. The server accepts a request that offers `mqtt` and answers with it; a
  WebSocket request that does not offer it is refused with HTTP 400.
- Binary messages only. A text message closes the connection with WebSocket status 1003
  (unsupported data).
- **MQTT packets are not aligned to WebSocket messages.** One message may hold several packets
  or part of one. The receiver treats the incoming messages as one byte stream.
- The server turns the WebSocket's own keep-alive pings off for this endpoint
  (`WebSocketAcceptContext.KeepAliveInterval`). MQTT has its own keep-alive, and this removes
  a timer per connection.

### 7.4 The server connection

One object per connection holds everything: client id, player, claims, MQTT state, the gateway's
state (rooms joined, rate limit, flags). It replaces today's `GatewayConnection` plus MQTTnet's
session, and there is no second per-connection object to look up through a dictionary of
"session items".

#### States

| State | Entered when | Leaves by | May hold |
|---|---|---|---|
| **Accepted** | the WebSocket is accepted | a complete CONNECT (to Authenticating); the connect deadline, a malformed or wrong first packet (to Closing) | one read buffer, at most 16 KB |
| **Authenticating** | CONNECT is valid so far | the authenticator answers (to Active or Closing); the authentication deadline; the socket closing | the CONNECT's fields and a short-lived cancellation source; no slot yet |
| **Active** | CONNACK success is handed to the link | any end of the connection (to Closing) | what the rest of 7.4 allows |
| **Closing** | the decision to end is made, once | the bounded close below | no new work is accepted |
| **Closed** | the link is closed or aborted | | nothing; clean-up has run or is finishing accepted requests |

A connection moves forward only. The move to Closing happens exactly once, whoever asks first
(the read loop, the sweep, the gateway, a takeover, an overflow); everyone else finds it already
closing.

#### Deadlines

All deadlines are measured on a monotonic clock. One shared sweep per gateway enforces them; it
runs every 2 seconds. **The lengths below are therefore nominal: a deadline is acted on at the
first sweep after it passes**, which is up to 2 seconds, plus the time one sweep takes, later.
Nothing in this design needs a tighter bound than that. How the sweep walks the connections is
the builder's choice; there is no timer per connection, and no cancellation source except the
short-lived one of the Authenticating state (below).

| Deadline | Length | What happens |
|---|---|---|
| Connect | 10 s from accept until a complete CONNECT has arrived | abort |
| Authentication | 10 s for `IConnectionAuthenticator` to answer; it is given a cancellation token that fires then | CONNACK `0x88` (server unavailable) if it can be written, then close. A result that arrives later is discarded and takes no slot. |
| Partial packet | 10 s from the first byte of a packet to its last | close (`0x82`) |
| Keep-alive | 1.5 times the connection's keep-alive without a packet from the client | close (`0x8D`) |
| Write | 15 s for a write handed to the link to complete; a write larger than 64 KB is handed over in pieces and the deadline applies to each piece | abort |
| Acknowledgement | 30 s for each phase of a QoS 1 or 2 exchange (see "exchange records") | close (`0x82`) |
| Close | 2 s from entering Closing | abort |

The keep-alive of a connection is what the client asked for, except that a client that asks for
none, or for more than 60 s, is told 60 s through the CONNACK property "server keep alive".

A client that stops reading may still send PINGREQ, so the keep-alive deadline does not bound a
stalled write. The write deadline does. It bounds how long one piece may take to be accepted by
the network; the link cannot see progress inside a piece, which is why large writes are cut
into pieces.

**The authenticator and its deadline.** `IConnectionAuthenticator` is the app's code:

- It is given a cancellation token that fires at the authentication deadline, or earlier if
  the socket closes. The source of that token exists only while the connection is
  Authenticating.
- If it does not answer in time, the socket is closed at the deadline and the gateway lets go
  of the connection. **An authenticator that ignores cancellation keeps running; the gateway
  cannot stop it.** What it still holds is the `ConnectionAuthContext` it was given, not the
  connection.
- Whatever it returns or throws afterwards is observed, logged and discarded. It takes no slot
  and is never left as an unobserved exception.

#### Life of a connection

1. The endpoint accepts the WebSocket and creates the connection object (Accepted).
2. The first packet must be a complete CONNECT, within the connect deadline.
3. CONNECT is checked in this order, with these answers:

   | Check | Refusal |
   |---|---|
   | protocol version is 5 | `0x84` (as today) |
   | the properties of 7.5 | as 7.5 says |
   | no Will | `0x87`, "Will messages are not supported." (7.11) |
   | client id pattern | `0x85` (as today). An empty client id is well-formed MQTT, so the codec accepts it and this check refuses it; it is not a malformed packet. |
   | authenticator, within its deadline | `0x86` with its reason, or `0x88` if it throws or is late (as today) |
   | takeover and the player's connection limit | below |

4. CONNACK: success, session present 0, user property `gw-player`, and the properties 7.5
   lists. A refusal carries the reason code and reason string and then closes.
5. The read loop runs until the connection ends.
6. On the end of the connection, for any reason, the clean-up runs once: remove from the
   gateway's tables, give back the player's slot (unless it was handed on, below), let the
   requests that were already accepted finish, then detach from every room.

#### Takeover and the player's slot

A gateway holds at most one connection per client id.

- **No connection with that client id:** the player needs a free slot
  (`MaxConnectionsPerPlayer`). Taking it is one atomic step that either succeeds or refuses the
  CONNECT with `0x97` and today's reason string. This replaces the two-step check of ADR 0002
  difference 10.
- **A connection with that client id exists and belongs to the same player:** the new
  connection **takes over and inherits the slot**. No second slot is needed, so a tab that
  reconnects while its old connection is still open is not refused when its player is at the
  limit. The old connection gets DISCONNECT `0x8E` (session taken over) if it can be written,
  and is closed. Its clean-up does not give the slot back, because it no longer holds one.
- **A connection with that client id exists and belongs to another player:** the CONNECT is
  refused with `0x85` and "The client id is in use." The existing connection is not touched.

**The commit point.** The three cases above are decided in **one atomic step, after
authentication has succeeded and before CONNACK is handed to the link.** That step either
refuses, or makes the new connection the one in the gateway's table with a slot of its own or
an inherited one. From that step on:

- The new connection is the authoritative one for its client id. An old connection it replaced
  is closing and is not brought back, whatever happens next.
- If the new connection then fails (its CONNACK cannot be written, its socket is already
  gone), that is an ordinary end of a connection: it gives back the slot it holds, inherited or
  not. The player is then one connection down and its client reconnects.
- Two CONNECTs for the same client id that authenticate at the same time each go through the
  step, one after the other. The second replaces the first. One connection and one slot remain.

Invariants:

- Every Active connection holds exactly one slot of its player, and a slot is given back exactly
  once. A takeover moves the slot; it is never counted twice and never dropped.
- A takeover and the old connection's own end can happen at the same moment. Whichever is first
  decides: if the old connection has already given its slot back, the new one takes a fresh
  slot under the first rule.
- Removing a connection from the gateway's tables compares the object, not the client id (as
  today), so an old connection's clean-up can never remove its replacement.
- Detaches sent to rooms name the old connection's generation (as today), so they cannot detach
  the replacement.

Slots and takeover are per gateway process, as limits are today. A reconnect to another gateway
is settled by generations at the room, unchanged.

#### Reading, with no buffer while idle

```
loop:
    wait for data (zero-byte read)            <- an idle connection is parked here, holding no GrainWire buffer
    rent a buffer from the shared pool
    read; handle every complete packet in the buffer
    while a packet is incomplete: read more (growing the buffer up to the packet's limit)
    return the buffer
```

- **A PUBLISH that is too large is refused without being buffered.** The first bytes of a
  packet give its total length. In a PUBLISH that length covers the topic, the packet
  identifier (only at QoS 1 and 2), the properties and the payload, in that order, so the rule
  has three bands:

  | Total length of the PUBLISH | What happens |
  |---|---|
  | up to `MaxPayloadBytes` + 5 KiB | The packet is read whole and judged exactly: a payload over `MaxPayloadBytes` is refused as today, anything else goes through the normal policy. The 5 KiB are room for the topic and the properties. |
  | above that, up to 16 times `MaxPayloadBytes` | **Skip.** Only what is needed to find the packet identifier is read: the topic's length, then the topic's bytes are passed over without being kept, then the identifier. The rest of the packet is discarded as it arrives, across as many WebSocket messages as it takes, until exactly its length has been consumed. Then it is refused. |
  | above 16 times `MaxPayloadBytes` | DISCONNECT `0x95` (packet too large), close. |

  The refusal is `QuotaExceeded` with "The message is too large.", as today, in the form the
  QoS calls for: **a PUBACK at QoS 1, a failing PUBREC at QoS 2 (and nothing is remembered),
  nothing at QoS 0.** A publish whose topic and properties alone take more than 5 KiB is
  therefore treated as too large, whatever its payload. If the skipped part is itself broken
  (the topic's length runs past the packet), that is a malformed packet (`0x81`).
- Any packet other than PUBLISH that is longer than 16 KB closes the connection with `0x95`.
- The connection uses the request's own cancellation (`HttpContext.RequestAborted`) and
  `Abort()` on the link to stop a parked read.
- **What the parked read keeps alive is part of the idle cost.** In C#, every local variable of
  an `async` method that is alive across an `await` becomes a field of an object on the heap,
  and for the read loop that object lives as long as the connection. So: the method that waits
  in the zero-byte read carries as few locals as possible; the work on a rented buffer happens
  in a separate method, whose state exists only while a packet is being handled; and no
  reference to a buffer is still held, in a local or a field, once the buffer has been
  returned (7.6).

#### What each packet does

| Packet | Behaviour |
|---|---|
| PUBLISH, QoS 0 or 1 | The policy of today's `OnPublish`, unchanged: retained refused, topic must be a room command or system topic, payload size, rate limit, pending-request limit. Accepted messages go to the connection's request queue. QoS 1 gets a PUBACK at once: success, or the refusal's reason code and string. QoS 0 gets nothing. |
| PUBLISH, QoS 2 | The same policy. The answer is a PUBREC, and the rest of the exchange is in "QoS 1 and 2 inside a connection" below. When the gateway's `MaxQoS` is 1: DISCONNECT `0x9B` (QoS not supported), close; CONNACK has announced "maximum QoS 1" in that case (7.9). |
| PUBACK, PUBREC, PUBREL, PUBCOMP | See "QoS 1 and 2 inside a connection" below. |
| SUBSCRIBE | One reason code per filter: the exact own inbox is granted the QoS it asked for, capped by `MaxQoS`, and delivery is turned on for this connection at that QoS; every other filter (another inbox, wildcards, `$share/...`) gets `0x87` (not authorized) with today's reason string. Subscription options are accepted and have no effect. |
| UNSUBSCRIBE | The own inbox turns delivery off. UNSUBACK with success, or "no subscription existed". |
| PINGREQ | PINGRESP. |
| DISCONNECT | Close. |
| A second CONNECT, AUTH | DISCONNECT `0x82` (protocol error), close. |
| A malformed packet | DISCONNECT `0x81` (malformed packet), close. |

A connection receives inbox messages only after it has subscribed to its inbox, as today.

#### Sending: accepted at once, written when the socket allows

**The contract.** A send to a connection (the gateway's `PublishAsync`, and the control packets
written by the read loop) **returns as soon as the connection has taken the message or refused
it. It never waits for the socket.** A room's delivery loop therefore runs at the speed of the
gateway, not of its slowest member. Writing to the network is the connection's own business.

A WebSocket allows one write at a time, so:

- **No write in progress:** the caller encodes the packet into a rented buffer and starts the
  write. If the write completes at once, which is the normal case, the buffer goes back to the
  pool and nothing was allocated for this connection. If it does not complete at once, the
  caller returns anyway; the write's completion carries on.
- **A write in progress:** the message is added to what waits for this connection, which is
  created when first needed and dropped when empty. Whoever completes the write in progress
  sends what has been added, by the scheduling rule below.
- The inbox topic is written into the packet from the client id each time. It is not stored per
  connection.
- **A rented encoding buffer is never larger than 64 KB.** A small message is copied behind its
  header into one buffer and written at once. For a larger one the header is written from a
  small buffer and the payload is then written straight from the array the gateway already
  holds, in pieces of at most 64 KB, so no second copy of a large payload ever exists. (MQTT
  packets need not line up with WebSocket messages, 7.3.)

**Scheduling: control packets never wait behind a delivery.** What waits is of two kinds, and
the rule between them is part of the design, because a single ordered list can deadlock (the
client's window is full, its PUBREC arrives, and the PUBREL that would free the window is
stuck behind the delivery that waits for the window):

- **Control packets:** CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK, PINGRESP,
  DISCONNECT. They are never held back by a QoS window. When a write completes, waiting control
  packets are written before any waiting delivery, in the order they were produced, which keeps
  MQTT's ordering of acknowledgements.
- **Deliveries:** inbox messages, in the order the gateway handed them over. A delivery at QoS
  1 or 2 that has no room in the client's window waits, and so do the deliveries after it (their
  order is kept). It never holds back a control packet.
- **What waits as control is bounded.** Answers to the client's publishes are bounded by the
  receive maximum the server announced (below). At most one PINGRESP waits: a PINGREQ that
  arrives while one is waiting adds nothing. Whatever the mix, more than 1,024 waiting control
  packets evict the connection, and their bytes count towards `MaxOutgoingBytes`.
- How this is stored (two lists, or one with a marker) is the builder's choice. The rule is not.

**Limits on what waits** (the owner's decisions A, C and H):

- `MaxOutgoingQueue` (existing, default 1,000) counts **deliveries that are waiting**: behind a
  write in progress, or for room in a QoS window.
- `MaxOutgoingBytes` (**new**, default 1 MiB, adjustable, at least 64 KiB) is a **hard bound**
  on the payload bytes this connection holds on the way out: every waiting delivery, the
  delivery being written, and the waiting control packets. There is no exception for a single
  large message.
- **A single message larger than `MaxOutgoingBytes` is never sent.** The connection it was for
  is closed with `0x95` (packet too large), and an error is logged that names the room, the
  message's size and the option. A snapshot of that size is a fault of the room's design, and
  it fails where it can be seen.
- A connection that would pass a limit is **evicted**: it is closed as below with `0x97` (quota
  exceeded). It reconnects and gets a snapshot.

**What the limits promise, exactly.** A connection has waiting deliveries only while a write is
in progress or a window is full. A burst can outrun even a healthy socket for a moment, because
a write that the network has not yet taken is "in progress". So the promise is not "a client
that reads normally survives any burst". It is: **a client survives a burst that fits within
the two limits; one that falls further behind is disconnected.** The network's own buffers
absorb some data before a write stalls, so a slow consumer is noticed after "what those buffers
hold, plus the limits", not at the exact message count.

**Worst case held for one connection** (with the defaults):

| What | Bound |
|---|---|
| Before CONNECT is complete | one read buffer of at most 16 KB, for at most 10 s |
| Read buffer while a packet is arriving | `MaxPayloadBytes` + 5 KiB (64 KB + 5 KiB), for at most 10 s per packet |
| Accepted requests waiting for their room | `MaxPendingRequests` × `MaxPayloadBytes` (32 × 64 KB = 2 MiB), as today |
| On the way out | `MaxOutgoingBytes` (1 MiB), plus one encoding buffer of at most 64 KB |
| QoS exchange records | 256 for what the client sends, 256 for what the server sends, a few bytes each |
| Waiting control packets | 1,024, counted in `MaxOutgoingBytes` |
| How long a stalled connection holds any of it | the write deadline (15 s) |

**A client's "maximum packet size".** A CONNECT that announces less than 4 KiB is refused with
`0x95`. Every control packet the server sends then fits, because the variable parts of a control
packet are bounded: a reason string is cut to at most 256 bytes (an authenticator's failure
reason included), and a player id is at most 256 characters (as today). A reason string that
would still make a packet too large for the client is left out, as MQTT requires. A message for a connection that is
larger than what that connection announced cannot be sent to it, and dropping it would leave the
client re-requesting the same snapshot for ever, so the connection is closed with `0x95` and the
reason "A message is larger than the maximum packet size the client announced." protocol.md
tells client authors to leave the property unset, or above their largest snapshot.

Sending several waiting packets in one WebSocket message is allowed by MQTT and is left to the
builder as an optimisation, if a measurement shows it helps.

#### QoS 1 and 2 inside a connection

The owner's decision F: the server supports all three QoS levels, in both directions, and the
**connecting client chooses**: per publish for its commands, and by the QoS it subscribes to
its inbox with for what it receives. Whoever chooses QoS 1 or 2 pays for it; a connection that
uses QoS 0 for its inbox and QoS 1 for its commands, as both GrainWire clients do by default,
costs exactly what it costs without this section.

**What it is and is not.** The exchanges below are complete and standard, so any MQTT 5 client
can use the QoS it prefers. They do not add a guarantee across reconnects: there is no stored
session, so nothing is kept for a client that is away, and MQTT 5 forbids sending a message
again inside a connection. The state below is therefore **a small record per exchange, never a
payload**.

*Exchange records.* A bare set of identifiers is not enough: the server must know what kind of
exchange an identifier belongs to, how far it has got, and since when. Each exchange in flight
has a record:

```text
packet identifier
kind and phase:   command at QoS 2, waiting for PUBREL
                  delivery at QoS 1, waiting for PUBACK
                  delivery at QoS 2, waiting for PUBREC
                  delivery at QoS 2, waiting for PUBCOMP
when it entered this phase
```

- **A record is in place before its packet is handed to the link**, so an acknowledgement that
  comes back at once always finds it.
- **The acknowledgement deadline (30 s) runs per phase**, from the moment the record enters the
  phase, and starts again when a QoS 2 exchange moves to its next phase. The sweep looks at the
  oldest record of a connection.
- **An acknowledgement of the wrong kind for a known identifier is a protocol error** (`0x82`,
  close): a PUBACK for a QoS 2 delivery, a PUBCOMP for a delivery still waiting for PUBREC, and
  so on. One repetition is harmless and expected by MQTT: a PUBREC for a delivery already
  waiting for PUBCOMP is answered with PUBREL again and changes nothing.
- The records of a connection are created with the first exchange and dropped with the last.

*Commands at QoS 2 (the server receives):*

1. PUBLISH at QoS 2 with identifier *n*, not seen before: the policy runs as for QoS 1. If the
   publish is accepted, it goes to the request queue, *n* is remembered, and the answer is
   PUBREC (success). If it is refused, the answer is PUBREC with the refusal's reason code and
   string, and *n* is **not** remembered: a failed PUBREC ends the exchange.
2. PUBLISH with an identifier that is remembered: it is not handled again; the answer is PUBREC
   (success) again. **This holds whatever the packet's DUP flag says.** MQTT's rule for the
   receiver is to acknowledge any further PUBLISH with that identifier until PUBREL arrives,
   and it does not make the DUP flag a condition (section 13 has the sources).
3. PUBREL *n*: *n* is forgotten; the answer is PUBCOMP. For an identifier that is not
   remembered, PUBCOMP carries `0x92` (packet identifier not found).
4. CONNACK announces **"receive maximum" 256**. Against it the server counts, together, the
   QoS 2 publishes whose PUBCOMP it has not yet written and the QoS 1 publishes whose PUBACK it
   has not yet written. A client that passes 256 is closed with `0x93` (receive maximum
   exceeded). This is also what bounds the acknowledgements that can be waiting for a client
   that does not read. A PUBREL that does not come within the acknowledgement deadline closes
   the connection.

*Deliveries at QoS 1 or 2 (the server sends):*

1. The gateway hands every inbox message to the connection as before. The connection sends it
   at the QoS granted to the inbox subscription.
2. At QoS 1 the PUBLISH carries an identifier, which is in use until the client's PUBACK. At
   QoS 2 it is in use until PUBCOMP: PUBLISH, the client's PUBREC, the server's PUBREL, the
   client's PUBCOMP. A PUBREC with a failure code ends the exchange and frees the identifier.
3. Identifiers come from a counter (1 to 65535, wrapping) that skips those that have a record.
4. **The client's "receive maximum" is honoured:** the server's window for a connection is the
   client's receive maximum or 256, whichever is smaller, and no more than that many deliveries
   are unacknowledged at once. A place in the window is free again on PUBACK, on PUBCOMP, and
   on a PUBREC with a failure code, which is the rule MQTT gives. A message that has to wait for room in that window waits in the
   outgoing list, and **counts towards `MaxOutgoingQueue` and `MaxOutgoingBytes`** like a
   message behind a stalled write. A client that asks for QoS 1 or 2 and does not acknowledge
   is evicted by those limits, or by the acknowledgement deadline.
5. A PUBACK, PUBREC or PUBCOMP for an identifier that has no record is ignored, except that an
   unknown PUBREC is answered with a PUBREL carrying `0x92`, as MQTT asks.
6. `seq`, snapshots and sync are untouched. They, not the QoS, are what orders events and
   repairs a loss.
7. **Subscribing again, or unsubscribing, while deliveries are in flight.** An exchange that has
   started finishes at the QoS it started with. Deliveries that are waiting and have not
   started are sent at the newly granted QoS after a new SUBSCRIBE, and are dropped after an
   UNSUBSCRIBE.

*The operator's cap.* `GrainWireGatewayOptions.MaxQoS` (**new**, 1 or 2, default 2) lets a
gateway refuse QoS 2: publishes at QoS 2 are refused as the table above says, and an inbox
subscription asking for QoS 2 is granted QoS 1. QoS 1 cannot be switched off, because commands
are published at QoS 1 and their refusals travel in the PUBACK.

*What it costs those who choose it.* An inbox at QoS 1 doubles the packets of every delivery,
and at QoS 2 it quadruples them; in a room of 1,000 members that is per member, per event. The
connection also keeps its exchange records while deliveries are in flight. The benchmark
reports this (section 8).

#### Closing, bounded

Every close the server starts follows the same steps, and none of them waits on the peer:

1. Enter Closing (once). No new message and no new request is accepted.
2. **If a write is in progress, abort the link now.** Nothing more is sent: a socket that is
   not taking data is never asked to carry its own eviction notice, and a WebSocket close would
   only queue behind the write that is stuck.
3. If no write is in progress: hand DISCONNECT with the reason code to the link, then start the
   WebSocket close, through the same one-at-a-time send path as every other write, without
   waiting for the peer's answer. The link is aborted at the close deadline.
4. **Aborting does not release anything by itself** (7.6). The read loop and the write in
   progress, if there is one, each end with an error once the link is aborted; each returns its
   own buffer when it ends.
5. The connection is done, and the clean-up of its gateway state runs, when the read loop has
   ended and no write is in progress. Only then does the endpoint's request return to Kestrel.

#### When the gateway stops

Today MQTTnet's hosted server takes part in the host's shutdown. The new gateway must do so
itself, in this order, and within a bound:

1. Stop accepting: a new WebSocket request is answered with HTTP 503.
2. Every connection is closed as above with `0x8B` (server shutting down).
3. Requests that were already accepted may finish, for at most 5 s in total. After that the
   gateway stops waiting for them: their answers are dropped, and what the rooms were not told
   is repaired by the rooms' leases, exactly as when a gateway dies.
4. Links that are still open are aborted.
5. The gateway waits for the read loops and writes to end, so that every buffer is back.
6. The sweep stops.

An app's room handler that never returns must not keep the process from stopping; step 3 is the
bound that prevents it.

#### Requests, with no worker while idle

Today every connection has a bounded channel and a worker task parked on it. The rule that must
survive is: **a connection's requests are handled one at a time, in the order they arrived, and
at most `MaxPendingRequests` wait.** The mechanism becomes the same "only while there is work"
pattern as sending: a request that arrives when nothing is running starts the handler; requests
that arrive meanwhile are appended to a list created on demand; the handler takes them in order
and stops when the list is empty.

The clean-up rule also survives, exactly as the code has it today (`Gateway.cs`: the queue is
completed on disconnect, the worker drains what is left, and the clean-up awaits the worker):

- After a connection is closed, **no new request is accepted** from it.
- **Every request accepted before the close is still handled**, in order. A command that got
  its PUBACK just before the tab closed is applied, as it is today.
- The detach from the connection's rooms happens only after the last of them has finished.
- A join that completes after the close is undone again (today: the `connection.Closed` check
  at the end of `JoinAsync`).
- **Replies and snapshots are addressed by client id and looked up when they are sent**, as
  today. A reply to a request of a connection that has since been replaced reaches the
  replacement, if it has subscribed; otherwise it is dropped, and the client's re-sent request
  gets the recorded outcome.

#### Smaller per-connection state

The rooms a connection has joined (at most `MaxRoomsPerConnection`, default 20) become a small
list created on the first join; the rate limiter's two numbers live in the connection object.
These and the gateway's per-room tables (`_directory`) are tuned by what the measurement of
step 0 shows; the design does not prescribe their layout.

### 7.5 What the server does with each MQTT property

"Ignored" means: validated for form, then without effect, which MQTT 5 allows for that property.
It never means "skipped by its length without looking": an ignored property must still be one
that is allowed in that packet, appear no more often than it may, and hold a value of the right
type.

**Which error is which.** Two failures are kept apart, in the codec's results and in the
reason code sent:

- **Malformed packet (`0x81`):** the bytes do not form a valid packet. A length that runs past
  the packet, a bad variable-length integer, invalid UTF-8, wrong reserved flags, an unknown
  property, a property in a packet it does not belong to, a once-only property twice.
- **Protocol error (`0x82`):** the packet is well formed, but not allowed now or with that
  value. A second CONNECT, an acknowledgement of the wrong kind (7.4), receive maximum 0,
  maximum packet size 0, a subscription identifier in a client's PUBLISH, an AUTH packet.

Where MQTT names a more specific reason code for a case, that one is used; the tables give it.

**CONNECT**

| Property | Treatment |
|---|---|
| Session Expiry Interval | Overridden. The server keeps no session. If the client asks for more than 0, CONNACK carries Session Expiry Interval 0. |
| Receive Maximum | **Honoured** for deliveries at QoS 1 and 2 (7.4). It has no effect on an inbox at QoS 0. The value 0 is a protocol error. |
| Maximum Packet Size | Honoured (7.4). The value 0 is a protocol error; less than 4 KiB is refused. |
| Topic Alias Maximum | Ignored: the server never uses aliases. |
| Request Response Information | Ignored: the server sends no response information. |
| Request Problem Information | **Honoured.** When it is 0, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK and UNSUBACK carry their reason code but no reason string and no user property. CONNACK and DISCONNECT may still carry them. |
| User Property | Ignored. |
| Authentication Method, Authentication Data | Refused: CONNACK `0x8C` (bad authentication method). |
| Will flag and Will properties | Refused: CONNACK `0x87` (7.4). |

**PUBLISH from a client**

| Property | Treatment |
|---|---|
| Payload Format Indicator, Content Type, Message Expiry Interval, Response Topic, Correlation Data, User Property | Ignored. The gateway reads the payload as JSON whatever they say, stores no message and forwards none. |
| Topic Alias | Refused: DISCONNECT `0x94` (topic alias invalid). The server announces no alias maximum. |
| Subscription Identifier | Protocol error (`0x82`): a client may not send it in a PUBLISH. |

**SUBSCRIBE, UNSUBSCRIBE, DISCONNECT from a client**

| Property | Treatment |
|---|---|
| Subscription Identifier (SUBSCRIBE) | Refused: DISCONNECT `0xA1` (subscription identifiers not supported). |
| User Property, Reason String, Session Expiry Interval (DISCONNECT) | Ignored. |

**CONNACK from the server**

Always: session present 0, `gw-player`, Receive Maximum 256, Subscription Identifiers Available
0. When needed: Maximum QoS 1 (only when the gateway's `MaxQoS` is 1), Session Expiry Interval
0, Server Keep Alive, Reason String. The other capabilities the server may announce are governed
by 7.9.

**What the client does with CONNACK**

| Property | Treatment |
|---|---|
| Session Present | Must be 0; otherwise the connection is closed (the client asked for a clean start). |
| Reason String, User Property | Used: the refusal's message; `gw-player`. |
| Server Keep Alive | Honoured. |
| Receive Maximum | Honoured (7.8). |
| Maximum Packet Size | Honoured: a larger publish is not sent, and is treated as not sent. A GrainWire gateway does not announce one. |
| Maximum QoS | Honoured: commands are published at the lower of `CommandQoS` and this. 0 means the client cannot work with this server: the connection attempt fails. |
| Everything else | Ignored. |

### 7.6 Who owns a buffer

Rented memory crosses asynchronous steps in several places. The rules:

1. **A decoded view is valid only until the buffer it points into is returned or reused.**
   Nothing that outlives the handling of one packet may keep a view.
2. **An accepted request owns its payload.** The payload is copied out of the read buffer when
   the request is accepted, as today's code does (`Gateway.cs`, `OnPublish`). The topic is
   parsed into its own value at the same moment.
3. **A message waiting to be sent is a payload the garbage collector owns** (the arrays the
   gateway's `Envelopes` produce today), so a waiting message never pins pooled memory.
4. **A buffer handed to the link belongs to that operation until the operation has ended.**
   "Ended" means its task has completed, with success or with an error. Asking for an abort is
   not the end of anything: a write or a read can still be touching its buffer after `Abort()`
   has returned.
5. **Only the code that awaits an operation returns that operation's buffer, and only after the
   await has ended.** Whoever aborts, closes or evicts a connection never returns a buffer on
   its behalf. If an operation were never to end, its buffer would be lost to the pool; that is
   a leak, which the tests catch, and never memory that two connections share.
6. **Every way out returns what is held, once:** normal completion, a refusal, an overflow, a
   deadline, an exception, an abort.
7. **Nothing keeps a reference to a buffer it has returned.** A local or a field that pointed at
   a rented array is cleared when the array goes back, before the next wait.

### 7.7 Gateway changes

- `Attach(MqttServer)` and the seven event handlers go. Their logic moves into methods the
  connection calls (`Authenticate`, `OnPublish`, and so on). The policy code, its messages and
  its reason codes stay as they are.
- `PublishAsync` looks the connection up in `_connections` by client id (as the callers already
  do) and calls its send, which returns at once (7.4). The `OutboundFilter` test hook stays.
- `DisconnectAsync` closes the connection directly. `tests/GrainWire.TestHost`'s
  `/control/disconnect` endpoint uses it in place of `MqttServer.DisconnectClientAsync`.
- `Wire.GatewaySender` and the "message injected by the gateway itself" check are no longer
  needed.
- `GatewayExtensions`: `AddGrainWireGateway` stops registering MQTTnet and the Connections
  layer. `MapGrainWire` adds the WebSocket middleware and maps `options.Path` to the endpoint
  that accepts the WebSocket and runs the connection.
- The delivery code (`PublishBatchAsync`, `EventTemplate`, `Envelopes`, `BatchSequencer`) is not
  changed.

### 7.8 The client connection

An internal class used by `GrainWireClient` where it uses `IMqttClient` today. As today, every
connection attempt gets a new instance, so nothing of a broken connection survives into the
next one.

- **Connect:** open the link, send CONNECT (protocol 5, clean start, client id, user name,
  password = access token, keep-alive, receive maximum 1,024; no Will, no maximum packet size),
  wait for CONNACK within `ConnectTimeout`. Gives back the reason code, the reason string and
  the user properties (`gw-player`).
- **The app's choice of QoS** (decision F), two new options: `CommandQoS` (1 or 2, default 1)
  for what the client publishes, and `InboxQoS` (0, 1 or 2, default 0) for what it receives.
  The defaults are today's behaviour and cost nothing extra.
- **Subscribe:** the inbox at `InboxQoS`. SUBACK must grant the subscription; a server may
  grant a lower QoS than asked, and the client then works at what was granted.
- **Packet identifiers.** PUBLISH and SUBSCRIBE draw from one space (1 to 65535). The client
  keeps a record for each identifier in use, like the server's exchange records (7.4): the
  identifier, what it belongs to (a subscribe, a publish at QoS 1, a publish at QoS 2 waiting
  for PUBREC or for PUBCOMP), and when it entered that phase. The record is in place before the
  packet is handed to the link. An acknowledgement of the wrong kind for a known identifier (a
  PUBACK for a subscribe, a SUBACK for a publish, a PUBCOMP before the PUBREC) is a protocol
  error and a lost connection. The rules:
  - a new identifier is the next one after the last, skipping any that is in use, wrapping from
    65535 to 1;
  - an identifier is in use from the moment its packet is handed to the link until the exchange
    ends: SUBACK for a subscribe, PUBACK for a publish at QoS 1, and for a publish at QoS 2 the
    PUBCOMP that follows the server's PUBREC and the client's PUBREL, or a PUBREC with a failure
    code. The reason code in a PUBACK or PUBREC is otherwise ignored, as today (the reply, or
    the lack of one, is what `GrainWireClient` acts on);
  - a PUBACK, PUBCOMP or SUBACK for an identifier that is not in use is ignored; an unknown
    PUBREC is answered with a PUBREL carrying `0x92`;
  - a packet whose write fails never frees its identifier on its own: a failed write is a lost
    connection, and the whole set goes with the connection;
  - the set may hold at most the server's "receive maximum", and never more than 1,024. A
    publish that would pass that is not sent, and is treated as not sent: the request is re-sent
    by the existing retry timer;
  - **if the oldest identifier in use is older than twice the keep-alive interval, the
    connection is treated as lost**, even while PINGRESPs arrive. This is what stops a
    connection from staying "healthy" with its publish window used up.
- **No MQTT-level re-send.** `GrainWireClient` already re-sends requests with the same command
  id, and a new connection starts with an empty set.
- **Receive:** a PUBLISH's payload is handed to `GrainWireClient.Receive` as a span, with no
  copy when it lies in one buffer. `Receive` parses it before it returns, so rule 1 of 7.6
  holds. A PUBLISH at QoS 1 is handed on and answered with PUBACK. A PUBLISH at QoS 2 is handed
  on once and answered with PUBREC; its identifier is remembered until the server's PUBREL,
  which is answered with PUBCOMP; a PUBLISH that repeats a remembered identifier is not handed
  on again. The set of remembered identifiers exists only while there are any, and cannot pass
  the receive maximum the client announced.
- **Keep-alive:** a PINGREQ when nothing was sent for the keep-alive interval; no PINGRESP
  within the interval means the connection is lost. This is driven by `GrainWireClient`'s
  existing ticker, not by a new timer.
- **Lost connection:** reported once, to `ConnectionLost`, as `DisconnectedAsync` is today. A
  DISCONNECT from the server, a failed write and a failed read are all a lost connection.
- **Order:** publishes started under the client's lock reach the connection in that order
  (`PublishLocked` today). The connection must keep that order on the wire.
- The client has one connection, so it does not need the server's "only while there is work"
  machinery. A plain send lock or queue is fine. It must not block a thread (Blazor
  WebAssembly).
- **The client cannot have the server's deadlock, and must keep it that way.** Nothing on the
  client waits for a QoS window: a publish that has no room in the server's window is not sent
  at all. So everything the client writes, its publishes and its acknowledgements (PUBACK,
  PUBREC, PUBREL, PUBCOMP, PINGREQ), goes out in the order it was produced, and an
  acknowledgement never stands behind something that is waiting for the server.
- **Options are checked when the client is created.** `KeepAlive` is between 1 second and 65,535
  seconds and is rounded up to whole seconds; `CommandQoS` and `InboxQoS` hold only the values
  7.12 allows. `ConfigureWebSocket` runs before the client adds the `mqtt` sub-protocol, so the
  callback cannot remove it, and an exception it throws fails that connection attempt as a
  `ConnectException`. In Blazor WebAssembly the browser supports only part of
  `ClientWebSocketOptions` (no request headers, no TLS or proxy settings); the API reference
  says so.

The transport behind the link is created through an internal factory on the options. The
default is `ClientWebSocket`. `ClientProtocolTests` points the client at `ScriptedGateway`,
which is an MQTTnet broker on raw TCP, with `ConfigureMqtt = mqtt => mqtt.WithTcpServer(...)`;
those tests supply a TCP link through the internal factory in its place.

### 7.9 Tests

1. **Codec** (`tests/GrainWire.Mqtt.Tests`):
   - known byte sequences for every packet in the table of 7.2, in both directions;
   - every malformed case listed there, and every row of 7.5;
   - every packet stream cut at every byte position and fed in pieces;
   - a seeded random test: valid packet streams split and joined at random positions must
     decode to the same packets; random and mutated bytes must end in a packet or an error
     result, never an exception, a hang, or a read outside the buffer. "No exception" is not
     the whole property. The test also asserts: the decoder allocates nothing; the work it does
     is bounded by the bytes it is given; every call either consumes bytes or reports that it
     needs more (no loop without progress); and a packet that is incomplete or rejected changes
     no state. A failing input is added to the fixed test cases with its seed.
2. **Server connection over an in-memory link:**
   - every row of "what each packet does", of the deadlines table and of the CONNECT checks;
   - several packets in one message; a packet split across messages; an oversized PUBLISH
     refused without being buffered;
   - takeover: by the same player (also at the connection limit), by another player, during
     the old connection's authentication, while a request of the old connection is running, and
     at the same moment as the old connection's own end; the slot count is exact after each;
   - a stalled write: the write deadline aborts, no DISCONNECT is attempted, and what was held
     is released;
   - a client with a small "maximum packet size";
   - "request problem information" 0: refusals carry no reason string;
   - **QoS 2 commands:** the full exchange; a refused publish answered by a failing PUBREC and
     not remembered; a repeated identifier not handled twice; PUBREL for an unknown identifier;
     more than the receive maximum waiting for PUBREL; a PUBREL that never comes; `MaxQoS` 1;
   - **deliveries at QoS 1 and 2:** the full exchanges; the client's receive maximum is never
     passed; messages waiting for the window count towards the outgoing limits and evict a
     client that does not acknowledge; the acknowledgement deadline; unknown acknowledgements;
     identifiers across the wrap; and after the last acknowledgement the connection holds no
     set (idle again).
3. **"Only while there is work", send path and request path.** Two kinds of test, both required:
   - **Deterministic:** the in-memory link lets the test hold a write open and release it at a
     chosen point, so the exact interleaving "the drain has just found the list empty" and
     "another producer adds a message" is forced, in both orders. The same for the request
     path.
   - **Stress:** many threads add work to one connection while the link completes writes
     sometimes at once and sometimes later; everything is handled exactly once, in order, and
     the connection ends idle (no list, no drain running). The request path's test also closes
     the connection at a random moment and asserts the clean-up rule of 7.4.
4. **Buffers.** A counting pool in the tests asserts that every rented buffer is returned
   exactly once across: normal traffic, every refusal, overflow, each deadline, an exception in
   a handler, and abort. Counting finds leaks and double returns but not a buffer that is used
   after it went back, so a second test pool **overwrites every returned buffer and hands it out
   again at once**; with it, a link that holds a read or a write open is aborted, and the test
   asserts that the operation still in flight never sees or changes the reused memory. The
   same is done with the abort arriving at the same moment as the operation's completion. A test of repeated connect, work and disconnect cycles asserts that
   nothing accumulates: buffers, table entries, slots, room attachments.
5. **Slow recipient.** In one room, one member stops reading; the other members keep receiving
   every event within the test's latency bound, and the stalled member is evicted by the limits
   or the write deadline.
6. **Client connection:** packet identifiers across the wrap from 65535 to 1 with identifiers
   still in use; a PUBACK that never arrives leads to a lost connection; an unknown PUBACK is
   ignored; a server that announces a small receive maximum; commands at QoS 2 and an inbox at
   QoS 1 and 2, including a server that grants less than was asked.
   **QoS interop:** a subset of the room tests is run again with QoS 2 in both directions in
   each pairing of item 7 (MQTTnet's client and MQTT.js against the new server; the new client
   against MQTTnet's broker and against the new server).
7. **Interop, with the existing tests unchanged in what they assert** (except as 7.10 says):
   - new server with MQTTnet's client: `GatewayTests`, `FailoverTests`, `RoomPersistenceTests`
     through `TestClient`;
   - new server with MQTT.js: `clients/ts` integration tests;
   - new client with MQTTnet's broker: `ClientProtocolTests` through `ScriptedGateway`;
   - new client with new server: `ClientIntegrationTests`.
8. **The chess sample, at the end of steps 3 and 4** (it is not part of "every test green" for
   the other steps, because it needs a browser):
   - the Playwright test `samples/Chess/Chess.WebClient/e2e/chess.spec.ts`, which drives MQTT.js
     in a browser against `Chess.Server`;
   - the Blazor client, run by hand once: two tabs play a few moves, one reloads and gets the
     position back.

   (`ChessSampleTests` in the main test project only tests the chess rules and has no bearing
   on this work.)

9. **Added after the reviews of v4.** All over the in-memory link unless said otherwise:
   - **The window deadlock, step by step:** the client announces receive maximum 1; delivery A
     goes out at QoS 2; delivery B waits; the client's PUBREC for A arrives; the PUBREL is
     written although B is still waiting; PUBCOMP arrives; B goes out. The same with a PINGREQ
     and with a command at QoS 1 arriving while B waits: their answers are written at once.
   - An acknowledgement of the wrong kind for a known identifier; an acknowledgement that
     arrives while the write of its own packet has not completed yet.
   - A QoS 2 PUBLISH that repeats a remembered identifier, once with DUP 0 and once with DUP 1:
     both get a PUBREC, neither is handled twice.
   - Many small valid packets (PINGREQ, commands at QoS 1) from a client that reads nothing:
     at most one PINGRESP waits, the receive maximum closes the connection, and what waits never
     passes the bounds of 7.4.
   - A PUBLISH with a large topic or large properties and a small payload, in each of the
     three bands of 7.4, at QoS 0, 1 and 2, cut across WebSocket messages at every position.
   - One connection that subscribes, unsubscribes and changes its QoS while deliveries are in
     flight and waiting.
   - An authenticator that never returns and ignores cancellation: the socket is closed at the
     deadline, no slot is taken, the connection object is not kept, and the late result is
     observed.
   - Takeover: two CONNECTs for one client id finishing authentication together; a new
     connection that fails right after the commit point.
   - **The gateway stopping** while connections are active, while a takeover is under way, and
     while a room handler does not return: it stops within its bound and every buffer is back.
   - **Bounds are asserted after every step of these tests, not only at the end:** the number of
     records, of waiting packets and bytes, of rented buffers.

If an existing test other than those of 7.10 cannot pass without changing what it asserts, that
is a behaviour change: it is brought to the owner, not adjusted.

**What CONNACK announces.** Three groups:

- **Always announced: "subscription identifiers available 0" and "receive maximum 256"; and
  "maximum QoS 1" when the gateway's `MaxQoS` is 1.** In MQTT 5 a capability that is not
  announced counts as supported, so the server may refuse a subscription identifier (`0xA1`) or
  a QoS 2 publish (`0x9B`) only if it has said so in CONNACK. The receive maximum is what
  bounds the identifiers a client can make the server remember (7.4). If announcing these
  changes the outcome of an existing test, that goes to the owner; it is not a reason to leave
  them out.
- **Announced only if no existing test changes: no wildcard and no shared subscriptions.** The
  server refuses those per filter with `0x87`, which needs no announcement, and a client library
  that honours the announcement might fail locally where a test expects the refusal in SUBACK.
- **Deliberately not announced: "maximum packet size" and "retain available".** A client library
  that honours them would fail locally where protocol.md promises a refusal in the PUBACK.

### 7.10 Existing tests that change, with the owner's decisions

| Test | Today | After | From |
|---|---|---|---|
| `FailoverTests.A_client_that_cannot_keep_up_is_disconnected_and_cleaned_up_and_can_come_back` | `MaxOutgoingQueue = 2` and a burst of 5,000 events disconnect two clients that are reading normally. | The slow client is one that has **stopped reading**; it is disconnected and cleaned up and can come back, as the test's name says. A second assertion is new: a client that keeps reading survives a burst **that fits within the limits the test configures** (7.4 says why "any burst" cannot be promised). | decision A |
| `GatewayTests.A_will_message_cannot_reach_another_client` | A client connects with a Will addressed to another inbox, is aborted, and the other inbox stays empty. | The CONNECT with a Will is refused with `0x87`, and the other inbox stays empty. | decision B |
| A new test beside `A_burst_of_connections_cannot_exceed_the_limit_per_player` | (none) | A player at the limit reconnects with a client id it already has open and is admitted; the count is still at the limit afterwards. | decision D |

No existing test reuses a client id for a different player (they reuse it for the same player,
in `FailoverTests` and `GatewayTests`), so the refusal of decision D changes none of them.

### 7.11 Changes in behaviour

| # | Today | After | Why |
|---|---|---|---|
| 1 | A Will is treated like any other publish from that client (protocol.md, section 1). | A CONNECT that carries a Will is **refused** (`0x87`). | The owner's decision B, and what the original design asked for. MQTT requires a server that accepts a Will to publish it; GrainWire has no topic a Will may go to. |
| 2 | Every message for a connection is queued; more than `MaxOutgoingQueue` at once disconnects it, even if it is reading normally. | Only deliveries that are waiting count (behind a write in progress, or for a QoS window); a hard byte limit applies beside the count. | Decisions A, C and H. A client is no longer dropped by a burst that fits within the limits, and a slow one is bounded in bytes. |
| 3 | A slow consumer loses a message at the limit and is then disconnected (ADR 0002, difference 4). | It is disconnected at the limit, or when one write of at most 64 KB has not completed after 15 s. | What the original design asked for, plus a bound in time. |
| 4 | Connections per player are checked twice (ADR 0002, difference 10). A reconnect with a client id that is still open needs a second slot. | Checked once, atomically. A takeover by the same player inherits the slot. | Decision D. |
| 5 | A second connection with a client id takes over whoever it belongs to. | A client id that is open for another player is refused (`0x85`). | Decision D. A client id names an inbox; another player must not be able to cut it off. |
| 6 | A client that asks for no keep-alive gets none. | It is told 60 s ("server keep alive"). | Without a keep-alive, a connection that vanished without closing is held until the operating system notices. |
| 7 | WebSocket ping frames are sent by the server. | They are not; MQTT's keep-alive is the only one. | One timer less per connection. |
| 8 | A client's "maximum packet size" is whatever MQTTnet makes of it. | Less than 4 KiB is refused; a message that does not fit closes the connection with `0x95`. | A silently dropped snapshot would leave the client asking for it for ever. |
| 9 | Refusals always carry a reason string. | Not in PUBACK, SUBACK and UNSUBACK when the client set "request problem information" to 0. | MQTT 5 forbids it. Neither GrainWire client sets it. |
| 10 | Enhanced authentication and subscription identifiers are whatever MQTTnet makes of them. | Refused (`0x8C`, `0xA1`). | Not supported; said clearly. |
| 11 | CONNACK announces what MQTTnet's broker supports. | CONNACK announces "no subscription identifiers", "receive maximum 256", and "maximum QoS 1" when the gateway is capped at QoS 1. | MQTT requires the announcement before the server may refuse them (7.9). |
| 12 | Every inbox message is delivered at QoS 0, whatever QoS the client subscribed with (protocol.md, section 1). | It is delivered at the QoS the client subscribed with, capped by `MaxQoS`. | Decision F. Both GrainWire clients subscribe at QoS 0 by default, so nothing changes for them. A third-party client that subscribed at QoS 1 or 2 now gets what it asked for. |
| 13 | A command published at QoS 2 is whatever MQTTnet makes of it. | It is accepted through the standard QoS 2 exchange. | Decision F. |
| 14 | A message of any size is sent to a connection. | A message larger than `MaxOutgoingBytes` (1 MiB by default) is not sent; the connection is closed with `0x95` and an error is logged. | Decision H. The limit is a hard bound, so a room whose snapshot outgrows it fails visibly. |
| 15 | A reason string is as long as its source makes it. | It is cut to 256 bytes, and left out where the client forbids it or it would not fit the client's maximum packet size. | So that every control packet has a known largest size. |
| 16 | When the host stops, MQTTnet's server stops with it. | The gateway closes every connection with `0x8B` (server shutting down) and stops within a bound (7.4). | The gateway now owns its shutdown. |

### 7.12 Public API changes

| Package | Change |
|---|---|
| `GrainWire.Client` | `GrainWireClientOptions.ConfigureMqtt` (`Action<MqttClientOptionsBuilder>`) is **removed**. |
| `GrainWire.Client` | **New** `ConfigureWebSocket` (`Action<ClientWebSocketOptions>`): TLS options, request headers, proxy, cookies. Called for every connection attempt. |
| `GrainWire.Client` | **New** `KeepAlive` (`TimeSpan`, default 15 s, which is MQTTnet's default today). |
| `GrainWire.Client` | `ConnectException.ReasonCode` keeps its meaning (the CONNACK reason code). |
| `GrainWire.Server` | **New** `GrainWireGatewayOptions.MaxOutgoingBytes` (`int`, default 1 MiB): a hard bound, set like the other gateway limits when the gateway is configured. `MaxOutgoingQueue` keeps its name and default; its documentation says what it counts now. |
| `GrainWire.Server` | **The gateway's options are checked at start-up**, and a gateway with an invalid value does not start: `MaxOutgoingBytes` at least 64 KiB; `MaxQoS` 1 or 2; the other limits positive; and no combination that overflows when multiplied (for example 16 times `MaxPayloadBytes`). |
| `GrainWire.Server` | **New** `GrainWireGatewayOptions.MaxQoS` (1 or 2, default 2): the highest QoS this gateway accepts and grants. |
| `GrainWire.Server` | The deadlines of 7.4 are internal constants. |
| `GrainWire.Client` | **New** `CommandQoS` (1 or 2, default 1) and `InboxQoS` (0, 1 or 2, default 0), with a small public enum for the three levels. |
| TypeScript client | **New** options `commandQos` and `inboxQos` with the same meaning and defaults. They are passed to MQTT.js, which already implements all three levels. |

The packages are a preview and not published, so the removal needs no deprecation step.

### 7.13 Documentation

- A new ADR (`docs/adr/0003-own-mqtt-implementation.md`): why, what was measured in step 0, and
  the differences of 7.11.
- `protocol.md`: the Will row; the client-id rule of 7.11 (5); the two QoS rows (a client may
  publish at QoS 1 or 2 and receives at the QoS it subscribed with), with a plain statement
  that a higher QoS costs more packets and adds no guarantee across reconnects, which come from
  `cmd`, `seq` and sync; a short section "The part of MQTT 5 a client needs" with the packets
  of 7.2, the property table of 7.5 and the advice on "maximum packet size", for authors of
  other clients.
- `docs/api-reference.md` and both client READMEs: the QoS options, with the same statement.
- `docs/security.md`: the Will line; the new byte limit, that it is a hard bound, and how to
  choose it (about four times the largest snapshot an app expects); the table of what one
  connection can hold at most (7.4); the log reasons of 7.15; a line on Kestrel's
  `MaxConcurrentUpgradedConnections`, which is unlimited by default and is the place to cap open
  WebSockets per gateway.
- `README.md`, `CHANGELOG.md`, `docs/hosting.md`, `docs/api-reference.md` (`ConfigureMqtt` row,
  the new options), the package descriptions in the two project files: MQTTnet is no longer
  named as the broker or the client library.
- `docs/benchmarks.md`: new results next to those of 2026-11-15, and the idle measurements.

### 7.14 Kept open for later: message retention

The owner wants message retention in a later version, because it is useful for messaging apps.
It is not designed here. It could take two forms, and this design must not close either:

- **Stored room history:** a client that joins or comes back gets the messages it missed, not
  only a snapshot of the state. This would live in the room grain and its storage, and on the
  wire in the existing `seq` and `sys/sync` exchange (a sync that is answered with the missed
  events). The MQTT layer of this design carries those messages like any others.
- **MQTT retained messages:** the last message of a topic is kept and given to later
  subscribers. This needs topics that more than one client may subscribe to, which today's
  "own inbox only" rule forbids, and a store shared by all gateways.

What this design does to keep both possible:

- The codec reads and writes the retain flag of a PUBLISH and the retain options of a
  SUBSCRIBE; it does not reject them.
- **Refusing a retained publish is gateway policy** (today's `OnPublish`), not something built
  into the connection or the codec. Changing the policy later touches one place.
- The subscribe rule ("own inbox only") is likewise one policy method that the connection
  calls. The connection itself keeps "is this connection subscribed" as its only subscription
  state today; a later version that allows more topics replaces that, and nothing else in the
  connection depends on it.
- "Retain available" is not announced in CONNACK (7.9), so nothing on the wire has to be taken
  back when retention arrives.

### 7.15 Logging: why a connection ended

Metrics are out of scope (section 3). Being able to tell from the log why a connection ended
is not: the gateway logs today, and with its own broker it knows the reason exactly. Every end
of a connection that the server decides on is logged once, with the client id, the player when
known, and one of these reasons, each with its own event id so that they can be filtered:

malformed packet · protocol error · authentication refused · authentication timed out ·
connect timed out · keep-alive timed out · partial packet timed out · write timed out ·
acknowledgement timed out · outgoing limit passed · receive maximum passed · message too large
to send · taken over · connection limit · gateway stopping.

Ordinary ends are logged at debug level, as today. The two that point at a fault in the app (a
message too large to send, an authenticator that does not answer) are logged as errors. **No
log line contains a token, a password or a payload.**

### 7.16 Keeping it maintained

Replacing MQTTnet trades a dependency for a protocol implementation that GrainWire has to keep
correct itself. What this work leaves behind so that it stays manageable:

- **Conformance and security fixes are GrainWire's own from now on.** `protocol.md` lists the
  part of MQTT 5 that is implemented (7.13), so a report can be checked against a stated scope.
- **Fuzz findings become fixed tests.** Every input that broke a random test is kept as a
  named case with its seed (7.9).
- **The independent implementations are pinned.** The versions of MQTTnet and MQTT.js that the
  tests run against are recorded in ADR 0003, and raising them is a deliberate change.
- **The baseline stays measurable.** The last commit that still uses MQTTnet gets a git tag
  when the owner makes it, and ADR 0003 names it, so the "before" side of the benchmark can be
  run again later.

## 8. Order of work

Each step ends with every test green.

**Step 0. Measure first.**

- Add an `idle` workload to `bench/GrainWire.Bench`. It reports the gateways' growth per
  connection, as managed heap after a full collection (the measure benchmarks.md uses) and as
  working set, in four states:
  1. connected and subscribed;
  2. joined (rooms of two; one run with five rooms per connection);
  3. **active, then idle:** every connection has sent commands and received a burst, then
     everything is quiet again. This shows lists, buffers and pools that are kept after use;
  4. **after churn:** every connection has disconnected. The live managed bytes must come back
     to about where they started. Then all reconnect, and state 2 is measured again.
- It runs at two connection counts, *N* and at least 4 *N*. Connections carry three claims each.
- **"Per connection" is a slope, not a subtraction from an empty process:**
  (bytes at the larger count − bytes at the smaller count) ÷ (the difference in connections).
  This cancels what does not grow with connections, such as Kestrel's fixed costs and the
  buffers the shared pool keeps per processor core after activity.
- **Three figures are reported, and only the first decides:**
  1. *live managed bytes* after a full collection, as a slope: this is the figure the target
     is about;
  2. *what the buffer pool keeps* after activity: it must not grow with the number of
     connections;
  3. *working set*: reported for information. It follows the high-water mark and need not
     come back, so it decides nothing.
- **What is charged to a connection:** everything that exists because of it, including its
  share of the gateway's per-room tables. The target is stated for one room of two per pair of
  connections.
- The time one sweep takes at the larger count is reported, because it is part of how late a
  deadline can be acted on (7.4).
- Run it on today's code and record the result. Take one heap dump of a gateway in state 2 and
  attribute the bytes to MQTTnet, the Connections layer, Kestrel/ASP.NET Core and GrainWire's
  own objects.
- Measure **the floor**: the same idle connections against a bare ASP.NET Core WebSocket
  endpoint that accepts with sub-protocol `mqtt`, keep-alive off, and waits in a zero-byte read.
  (The floor is not zero: .NET's WebSocket object keeps, among other things, a 125-byte receive
  buffer of its own per socket.)
- One extra run of the floor and of state 2 with TLS in Kestrel is **reported, not decisive**:
  most deployments end TLS at a proxy ([hosting.md](https://github.com/mo-esmp/grainwire/blob/main/docs/hosting.md)).
- **Report to the owner** before step 1: today's figures and their split, the floor, and the
  target. The proposed target is **floor + 2 KB** per idle, joined connection, in states 2 and
  3 alike; it is provisional until the attribution shows whether it is realistic. If the floor
  alone is more than about 70% of today's figure, approach A cannot deliver much, and the owner
  decides whether to continue, or to look at approach B.

Step 0 depends on nothing in sections 7.4 to 7.8: it measures today's code and a bare endpoint.
It can start as soon as this design is approved.

**Step 1. The codec** and its tests. Before the QoS exchanges are written (steps 2 and 3), the
builder reads chapter 4 of the MQTT 5 standard (section 13).

**Step 2. The client.** The server is still MQTTnet. `GrainWire.Client` loses its MQTTnet
reference.

**Step 3. The server.** `GrainWire.Server` loses its MQTTnet references; `MQTTnet.AspNetCore`
leaves `Directory.Packages.props`.

**Step 4. Measure again and write up.**

- The `idle` workload and both workloads of benchmarks.md, old and new code on the same machine
  on the same day, three runs each. The old code is measured against itself first (two sets of
  three runs), to learn how much the figures move with no change at all. The machine is a
  virtual machine, so small differences are not read as results.
- For each run, the gateways' CPU time, bytes allocated, number of collections and total pause
  time are recorded beside the latencies.
- The new code also reports, for the big room, **the largest number of deliveries and bytes
  that were waiting for one connection**. If that comes near the defaults (1,000 messages,
  1 MiB), the defaults are raised before they are published.
- **Higher load** (decision E): the "many small rooms" workload is also run at 2 and 4 times
  its command rate, old and new.
- **The price of QoS** (decision F), new code only, reported and not part of success: the big
  room with every inbox at QoS 1 and at QoS 2, and the `idle` workload's state 3 after activity
  at QoS 2, which must come back to the same idle figure as at QoS 0.
- Documentation of 7.13.

## 9. Success

- Goals 1, 2 and 5 hold: no MQTTnet in the two packages; all existing tests pass with their
  assertions unchanged, except those of 7.10.
- The idle workload shows the target of step 0 is met in states 2 and 3, as the slope of live
  managed bytes; state 4 shows the live managed bytes coming back; and what the buffer pool
  keeps does not grow with the number of connections.
- For both workloads of benchmarks.md, no failed commands and all deliveries made, and the
  **p50 and p95** of "command to event" (both workloads) and of "request to reply" (many small
  rooms) are not worse than the old code's, comparing medians of three runs. "Not worse" means
  within 10%, or within the spread the old code shows against itself, whichever is larger.
- At 2 and 4 times the load, the new code does not fail where the old code succeeds, and the
  same "not worse" rule holds wherever the old code keeps up.
- p99, max, and the big room's "request to reply" (about 150 samples per run) are reported but
  do not decide success: they move too much between runs. CPU, allocation and collection
  figures are reported too; a clear regression in them is brought to the owner.

## 10. Decided by the owner

- Replace MQTTnet in both the server and the .NET client with GrainWire's own minimal
  implementation; the reasons in section 1.
- Unity and .NET Standard 2.1 wait for Unity 7.
- The number to design for is memory per connection.
- Approach A: ASP.NET Core's WebSocket with own MQTT on top.
- **A.** `MaxOutgoingQueue` counts only messages waiting behind a stalled write; the existing
  slow-consumer test is rewritten around a client that stops reading.
- **B.** A CONNECT with a Will is refused.
- **C.** A byte limit for waiting messages beside the count, as a new gateway option.
- **D.** A takeover by the same player inherits the slot; a client id open for another player
  is refused.
- **E.** CPU, allocation and collection figures are recorded, and the benchmark also runs at 2
  and 4 times the load. No search for the maximum.
- **F.** QoS 2 is supported, and with it QoS 1 for deliveries: inside a connection, in both
  directions, chosen by the connecting client, who pays its cost. No stored sessions.
- **G.** Message retention is wanted later, for messaging apps, and is not built now (7.14).
- **H.** `MaxOutgoingBytes` is a hard bound with a default of 1 MiB, adjustable in the gateway's
  options. A single message over it is not sent and the connection is closed with an error in
  the log. The deadlines stay internal constants.

Put to the owner as assumptions and not objected to:

- The wire stays standard MQTT 5, and the TypeScript client stays on MQTT.js.
- MQTTnet stays as a dependency of the tests and the benchmark.
- The order: codec, client, server.

## 11. Proposed by Claude (owner to check)

1. The codec is shared as linked source files, not as a third package (7.2). The alternative, a
   `GrainWire.Mqtt` package with only internal types, would be one more package to publish for
   no benefit to users.
2. `ConfigureMqtt` is replaced by `ConfigureWebSocket` and `KeepAlive` (7.12).
3. The reason code for a refused Will is `0x87` (not authorized). MQTT has no code that says
   "Will messages are not supported".
4. `MaxOutgoingBytes` may not be set below 64 KiB, and a message over it closes the connection
   with `0x95` (7.4). The default and the hard bound are the owner's decision H.
5. The deadlines and their lengths (7.4): connect 10 s, authentication 10 s, partial packet
   10 s, write 15 s per piece of at most 64 KB, close 2 s, sweep every 2 s. They are internal
   constants, not options.
6. A client that asks for no keep-alive is given 60 s (7.11, 6).
7. An oversized PUBLISH is still refused in its PUBACK, without being buffered, up to 16 times
   the limit (7.4).
8. A client's "maximum packet size" below 4 KiB is refused, and a message that does not fit
   closes the connection (7.4). MQTT says to drop such a message; closing as well is this
   server's policy, so that the failure can be seen.
9. The client tracks packet identifiers in a set of at most 1,024 and treats a PUBACK that is
   later than twice the keep-alive as a lost connection (7.8).
10. The target "floor + 2 KB", and the 70% rule for raising approach B again (section 8, step
    0).
11. "Not worse" is judged on p50 and p95 only, within 10% or the old code's own run-to-run
    spread (section 9).
12. The gateway's request handling per connection changes from a channel and a parked worker to
    work that exists only while there are requests (7.4). This goes beyond replacing MQTTnet,
    but it is part of what an idle connection costs today.
13. Three things the external review asked for are left out of this work (section 3):
    operational metrics, limits across connections, and an automated Blazor browser test.
14. For QoS 1 and 2 (7.4): the operator's cap `MaxQoS` with default 2, and that QoS 1 cannot be
    switched off; the server's receive maximum of 256 and the client's of 1,024; the
    acknowledgement deadline of 30 s; that messages waiting for a client's acknowledgements
    count towards the outgoing limits; and that only small records are kept, never payloads.
15. The app chooses QoS through `CommandQoS` and `InboxQoS` on the .NET client and the same two
    options on the TypeScript client (7.12). This is the one change to the TypeScript client.
16. Message retention is described in 7.14 in two possible forms without choosing between them.
17. From the reviews of v4, the numbers and rules I chose: control packets go before waiting
    deliveries, at most 1,024 of them and one PINGRESP may wait (7.4); the server's delivery
    window is the client's receive maximum or 256, whichever is smaller; the acknowledgement
    deadline runs per phase; an acknowledgement of the wrong kind closes the connection.
18. An oversized PUBLISH is judged in three bands with 5 KiB of room for topic and properties
    (7.4), and a reason string is cut to 256 bytes.
19. A takeover commits before CONNACK is sent, and a new connection that fails after that does
    not bring the old one back (7.4).
20. When the gateway stops, accepted requests get 5 s in total to finish (7.4).
21. A repeated QoS 2 identifier is answered with PUBREC whatever its DUP flag says. One of the
    v4 reviews asked for the connection to be closed when DUP is 0; I did not follow it, for
    the reason given in section 13.
22. The log reasons of 7.15 and the maintenance points of 7.16, including that the last
    MQTTnet-based commit gets a tag.

## 12. Left to the builder

- The names and the shape of the internal types, including how the link of 7.3 is expressed.
- Buffer sizes, the pool (the shared `ArrayPool` is the default assumption), and how the sweep
  walks the connections.
- How the slot and table updates of a takeover are made atomic (7.4 states the invariants).
- The layout of per-connection and per-room gateway state, guided by the heap dump of step 0.
- Whether waiting packets are sent together in one WebSocket message.
- How control packets and deliveries that wait are stored, and below which size a message is
  copied behind its header into one buffer (7.4 fixes the rules, not the storage).
- Whether the floor endpoint of step 0 stays in the benchmark project or is thrown away.
- Whether double registration of the WebSocket middleware (the app also calls `UseWebSockets`)
  needs handling.
- The connection counts of the `idle` workload, within what the test machine can drive.

## 13. What was checked, and what was not

**Checked against the text of the MQTT 5 standard** (OASIS, 7 March 2019): the Will rule; request
problem information; maximum packet size, including "discard without sending"; receive maximum;
the server overriding session expiry and keep-alive in CONNACK; the 1.5 times keep-alive rule;
packet identifiers (one space for PUBLISH, SUBSCRIBE and UNSUBSCRIBE, free again on the
acknowledgement); the refusals for subscription identifiers, topic aliases and QoS 2; session
takeover; session present; and that every reason code this design uses is valid in the packet
it is used in. One correction came out of it: QoS 2 may be refused only if "maximum QoS" was
announced (7.9).

**Checked against the .NET 10 source and documentation:**

- A zero-byte `ReceiveAsync` on .NET's WebSocket waits for data without consuming it, and
  ASP.NET Core's own SignalR transport uses it for the same purpose ("so that idle connections
  don't allocate a buffer").
- A keep-alive interval of zero or less creates no keep-alive timer; with keep-alive on, there
  is one timer per socket.
- .NET's WebSocket does not support two sends at once.
- Kestrel's response buffer is 64 KB by default, and a write does not complete until the
  buffer has drained below that.
- The WebSocket middleware accepts WebSockets over HTTP/2 through the same API.

**Not checked:**

- **Nothing was run.** All of the above was read, not measured. Step 0 exercises the zero-byte
  read, and the stalled-write test over a real socket (7.9, 5) shows that a write stays
  incomplete while the peer is not reading.
- What Kestrel and the WebSocket object cost per connection (the floor).
- How MQTTnet's client and MQTT.js react to the CONNACK properties of 7.9.
- The MQTT rules for WebSockets (chapter 6 of the standard: binary frames, packets not aligned
  to frames, the `mqtt` sub-protocol). The fact-check could not reach that chapter; 7.3 states
  them from knowledge of the standard.
- Part of the QoS rules. Confirmed against the standard: receive maximum in both directions and
  `0x93`, maximum QoS and when it must be announced, that a subscription may be granted a lower
  QoS, and that a delivery uses the lower of the publish's and the subscription's QoS. **Not
  reached** (chapter 4): that a message is never sent again inside a connection, the steps of
  the QoS 2 exchange on the receiving side, that a failing PUBREC ends the exchange, and the
  ordering rules for acknowledgements. 7.4 and 7.8 state these from knowledge of the standard.
  The builder reads chapter 4 (sections 4.3, 4.4, 4.6 and 4.9) before writing the exchanges;
  the interop tests with MQTTnet and MQTT.js at QoS 2 are the independent check.
- **Secondary sources for two of those rules**, found after the reviews of v4, since the
  standard's own chapter 4 stayed out of reach:
  - *A repeated QoS 2 identifier.* An Apache ActiveMQ Artemis issue quotes the standard's rule
    for the receiver: until PUBREL arrives it "MUST acknowledge any subsequent PUBLISH packet
    with the same Packet Identifier by sending a PUBREC". The rule has no condition on the DUP
    flag. One review of v4 said such a packet with DUP 0 must close the connection, citing
    MQTT-4.3.3-3; no text matching that description was found. 7.4 follows the quoted rule.
  - *No re-sending inside a connection.* HiveMQ's and the Paho project's write-ups of
    MQTT-4.4.0-1 agree with 7.4: messages are sent again only when a stored session is resumed.
- The same review restated the WebSocket rules of chapter 6 and the rules of chapter 4 as
  confirmed. That is a model's recollection, as 7.3's statement is; it counts as agreement, not
  as a check against the text.
- Whether Kestrel itself enables HTTP/2 WebSockets by default.

## 14. Review history

**Builder check (a fresh reader asked what it could not plan from), 2026-11-19.** Four points,
all confirmed against the code:

1. The slow-consumer test depended on MQTTnet's queueing. Became decision A.
2. v1 said only the request in progress finishes after a close; the code drains every accepted
   request. Corrected in 7.4.
3. "Within 10% on every percentile" could not be shown on about 150 samples. Section 9 now uses
   p50 and p95 and the old code's own spread.
4. `ChessSampleTests` tests chess rules, not client and server. 7.9 names the right tests.

**External review, 2026-11-19** ([the review](2026-11-19-minimal-mqtt-design-review.md)). Ten
findings; none was wrong.

| # | Finding | Taken | Where |
|---|---|---|---|
| 1 | Does a send complete on enqueue or on delivery? | Yes | 7.4 "Sending": the contract; 7.9 (5) |
| 2 | Will handling; a property matrix | Yes | Decision B; 7.5 |
| 3 | Buffer ownership | Yes | 7.6; 7.9 (4) |
| 4 | Byte budgets; stalled writes | In part | Decision C, the write deadline and the bounded close in 7.4. Not taken: a byte limit on incoming requests (already bounded by existing options) and limits across connections (section 3). |
| 5 | Lifecycle and takeover | Yes | 7.4 "States", "Deadlines", "Takeover"; decision D |
| 6 | QoS 1 packet identifiers | Yes | 7.8 |
| 7 | A snapshot over the client's packet limit | Yes | 7.4; 7.11 (8) |
| 8 | Idle target wording; memory after activity | In part | Goal 3; step 0. TLS is reported, not decisive. |
| 9 | Capacity test | In part | Decision E: higher-load runs and resource figures, no search for the maximum. |
| 10 | Wider verification | In part | 7.9 (1) to (6). Not taken: operational metrics and an automated Blazor test (section 3). |

**Fact check, 2026-11-19.** The MQTT and .NET statements this design depends on were checked
against the standard and the .NET 10 source (section 13). Everything that could be reached was
confirmed. One point changed the design: "maximum QoS 1" and "no subscription identifiers" are
now always announced in CONNACK, because MQTT lets a server refuse those features only after
announcing that it does not support them (7.9; 7.11, 11).

**The owner's changes after v3, 2026-11-19.**

- **QoS 2 is supported** (decision F). v3 refused it. v4 supports QoS 1 and 2 inside a
  connection, in both directions, chosen by the client: 7.4 "QoS 1 and 2 inside a connection",
  7.5, 7.8, 7.9, 7.11 (12, 13), 7.12. "Maximum QoS 1" is announced only when the operator caps
  the gateway. A second fact check covered the QoS rules; section 13 says which parts it could
  not reach.
- **Message retention is a later feature** (decision G): 7.14.

**Two external reviews of v4, 2026-11-19** (a saved conversation, not kept in the repository).
They agreed on the architecture and on step 0 as the gate, and both found the main defect of
v4. v5 is the result.

| Finding | From | Taken | Where |
|---|---|---|---|
| Control packets can be stuck behind a delivery that waits for the QoS window (a deadlock) | both | Yes. A real defect, introduced with QoS 1 and 2 in v4. | 7.4 "Scheduling"; 7.8; 7.9 (9) |
| A set of identifiers is not enough state; deadlines per exchange | both | Yes | 7.4 "Exchange records"; 7.8 |
| Receive maximum must count QoS 1 and QoS 2 together | first | Yes | 7.4, commands, item 4 |
| An abort does not make a buffer free; closing must not start a second send | first | Yes | 7.6 rules 4, 5 and 7; 7.4 "Closing, bounded"; 7.9 (4) |
| The byte limit was not a hard bound | first | Yes | Decision H; 7.4 "Limits on what waits" |
| Reason strings and "request problem information" for the QoS 2 packets; control packets not provably under 4 KiB | first | Yes | 7.5; 7.4 "maximum packet size" |
| An oversized PUBLISH: where the identifier is, large topic or properties, the answer per QoS | both | Yes | 7.4 "Reading" |
| Malformed packet and protocol error must be told apart | first | Yes | 7.5 |
| Where a takeover becomes final; an authenticator that ignores cancellation | first | Yes | 7.4 "The commit point"; "The authenticator and its deadline" |
| A 2 s sweep cannot give a strict 2 s close; "progress" against "completion" of a write | first | Yes | 7.4 "Deadlines" |
| No shutdown sequence for the gateway | first | Yes | 7.4 "When the gateway stops" |
| What the memory target includes; slope between two counts; pool retention; sweep time | both | Yes | Section 8, step 0; section 9 |
| More tests; what the random tests must assert | first | Yes | 7.9 (1), (4), (9) |
| "A healthy client survives any burst" is too strong | first | Yes | 7.4 "What the limits promise"; 7.10 |
| Option validation; telling close reasons apart in the log; maintenance | first | Yes | 7.8; 7.12; 7.15; 7.16 |
| What the parked read keeps alive; `ValueTask`; no reference to a returned buffer | second | Yes | 7.3; 7.4 "Reading"; 7.6 rule 7 |
| An empty client id is well formed and refused by policy, not by the codec | second | Yes | 7.4, CONNECT checks |
| WebSocket close status for a text message | second | Yes | 7.3 |
| A repeated QoS 2 identifier with DUP 0 must close the connection | second | **No** | Section 13 gives the reason. 7.4 states the rule that was kept. |
| A conformance matrix with a row for every packet and property | first | In part | 7.5 has the properties and the two error classes; a full matrix with every column is left to the codec's tests. |
