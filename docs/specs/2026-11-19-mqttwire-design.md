# MqttWire: a lean MQTT 5 connection server and client for .NET (design)

Date: 2026-11-19
Status: final design (v6), approved by the owner on 2026-11-19
Updated on 2026-11-22 to the server as built in step 3 (mo-esmp/mqttwire#6).
Moved to the `mqttwire` repository on 2026-11-20.
Companion: [GrainWire on MqttWire](2026-11-19-grainwire-on-mqttwire-design.md), the first host.
History: this design is v5 of ["GrainWire: its own minimal MQTT 5 implementation"](2026-11-19-minimal-mqtt-design.md),
cut loose from GrainWire. That document keeps the review history and the record of what was
checked against the MQTT standard and the .NET source.

## 1. Intent

MqttWire is the MQTT 5 layer that GrainWire needs, built as a library of its own, in its own
repository, usable without GrainWire.

It is **a connection server, not a broker.** It speaks MQTT 5 over WebSockets with each client:
connections, the protocol, QoS, limits, deadlines, shutdown. It does not decide who may connect,
what a subscription means, or where a published message goes. **The host application decides
all of that**, through one small interface, and sends messages to connections itself.

Why the owner wants it this way:

1. **Only what is used.** A small, fixed part of MQTT 5, with nothing a host does not ask for.
2. **Memory per connection.** A gateway for turn-based games or chat holds mostly idle
   connections. What an idle connection costs sets how many one server can hold.
3. **Control over the APIs.** The library is the owner's; nothing has to be worked around.
4. **A package of its own.** Other applications can build on it, for example a chat server
   that puts all members of a group on one topic (6.10).

## 2. Goals

1. **Standard MQTT 5 over WebSockets on the wire**, so that MQTT.js, MQTTnet and other MQTT 5
   clients can talk to a host built on MqttWire, and the MqttWire client can talk to other
   MQTT 5 servers.
2. **An idle connection costs MqttWire almost nothing.** For a connection that is connected
   and has nothing in flight, MqttWire holds: no payload or packet buffer, no backing storage
   for a queue, no timer, no cancellation source, and no record for QoS 1 or 2. What remains is
   the connection object, the parked read, and what Kestrel and .NET's WebSocket hold (the
   floor). The target is in section 6.14.
3. **The host drives it through one interface** (6.2). No events, no interception of a broker's
   routing.
4. **Bounded for every connection**, in bytes and in time, including one that has stopped
   reading and one that has not authenticated (6.6, "worst case").
5. **QoS 0, 1 and 2 in both directions inside a connection**, chosen by the connecting client
   and the host, costing nothing when not used.

## 3. What it is not

Non-goals:

- **Routing between clients.** No subscription table, no wildcard matching, no fan-out inside
  MqttWire. A host can do all of that on top (6.10); a helper for it may come later (6.11).
- **Sessions that survive a reconnect.** Nothing is stored for a client that is away, and
  nothing is sent a second time. QoS 1 and 2 are complete, standard exchanges inside one
  connection; they add no guarantee across reconnects.
- **Retained messages** as a built-in feature (the retain flag is passed to the host, 6.11).
- Will messages, topic aliases, shared subscriptions, subscription identifiers, enhanced
  authentication (AUTH), WebSocket compression.
- A raw TCP endpoint, MQTT 3.x.
- Own WebSocket framing. .NET's WebSocket is used; the design keeps the other way open (6.5).
- .NET Standard 2.1 and Unity. Postponed by the owner until Unity 7.
- Metrics, and limits across connections. The log tells why a connection ended (6.12);
  Kestrel has its own cap on open WebSockets.

Anti-goals (failures even if it technically works):

- It grows into a general broker by accretion: options and features "for completeness".
- A client that has not authenticated can make the server hold more than a small, fixed amount
  of memory, or hold it for longer than the connect and authentication deadlines. (The
  deadlines bound the socket and MqttWire's own memory. They cannot stop a host's code that
  ignores cancellation; 6.6 says what is released regardless.)
- A connection that has stopped reading can hold memory without a bound in bytes and in time.
- One slow recipient delays a host's delivery to other connections.
- An acknowledgement or a ping response waits behind a delivery that is itself waiting for the
  client.
- A message is silently never sent because the "send only when there is work" logic lost a
  wake-up.
- A buffer is used after it went back to the pool, or never goes back.
- Memory is saved per connection by moving the cost somewhere unmeasured.

## 4. Constraints

- .NET 10, C# 14, warnings are errors, MIT licence: the same conventions as GrainWire.
- The server sits on **ASP.NET Core's WebSocket** (`HttpContext.WebSockets`). No use of the
  ASP.NET Core "Connections" layer.
- The client sits on `System.Net.WebSockets.ClientWebSocket` and **must work in Blazor
  WebAssembly** (single-threaded, no blocking waits).
- `MqttWire` (codec and client) has no package dependencies. `MqttWire.Server` depends on
  `MqttWire` and on the ASP.NET Core shared framework, nothing else.
- MQTTnet and MQTT.js are used **in the tests only**, as two independent MQTT implementations
  that check this one.

## 5. Approaches considered

**The layer under the server's MQTT code** (the owner chose A):

| | Approach | Trade-off |
|---|---|---|
| **A (chosen)** | ASP.NET Core's WebSocket, own MQTT on top. An idle connection waits with a zero-byte read. | The framework keeps handling WebSocket framing, ping/pong, the close handshake and HTTP/2. The floor is what Kestrel and its WebSocket object cost per connection. |
| B | Own WebSocket framing as well. | Removes the framework's WebSocket object and a copy per message. Adds a second protocol to own and defend. |
| C | The ASP.NET Core "Connections" layer. | Least new I/O code, but a pipe pair and transport loops per connection. |

**What the server is** (the owner chose the first):

| | Approach | Trade-off |
|---|---|---|
| **Connection server (chosen)** | The host decides who connects, what a subscription means and where a publish goes. | Small, lean, and exactly what GrainWire needs. Not a drop-in replacement for a broker; the README says so. |
| Routing broker | Subscription table, wildcards, fan-out between clients. | What "broker" usually means, and much larger. Per-connection state that works against the memory goal. Can be added later as a layer on top. |

**How the host plugs in** (the owner chose A):

| | Approach | Trade-off |
|---|---|---|
| **A (chosen)** | One handler object for all connections, and a state slot on each connection. | Nothing allocated per call; one small interface to keep stable. The host's own per-connection state is a second object. |
| B | Events, as MQTTnet does. | Familiar. Allocates an arguments object per event; unclear who decides when several subscribe. |
| C | The host derives its connection class from MqttWire's. | One object per connection. Makes the base class's protected members a public contract for good. |

**The name:** MqttWire, over LeanMqtt: it describes a wire layer that the host drives, and it
reads as a sibling of GrainWire.

## 6. Design

### 6.1 Packages and repository

| Package | Contains | Depends on |
|---|---|---|
| `MqttWire` | The codec (internal), the client and the opt-in reconnecting client (public). | nothing |
| `MqttWire.Server` | The connection server for ASP.NET Core. | `MqttWire`, the ASP.NET Core shared framework |

The two are built and versioned together. `MqttWire.Server` uses the codec through
`InternalsVisibleTo`, so the codec's types never become public and need no linked source files.

```
mqttwire/
  src/MqttWire/                 codec + client
  src/MqttWire.Server/          connection server
  tests/MqttWire.Tests/         codec, client and server tests; MQTTnet as a test dependency
  tests/MqttWire.Interop.Tests/ interops over Kestrel: MQTTnet's client and broker against
                                MqttWire's server and client
  tests/MqttWire.TestHost/      a small host with an exact-topic handler, shared by the interops,
                                MQTT.js and the shared-topics test
  tests/interop-js/             (part B) MQTT.js against the test host
  bench/MqttWire.Microbench/    BenchmarkDotNet runs of the codec and the client
  bench/MqttWire.Bench/         (part B) the floor, and idle connections on a handler that does
                                nothing
  samples/GroupChat/            (part B) shared topics through the host (6.10)
  docs/                         this design; (step 4) "the part of MQTT 5 that is implemented"
```

### 6.2 Public API: the server

The whole public surface of the server is: one interface the host implements, the connection,
the server, the options, and the two calls that register and map it. The shapes below fix what
exists and what each member means. Exact type and member names are the builder's choice (the
`MqttWire` prefix avoids clashes with MQTTnet's names in tests that use both).

```csharp
builder.Services.AddMqttWireServer<MyHandler>(options => { options.MaxOutgoingBytes = 1 << 20; });
app.UseWebSockets();
app.MapMqttWire("/mqtt");
```

```csharp
public interface IMqttWireHandler
{
    // 1. Who is this? May take time and may do I/O. Has a deadline (6.6).
    ValueTask<MqttConnectResult> OnConnectAsync(MqttConnectRequest request, CancellationToken cancellationToken);

    // 2. May it come in now? Called inside the admission step (6.6). Synchronous and quick.
    //    'replaces' is the connection that currently has this client id, if there is one.
    MqttAdmitResult OnAdmit(MqttWireConnection connection, MqttWireConnection? replaces);

    MqttSubscribeResult OnSubscribe(MqttWireConnection connection, string topicFilter, MqttQoS requested);
    void OnUnsubscribe(MqttWireConnection connection, string topicFilter);

    // The payload is valid only during the call. A host that keeps it copies it.
    MqttPublishResult OnPublish(MqttWireConnection connection, in MqttPublish publish);

    // Exactly once for every connection that OnAdmit accepted.
    void OnClosed(MqttWireConnection connection, MqttCloseReason reason);
}
```

```csharp
public sealed class MqttWireConnection
{
    public string ClientId { get; }
    public object? State { get; }             // what the host returned from OnConnectAsync
    public MqttSendResult TrySend(MqttTopic topic, ReadOnlyMemory<byte> payload, MqttQoS qos);
    public void Close(MqttReasonCode reason, string? reasonString = null);
}

public sealed class MqttWireServer
{
    public int ConnectionCount { get; }
    public bool TryGetConnection(string clientId, [NotNullWhen(true)] out MqttWireConnection? connection);
}
```

**What each part means**

| Member | Contract |
|---|---|
| `MqttConnectRequest` | The CONNECT as the host needs it: client id, user name, password (bytes), the user properties the client sent, the remote end point, whether the connection is secure, and the `HttpContext` of the WebSocket request (headers, cookies, the authenticated user). It owns copies of what it holds, so it may be used across awaits inside `OnConnectAsync`. |
| `MqttConnectResult` | Accept, with the host's state object for the connection and optional user properties for CONNACK; or refuse, with a reason code and a reason string. `Accept` throws `ArgumentException` for user properties that cannot be encoded or that are over 1 KiB encoded; a host that changes the array afterwards so that it cannot be encoded is treated as a handler that threw (CONNACK `0x83`, and `OnClosed` says "handler threw"). Every `Refuse` throws `ArgumentOutOfRangeException` for a reason code below `0x80`, and `ArgumentException` for a reason string that no packet can carry (U+0000, an unpaired surrogate); `Close` throws the same `ArgumentException`. |
| `MqttAdmitResult` | Accept or refuse (reason code and string). When `replaces` is not null and the host accepts, the new connection takes over and the old one is closed with "session taken over" (6.6). |
| `MqttSubscribeResult` | Grant at a QoS (MqttWire lowers it to `MaxQoS` if needed), or refuse with a reason code and string. MqttWire keeps no record of subscriptions: what a subscription means, and remembering it, is the host's. |
| `MqttPublish` | Topic, payload, QoS, the retain flag. A view; valid during the call only. |
| `MqttPublishResult` | Accept, or refuse with a reason code and string. MqttWire sends the answer in the form the QoS calls for. |
| `TrySend` | Hands a message for this connection to MqttWire and **returns at once**; it never waits for the network (6.6). Safe to call from any thread. The payload and the topic are not copied: the caller must not change them afterwards, and one payload may be handed to any number of connections. The result says: taken, connection not open, too large, or the connection was just evicted. "Taken" means accepted, not delivered. A QoS above `MaxQoS` is lowered to it; `default(MqttTopic)` and a QoS that is not 0, 1 or 2 throw `ArgumentException`; an empty payload is a valid message. |
| `MqttTopic` | A topic name encoded once. A host that sends to the same topic often keeps the value; one that does not builds it per call. It accepts at most 65,526 bytes of UTF-8, 9 fewer than MQTT allows, so that the header of every PUBLISH fits one 64 KB buffer (6.6). |
| `Close` | Ends the connection as 6.6 describes. Safe from any thread, any number of times. |
| `State` | The host's own object for this connection. MqttWire never looks at it. |

**The result types fail closed.** `MqttConnectResult`, `MqttAdmitResult`, `MqttSubscribeResult`
and `MqttPublishResult` are values that only their factory members make accepting (`Accept`,
`Accepted`, `Granted`). The default value of each (`default`, an unset field, a forgotten
branch) is a refusal with `0x87` (not authorized) and no reason string: a handler that returns
`default` authenticates nobody, admits nobody, grants nothing and lets no publish through.

**The rules for a handler**

- The handler is a singleton that dependency injection creates when the host starts, before
  the first connection. The server is built without it and resolves it at its start, so a
  handler may take `MqttWireServer` in its constructor, to look connections up with
  `TryGetConnection`. A handler whose constructor throws makes the host's start fail with that
  exception.
- `OnSubscribe`, `OnUnsubscribe` and `OnPublish` are called from the connection's read path:
  for one connection one at a time, in the order the packets arrived; for different
  connections at the same time. They are synchronous and must return quickly. Work that takes
  time is queued by the host.
- `OnAdmit` runs inside MqttWire's admission step and must not block or call back into the
  server. It runs under the server's lock that `TryGetConnection` and `ConnectionCount` take
  for a moment, so a lock of the host's own that `OnAdmit` takes must never be held, on another
  thread, across a call of either of the two: the locks would be taken in opposite orders.
- `OnClosed` is called once, after the last other call for that connection has returned. A
  connection that was refused in `OnConnectAsync` or `OnAdmit` gets no `OnClosed`. It must not
  wait on the server's stop: the stop waits for every `OnClosed` to return.
- `OnClosed` may run on a thread-pool thread, and never runs inside an abort of the link: the
  end of a connection waits until an abort that is in flight has returned, and then carries on
  on a thread-pool thread (6.6, "Closing, bounded"). So `OnClosed` never runs on the sweep's
  thread or on the thread that stops the server, and never inside the host's own `TrySend` or
  `Close`, whatever the transport does when its read is aborted.
- A handler method that throws is the host's fault: the exception is logged as an error and
  the connection is closed with `0x83` (implementation specific error): with CONNACK `0x83` from
  `OnConnectAsync` or `OnAdmit`, and then no `OnClosed`; with DISCONNECT `0x83` from the others,
  and `OnClosed` says "handler threw". In `OnClosed` it is logged only.

**Options** (`MqttWireServerOptions`, validated when the server is first resolved, which in a
`WebApplication` is the call to `MapMqttWire`, or at the latest when the host starts; an invalid
value throws `OptionsValidationException`, and the host does not start):

| Option | Default | Meaning |
|---|---|---|
| `MaxQoS` | 2 | The highest QoS accepted from clients and used towards them. 1 or 2. |
| `MaxPublishPayloadBytes` | 64 KiB | Largest payload a client may publish. From 1 byte to 16 MiB. |
| `MaxOutgoingMessages` | 1,000 | Deliveries that may wait for one connection. At least 1. |
| `MaxOutgoingBytes` | 1 MiB | Hard bound on what one connection holds on the way out. At least 64 KiB. |

The deadlines, the receive maximum (256) and the other bounds of 6.6 are internal constants.

### 6.3 Public API: the client

A general MQTT 5 client over WebSockets. One instance is one connection: it does not reconnect
by itself, so nothing of a broken connection survives into the next one. An application that
wants reconnecting either creates a new instance itself, as GrainWire does, or uses the opt-in
wrapper at the end of this section.

```csharp
await using MqttWireClient client = new(new MqttWireClientOptions
{
    Uri = new Uri("wss://example.com/mqtt"),
    ClientId = "…",
    UserName = "…",
    Password = token,
    KeepAlive = TimeSpan.FromSeconds(15),
    MaxIncomingPacketBytes = 1_048_576,                       // the largest packet the client accepts
    ConfigureWebSocket = ws => ws.SetRequestHeader("X-Thing", "…"),
    OnMessage = (in MqttPublish message) => { /* payload valid during the call */ },
    OnClosed = reason => { },
});

MqttConnectAck ack = await client.ConnectAsync(cancellationToken);   // reason code, reason string, user properties
MqttRequestResult subscribed = await client.SubscribeAsync("some/topic", MqttQoS.AtMostOnce, cancellationToken);
MqttQoS? granted = subscribed.GrantedQoS;                            // null when the server refused
ValueTask<MqttRequestResult> done = client.PublishAsync("some/topic", payload, MqttQoS.AtLeastOnce);   // completes on the acknowledgement
```

- **`ConnectAsync` reports every outcome in the `MqttConnectAck` it returns.** Connected;
  refused by the server (its CONNACK reason code, reason string and user properties); or no
  valid CONNACK (unreachable, timed out, malformed, a protocol error). When the client is not
  connected, `Failure` says why. It throws only for a caller bug (invalid options, a second call
  to `ConnectAsync`), for the caller's own cancellation, for an exception from
  `ConfigureWebSocket`, and with `ObjectDisposedException` when the client is disposed during
  the connect. `OnClosed` is called only for a connection that `ConnectAsync` reported as
  connected.
- **`PublishAsync`, `SubscribeAsync` and `UnsubscribeAsync` return
  `ValueTask<MqttRequestResult>`.** The result holds a status and the server's reason code.
  `IsSuccess` is true when the exchange completed with a reason code below `0x80`;
  `GrantedQoS` gives the QoS a SUBACK granted, and is null for a refusal or an exchange that did
  not complete. It is meaningful for a subscribe's result only: the result does not know its
  request's kind, so a successful publish or unsubscribe (reason code `0x00`) reads as QoS 0. The
  statuses are:
  `Completed` (the exchange ended; the reason code is the server's); `NotConnected`,
  `ReceiveMaximumReached`, `PacketTooLarge`, `QoSNotSupported`, `NoFreePacketIdentifier` and
  `SendQueueFull` (the request failed at once and was not sent); and `ConnectionLost` (the connection ended
  first, so the server may or may not have received the request). A bare QoS could not say
  "refused" or "connection lost".
- `PublishAsync` hands the message over in the order of the calls. It encodes the whole packet
  before it returns, so the caller may reuse its payload at once. The task it returns
  completes when the exchange ends (at once for QoS 0), with the server's reason code; a caller
  that does not care about the acknowledgement does not await it.
- A publish that has no room in the server's receive window, or is larger than the server's
  maximum packet size, fails at once and is not sent. So does a publish while 1 MiB of earlier
  publishes waits to be written, because the link is slower than the caller: it fails with
  `SendQueueFull`. One publish is always taken when nothing waits, so a larger message can
  still be sent.
- `MaxIncomingPacketBytes` is the largest packet the client accepts (default 1 MiB, from 4,096
  to 268,435,460). The client announces it in CONNECT (6.9).
- `OnMessage` and `OnClosed` are called one at a time, in order. `OnClosed` is called once.
  An `OnMessage` that throws ends the connection (6.9); an exception from `OnClosed` is
  swallowed.
- Options are checked when the client is created: `KeepAlive` is between 1 second and 65,535
  seconds and is rounded up to whole seconds. `ConfigureWebSocket` runs before the client adds
  the `mqtt` sub-protocol, so it cannot remove it; an exception it throws fails that
  `ConnectAsync`. In Blazor WebAssembly the browser supports only part of
  `ClientWebSocketOptions` (no request headers, no TLS or proxy settings); the documentation
  says so. For the same reason the client does not set `KeepAliveInterval` there (6.5).

#### The reconnecting client (opt-in)

A second public class in the `MqttWire` package, for applications that want the connection kept
up for them. It wraps the client above and changes nothing in it; an application that does not
create one pays nothing for it. GrainWire does not use it.

```csharp
await using MqttWireReconnectingClient client = new(new MqttWireReconnectOptions
{
    // Called before every attempt, so a token that has expired can be replaced.
    ClientOptions = async cancellationToken => await BuildOptionsWithFreshToken(cancellationToken),
    InitialDelay = TimeSpan.FromMilliseconds(500),
    MaxDelay = TimeSpan.FromSeconds(15),
    OnConnected = info => { /* info.IsReconnect: catch up on what was missed */ },
    OnDisconnected = reason => { },
    OnStopped = ack => { /* it gave up; see below */ },
    OnCallbackError = exception => { },                     // there is no logger; see Rules
    OnMessage = (in MqttPublish message) => { },
});

MqttConnectAck started = await client.StartAsync(cancellationToken);   // the first connection, or the refusal that stopped it
await client.SubscribeAsync("groups/42", MqttQoS.AtLeastOnce, cancellationToken);
ValueTask<MqttRequestResult> done = client.PublishAsync("groups/42", payload, MqttQoS.AtLeastOnce);
```

**What it does**

- **Every attempt is a new `MqttWireClient`.** The rule "nothing of a broken connection
  survives" holds here too.
- **Back-off:** attempt *n* waits `min(MaxDelay, InitialDelay × 2^(n−1))` times a random
  factor in [0.8, 1.2], capped at `MaxDelay`, so that many clients do not come back in the same
  instant. A connection that stays up for at least `MaxDelay` puts the delay back to the start;
  one that ends sooner counts as a failed attempt, so a server that drops every connection at
  once (two clients with the same identifier taking each other over, for example) is not retried
  every `InitialDelay` for ever.
- **Fresh options for every attempt.** The `ClientOptions` callback is called before each one.
  An exception from that callback, or from creating the client, counts as a failed attempt and
  is retried.
- **It remembers subscriptions and makes them again.** What the application subscribed to
  through the wrapper is subscribed again on every new connection, before `OnConnected` is
  called. A subscription the server refuses this time is dropped from what is remembered and
  reported through `OnSubscriptionRefused`. A connection lost while it subscribes again counts
  as a failed attempt: `OnConnected` is not called. `UnsubscribeAsync` forgets the filter even
  while disconnected. A filter unsubscribed while the new connection subscribes to it again is
  unsubscribed on that connection before `OnConnected`, and a subscribe whose SUBACK comes
  after an unsubscribe of the same filter is not remembered.
- **It stops when retrying cannot help.** A refusal that the same CONNECT would meet again
  (unsupported protocol version, client id not valid, bad user name or password, not
  authorized, banned) ends the wrapper: `OnStopped` is called once with the reason, and
  `StartAsync` returns its `MqttConnectAck` if that happens on the first attempt. Every other
  failure (unreachable, timed out, server unavailable, a lost connection) is retried until the
  wrapper is disposed. The token of `StartAsync` matters only until the start completes:
  cancelling it before then stops the wrapper.

**What it does not do, and the application must know**

- **It does not bring back what was missed.** There are no stored sessions: messages sent to
  the client's topics while it was away are gone. `OnConnected` with `IsReconnect` is the
  application's signal to catch up by its own means.
- **It does not queue while disconnected.** `PublishAsync` and `SubscribeAsync` fail at once
  when there is no connection. A topic or filter that is not valid throws `ArgumentException`,
  connected or not.
- **It does not send anything a second time.** A publish that was in flight when the
  connection was lost completes with "connection lost": it may or may not have arrived, and
  what to do about that is the application's decision.

**Rules**

- `OnConnected`, `OnDisconnected`, `OnSubscriptionRefused`, `OnStopped` and `OnMessage` are
  called one at a time, in order. Every `OnConnected` is followed by exactly one
  `OnDisconnected` before the next `OnConnected`.
- `MqttWire` takes no dependency on a logging package, so the wrapper has no logger. A callback
  that throws is reported to `OnCallbackError`, and the wrapper carries on.
- Disposing it stops everything at once: during a back-off, during an attempt, or while
  connected. No callback is called after `DisposeAsync` has returned.
- It has no thread of its own and works in Blazor WebAssembly. The wrapper holds one lock while
  `OnConnected`, `OnSubscriptionRefused` and `OnMessage` run, so that they are called one at a
  time. On a runtime with several threads, the read loop's thread waits while `OnConnected` or
  `OnSubscriptionRefused` runs; in single-threaded WebAssembly nothing can wait. A callback
  must not synchronously wait on a request to the client: that would wait on the thread it
  stands on.
- Its delays use an internal clock that the tests replace, so its tests do not wait in real
  time.

### 6.4 The codec

Internal to `MqttWire`. Reads and writes MQTT 5 packets; pure functions over spans.

- **It never allocates and never owns a buffer.** Decoding takes a span (or a sequence, if the
  builder finds it needs one) and returns views into it; a string is made only by the caller,
  when it needs one. Encoding writes into a span or an `IBufferWriter<byte>` it is given.
- **Decoding is incremental at packet level.** "Is there a complete packet at the start of this
  buffer, and how long is it?" is a separate, cheap question from "decode it".
- **Every property is validated, then honoured, ignored or refused as 6.7 says.**
- **Malformed input is an error result, not an exception** (6.7 lists the cases).

| Packet | Server reads | Server writes | Client reads | Client writes |
|---|---|---|---|---|
| CONNECT | yes | | | yes |
| CONNACK | | yes | yes | |
| PUBLISH (QoS 0, 1, 2) | yes | yes | yes | yes |
| PUBACK, PUBREC, PUBREL, PUBCOMP | yes | yes | yes | yes |
| SUBSCRIBE / SUBACK | reads SUBSCRIBE | writes SUBACK | reads SUBACK | writes SUBSCRIBE |
| UNSUBSCRIBE / UNSUBACK | reads UNSUBSCRIBE | writes UNSUBACK | reads UNSUBACK | writes UNSUBSCRIBE |
| PINGREQ / PINGRESP | reads PINGREQ | writes PINGRESP | reads PINGRESP | writes PINGREQ |
| DISCONNECT | yes | yes | yes | yes |
| AUTH | recognised only to refuse it | | | |

The codec reads and writes the retain flag of a PUBLISH and the options of a SUBSCRIBE; it does
not reject them. What they mean is decided above it.

### 6.5 The link below the MQTT code

Neither the server connection nor the client calls `WebSocket` directly. Each uses a small
internal abstraction with five operations: wait until data is available, read into a buffer,
write a buffer, close with a status, abort. One implementation wraps a `WebSocket`. Its
operations return `ValueTask`, so an operation that completes at once allocates nothing, and on
the server it must not add a noticeable object per connection. The wait, the read and the client's
read loop use the pooling method builder, so an idle connection waking for data allocates
nothing in steady state either. The client's writes are allocation-free when the send lock is
uncontended and the write completes synchronously; a write that completes asynchronously
(always in the browser) allocates its state machine.

This keeps own WebSocket framing possible later, and it lets tests drive a connection over an
in-memory link, byte by byte, and decide exactly when a write completes. It stays internal: a
public transport interface would invite the growth section 3 warns of. The tests reach it
through `InternalsVisibleTo`.

Rules for the WebSocket implementation, both sides:

- Sub-protocol `mqtt`. The server accepts a request that offers it and answers with it; a
  WebSocket request that does not offer it is refused with HTTP 400.
- Binary messages only. A text message closes the connection with WebSocket status 1003. On the
  server it ends the connection as a lost connection; the peer sees 1003 unless a server write
  was in progress, in which case the close rule of 6.6 aborts the link and the peer may see a
  reset instead.
- A link starts at most one close frame, whichever side closes first.
- An empty binary message carries no bytes and does not end the wait for data: the wait goes
  on. What the link cannot see is a frame whose header has arrived and whose payload the peer
  withholds: the wait ends and the read that follows waits, which on the server is bounded by
  the partial-packet deadline (6.6).
- **MQTT packets are not aligned to WebSocket messages.** One message may hold several packets
  or part of one. The receiver treats the incoming messages as one byte stream.
- The server turns the WebSocket's own keep-alive pings off for this endpoint. MQTT has its
  own keep-alive, and this removes a timer per connection. The client does the same, except in
  the browser (Blazor WebAssembly), where setting `ClientWebSocketOptions.KeepAliveInterval`
  throws `PlatformNotSupportedException`; there the client leaves it alone.

### 6.6 The server connection

One `MqttWireConnection` per connection holds the MQTT state and the host's state slot.

#### States

| State | Entered when | Leaves by | May hold |
|---|---|---|---|
| **Accepted** | the WebSocket is accepted | a complete CONNECT (to Authenticating); the connect deadline, a malformed or wrong first packet, a CONNECT that fails MqttWire's own checks, the server stopping (to Closing) | one read buffer, at most 16 KB |
| **Authenticating** | CONNECT is valid so far | `OnConnectAsync` answers (to admission); its deadline; the socket closing; the server stopping | the request and a short-lived cancellation source; the read buffer only when packets came behind the CONNECT in it, which are handled, in order, after the CONNACK. With nothing behind the CONNECT, the buffer is returned before `OnConnectAsync` is called. |
| **Active** | the admission step accepted | any end of the connection (to Closing) | what the rest of 6.6 allows |
| **Closing** | the decision to end is made, once | the bounded close below | no new work is accepted |
| **Closed** | the read loop has ended and no write is in progress | | nothing |

A connection moves forward only. The move to Closing happens exactly once, whoever asks first.

#### Deadlines

All deadlines are measured on a monotonic clock. One shared sweep per server enforces them; it
runs every 2 seconds. **The lengths below are nominal: a deadline is acted on at the first
sweep after it passes**, which is up to 2 seconds, plus the time one sweep takes, later. There
is no timer per connection, and no cancellation source except the short-lived one of the
Authenticating state.

| Deadline | Length | What happens |
|---|---|---|
| Connect | 10 s from accept until a complete CONNECT has arrived | abort |
| Authentication | 10 s for `OnConnectAsync` to answer | CONNACK `0x88` (server unavailable) if it can be written, then close; abort when the call has not even come back 2 s after its token was cancelled |
| Partial packet | 10 s from the moment the read buffer is rented for a packet (the wait for data has ended) to the packet's last byte; for a packet that follows another in the same read, from that read | close (`0x82`) |
| Keep-alive | 1.5 times the connection's keep-alive without a packet from the client, counted from the move to Active until the first packet, so a slow `OnConnectAsync` or `OnAdmit` does not use it up | close (`0x8D`) |
| Write | 15 s for one piece of at most 64 KB handed to the link to complete | abort |
| Acknowledgement | 30 s for each phase of a QoS 1 or 2 exchange | close (`0x82`) |
| Close | 2 s from entering Closing | abort |

The keep-alive of a connection is what the client asked for, except that a client that asks for
none, or for more than 60 s, is told 60 s through the CONNACK property "server keep alive".

A client that stops reading may still send PINGREQ, so the keep-alive deadline does not bound a
stalled write. The write deadline does. It bounds how long one piece may take to be accepted by
the network; the link cannot see progress inside a piece, which is why large writes are cut
into pieces.

**`OnConnectAsync` and its deadline.** It is the host's code:

- Its cancellation token fires at the authentication deadline, or earlier if the socket closes
  or the server stops. At the deadline and at the stop the token is cancelled on a thread-pool
  thread, so a host's token callback never runs on the sweep's thread or on the thread that
  stops the server. The causes are told apart in this order: the request was aborted (nothing
  is written); the server is stopping (CONNACK `0x8B`); otherwise the deadline passed (CONNACK
  `0x88`).
- If it does not answer in time, the socket is closed and MqttWire lets go of the connection.
  **A handler that ignores cancellation keeps running; MqttWire cannot stop it.** What it still
  holds is the request it was given, not the connection.
- Whatever it returns or throws afterwards is observed, logged and discarded.
- **A handler that blocks the calling thread** (it does not return a pending task, and it
  ignores its token) keeps the connection's own thread inside the host, so the CONNACK, which
  that thread would write, cannot be written. The sweep bounds the socket all the same: when the
  call has not come back one close deadline (2 s) after its token was cancelled, the link is
  aborted and the connection is Closing ("authentication timed out", or "server stopping"
  during a stop). What only the connection's run can release stays held until the call returns:
  the connection's slot, the read buffer if packets came behind the CONNECT (6.8), and the
  request. A late answer changes nothing: the admission step refuses a connection that is no
  longer Authenticating.

#### Life of a connection

1. The endpoint accepts the WebSocket and creates the connection (Accepted).
2. The first packet must be a complete CONNECT, within the connect deadline. A first packet that
   is not a CONNECT, or whose fixed header is malformed, gets no MQTT answer: the WebSocket is
   closed with status 1002. A CONNECT longer than 16 KB gets CONNACK `0x95`, judged from its
   fixed header. A CONNECT the codec refuses gets a CONNACK with the codec's reason code.
3. MqttWire checks what is its own to check:

   | Check | Refusal |
   |---|---|
   | protocol version is 5 | `0x84` |
   | the properties of 6.7 | as 6.7 says |
   | no Will | `0x87`, "Will messages are not supported." |
   | the client id is not empty and not longer than 256 bytes | `0x85`. An empty client id is well-formed MQTT, so the codec accepts it and this check refuses it. |

4. `OnConnectAsync`: the host accepts or refuses. Any further rule for client ids, and all of
   authentication, is the host's.
5. **The admission step** (below): `OnAdmit`.
6. CONNACK: success, session present 0, the user properties the host returned, and the
   properties 6.7 lists. A refusal carries the reason code and reason string and then closes.
7. The read loop runs until the connection ends.
8. `OnClosed`, once.

#### Admission and takeover

A server holds at most one Active connection per client id.

**The admission step is one atomic step, after `OnConnectAsync` has accepted and before CONNACK
is handed to the link.** Inside it MqttWire looks up the connection that currently has the
client id, calls `OnAdmit(new, thatOneOrNull)`, and, if the host accepts, makes the new
connection the one in its table. From that step on:

- The new connection is the authoritative one for its client id. A connection it replaced is
  closing, with DISCONNECT `0x8E` (session taken over) if that can be written, and its
  `OnClosed` reason says "taken over". It is not brought back, whatever happens next.
- If the new connection then fails (its CONNACK cannot be written, its socket is already
  gone), that is an ordinary end: `OnClosed` is called for it.
- Two CONNECTs for one client id that finish `OnConnectAsync` at the same time each go through
  the step, one after the other. The second sees the first as `replaces`.

**No MQTT packet precedes the CONNACK.** The send path is reserved for the CONNACK from the start
of the admission step, and the CONNACK is its first write. A host's `TrySend` between the
admission and the CONNACK is taken and waits behind it. A connection that is closed in that
window (a takeover, the stop, the host closing it from inside `OnAdmit`) finds a write in
progress, so its link is aborted: it gets no CONNACK and no DISCONNECT, and `OnClosed` is still
called once. The window ends in the step, under the lock of the send path, that finds the
connection still Active and starts the CONNACK's write: a close before that step means no
CONNACK is handed to the link; a close after it meets a write in progress like any other.

The host decides in `OnAdmit` whether a replacement is allowed (for example only for the same
user) and does its own accounting there (for example connections per user). Because the step is
atomic and `OnClosed` is exactly-once, such accounting cannot drift: every accepted `OnAdmit`
is matched by one `OnClosed`, and a takeover is visible on both sides (`replaces` in one,
"taken over" in the other).

A connection stays in the table from its admission until it is Closed: it is removed just before
its `OnClosed`, and until then `TryGetConnection` finds it and `ConnectionCount` counts it. So a
CONNECT for a client id whose connection is already closing sees that connection as `replaces`;
the old one keeps the close reason it already had. Removing a connection from the table compares
the object, not the client id, so the end of an old connection can never remove its replacement.

#### Reading, with no buffer while idle

```
loop:
    wait for data (zero-byte read)            <- an idle connection is parked here, holding no MqttWire buffer
    rent a buffer from the shared pool
    read; handle every complete packet in the buffer
    while a packet is incomplete: read more (growing the buffer up to the packet's limit)
    return the buffer
```

- **A PUBLISH that is too large is refused without being buffered.** The first bytes of a
  packet give its Remaining Length, on which the bands below are judged. In a PUBLISH that length
  covers the topic, the packet identifier (only at QoS 1 and 2), the properties and the payload,
  in that order. A QoS above `MaxQoS` is judged from the first byte, before anything is skipped:

  | Remaining Length of the PUBLISH | What happens |
  |---|---|
  | up to `MaxPublishPayloadBytes` + 5 KiB | The packet is read whole and judged exactly: a payload over the limit is refused, anything else goes to `OnPublish`. The 5 KiB are room for the topic and the properties. |
  | above that, up to 16 times `MaxPublishPayloadBytes` | **Skip.** Only what is needed to find the packet identifier is read: the topic's length, then the topic's bytes are passed over without being kept, then the identifier. The rest is discarded as it arrives, across as many WebSocket messages as it takes, until exactly the packet's length has been consumed. Then it is refused. |
  | above 16 times `MaxPublishPayloadBytes` | DISCONNECT `0x95` (packet too large), close. |

  The refusal is `0x97` (quota exceeded) with "The message is too large.", in the form the QoS
  calls for: **a PUBACK at QoS 1, a failing PUBREC at QoS 2 (and nothing is remembered), nothing
  at QoS 0.** `OnPublish` is not called for it. At QoS 1 and 2 it counts against the receive
  maximum like any other publish, and a QoS 2 publish whose identifier is remembered is answered
  as a repetition (PUBREC success). If the skipped part is itself broken, that is a malformed
  packet (`0x81`): its topic length runs past the packet, or it ends before its identifier, or
  it ends right after its identifier (at QoS 0, right after its topic) with no property length.
  An identifier of 0 is a protocol error (`0x82`), as in a PUBLISH read whole.
- Any packet other than PUBLISH that is longer than 16 KB closes the connection with `0x95`.
- **A rented read buffer is never held for longer than the partial-packet deadline without a
  complete packet.** The deadline runs from the rent, not from the first byte read: the wait
  can end with nothing to read yet (a WebSocket frame whose payload the peer withholds), and
  the read would otherwise hold its buffer until the keep-alive deadline. In the Accepted
  state the connect deadline is that bound.
- The connection uses the request's own cancellation (`HttpContext.RequestAborted`) and
  `Abort()` on the link to stop a parked read.
- **What the parked read keeps alive is part of the idle cost.** In C#, every local variable of
  an `async` method that is alive across an `await` becomes a field of an object on the heap,
  and for the read loop that object lives as long as the connection. So: the method that waits
  in the zero-byte read carries as few locals as possible; the work on a rented buffer happens
  in a separate method, whose state exists only while a packet is being handled; and no
  reference to a buffer is still held once the buffer has been returned (6.8).

#### What each packet does

| Packet | Behaviour |
|---|---|
| PUBLISH, QoS 0, 1 or 2 | `OnPublish`. QoS 1 gets a PUBACK at once, QoS 2 a PUBREC (see "QoS 1 and 2"): success, or the host's refusal. QoS 0 gets nothing. A QoS above `MaxQoS`: DISCONNECT `0x9B` (QoS not supported), close; CONNACK has announced "maximum QoS 1" in that case. |
| PUBACK, PUBREC, PUBREL, PUBCOMP | See "QoS 1 and 2 inside a connection". |
| SUBSCRIBE | `OnSubscribe` once per filter; SUBACK carries one reason code per filter, and the reason string of the host's first refusal. A filter that is not a valid MQTT filter gets `0x8F` (topic filter invalid) without the host being called. Subscription options are passed on to nobody and have no effect. |
| UNSUBSCRIBE | `OnUnsubscribe` once per filter; UNSUBACK with success. |
| PINGREQ | PINGRESP. |
| DISCONNECT | Close, with no DISCONNECT back. |
| A second CONNECT, AUTH | DISCONNECT `0x82` (protocol error), close. |
| A packet the server never reads: CONNACK, SUBACK, UNSUBACK, PINGRESP, the reserved type 0 | DISCONNECT `0x82` (protocol error), close. |
| A malformed packet | DISCONNECT `0x81` (malformed packet), close. |

#### Sending: taken at once, written when the socket allows

**The contract.** `TrySend`, and the control packets the read loop produces, **return as soon
as the connection has taken the message or refused it. They never wait for the socket.** A host
that sends one message to many connections therefore runs at its own speed, not at the speed of
the slowest recipient.

A WebSocket allows one write at a time, so the connection has one send path, and **a write is
in progress whenever the send path is taken**: a piece or a close is with the link, a caller is
about to start one, the path is reserved for the CONNACK (6.6, "Admission and takeover"), or it
is being handed to the thread pool (below), even when no bytes are with the link at that
instant.

- **No write in progress:** the caller encodes the packet into a rented buffer and starts the
  write. If the write completes at once, which is the normal case, the buffer goes back to the
  pool and nothing was allocated for this connection. If it does not, the caller returns
  anyway; the write's completion carries on.
- **A write in progress:** the message is added to what waits for this connection. Whoever
  completes the write in progress sends what has been added, by the scheduling rule below. A
  caller writes at most 16 packets on its own thread while the writes complete at once (one
  PUBLISH counts once, however many pieces it is written in), then hands the send path to the
  thread pool; so the drain, and the end of a connection with its `OnClosed`, can run on a
  thread-pool thread.
- **What waits is pooled per server.** The queue of waiting deliveries and the QoS exchange
  records are objects taken from a pool of the server's with the first waiting delivery or the
  first exchange, and returned, cleared, with the last. At most 1,024 of each are retained, and
  a queue that grew past 64 entries is not retained. An idle connection holds neither.
- **A rented encoding buffer is never larger than 64 KB.** A message whose whole packet is at
  most 16 KB is copied behind its header into one buffer and written at once. For a larger one
  the header is written from a buffer of its own and the payload is then written straight from
  the memory the host handed over, in pieces of at most 64 KB, so no second copy of a large
  payload ever exists.
- **A writer that throws inside MqttWire** (an encoding, the array pool, an object pool, the
  clock) gives the send path up: the connection ends as a lost connection, the fault is logged
  as an error (6.12, event 20), what waited is dropped, and nothing is left taken. A link whose
  write throws is a failed write like any other: a lost connection (event 18), and the link is
  aborted.

**Scheduling: control packets never wait behind a delivery.** A single ordered list can
deadlock: the client's window is full, its PUBREC arrives, and the PUBREL that would free the
window is stuck behind the delivery that waits for the window. So what waits is of two kinds:

- **Control packets:** CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK, PINGRESP,
  DISCONNECT. They are never held back by a QoS window. A control packet that has to wait is
  encoded at once into one control buffer per connection, which exists only while control
  packets wait. When a write completes, the waiting control packets are written before any
  waiting delivery, in the order they were produced, together as one WebSocket message. One
  PINGRESP that waits answers a further PINGREQ too.
- **Deliveries:** what the host sent, in the order of the `TrySend` calls. A delivery at QoS 1
  or 2 that has no room in the client's window waits, and so do the deliveries after it. It
  never holds back a control packet.
- **What waits as control is bounded.** Answers to the client's publishes are bounded in number
  by the receive maximum the server announced. At most one PINGRESP waits. Whatever the mix,
  what waits as control is bounded twice: 1,024 packets, and the 64 KB of the one control
  buffer. Passing either evicts the connection with `0x97`, and the bytes count towards
  `MaxOutgoingBytes`. A client that lets 64 KB of answers wait behind one stuck write has
  stopped reading.
- How this is stored is the builder's choice. The rule is not.

**Limits on what waits:**

- `MaxOutgoingMessages` counts **deliveries that are waiting**: behind a write in progress, or
  for room in a QoS window.
- `MaxOutgoingBytes` is a **hard bound** on the payload bytes this connection holds on the way
  out: every waiting delivery, the delivery being written, and the waiting control packets.
- **A single message larger than `MaxOutgoingBytes` is never sent.** `TrySend` says "too
  large", and the connection is closed with `0x95`. What to log about it is the host's (it
  knows what the message was).
- A connection that would pass a limit is **evicted**: closed with `0x97` (quota exceeded).

**What the limits promise, exactly.** A connection has waiting deliveries only while a write is
in progress or a window is full. A burst can outrun even a healthy socket for a moment. So the
promise is: **a client survives a burst that fits within the two limits; one that falls further
behind is disconnected.** The network's own buffers absorb some data before a write stalls, so
a slow consumer is noticed after "what those buffers hold, plus the limits".

**A client's "maximum packet size".** A CONNECT that announces less than 4 KiB is refused with
`0x95`. Every control packet then fits, because its variable parts are bounded: a reason string
is cut to 256 bytes, and the user properties a host may put into CONNACK are limited to 1 KiB
in total. A reason string that would still make a packet too large for the client is left out.
The one exception is a SUBACK or UNSUBACK, which carries a reason code per topic filter: one that
would be too large even without a reason string (thousands of filters against a client that
announced 4 KiB) cannot be sent, so the connection is closed with `0x95` and the host is not
called for that packet.
A message for a connection that is larger than what that connection announced cannot be sent to
it; dropping it silently could leave the client waiting for ever, so `TrySend` says "too large"
and the connection is closed with `0x95`.

**Worst case held for one connection** (with the defaults):

| What | Bound |
|---|---|
| Before CONNECT is complete | one read buffer of at most 16 KB, for at most 10 s; when packets came behind the CONNECT in it, the same buffer through Authenticating, for at most 10 s more |
| Read buffer while a packet is arriving | `MaxPublishPayloadBytes` + 5 KiB, for at most 10 s without a complete packet |
| On the way out | `MaxOutgoingBytes` (1 MiB), plus one encoding buffer of at most 64 KB |
| QoS exchange records | 256 for what the client sends, 256 for what the server sends, a few bytes each |
| Waiting control packets | 1,024 and 64 KB, counted in `MaxOutgoingBytes` |
| How long a stalled connection holds any of it | the write deadline (15 s) |

What the host holds for a connection (its state, anything it queued from `OnPublish`) is the
host's to bound.

#### QoS 1 and 2 inside a connection

The server supports all three QoS levels in both directions. The connecting client chooses the
QoS of what it publishes; the host chooses, per `TrySend`, the QoS of what it sends (normally
the QoS it granted to the subscription). `MaxQoS` caps both. A connection that uses QoS 0 for
what it receives costs exactly what it costs without this section.

The exchanges are complete and standard. They do not add a guarantee across reconnects: there
is no stored session, and MQTT 5 forbids sending a message again inside a connection. The state
is therefore **a small record per exchange, never a payload**:

```text
packet identifier
kind and phase:   publish from the client at QoS 2, waiting for PUBREL
                  delivery at QoS 1, waiting for PUBACK
                  delivery at QoS 2, waiting for PUBREC
                  delivery at QoS 2, waiting for PUBCOMP
when it entered this phase
```

- **A record is in place before its packet is handed to the link.**
- **The acknowledgement deadline runs per phase**, and starts again when a QoS 2 exchange moves
  to its next phase. The sweep looks at the oldest record of a connection.
- **An acknowledgement of the wrong kind for a known identifier is a protocol error** (`0x82`,
  close). One repetition is harmless: a PUBREC for a delivery already waiting for PUBCOMP, with
  a failure code or without one, is answered with PUBREL again and changes nothing.
- The records of a connection are taken from the server's pool with the first exchange and
  returned with the last (see "Sending").

*Publishes from the client at QoS 2:*

1. PUBLISH with identifier *n*, not seen before: `OnPublish`. If the host accepts, *n* is
   remembered and the answer is PUBREC (success). If it refuses, the answer is PUBREC with the
   refusal, and *n* is not remembered.
2. PUBLISH with an identifier that is remembered: `OnPublish` is not called again; the answer
   is PUBREC (success) again. **This holds whatever the packet's DUP flag says.**
3. PUBREL *n*: *n* is forgotten; the answer is PUBCOMP. For an identifier that is not
   remembered, PUBCOMP carries `0x92` (packet identifier not found).
4. CONNACK announces **"receive maximum" 256**. Against it the server counts, together, the
   QoS 2 identifiers it remembers and the answers to the client's publishes (PUBACKs, PUBCOMPs
   and failing PUBRECs) that wait and have not yet been handed to the link. A publish refused
   as too large counts like any other, and a repetition of a remembered identifier takes no new
   place. The 257th closes the connection with `0x93`, before `OnPublish`.

*Deliveries at QoS 1 or 2:*

1. At QoS 1 the PUBLISH carries an identifier, in use until the client's PUBACK. At QoS 2 it is
   in use until PUBCOMP. A PUBREC with a failure code ends the exchange.
2. Identifiers come from a counter (1 to 65535, wrapping) that skips those that have a record.
   The counter is the connection's: it goes on from where it was when the records go back to
   the pool, so an identifier whose exchange has just ended is not handed out again at once.
3. **The client's "receive maximum" is honoured:** the window for a connection is the client's
   receive maximum or 256, whichever is smaller. A place in it is free again on PUBACK, on
   PUBCOMP, and on a PUBREC with a failure code. A delivery that has to wait for the window
   counts towards the outgoing limits, so a client that asks for QoS 1 or 2 and does not
   acknowledge is evicted by them, or by the acknowledgement deadline.
4. A PUBACK, PUBREC or PUBCOMP for an identifier that has no record is ignored, except that an
   unknown PUBREC is answered with a PUBREL carrying `0x92`, unless it carries a failure code
   itself: a failed PUBREC ends its exchange, so no PUBREL may follow it (as in the client, 6.9).
5. A delivery that MqttWire has taken is delivered even if the client unsubscribes meanwhile;
   MQTT allows that, and MqttWire does not know which subscription a delivery belongs to.

#### Closing, bounded

Every close the server starts follows the same steps, and none of them waits on the peer:

1. Enter Closing (once). `TrySend` now says "not open".
2. **If a write is in progress, abort the link now.** A socket that is not taking data is never
   asked to carry its own eviction notice, and a WebSocket close would only queue behind the
   write that is stuck. "A write in progress" is the send path being taken ("Sending"), so this
   holds also while the path is reserved for the CONNACK or handed to the thread pool.
3. If no write is in progress: hand DISCONNECT with the reason code to the link, then start the
   WebSocket close through the same one-at-a-time send path, without waiting for the peer's
   answer. The link is aborted at the close deadline. A connection that was never admitted gets
   a CONNACK with a failure code instead of the DISCONNECT; a client's DISCONNECT, a lost link
   get no packet, only the WebSocket close; a fault of the send path gets no packet, and the
   link is aborted.
4. **Aborting does not release anything by itself** (6.8). The read loop and the write in
   progress each end with an error once the link is aborted; each returns its own buffer.
5. The connection is Closed when the read loop has ended, no write is in progress and no abort
   of its link is in flight. In this order: it lets go of the link, it is removed from the
   client-id table (by object), it becomes Closed, `OnClosed` is called (if it was admitted),
   and only then is its slot freed and does the endpoint's request return to Kestrel. The stop
   waits for the slots.

**A link is never aborted once its request has ended.** ASP.NET Core's WebSocket aborts its
`HttpContext`, and the first abort after the request has ended throws; over HTTP/2, where
Kestrel reuses the stream and its context for a later request of the same connection, it could
reach that request instead. An abort decided on one thread (the sweep, the stop, a takeover, a
host's `Close` or `TrySend`) can meet the end of the run on another (the peer closes at that
moment). So every abort goes through one rule, under the lock of the send path:

- Whoever aborts first registers the abort, in a hold of the lock that finds the run not ended
  and no abort in flight; where the abort follows a decision to end the connection, that is the
  hold that makes the move to Closing. It then aborts outside the lock, and clears the
  registration in a second hold. With an abort in flight, a second one is not issued: the first
  does its work.
- The run ends only in a hold that finds no write in progress and no abort in flight, and in
  that hold it lets go of the link and of the request's `HttpContext`. From then on nothing
  aborts: a late `Close`, `TrySend` or sweep finds the link gone. And a connection object the
  host keeps after `OnClosed` keeps neither the socket nor the request alive.
- A run that finds an abort in flight waits for it as it waits for a write in progress, and
  carries on on a thread-pool thread; so the end of a connection, `OnClosed` included, never
  runs inside an abort.
- A link that throws when it is aborted is logged (6.12, event 20) and nothing is thrown at the
  caller; the sweep aborts it again at the close deadline. The sweep itself lets no exception
  out of a connection's check: it ends that connection as a fault (event 20) and goes on to the
  next, because an exception on its timer's thread would end the process.

#### When the host stops

1. Stop accepting: a new WebSocket request is answered with HTTP 503, and the admission step
   lets no connection in (it refuses with `0x8B` without calling `OnAdmit`).
2. Every connection is ended as its state calls for: an Active one is closed as above with
   `0x8B` (server shutting down); an Accepted one is aborted; an Authenticating one has its
   token cancelled and gets CONNACK `0x8B`. The stopping flag is read in the same step in which
   a connection takes its slot, so a connection either is seen by the stop's walk or sees the
   flag: one that takes its slot after the stop began is aborted at once.
3. Links that are still open after the close deadline are aborted.
4. The server waits for the read loops and writes to end, so that every buffer is back and
   every `OnClosed` has been called.
5. The sweep stops.

The whole takes a few seconds at most and does not depend on the host's handlers returning from
long work, because they do no long work on MqttWire's threads (6.2). A host must not wait on the
stop from inside `OnClosed`.

**The stop does not wait for an `OnConnectAsync` that does not return.** The stop completes
when every connection that can end has ended. A connection whose `OnConnectAsync` has not come
back one close deadline after its token was cancelled (which the stop does when it begins,
unless the authentication deadline already had) has its link aborted, is logged as an error
(6.12, event 22, with the client id and the method's name), and is no longer counted by the
stop. It keeps its slot, and what the paragraph on `OnConnectAsync` lists, until the call
returns; the sweep has stopped by then, and nothing more is done for it.

### 6.7 What is done with each MQTT property

"Ignored" means: validated for form, then without effect, which MQTT 5 allows for that
property. It never means "skipped by its length without looking": an ignored property must
still be one that is allowed in that packet, appear no more often than it may, and hold a value
of the right type.

**Which error is which:**

- **Malformed packet (`0x81`):** the bytes do not form a valid packet. A length that runs past
  the packet, a bad variable-length integer, invalid UTF-8, wrong reserved flags, an unknown
  property, a property in a packet it does not belong to, a once-only property twice.
- **Protocol error (`0x82`):** the packet is well formed, but not allowed now or with that
  value. A second CONNECT, an acknowledgement of the wrong kind, receive maximum 0, maximum
  packet size 0, a subscription identifier in a client's PUBLISH, an AUTH packet.

Where MQTT names a more specific reason code, that one is used.

**CONNECT, read by the server**

| Property | Treatment |
|---|---|
| Session Expiry Interval | Overridden: if the client asks for more than 0, CONNACK carries Session Expiry Interval 0. |
| Receive Maximum | Honoured for deliveries at QoS 1 and 2. The value 0 is a protocol error. |
| Maximum Packet Size | Honoured (6.6). 0 is a protocol error; less than 4 KiB is refused. |
| Topic Alias Maximum | Ignored: the server never uses aliases. |
| Request Response Information | Ignored. |
| Request Problem Information | **Honoured.** When it is 0, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK and UNSUBACK carry their reason code but no reason string and no user property. |
| User Property | Passed to the host in `MqttConnectRequest`. |
| Authentication Method, Authentication Data | Refused: CONNACK `0x8C`. |
| Will flag and Will properties | Refused: CONNACK `0x87`. |

**PUBLISH, SUBSCRIBE, UNSUBSCRIBE, DISCONNECT from a client**

| Property | Treatment |
|---|---|
| Payload Format Indicator, Content Type, Message Expiry Interval, Response Topic, Correlation Data, User Property (PUBLISH) | Ignored; not passed to the host in the first version. |
| Topic Alias | Refused: DISCONNECT `0x94`. The server announces no alias maximum. |
| Subscription Identifier in a PUBLISH | Protocol error (`0x82`). |
| Subscription Identifier in a SUBSCRIBE | Refused: DISCONNECT `0xA1`. |
| User Property, Reason String, Session Expiry Interval (others) | Ignored. |

**CONNACK from the server.** A success CONNACK always carries: session present 0, Receive
Maximum 256, Subscription Identifiers Available 0, and the host's user properties. When needed:
Maximum QoS 1 (only when `MaxQoS` is 1), Session Expiry Interval 0, Server Keep Alive. A
refusing CONNACK carries its reason code and, when there is one, its Reason String, and nothing
else. In MQTT 5 a
capability that is not announced counts as supported, so a server may refuse a subscription
identifier or a QoS 2 publish only after announcing that. "Maximum packet size" and "retain
available" are deliberately not announced: what a host does with a large or a retained publish
is the host's answer, in the acknowledgement. Wildcard and shared subscription availability are
not announced either: whether a filter is granted is the host's answer in SUBACK.

**CONNACK, read by the client**

| Property | Treatment |
|---|---|
| Session Present | Must be 0, on an accepting CONNACK and on a refusal alike; otherwise the connection is closed with DISCONNECT `0x82` (the client asked for a clean start, and a refusal may not claim a session). |
| Reason String, User Property | Given to the caller in the connect result. |
| Server Keep Alive, Receive Maximum, Maximum Packet Size | Honoured. Maximum Packet Size limits every packet sent after the CONNACK, acknowledgements, PINGREQ and DISCONNECT included: an acknowledgement or PINGREQ over it ends the connection without it, and a DISCONNECT over it is left out of the close. |
| Maximum QoS | Honoured: a publish above it fails at once. |
| Everything else | Ignored. |

### 6.8 Who owns a buffer

1. **A decoded view is valid only until the buffer it points into is returned or reused.**
   This is why `MqttPublish` is valid only during `OnPublish` and `OnMessage`.
2. **What the host hands to `TrySend` stays the host's memory**, owned by the garbage
   collector. MqttWire keeps a reference while the message waits or is being written, never
   changes it, and never puts it into a pool.
3. **A buffer handed to the link belongs to that operation until the operation has ended.**
   "Ended" means its task has completed, with success or with an error. Asking for an abort is
   not the end of anything.
4. **Only the code that awaits an operation returns that operation's buffer, and only after the
   await has ended.** Whoever aborts, closes or evicts a connection never returns a buffer on
   its behalf. If an operation were never to end, its buffer would be lost to the pool; that is
   a leak, which the tests catch, and never memory that two connections share.
5. **Every way out returns what is held, once:** normal completion, a refusal, an overflow, a
   deadline, an exception, an abort.
6. **Nothing keeps a reference to a buffer it has returned.**

### 6.9 The client connection

The inside of `MqttWireClient`.

- **Connect:** open the link, send CONNECT (protocol 5, clean start, client id, user name,
  password, keep-alive, receive maximum 1,024, maximum packet size; no Will), wait for
  CONNACK within the connect timeout. The Maximum Packet Size comes from
  `MaxIncomingPacketBytes` (default 1 MiB, 4,096 to 268,435,460). A packet over it is refused as
  soon as its header is read, before it is buffered, with DISCONNECT `0x95`.
- **Packet identifiers.** PUBLISH, SUBSCRIBE and UNSUBSCRIBE draw from one space (1 to 65535).
  The client keeps a record for each identifier in use: the identifier, what it belongs to (a
  subscribe, an unsubscribe, a publish at QoS 1, a publish at QoS 2 waiting for PUBREC or for
  PUBCOMP), and when it entered that phase. The record is in place before the packet is handed
  to the link. An acknowledgement of the wrong kind for a known identifier is a protocol error
  and the end of the connection. The rules:
  - a new identifier is the next one after the last, skipping any that is in use, wrapping from
    65535 to 1;
  - an identifier is in use until its exchange ends: SUBACK, UNSUBACK, PUBACK, or for QoS 2 the
    PUBCOMP that follows PUBREC and PUBREL, or a PUBREC with a failure code;
  - an acknowledgement for an identifier that is not in use is ignored; an unknown PUBREC with a
    code below `0x80` is answered with a PUBREL carrying `0x92`, and one with a failure code is
    ignored, because a failed PUBREC ends its exchange and no PUBREL may follow it;
  - a packet whose write fails never frees its identifier on its own: a failed write is the end
    of the connection, and all records go with it;
  - there may be at most as many publishes in flight as the server's "receive maximum", and
    never more than 1,024. A publish beyond that fails at once and is not sent;
  - **if the oldest record is older than twice the keep-alive interval, the connection is
    treated as lost**, even while PINGRESPs arrive.
- **No re-sending.** A caller that wants a publish repeated after a lost connection sends it
  again on a new connection.
- **Receive:** a PUBLISH's payload is handed to `OnMessage` as a view, with no copy when it
  lies in one buffer. At QoS 1 it is handed on and answered with PUBACK. At QoS 2 it is handed
  on once and answered with PUBREC; its identifier is remembered until the server's PUBREL,
  which is answered with PUBCOMP; a PUBLISH that repeats a remembered identifier is not handed
  on again, whatever its DUP flag says. A PUBREL for an identifier the client does not remember
  is answered with PUBCOMP `0x92`. A PUBLISH with a Topic Alias ends the connection with
  DISCONNECT `0x94`. The client announces a receive maximum of 1,024: a new QoS 1 or 2
  PUBLISH while 1,024 QoS 2 identifiers wait for their PUBREL ends the connection with `0x93`
  before it is handed on; a repeat of a remembered identifier takes no new place.
- **Keep-alive:** one timer per client, ticking every half interval. On a tick the client sends
  a PINGREQ if nothing was sent for half the interval, so the time between packets never
  exceeds the keep-alive, as MQTT requires. One PINGREQ waits at a time: no second is queued
  while one waits for the send lock or for its PINGRESP. It ends the connection if a PINGREQ has
  waited a full interval for its PINGRESP, counted from when its write began, so a write stalled
  ahead of it is not taken for a silent server; or if the oldest record is older than twice the
  interval
  (above). The interval is the Server Keep Alive from the CONNACK when there is one, else the
  client's `KeepAlive`. A Server Keep Alive of 0 stops the PINGREQs and the ping deadline, but
  the acknowledgement deadline stays, at twice the client's own `KeepAlive`.
- **The end of a connection** is reported once, through `OnClosed`, for a connection that
  connected: a DISCONNECT from the server, a failed write, a failed read, a timeout. What the
  client sends when it ends one:
  - for a fault in the server's packets, DISCONNECT with `0x81`, `0x82`, `0x93`, `0x94` or
    `0x95`, then close 1000;
  - when `OnMessage` throws, DISCONNECT `0x83`, then close 1000;
  - on `DisposeAsync`, DISCONNECT `0x00`, then close 1000;
  - each close is bounded by a close timeout of 2 seconds, then the link is aborted;
  - a timeout, or a failed read or write, aborts the link without a DISCONNECT.
- **The client cannot have the server's deadlock, and must keep it that way.** Nothing on the
  client waits for a QoS window. So everything the client writes goes out in the order it was
  produced, and an acknowledgement never stands behind something that is waiting for the
  server.
- It must not block a thread (Blazor WebAssembly). A plain send lock or queue is fine: a client
  has one connection and does not need the server's "only while there is work" machinery.

### 6.10 Shared topics through the host

MqttWire does not route, but a host can put many connections on one topic in a few lines. This
is the design of a classic MQTT chat: a topic per group, every member subscribed.

```csharp
MqttSubscribeResult OnSubscribe(MqttWireConnection connection, string topicFilter, MqttQoS requested)
{
    if (!MayJoin(connection, topicFilter)) return MqttSubscribeResult.NotAuthorized;
    _subscribers.Add(topicFilter, connection, requested);      // the host's own table
    return MqttSubscribeResult.Granted(requested);
}

MqttPublishResult OnPublish(MqttWireConnection connection, in MqttPublish publish)
{
    byte[] payload = publish.Payload.ToArray();                // one copy, shared by all recipients
    MqttTopic topic = MqttTopic.Create(publish.Topic);
    foreach ((MqttWireConnection member, MqttQoS granted) in _subscribers.Get(publish.Topic))
        member.TrySend(topic, payload, Lower(publish.QoS, granted));
    return MqttPublishResult.Accepted;
}
```

What MqttWire guarantees for this, and what `samples/GroupChat` and a test show:

- **Any number of subscriptions per connection**, each granted or refused by the host.
- **One payload goes to many connections without a copy per recipient.**
- **A slow member never delays the others**, and one who falls too far behind is disconnected.

What the host does itself: the table and its clean-up in `OnUnsubscribe` and `OnClosed`; who
may subscribe and publish; wildcards, if it wants them; whether the sender gets its own message
back; and using the lower of the publish's QoS and the granted QoS.

`_subscribers.Get` above stands for a snapshot of the recipients. `TrySend` can close a
connection (an eviction, a message too large). That connection's `OnClosed` never runs inside
the call (6.2); it runs on another thread, where it waits for any lock the host holds across
`TrySend`. So the host takes a snapshot of the recipients under its lock and sends after
releasing it, as `tests/MqttWire.TestHost/ExactTopicHandler.cs` does.

### 6.11 Kept open for later

- **A routing helper.** An optional component that keeps the topic-to-connections table and
  does the fan-out, so a chat host needs only its authorization checks. It sits on top of the
  handler interface, so adding it changes nothing underneath. Not in the first version: what it
  should do (wildcards, retained messages) is clearer once a messaging app is built on it.
- **Message retention.** The owner wants it later, for messaging apps. MqttWire passes the
  retain flag of a publish to the host and does not announce "retain available 0", so a host,
  or the helper above, can add retained messages without a change on the wire. Stored history
  for clients that come back is a matter for the application above MqttWire.
- **Own WebSocket framing**, behind the link of 6.5, if the floor of 6.14 turns out to be most
  of the cost.
- **.NET Standard 2.1 / Unity** for the client. The client uses nothing that could not be
  brought to it, but it is not built or tested there now.

### 6.12 Logging: why a connection ended

Every end of a connection is logged once, by whoever wins the move to Closing, with the client
id and one of these reasons, each with its own event id. Events 19 and 22 are not ends: 19 is
how a host task that the connection stopped waiting for ended, 22 is a host call the stop did
not wait for. `MqttCloseReason` names the reasons
an admitted connection can end with, two of which the server does not decide:
`ClientDisconnected` and `ConnectionLost`.

| Event | Reason | Level, and what the line carries besides the client id |
|---|---|---|
| 1 | malformed packet | debug; the reason code |
| 2 | protocol error | debug; the reason code |
| 3 | refused by the host | debug; the host's reason code |
| 4 | authentication timed out: `OnConnectAsync` did not answer in time, and the connection was refused with CONNACK `0x88`, or aborted when the call did not come back | error |
| 5 | connect timed out | debug; the remote end point instead of the client id |
| 6 | keep-alive timed out | debug |
| 7 | partial packet timed out | debug |
| 8 | write timed out | debug |
| 9 | acknowledgement timed out | debug |
| 10 | outgoing limit passed | debug |
| 11 | receive maximum passed | debug |
| 12 | message too large to send | debug; the payload's length |
| 13 | taken over | debug |
| 14 | closed by the host | debug; the host's reason code |
| 15 | handler threw | error; the method's name and the exception |
| 16 | server stopping | debug |
| 17 | the client disconnected | debug; the client's reason code |
| 18 | the connection was lost | debug |
| 19 | a late `OnConnectAsync` result, observed and discarded | debug; error, with the exception, when it threw |
| 20 | a fault inside MqttWire: something threw while it sent (6.6, "Sending"), a link threw when it was aborted, or the sweep's check of the connection threw (6.6, "Closing, bounded"); a link whose write fails or throws is event 18 | error; the exception |
| 21 | the run of a connection ended with an exception that is not what a broken link gives (a WebSocket, I/O, cancellation or disposed-object exception): a bug, never hidden behind event 18 | error; the exception |
| 22 | a host call had not returned when the server stopped; the stop does not wait for it (6.6, "When the host stops"). Not an end: the connection's end is logged with event 16 or 4 | error; the method's name |

A connection whose CONNECT has not passed MqttWire's own checks has no client id yet: its line
names the remote end point instead. An incoming packet that is too large, a topic alias, an
unsupported QoS and a subscription identifier end with "protocol error"; so do a first packet
that is not a CONNECT, a CONNECT the codec refuses with a code other than `0x81` (such as
`0x84`, unsupported protocol version), and a CONNECT refused by MqttWire's own checks: `0x8C`
(bad authentication method), `0x87` (a Will), `0x95` (a maximum packet size under 4 KiB, or a
CONNECT over 16 KB) and `0x85` (client id not valid). The specific reason code is in the line
and, where a packet can carry it, on the wire.

**No log line contains a password, a user name, a payload, a topic, user properties or the
host's reason string.** The same reason is what `OnClosed` receives, with one exception: a
fault that ends a connection is logged as event 20 and reported to `OnClosed` as a lost
connection (a link that throws when it is aborted ends nothing: the connection keeps the reason
it was closing with). The three
ends before admission (refused by the host, authentication timed out, connect timed out) are
logged but never reach `OnClosed`. And an end can show two lines: events 15, 20 and 21 are
written by the code that caught the exception, even when its own close lost the race to another
(for example "closed by the host" followed by "handler threw", or "the connection was lost"
followed by event 21, which `OnClosed` receives as a lost connection).

### 6.13 Tests

1. **Codec:**
   - known byte sequences for every packet of 6.4, in both directions;
   - every malformed case and every row of 6.7;
   - every packet stream cut at every byte position and fed in pieces;
   - a seeded random test: valid packet streams split and joined at random positions decode to
     the same packets; random and mutated bytes end in a packet or an error result. The test
     also asserts that the decoder allocates nothing, that its work is bounded by the bytes it
     is given, that every call either consumes bytes or reports that it needs more, and that an
     incomplete or rejected packet changes no state. A failing input is kept as a fixed case
     with its seed.
2. **Server connection over the in-memory link:**
   - every row of "what each packet does", of the deadlines table and of the CONNECT checks;
   - several packets in one message; a packet split across messages;
   - a PUBLISH with a large topic or large properties and a small payload, in each of the three
     bands, at QoS 0, 1 and 2, cut across messages at every position;
   - admission: a takeover the host accepts and one it refuses; two CONNECTs for one client id
     finishing `OnConnectAsync` together; a new connection that fails right after admission;
     every accepted `OnAdmit` is matched by exactly one `OnClosed`;
   - an `OnConnectAsync` that never returns and ignores cancellation, and one that blocks its
     calling thread, at the deadline and at the stop;
   - a link that throws when it is aborted, at every place that aborts; a peer that closes
     between the decision to abort and the abort: the run ends only after the abort;
   - a handler method that throws, in each of the six methods;
   - an idle, connected connection costs MqttWire at most 768 bytes (6.14);
   - "request problem information" 0; a client with a small "maximum packet size";
   - QoS 2 from the client: the full exchange; a refused publish; a repeated identifier with
     DUP 0 and with DUP 1; PUBREL for an unknown identifier; the receive maximum; a PUBREL that
     never comes; `MaxQoS` 1;
   - deliveries at QoS 1 and 2: the full exchanges; the window is never passed; waiting
     deliveries count towards the limits; the acknowledgement deadline; unknown and wrong-kind
     acknowledgements; an acknowledgement that arrives while the write of its own packet has
     not completed; identifiers across the wrap; idle again after the last acknowledgement;
   - **the window deadlock, step by step:** receive maximum 1; delivery A at QoS 2; delivery B
     waits; PUBREC for A arrives; PUBREL is written although B still waits; PUBCOMP arrives; B
     goes out. The same with a PINGREQ and with a QoS 1 publish arriving while B waits;
   - many small valid packets from a client that reads nothing: at most one PINGRESP waits, the
     receive maximum closes the connection, and what waits never passes the bounds;
   - a stalled write: the write deadline aborts, no DISCONNECT is attempted, and what was held
     is released;
   - the host stopping while connections are active and while a takeover is under way.
3. **"Only while there is work" on the send path.** Two kinds of test, both required:
   - **Deterministic:** the in-memory link holds a write open and releases it at a chosen
     point, so the exact interleaving "the drain has just found nothing waiting" and "another
     caller adds a message" is forced, in both orders.
   - **Stress:** many threads call `TrySend` on one connection while the link completes writes
     sometimes at once and sometimes later; everything is written exactly once, in order, and
     the connection ends idle. The sweep runs on a thread of its own meanwhile, and in the test
     of a close racing the senders it moves the clock, so that its deadlines pass during the
     race.
4. **Buffers.** A counting pool asserts that every rented buffer is returned exactly once
   across every way out. A second pool **overwrites every returned buffer and hands it out
   again at once**; with it, a link that holds a read or a write open is aborted, also at the
   same moment as the operation's completion, and the test asserts that the operation still in
   flight never sees or changes the reused memory. Repeated connect, work and disconnect cycles
   assert that nothing accumulates.
5. **Bounds are asserted after every step of these tests, not only at the end:** records,
   waiting packets and bytes, rented buffers, on whichever pool the server rents from; and what
   a WebSocket demands of the server on every link: one write or close at a time, nothing begun
   after its abort but the one write already decided on, and no abort after the run has ended.
6. **Client:** identifiers across the wrap with identifiers still in use; a PUBACK that never
   arrives ends the connection; unknown and wrong-kind acknowledgements; a small receive
   maximum; QoS 2 in both directions; a server that grants less than was asked; option checks.
   **The reconnecting client**, with its clock replaced: a lost connection is followed by
   attempts at the documented delays; the options callback runs before each attempt; a new
   client instance is used every time; subscriptions are made again before `OnConnected`; a
   refused one is reported and forgotten; "bad user name or password" stops it and a timeout
   does not; a publish fails at once while disconnected and one in flight completes with
   "connection lost"; the callbacks come one at a time and in the documented order; disposing
   during a back-off, during an attempt and while connected ends it with no callback after.
7. **Interop:**
   - MqttWire's server with MQTTnet's client, and with MQTT.js (`tests/interop-js`);
   - MqttWire's client with MQTTnet's broker;
   - MqttWire's client with MqttWire's server;
   - each at QoS 0, 1 and 2 in both directions;
   - over Kestrel: sockets closed by the client just as the sweep acts on their connect
     deadline, and a WebSocket frame whose payload is withheld.
8. **Shared topics** (6.10): ten connections on one topic through a small host; one message
   reaches all; one member stops reading and the others keep receiving in time.
9. **The client in a browser:** the sample is run once in Blazor WebAssembly by hand.

### 6.14 Measurement and the memory target

`bench/MqttWire.Bench` holds idle connections against three things:

1. **The floor:** a bare ASP.NET Core WebSocket endpoint that accepts with sub-protocol `mqtt`,
   keep-alive off, and waits in a zero-byte read. (It is not zero: .NET's WebSocket object
   keeps, among other things, a 125-byte receive buffer of its own per socket.)
2. **MqttWire with a handler that accepts everything and does nothing.**
3. The same after activity (every connection has received a burst and published, at QoS 0 and
   at QoS 2, then all is quiet), and after churn (all disconnected, then reconnected).

- It runs at two connection counts, *N* and at least 4 *N*. **"Per connection" is a slope:**
  (bytes at the larger count − bytes at the smaller) ÷ (the difference in connections). This
  cancels what does not grow with connections.
- **Three figures are reported, and only the first decides:** live managed bytes after a full
  collection, as a slope; what the buffer pool keeps after activity, which must not grow with
  connections; and working set, for information.
- The time one sweep takes at the larger count is reported.
- One run with TLS in Kestrel is reported, not decisive.

**The target: MqttWire adds no more than 1 KB to the floor for an idle connection**, before
and after activity alike. The owner kept it on 2026-11-20, once GrainWire's step 0 had measured
the floor at about 11,900 bytes (11,917, 11,921 and 11,915 bytes in three runs, in
[the step 0 report](https://github.com/mo-esmp/grainwire/blob/main/docs/step-0-idle-memory.md)).
If the floor alone is most of what a connection costs today in
GrainWire, the owner decides whether own WebSocket framing is worth looking at (see the
companion design).

The benchmark comes with the end of step 3. Until it runs, the only measurement is a unit test
over the in-memory link: there an idle connection costs MqttWire 668 bytes (in a Release build,
measured on 2026-11-23; the smallest of 669, 668 and 668), held to 768, which leaves out the
WebSocket link object and its parked read.

### 6.15 Documentation, versions, maintenance

- **README:** what MqttWire is and is not (section 3, in plain words), a short, complete
  handler that shows the two rules a host most easily gets wrong (copy a payload that outlives
  `OnPublish`; send at no higher QoS than was granted), the group chat sample.
- **`docs/mqtt-support.md`:** the part of MQTT 5 that is implemented: the packets of 6.4, the
  property tables of 6.7, the limits and deadlines of 6.6. A report can be checked against a
  stated scope.
- **Versions:** semantic versioning; the two packages always carry the same version. The first
  release is a preview (`0.1.0-preview.1`), and the public API may still change until 1.0.
- **Conformance and security fixes are the owner's own.** Fuzz findings become fixed tests. The
  versions of MQTTnet and MQTT.js that the tests run against are pinned, and raising them is a
  deliberate change.

## 7. Order of work

MqttWire and GrainWire's move to it were first planned to be built together, a part at a time,
because GrainWire's tests are the most demanding user MqttWire has. **On 2026-11-21, after step
2, the owner changed that: MqttWire's four steps below are all done first, and GrainWire moves
to it afterwards.** The [roadmap](../roadmap.md) holds the full order; MqttWire's own steps
are:

1. **The repository and the codec**, with the codec's tests. Before the QoS exchanges are
   written, the builder reads chapter 4 of the MQTT 5 standard (sections 4.3, 4.4, 4.6, 4.9),
   which the fact checks behind this design could not reach.
2. **The client**, with its tests and interop with MQTTnet's broker. The reconnecting client
   comes after it in the same step.
3. **The server**, with its tests, both interops, the benchmark and the sample.
4. **A preview package.**

Each step ends with every test green.

## 8. Success

- The interop tests pass with MQTTnet and MQTT.js at every QoS in both directions.
- The benchmark shows the target of 6.14 met, live managed bytes coming back after churn, and
  pool retention that does not grow with connections.
- GrainWire runs on it with its own tests passing (the companion design).
- The public API is what 6.2 and 6.3 describe and nothing more.

## 9. Decided by the owner

- The MQTT implementation is a package of its own, in its own repository, named **MqttWire**,
  holding the codec, the client and the server.
- The server is a **connection server that the host drives**, not a routing broker.
- The host plugs in through **one handler object and a state slot per connection** (option A).
- **Shared topics are possible through the host from the first version**, with a sample and a
  test; a routing helper is left for later.
- ASP.NET Core's WebSocket with own MQTT on top; memory per connection as the number to design
  for.
- QoS 0, 1 and 2 inside a connection, in both directions; no stored sessions.
- A CONNECT with a Will is refused.
- `MaxOutgoingBytes` is a hard bound with a default of 1 MiB, adjustable. The deadlines are
  internal constants.
- Message retention is wanted later and not built now.
- Unity and .NET Standard 2.1 wait for Unity 7.
- **An opt-in reconnecting client is part of the first version**, as a wrapper around the
  client that leaves the client itself one connection per instance.
- The memory target "floor + 1 KB" for MqttWire alone is kept (2026-11-20, 6.14).
- `State` is an `object?` (2026-11-22).
- Step 3 lands as two pull requests: the server with its tests and the .NET interops, then
  MQTT.js, the sample and the benchmark (2026-11-22).
- The `GroupChat` sample is a server and a Blazor WebAssembly client (2026-11-22).
- MQTT.js runs in its own Node job in CI (2026-11-22).

## 10. Proposed by Claude (owner to check)

1. The handler has six methods: `OnConnectAsync`, `OnAdmit`, `OnSubscribe`, `OnUnsubscribe`,
   `OnPublish`, `OnClosed` (6.2). `OnAdmit` is the hook inside the atomic admission step that a
   host needs for its own accounting; the owner agreed to "one hook" there, and this is it.
2. `MqttConnectRequest` gives the host the `HttpContext` of the WebSocket request, so a host
   can authenticate by cookie or header. MQTTnet did not offer that.
3. **MqttWire keeps no record of subscriptions.** The host remembers what it granted and says
   at which QoS each message goes. This is what keeps a connection lean, and it means a host
   must not send at a higher QoS than it granted.
4. Types carry the `MqttWire` prefix where a clash with MQTTnet's names is likely.
5. The link (6.5) stays internal, and so does the codec.
6. The client is one connection per instance and does not reconnect; `PublishAsync` completes
   on the acknowledgement (6.3).
7. Client ids: not empty and at most 256 bytes; everything else about them is the host's.
8. Options: `MaxQoS`, `MaxPublishPayloadBytes`, `MaxOutgoingMessages`, `MaxOutgoingBytes`
   (6.2). Internal constants: receive maximum 256 on the server and 1,024 on the client; 1,024
   waiting control packets; 5 KiB of room for topic and properties; reason strings cut to 256
   bytes; CONNACK user properties limited to 1 KiB; the deadlines of 6.6.
9. A handler that throws closes the connection with `0x83`.
10. On unsubscribe, deliveries MqttWire has already taken are still delivered (6.6).
11. The target "floor + 1 KB" for MqttWire alone (6.14); kept by the owner on 2026-11-20.
12. Target framework `net10.0` only, for now.
13. The repository layout of 6.1, with a `GroupChat` sample.
14. For the reconnecting client (6.3): it lives in the `MqttWire` package beside the client; it
    remembers subscriptions and makes them again; it has no offline queue and repeats no
    publish; it stops for good on a refusal that retrying cannot fix and retries everything
    else until it is disposed; delays of 500 ms doubling to 15 s by default. The `GroupChat`
    sample's client uses it.

Decided while the server was built in step 3 (2026-11-22), not yet seen by the owner:

15. No MQTT packet precedes the CONNACK: the send path is reserved for it from the start of the
    admission step, and a close in that window aborts the link, with no CONNACK and no
    DISCONNECT (6.6).
16. The keep-alive is measured from the move to Active, not from the CONNECT (6.6).
17. Packets that came behind the CONNECT in the same read keep its buffer through
    Authenticating and are handled after the CONNACK (6.6).
18. A first packet that is not a CONNECT gets WebSocket status 1002 and no MQTT answer; a
    CONNECT over 16 KB gets CONNACK `0x95` (6.6).
19. `MqttTopic` accepts at most 65,526 bytes; a delivery whose whole packet is at most 16 KB is
    copied behind its header, a larger one is written from the host's memory in pieces (6.2,
    6.6).
20. Waiting control bytes are bounded by the one 64 KB control buffer as well as by 1,024
    packets; passing either evicts with `0x97` (6.6).
21. "A write in progress" is "the send path is taken", also when no bytes are with the link at
    that instant (6.6).
22. A caller writes at most 16 packets at once on its own thread, then hands the send path to
    the thread pool (6.6).
23. A writer that throws inside MqttWire (an encoding, a pool, the clock) ends the connection as
    a lost connection, logged as event 20; a link whose write throws is a failed write, event
    18 (6.6, 6.12).
24. The queue of waiting deliveries and the exchange records are pooled per server, at most
    1,024 of each retained, and a queue grown past 64 entries is not retained (6.6).
25. The count against the receive maximum of 256 is the remembered QoS 2 identifiers plus the
    answers to the client's publishes not yet handed to the link (6.6).
26. A failing PUBREC for a delivery already waiting for PUBCOMP is the harmless repetition:
    PUBREL again, nothing changes (6.6).
27. A connection that takes its slot after the stop began is aborted by its own run (6.6).
28. A link starts at most one close frame, whichever side closes first (6.5).
29. The log's event ids; the remote end point in a line of a connection that has no client id
    yet; and the two lines of an exception whose close lost the race (6.12).

Decided in the fix wave after the review of the whole server (2026-11-22), not yet seen by the
owner:

30. A link is aborted only while its connection's run has not ended, and the run waits for an
    abort in flight; so `OnClosed` never runs inside an abort, on the sweep's thread, the
    stopping thread or inside the host's `TrySend` or `Close` (6.2, 6.6).
31. A link that throws when it is aborted, and a sweep's check that throws, are logged as event
    20; the first ends nothing, the second ends that connection as a lost connection (6.6,
    6.12).
32. The partial-packet deadline runs from the rent of the read buffer, and an empty binary
    message does not end the link's wait for data, on the client too (6.5, 6.6).
33. An `OnConnectAsync` that blocks its calling thread: the link is aborted, with no CONNACK,
    2 s after the token was cancelled; the stop then no longer waits for that connection and
    logs event 22; its slot and a read buffer kept behind the CONNECT stay held until the call
    returns (6.6, 6.12).
34. The server resolves its handler when it starts, not when it is built, so a handler may take
    `MqttWireServer` in its constructor (6.2).
35. The default value of each result type is a refusal with `0x87`; it used to accept (6.2). A
    change of behaviour for a host that returned `default` to accept.
36. Event 21: an exception that ends a connection's run and is not a failure of its link is
    logged as an error, and still reported to `OnClosed` as a lost connection (6.12).
37. A connection closed after its admission gets no CONNACK up to the step, under the lock of
    the send path, that starts the CONNACK's write (6.6).
38. A connection lets go of its link and of the request's `HttpContext` when its run ends, so a
    connection object the host keeps holds nothing of the request (6.6).

## 11. Left to the builder

What was left here, and what step 3 decided:

- Exact names and shapes of the public types, within what 6.2 and 6.3 fix: as 6.2 now shows.
  The result types are readonly structs with factory members, whose default value refuses
  (6.2).
- Whether `State` is an `object` or the server is generic over the host's state type: an
  `object?`, decided by the owner (section 9).
- How the registration and mapping calls look, and how the server joins the host's shutdown:
  `AddMqttWireServer<THandler>` registers the server through a factory (it has no public
  constructor), the handler as a singleton that must not depend on scoped services and that the
  server resolves when it starts, and the options with validation (6.2); it throws
  `InvalidOperationException` when called twice.
  `MapMqttWire` returns `IEndpointConventionBuilder` and throws `InvalidOperationException`
  when the server is not registered; a request that arrives without `UseWebSockets()` throws
  `InvalidOperationException` naming it. An internal `IHostedLifecycleService` starts the sweep
  in `StartingAsync` and runs the stop in `StoppingAsync`, before any hosted service, Kestrel
  included, is stopped. For now the clock is `TimeProvider.System`; whether a host may supply
  its own is open.
- The names and shapes of all internal types, including the link: the builder's, as built.
- Buffer sizes, the pool, how the sweep walks the connections, how the admission step is made
  atomic: the shared `ArrayPool<byte>`; a first read buffer of 4 KB; every connection has a slot
  in an array the sweep walks without a lock; the client-id table and the slots share one lock,
  inside which the admission step runs, `OnAdmit` included (6.6).
- How waiting control packets and deliveries are stored, below which size a message is copied
  behind its header, and whether waiting packets are written together in one WebSocket message:
  6.6, "Sending" (the control buffer, written as one message; a pooled queue of deliveries;
  the copy up to 16 KB).

Still left to the builder for the rest of step 3: the shape of the benchmark of 6.14, the
`GroupChat` sample, and the MQTT.js interop with its CI job.

## 12. What was checked

The MQTT rules and the .NET behaviour this design relies on were checked against the standard
and the .NET 10 source, as far as they could be reached. Section 13 of
[the v5 design](2026-11-19-minimal-mqtt-design.md) lists what was confirmed, what could not be
reached (chapter 4 and chapter 6 of the standard), and the secondary sources used for the
repeated-identifier rule and for "no re-sending inside a connection". What has been measured
is the starting point: today's idle cost per connection and the bare-WebSocket floor, in
[the step 0 report](https://github.com/mo-esmp/grainwire/blob/main/docs/step-0-idle-memory.md).

The codec and the client were built and run in steps 1 and 2. The server has now been built and
run: its unit tests over the in-memory link, and the interops with MQTTnet's client and
MqttWire's client over Kestrel, at QoS 0, 1 and 2 in both directions. Not yet run: MQTT.js, the
sample and the benchmark (the rest of step 3), so MqttWire's memory over a real WebSocket has
not been measured yet.
