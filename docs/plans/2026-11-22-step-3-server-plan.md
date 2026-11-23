# Step 3: the server, its tests, both interops, the sample and the benchmark — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `MqttWire.Server`, the MQTT 5 connection server for ASP.NET Core that a host drives through the six-method handler of design 6.2, with QoS 0, 1 and 2 inside a connection in both directions, the two outgoing limits, the deadlines and a bounded shutdown. Prove it over the in-memory link, against MQTTnet and MQTT.js, with the `GroupChat` sample and the idle-memory benchmark.

**Architecture:** One `MqttWireConnection` per WebSocket holds all MQTT state and the host's state slot. It has **one async method parked per connection**: the run method accepts the WebSocket, runs the connect phase, then loops on the link's zero-byte wait and handles what arrived in a second, pooled method that owns the rented buffer. Sending never waits: a caller either starts the one write a WebSocket allows, or adds to what waits, and whoever completes a write sends what was added, control packets first. One `MqttWireServer` holds the connection registry (a slot array for the sweep, a client-id table for admission, one lock), the single 2-second sweep that enforces every deadline, and the bounded stop.

**Tech Stack:** .NET 10, C# 14, the ASP.NET Core shared framework (`Microsoft.AspNetCore.App`: WebSockets, routing, hosting, options, logging, `Microsoft.Extensions.ObjectPool`), `TimeProvider`, xUnit 2.9.3 + Shouldly 4.3.0, `Microsoft.Extensions.TimeProvider.Testing`, MQTTnet 5.2.0.1603 (tests only), MQTT.js 5 and Node 24 (`tests/interop-js` only), Blazor WebAssembly 10.0 (the sample only).

**Spec:** [docs/specs/2026-11-19-mqttwire-design.md](../specs/2026-11-19-mqttwire-design.md), sections 6.1, 6.2, 6.6, 6.7 (the server's side), 6.8, 6.10, 6.12, 6.13 (items 2 to 5, 7, 8 and 9), 6.14 and 7 (step 3). Tracked in mo-esmp/mqttwire#6.

**Two pull requests** (the owner's decision, 2026-11-22):

| Part | Tasks | Branch | Pull request title | Links |
|---|---|---|---|---|
| A | 1 to 15 | `feature/6-server-and-dotnet-interops` | `#6 - Step 3: the server, its tests and the .NET interops.` | `Part of #6` |
| B | 16 to 19 | `feature/6-mqttjs-sample-and-benchmark` | `#6 - Step 3: the MQTT.js interop, the sample and the benchmark.` | `Closes #6` |

## Global Constraints

- .NET 10 (`net10.0` only), C# 14, warnings are errors. `AGENTS.md` and `.editorconfig` are binding: no `var`, target-typed `new`, file-scoped namespaces matching the folder, `s_` for private static fields, braces always, XML documentation in the multi-line form on every type and member in `src`, `tests`, `bench` and `samples`.
- `MqttWire.Server` depends on `MqttWire` and the ASP.NET Core shared framework, **nothing else**. `MqttWire` keeps no package dependency. Every package a test, the sample or the benchmark needs is pinned in `Directory.Packages.props`.
- **The read loop, `TrySend` and the write path allocate nothing in steady state** (AGENTS "Hot Paths"): no LINQ, closures, boxing, string building or `params`. Allocation tests enforce it. **An idle connection holds no MqttWire buffer, no queue storage, no timer, no cancellation source and no QoS record** (design goal 2).
- **Malformed input from a client is a reason code and the end of the connection, never an exception.** Exceptions are only for the host's bugs: an invalid option, an invalid topic, a result built with a reason code that is not a failure.
- **A handler that throws closes its connection with `0x83`**; in `OnClosed` it is logged only.
- **Buffers follow design 6.8 to the letter:** only the code that awaits an operation returns that operation's buffer, after the await has ended; whoever aborts, closes or evicts returns nothing on another's behalf; every way out returns what is held, once.
- `ConfigureAwait(false)` on every await in `src`. `ValueTask` on the read and write paths. `CancellationToken` last. No `async void`, `.Result` or `.Wait()`.
- Unit tests live in `tests/MqttWire.Tests/Server`, use the in-memory link and a replaced clock, and **never wait in real time**. Only `tests/MqttWire.Interop.Tests`, `tests/interop-js` and `bench/MqttWire.Bench` use sockets.
- Tests: `MethodName_StateUnderTest_ExpectedBehavior`, `// Arrange`, `// Act`, `// Assert` in every test, a one-sentence summary on each.
- Every commit: `dotnet build -c Release` with 0 warnings and `dotnet test` green. Commit messages are one imperative sentence ending with a period, with no co-author trailer.

## Decisions this plan makes

Decision 1 records the owner's answers. The others are the plan's rulings on what design section 11 leaves to the builder or the design is silent on; the owner can overturn any of them at review. Those marked **(spec)** change design text, which Task 15 updates in the same pull request.

1. **The owner's answers to the open questions of #6.** `State` is `object?` (2026-11-22: MQTTnet's `SessionItems` without the dictionary). Two pull requests, as the table above. `samples/GroupChat` is a server and a Blazor WebAssembly client. MQTT.js runs in its own Node job in CI. The memory target is **floor + 1 KB** (kept by the owner on 2026-11-20 after step 0: about 12,900 bytes against a floor of about 11,900).
2. **Sizes.** "16 KB" is 16,384 bytes and "64 KB" is 65,536 bytes wherever design 6.6 says them. A deadline has passed when `now − start >= length`; it is acted on at the first sweep that sees that.
3. **One parked async method per connection.** The endpoint's request delegate is not `async`: it checks the request and returns the task of `MqttWireConnection.RunAsync`, which accepts the WebSocket itself. The connect phase is a separate method that has completed before the loop starts, so none of its locals live on. Packets are handled in a method built with `PoolingAsyncValueTaskMethodBuilder`.
4. **The registry.** Every connection has a slot in an array (`MqttWireConnection?[]`, grown by doubling, with a free list); the sweep walks the array without the lock. The client-id table is a `Dictionary<string, MqttWireConnection>`. Both are guarded by one `System.Threading.Lock`, and **the admission step runs inside it**, `OnAdmit` included. A connection stays in the table from its admission until it is Closed, and is removed by object just before `OnClosed`. So a CONNECT for a client id whose connection is already closing sees that connection as `replaces`; the old one keeps the close reason it already had **(spec)**.
5. **Timestamps are `long` values of `TimeProvider.GetTimestamp()`**, four per connection (state entered, partial packet started, last packet received, write started), 0 meaning "not running". One `ITimer` from the server's `TimeProvider`, period 2 s, calls the sweep.
6. **The PUBLISH bands are judged on the packet's Remaining Length** (design 6.6 "total length"). In the skip band a packet is malformed (`0x81`) when its topic length runs past the packet or the packet ends before its identifier; an identifier of 0 gets the code `PublishCodec` gives it. Reserved flags, a QoS of 3 and a QoS above `MaxQoS` are judged from the first byte, before anything is skipped. The partial-packet deadline covers a skipped packet too.
7. **A CONNECT that cannot be accepted.** A decoding failure or a failed check of MqttWire's own is answered with a CONNACK carrying that code, then the close. A first packet that is not a CONNECT gets no MQTT answer: the WebSocket is closed with status 1002. A CONNECT longer than 16,384 bytes gets CONNACK `0x95`. The connect deadline aborts.
8. **Bytes behind the CONNECT.** MQTT lets a client send packets before it has the CONNACK. If the buffer that held the CONNECT holds more, the buffer is kept through Authenticating and those packets are handled, in order, after the CONNACK. A buffer with nothing behind the CONNECT is returned before `OnConnectAsync` is called, which is the normal case **(spec: the states table)**.
9. **Waiting control packets are encoded at once** into one rented "control buffer" per connection, which exists only while control packets wait behind a write. It grows by renting the next size and copying, up to 65,536 bytes; whoever completes the write in progress writes it as one WebSocket message and then returns it. It counts as packets (1,024) and as bytes (`MaxOutgoingBytes`). If one already waits, a PINGRESP is not added.
10. **Deliveries.** A message whose whole packet is at most 16,384 bytes is copied behind its header into one rented buffer and written as one message. A larger one is written as the header from a rented buffer, then the payload from the host's memory in pieces of at most 65,536 bytes. `MqttTopic` accepts at most **65,526 bytes** of UTF-8, 9 fewer than MQTT's field allows, so that the header of every PUBLISH fits one 64 KB buffer **(spec)**.
11. **`TrySend`'s edges.** A QoS above `MaxQoS` is lowered to `MaxQoS`. `default(MqttTopic)` and a QoS that is not 0, 1 or 2 throw `ArgumentException` (the host's bug). An empty payload is a valid message. "Too large" (over `MaxOutgoingBytes`, or over the client's maximum packet size) closes the connection with `0x95`. `MaxOutgoingBytes` counts payload bytes of deliveries and all bytes of the control buffer.
12. **The objects a busy connection needs are pooled per server**, so that an idle connection holds none and steady traffic allocates none: the exchange records and the queue of waiting deliveries come from two `Microsoft.Extensions.ObjectPool.DefaultObjectPool<T>` instances (at most 1,024 retained each), are taken with the first exchange or the first waiting delivery, and go back, cleared of every payload reference, with the last.
13. **Exchange records** are 16-byte structs (identifier, phase, when the phase was entered) in two arrays per connection, searched linearly: at most 256 for the client's publishes and 256 for deliveries. The count against the announced receive maximum of 256 is: QoS 1 publishes whose PUBACK is not yet written, plus QoS 2 publishes whose PUBCOMP is not yet written, plus refused QoS 2 publishes whose failing PUBREC is not yet written.
14. **The result types** are readonly structs with factory members; a refusal built with a reason code below `0x80` throws `ArgumentOutOfRangeException` in the host's code, and so does `MqttConnectResult.Accept` with user properties over 1,024 bytes in total. A SUBACK has one reason string, so it carries the reason string of the first refused filter. A filter that is not a valid MQTT filter is answered `0x8F` without calling `OnSubscribe`; a `$share/` filter is passed to the host like any other.
15. **`MqttCloseReason`** has the reasons of design 6.12 that an admitted connection can end with, plus two for ends the server does not decide: `ClientDisconnected` and `ConnectionLost`. An incoming packet that is too large, a topic alias, an unsupported QoS and a subscription identifier end with `ProtocolError`; the specific code is on the wire and in the log **(spec: 6.12)**.
16. **Hosting.** `MqttWireServer` has no public constructor; `AddMqttWireServer<THandler>` registers it through a factory. Options are validated with `IValidateOptions<MqttWireServerOptions>` and `ValidateOnStart`. An internal `IHostedLifecycleService` starts the sweep in `StartAsync` and runs the stop in `StoppingAsync`, which every hosted service gets before any is stopped, so Kestrel is never left waiting on open WebSockets. `MapMqttWire` returns `IEndpointConventionBuilder`, and throws `InvalidOperationException` on the first request when `UseWebSockets()` is missing **(spec: section 11)**.
17. **Handler exceptions.** `OnConnectAsync` or `OnAdmit` throwing: CONNACK `0x83`, no `OnClosed`. `OnSubscribe`, `OnUnsubscribe` or `OnPublish` throwing: DISCONNECT `0x83`, reason `HandlerThrew`. `OnClosed` throwing: logged as an error, nothing else.
18. **Tests.** The handler double is a hand-written `RecordingHandler`: NSubstitute is not among the pinned packages, and no package is added for it. A unit test measures an idle connection over the in-memory link from Task 5 on and holds it to **768 bytes**, which leaves 256 for the `WebSocketLink` and its parked wait; the benchmark of Task 18 is what decides. `tests/MqttWire.TestHost` is a small host (an exact-topic fan-out handler) shared by the unit test of shared topics, the .NET interops and MQTT.js **(spec: 6.1)**.
19. **The benchmark** is a console program with two roles in two processes, so that the load's own memory is not measured: `server` (the floor endpoint, MqttWire, and a `/measure` endpoint) and `run` (starts the server, opens the connections, reads the figures). It runs at 3,000 and 12,000 connections, the counts of GrainWire's step 0, so the floor can be compared. Pool retention is the change in the fixed part of the line between idle and after activity.

Not in this step: the preview package and `docs/mqtt-support.md` (step 4, #7), a routing helper, retained messages, metrics, own WebSocket framing.

## Review Focus

The five inputs most likely to bite a host or a client that the design implies but its test list does not name. Each has a test in the task that owns the code.

1. **The host calls back from inside a handler.** `OnPublish` sends to the same connection at QoS 1 (an echo): no deadlock on the connection's lock, and both the PUBACK and the echo go out. `OnClosed` calls `TrySend` and `Close`: "not open", and nothing happens. Tests in Tasks 8 and 5.
2. **Packets right behind the CONNECT, in the same WebSocket message** (CONNECT, SUBSCRIBE, PINGREQ): handled in order after the CONNACK, not lost and not answered before it. Test in Task 4.
3. **A CONNECT for a client id whose connection is already closing:** `OnAdmit` sees it as `replaces`, each connection gets exactly one `OnClosed`, and the old one keeps its own reason. Test in Task 5.
4. **A reason string from the host that is too long:** 300 `é` (600 bytes) is cut to 256 bytes, 128 characters; 100 `€` (300 bytes) is cut to 255 bytes, 85 characters. The cut never splits a character. Test in Task 7.
5. **What `TrySend` is handed:** `default(MqttTopic)` throws and nothing is written; an empty payload is delivered; QoS 2 with `MaxQoS` 1 goes out at QoS 1. Tests in Task 8.

---

## File structure

```
src/MqttWire.Server/
  MqttWire.Server.csproj
  IMqttWireHandler.cs                      public: the six methods
  MqttWireServer.cs                        public: ConnectionCount, TryGetConnection; internal: registry, sweep, stop
  MqttWireServerOptions.cs                 public: the four options
  MqttWireConnection.cs                    public members, the fields, the state
  MqttWireConnection.Run.cs                RunAsync: accept, connect phase, the loop, the end
  MqttWireConnection.Connect.cs            the CONNECT checks, OnConnectAsync and its deadline, admission, CONNACK
  MqttWireConnection.Read.cs               the buffer, framing, the three PUBLISH bands, the skip
  MqttWireConnection.Packets.cs            what each packet does
  MqttWireConnection.Send.cs               TrySend, control packets, the drain, the pieces
  MqttWireConnection.QoS.cs                the exchanges in both directions
  MqttWireConnection.Close.cs              BeginClose, the sweep's checks, Close
  MqttConnectRequest.cs  MqttConnectResult.cs  MqttAdmitResult.cs  MqttSubscribeResult.cs  MqttPublishResult.cs
  MqttSendResult.cs  MqttCloseReason.cs  MqttTopic.cs
  MqttWireServiceCollectionExtensions.cs  MqttWireEndpointRouteBuilderExtensions.cs
  Internal/
    ServerLimits.cs  ConnectionState.cs  ConnectionCounts.cs  ConnectionRegistry.cs  ServerPools.cs
    PacketReadBuffer.cs  PublishSkip.cs  ReasonStrings.cs
    ExchangeRecord.cs  ExchangePhase.cs  ExchangeRecords.cs
    PendingDelivery.cs  WaitingDeliveries.cs  ControlBuffer.cs
    MqttWireServerOptionsValidator.cs  MqttWireServerLifetime.cs  ServerLog.cs
tests/MqttWire.Tests/Server/
  ServerHarness.cs  TestConnection.cs  RecordingHandler.cs  RecordingLogger.cs  ServerPackets.cs
  OptionsTests.cs  PublicTypesTests.cs  ConnectionRegistryTests.cs  AcceptTests.cs  ServerConnectTests.cs
  AdmissionTests.cs  ReadTests.cs  PublishBandTests.cs  PacketTests.cs  SendTests.cs  SendInterleavingTests.cs
  IncomingQoSTests.cs  DeliveryQoSTests.cs  WindowDeadlockTests.cs  ServerCloseTests.cs  StopTests.cs
  HostingTests.cs  LoggingTests.cs  ServerBufferTests.cs  ServerStressTests.cs  ServerAllocationTests.cs
  IdleCostTests.cs  SharedTopicTests.cs
tests/MqttWire.TestHost/                   MqttWire.TestHost.csproj  ExactTopicHandler.cs  Program.cs
tests/MqttWire.Interop.Tests/              ServerHost.cs  ServerInteropTests.cs  ClientServerInteropTests.cs  EndpointTests.cs
tests/interop-js/                          package.json  package-lock.json  host.mjs  interop.test.mjs  README.md
samples/GroupChat/                         GroupChat.Server/  GroupChat.Client/  README.md
bench/MqttWire.Bench/                      MqttWire.Bench.csproj  Program.cs  ServerRole.cs  RunRole.cs  IdleClients.cs  Figures.cs
docs/bench/2026-11-idle-memory.md          the benchmark's report
```

The public types are in the namespace `MqttWire.Server`; everything under `Internal/` is in `MqttWire.Server.Internal`. `MqttWireConnection` is one partial class across its files, as `MqttWireClient` is.

## Shared test data

`tests/MqttWire.Tests/Server/ServerPackets.cs` (`internal static class ServerPackets`, built with the existing `Hex.Parse`). The CONNACK bytes assume `ConnAckCodec` writes its properties in the order of `ConnAckFields`; if it writes another order, the expected bytes follow the codec and nothing else changes.

```
Connect                    10 0F 00 04 4D 51 54 54 05 02 00 1E 00 00 02 63 31                      client id "c1", keep-alive 30
ConnectKeepAlive0          10 0F 00 04 4D 51 54 54 05 02 00 00 00 00 02 63 31
ConnectVersion4            10 0E 00 04 4D 51 54 54 04 02 00 1E 00 02 63 31
ConnectEmptyClientId       10 0D 00 04 4D 51 54 54 05 02 00 00 00 00 00
ConnectWithWill            10 15 00 04 4D 51 54 54 05 06 00 1E 00 00 02 63 31 00 00 01 74 00 00
ConnectWithAuthMethod      10 13 00 04 4D 51 54 54 05 02 00 1E 04 15 00 01 78 00 02 63 31
ConnectUserAndPassword     10 15 00 04 4D 51 54 54 05 C2 00 1E 00 00 02 63 31 00 01 75 00 01 70    user "u", password "p"
ConnectSessionExpiry60     10 14 00 04 4D 51 54 54 05 02 00 1E 05 11 00 00 00 3C 00 02 63 31
ConnectReceiveMaximum1     10 12 00 04 4D 51 54 54 05 02 00 1E 03 21 00 01 00 02 63 31
ConnectMaxPacketSize4096   10 14 00 04 4D 51 54 54 05 02 00 1E 05 27 00 00 10 00 00 02 63 31
ConnectNoProblemInfo       10 11 00 04 4D 51 54 54 05 02 00 1E 02 17 00 00 02 63 31
ConnAck                    20 08 00 00 05 21 01 00 29 00                                           Receive Maximum 256, no subscription identifiers
ConnAckKeepAlive60         20 0B 00 00 08 21 01 00 29 00 13 00 3C
ConnAckMaxQoS1             20 0A 00 00 07 21 01 00 24 01 29 00
ConnAckSessionExpiry0      20 0D 00 00 0A 11 00 00 00 00 21 01 00 29 00
ConnAckRefused(code)       20 03 00 <code> 00
ConnAckWillRefused         20 26 00 87 23 1F 00 20 + "Will messages are not supported." (32 bytes)
Subscribe                  82 09 00 01 00 00 03 61 2F 62 01                                        id 1, "a/b", QoS 1
SubAckGrantedQoS1          90 04 00 01 00 01
SubAckNotAuthorized        90 04 00 01 00 87
Unsubscribe                A2 08 00 02 00 00 03 61 2F 62                                           id 2, "a/b"
UnsubAck                   B0 04 00 02 00 00
Auth                       F0 00
TooLargePubAck1            40 20 00 01 97 1C 1F 00 19 + "The message is too large." (25 bytes)
TooLargePubRec1            50 20 00 01 97 1C 1F 00 19 + the same 25 bytes
Disconnect(code)           E0 01 <code>
```

The PUBLISH and acknowledgement bytes are those of the existing `ClientPackets` (`PublishQoS0` = `30 08 00 03 61 2F 62 00 68 69`, topic `a/b`, payload `hi`; `PublishQoS1`, `PublishQoS2`, `PubAck1`, `PubRec1`, `PubRel1`, `PubComp1`, `PublishQoS2Id7`, `PublishQoS2Id7Dup`, `PubRel7`, `PubCompNotFound7`, `PubRelNotFound7`, `PingReq`, `PingResp`, `Disconnect`).

---

# Part A: the server, its tests and the .NET interops

### Task 1: The project, the options and the limits

**Files:**
- Create: `src/MqttWire.Server/MqttWire.Server.csproj`, `src/MqttWire.Server/MqttWireServerOptions.cs`, `src/MqttWire.Server/Internal/MqttWireServerOptionsValidator.cs`, `src/MqttWire.Server/Internal/ServerLimits.cs`
- Modify: `MqttWire.slnx` (add the project under `/src/`), `tests/MqttWire.Tests/MqttWire.Tests.csproj` (reference `MqttWire.Server`)
- Test: `tests/MqttWire.Tests/Server/OptionsTests.cs`

**Interfaces:**
- Produces:
  ```csharp
  namespace MqttWire.Server;
  public sealed class MqttWireServerOptions
  {
      public MqttQoS MaxQoS { get; set; } = MqttQoS.ExactlyOnce;        // AtLeastOnce or ExactlyOnce
      public int MaxPublishPayloadBytes { get; set; } = 65_536;         // 1 to 16,777,216
      public int MaxOutgoingMessages { get; set; } = 1_000;             // at least 1
      public int MaxOutgoingBytes { get; set; } = 1_048_576;            // at least 65,536
  }
  namespace MqttWire.Server.Internal;
  internal sealed class MqttWireServerOptionsValidator : IValidateOptions<MqttWireServerOptions>
  internal static class ServerLimits
  ```
- `ServerLimits` constants: `SweepInterval` 2 s; `ConnectDeadline`, `AuthenticationDeadline`, `PartialPacketDeadline` 10 s; `WriteDeadline` 15 s; `AcknowledgementDeadline` 30 s; `CloseDeadline` 2 s; `MaxKeepAliveSeconds` 60; keep-alive factor 3/2; `ReceiveMaximum` 256; `MaxWaitingControlPackets` 1,024; `MaxControlPacketBytes` 16,384; `MaxEncodingBufferBytes` 65,536; `CopyThresholdBytes` 16,384; `FirstReadBufferBytes` 4,096; `PublishOverheadBytes` 5,120; `SkipFactor` 16; `MinClientMaximumPacketSize` 4,096; `MaxReasonStringBytes` 256; `MaxConnAckUserPropertyBytes` 1,024; `MaxClientIdBytes` 256; `MaxTopicBytes` 65,526; `MaxPooledObjects` 1,024.
- The project file: `Microsoft.NET.Sdk`, `<FrameworkReference Include="Microsoft.AspNetCore.App" />`, a project reference to `MqttWire`, `IsPackable` true, `GenerateDocumentationFile` true, the README packed, the same four analyzers and warning settings as `MqttWire.csproj`, and `InternalsVisibleTo` for `MqttWire.Tests` and `MqttWire.Bench`.

- [ ] **Step 1: Commit this plan** (the branch `feature/6-server-and-dotnet-interops` exists)

```bash
git add docs/plans/2026-11-22-step-3-server-plan.md
git commit -m "Add the plan for step 3, the server."
```

- [ ] **Step 2: Write the failing tests** in `OptionsTests.cs`

```csharp
[Fact] Validate_Defaults_Succeeds()                              // MaxQoS ExactlyOnce, 65,536, 1,000, 1,048,576
[Fact] Validate_MaxQoSAtMostOnce_Fails()                         // the failure message names MaxQoS
[Fact] Validate_MaxQoSOutOfRange_Fails()                         // (MqttQoS)3
[Theory] [InlineData(0)] [InlineData(16_777_217)] Validate_MaxPublishPayloadBytesOutOfRange_Fails(int value)
[Fact] Validate_MaxOutgoingMessagesZero_Fails()
[Fact] Validate_MaxOutgoingBytesBelow64KiB_Fails()               // 65,535 fails; 65,536 succeeds
[Fact] Validate_TwoInvalidValues_ReportsBoth()                   // ValidateOptionsResult.Failures has two entries
```

- [ ] **Step 3: Run them to see them fail**

Run: `dotnet test --filter "FullyQualifiedName~MqttWire.Tests.Server.OptionsTests"`
Expected: build errors (`MqttWireServerOptions` does not exist).

- [ ] **Step 4: Implement the project, the options, the validator and the limits**

- [ ] **Step 5: Run them to see them pass**, then `dotnet build -c Release` with 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add MqttWire.slnx src/MqttWire.Server tests/MqttWire.Tests
git commit -m "Add the MqttWire.Server project with its options and limits."
```

### Task 2: The public surface

**Files:**
- Create: `IMqttWireHandler.cs`, `MqttConnectRequest.cs`, `MqttConnectResult.cs`, `MqttAdmitResult.cs`, `MqttSubscribeResult.cs`, `MqttPublishResult.cs`, `MqttSendResult.cs`, `MqttCloseReason.cs`, `MqttTopic.cs`, `MqttWireConnection.cs`, `MqttWireServer.cs`, `Internal/ReasonStrings.cs` (all under `src/MqttWire.Server/`)
- Test: `tests/MqttWire.Tests/Server/PublicTypesTests.cs`

**Interfaces:**
- Consumes: `MqttQoS`, `MqttReasonCode`, `MqttPublish`, `MqttWireUserProperty` from `MqttWire`; `Topics.IsValidName` and `TopicChecks` from the codec and the client.
- Produces (namespace `MqttWire.Server`):
  ```csharp
  public interface IMqttWireHandler
  {
      ValueTask<MqttConnectResult> OnConnectAsync(MqttConnectRequest request, CancellationToken cancellationToken);
      MqttAdmitResult OnAdmit(MqttWireConnection connection, MqttWireConnection? replaces);
      MqttSubscribeResult OnSubscribe(MqttWireConnection connection, string topicFilter, MqttQoS requested);
      void OnUnsubscribe(MqttWireConnection connection, string topicFilter);
      MqttPublishResult OnPublish(MqttWireConnection connection, in MqttPublish publish);
      void OnClosed(MqttWireConnection connection, MqttCloseReason reason);
  }

  public sealed class MqttConnectRequest          // internal constructor; owns copies of everything it holds
  {
      public string ClientId { get; }
      public string? UserName { get; }
      public bool HasPassword { get; }
      public ReadOnlyMemory<byte> Password { get; }
      public IReadOnlyList<MqttWireUserProperty> UserProperties { get; }
      public IPEndPoint? RemoteEndPoint { get; }
      public bool IsSecure { get; }
      public HttpContext HttpContext { get; }
  }

  public readonly struct MqttConnectResult
  {
      public static MqttConnectResult Accept(object? state = null, MqttWireUserProperty[]? userProperties = null);
      public static MqttConnectResult Refuse(MqttReasonCode reasonCode, string? reasonString = null);
  }
  public readonly struct MqttAdmitResult
  {
      public static MqttAdmitResult Accepted { get; }
      public static MqttAdmitResult Refuse(MqttReasonCode reasonCode, string? reasonString = null);
  }
  public readonly struct MqttSubscribeResult
  {
      public static MqttSubscribeResult Granted(MqttQoS qos);
      public static MqttSubscribeResult NotAuthorized { get; }                       // 0x87, no reason string
      public static MqttSubscribeResult Refuse(MqttReasonCode reasonCode, string? reasonString = null);
  }
  public readonly struct MqttPublishResult
  {
      public static MqttPublishResult Accepted { get; }
      public static MqttPublishResult Refuse(MqttReasonCode reasonCode, string? reasonString = null);
  }
  // each result also has: internal bool IsAccepted, internal MqttReasonCode ReasonCode, internal string? ReasonString

  public enum MqttSendResult { Taken, NotOpen, TooLarge, Evicted }

  public enum MqttCloseReason
  {
      ClientDisconnected, ConnectionLost, MalformedPacket, ProtocolError, KeepAliveTimedOut, PartialPacketTimedOut,
      WriteTimedOut, AcknowledgementTimedOut, OutgoingLimitPassed, ReceiveMaximumPassed, MessageTooLargeToSend,
      TakenOver, ClosedByHost, HandlerThrew, ServerStopping,
  }

  public readonly struct MqttTopic
  {
      public static MqttTopic Create(string topic);
      public static MqttTopic Create(ReadOnlySpan<byte> utf8Topic);                  // copies
      internal ReadOnlySpan<byte> Utf8 { get; }  internal bool IsDefault { get; }
  }

  public sealed class MqttWireConnection          // internal constructor
  {
      public string ClientId { get; }
      public object? State { get; }
      public MqttSendResult TrySend(MqttTopic topic, ReadOnlyMemory<byte> payload, MqttQoS qos);
      public void Close(MqttReasonCode reason, string? reasonString = null);
  }
  public sealed class MqttWireServer              // no public constructor
  {
      public int ConnectionCount { get; }
      public bool TryGetConnection(string clientId, [NotNullWhen(true)] out MqttWireConnection? connection);
  }
  ```
- `internal static class ReasonStrings`: `static string? Cut(string? reasonString)` returns the string cut to at most 256 UTF-8 bytes at a character boundary (never between the two halves of a surrogate pair), the same instance when it already fits, and `null` for `null`.
- In this task `TrySend` returns `NotOpen`, `Close` does nothing and `MqttWireServer` is empty; later tasks fill them.

- [ ] **Step 1: Write the failing tests** in `PublicTypesTests.cs`

```csharp
[Fact] Refuse_ReasonCodeBelow0x80_Throws()                 // for each of the four results: Refuse(MqttReasonCode.Success) → ArgumentOutOfRangeException
[Fact] Accept_UserPropertiesOver1024Bytes_Throws()         // 11 properties of ("k", 100 × "v"): ArgumentException; 1,024 bytes exactly is accepted
[Fact] Granted_QoSOutOfRange_Throws()                      // (MqttQoS)3
[Fact] NotAuthorized_Always_Carries0x87AndNoReasonString()
[Fact] Create_ValidTopic_KeepsItsUtf8()                    // "a/b" → 61 2F 62
[Theory] [InlineData("")] [InlineData("a/+")] [InlineData("a/#")] [InlineData("a\0b")] Create_InvalidTopic_Throws(string topic)   // ArgumentException
[Fact] Create_UnpairedSurrogate_Throws()
[Fact] Create_TopicOf65527Bytes_Throws()                   // 65,526 is accepted
[Fact] Create_Utf8Span_CopiesTheBytes()                    // changing the source afterwards does not change the topic
[Fact] Cut_600BytesOfTwoByteCharacters_Returns256Bytes()   // 300 × "é" → 128 characters
[Fact] Cut_300BytesOfThreeByteCharacters_Returns255Bytes() // 100 × "€" → 85 characters
[Fact] Cut_SurrogatePairAtTheLimit_IsNotSplit()            // 63 × "😀" (252 bytes) + "😀" → 63 characters
[Fact] Cut_ShortString_ReturnsTheSameInstance()
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~PublicTypesTests"`. Expected: build errors.

- [ ] **Step 3: Implement the types.** `MqttTopic` wraps one `byte[]`; `Create(string)` validates with `TopicChecks.ThrowIfInvalidTopicName` and the 65,526 limit. Every XML comment says what design 6.2's contract table says about the member, in particular: the payload and topic handed to `TrySend` are not copied; `OnAdmit` must not block or call back into the server; `MqttPublish` is valid only during `OnPublish`.

- [ ] **Step 4: Run them to see them pass.**

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire.Server tests/MqttWire.Tests/Server
git commit -m "Add the server's public surface."
```

### Task 3: The registry, the sweep and a connection that waits for its CONNECT

**Files:**
- Create: `Internal/ConnectionState.cs`, `Internal/ConnectionCounts.cs`, `Internal/ConnectionRegistry.cs`, `Internal/PacketReadBuffer.cs`, `MqttWireConnection.Run.cs`, `MqttWireConnection.Read.cs`, `MqttWireConnection.Close.cs`
- Modify: `MqttWireServer.cs`, `MqttWireConnection.cs`
- Test: `tests/MqttWire.Tests/Server/ServerHarness.cs`, `TestConnection.cs`, `RecordingHandler.cs`, `RecordingLogger.cs`, `ServerPackets.cs`, `ConnectionRegistryTests.cs`, `AcceptTests.cs`

**Interfaces:**
- Consumes: `IMqttLink`, `PacketHeader.TryRead`, `MemoryLink`, `CountingPool`, `CountingTimeProvider`, `Hex`.
- Produces:
  ```csharp
  internal enum ConnectionState : byte { Accepted, Authenticating, Active, Closing, Closed }

  internal readonly record struct ConnectionCounts(
      int IncomingRecords, int DeliveryRecords, int WaitingDeliveries, int WaitingControlPackets,
      long OutgoingBytes, bool IsWriting, bool HoldsReadBuffer);

  internal sealed class ConnectionRegistry
  {
      public Lock Gate { get; }
      public int ActiveCount { get; }                                   // connections in the client-id table
      public void Add(MqttWireConnection connection);                   // takes a slot
      public void Remove(MqttWireConnection connection);                // frees the slot; removes the table entry only if it is this object
      public MqttWireConnection? Find(string clientId);                 // the caller holds Gate
      public void SetCurrent(MqttWireConnection connection);            // the caller holds Gate
      public bool TryGet(string clientId, [NotNullWhen(true)] out MqttWireConnection? connection);
      public MqttWireConnection?[] Slots { get; }                       // read without the lock; entries may be null
      public Task WhenEmptyAsync();                                     // completes when no slot is taken
  }

  // MqttWireServer
  internal MqttWireServer(IMqttWireHandler handler, MqttWireServerOptions options, ILogger<MqttWireServer> logger,
      TimeProvider time, ArrayPool<byte> pool);
  internal void Start();                                                // creates the one sweep timer
  internal Task RunConnectionAsync(HttpContext httpContext, IMqttLink? link);   // link null: accept the WebSocket of httpContext
  internal void Sweep();                                                // the timer's callback
  internal ConnectionRegistry Registry { get; }

  // MqttWireConnection
  internal Task RunAsync(IMqttLink? link);
  internal void BeginClose(MqttCloseReason reason, MqttReasonCode code, string? reasonString);   // once; later calls do nothing
  internal void CheckDeadlines(long now);                               // called by the sweep
  internal ConnectionCounts GetCounts();
  internal ConnectionState StateNow { get; }
  ```
- `PacketReadBuffer` is a struct field of the connection (`byte[]? _array`, `_start`, `_end`): `Rent(pool, size)`, `GetReadMemory()`, `Advance(count)`, `Grow(pool, size)` (rents, copies, returns the old one), `Consume(length)`, `Return(pool)` (returns the array and forgets it). It is modelled on the client's `ReceiveBuffer`, which stays as it is: the server's limits depend on the packet's type.
- Test doubles:
  ```csharp
  internal sealed class ServerHarness
  {
      public static ServerHarness Create(Action<MqttWireServerOptions>? configure = null, ArrayPool<byte>? pool = null);
      public MqttWireServer Server { get; }  public RecordingHandler Handler { get; }  public RecordingLogger Log { get; }
      public FakeTimeProvider Time { get; }  public CountingTimeProvider Timers { get; }  public CountingPool Pool { get; }
      public TestConnection Accept();                        // a new MemoryLink and a DefaultHttpContext, handed to RunConnectionAsync
      public TestConnection Connect(string clientId = "c1", byte[]? connect = null);   // from Task 5: Accept, CONNECT, asserts the CONNACK, empties what was written
      public void Advance(TimeSpan time);                    // Time.Advance in steps of 2 s, so every sweep runs
      public void AssertBounds();                            // design 6.13 item 5; see Task 13
  }
  internal sealed class TestConnection
  {
      public MemoryLink Link { get; }  public Task Run { get; }  public CancellationTokenSource RequestAborted { get; }
      public MqttWireConnection? Connection { get; }         // set when OnAdmit has seen it
      public void Deliver(ReadOnlySpan<byte> bytes);         // Link.Deliver, then harness.AssertBounds()
      public byte[] TakeWritten();
  }
  internal sealed class RecordingHandler : IMqttWireHandler   // settable delegates: Connect, Admit, Subscribe, Unsubscribe, Publish, Closed
  {
      public List<string> Calls { get; }    // "OnConnectAsync c1", "OnAdmit c1 replaces=-", "OnSubscribe c1 a/b q1", "OnUnsubscribe c1 a/b",
  }                                         // "OnPublish c1 a/b hi q0 retain=0", "OnClosed c1 TakenOver"; defaults accept everything
  internal sealed class RecordingLogger : ILogger<MqttWireServer>   // List<(LogLevel Level, EventId Id, string Message, Exception? Exception)> Entries
  ```

- [ ] **Step 1: Write the failing tests**

`ConnectionRegistryTests.cs`:
```csharp
[Fact] Add_AfterRemove_ReusesTheFreedSlot()
[Fact] Add_MoreThanTheArrayHolds_GrowsAndKeepsEveryConnection()          // 100 connections, all found in Slots
[Fact] Remove_ConnectionThatWasReplaced_LeavesItsReplacementInTheTable() // SetCurrent(a); SetCurrent(b) for one id; Remove(a) → TryGet gives b
[Fact] WhenEmptyAsync_LastConnectionRemoved_Completes()
```
`AcceptTests.cs`:
```csharp
[Fact] RunAsync_NoConnectForTenSeconds_AbortsAtThatSweep()               // Advance(8 s) → IsAborted false; Advance(2 s) → true; nothing written; Run completes; no handler call
[Fact] RunAsync_FirstPacketIsNotConnect_ClosesWith1002()                 // Deliver C0 00 → Link.ClosedWith == ProtocolError; nothing written
[Fact] RunAsync_PeerEndsBeforeConnect_ReturnsEveryBuffer()               // EndFromPeer → Run completes, Pool.Outstanding == 0
[Fact] RunAsync_ConnectCutAtEveryPosition_ReachesOnConnectAsyncOnce()    // for each cut 1..16: Calls == ["OnConnectAsync c1"]
[Fact] RunAsync_HalfAConnectThenSilence_AbortsAtTheConnectDeadline()     // the partial buffer is back in the pool afterwards
[Fact] Start_HundredConnections_CreatesOneTimer()                        // Timers.PeriodicTimers().Count == 1, Period == 2 s
[Fact] Sweep_ConnectionEnded_NoLongerHoldsItsSlot()                      // Server.Registry.Slots are all null after Run completes
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~ConnectionRegistryTests|FullyQualifiedName~AcceptTests"`. Expected: build errors.

- [ ] **Step 3: Implement the registry, the sweep, `RunAsync` up to a complete CONNECT, and `BeginClose`**

`RunAsync` shape (the only method whose state lives as long as the connection):
```
accept the WebSocket when no link was given (sub-protocol "mqtt", KeepAliveInterval zero, no compression)
registry.Add(this); stamp "state entered"
try:    if (await ConnectPhaseAsync()) { while (await link.WaitForDataAsync(requestAborted) …; await HandleAvailableAsync()) { } }
catch:  the read or the wait failed → BeginClose(ConnectionLost)
finally: await FinishAsync()        // waits until no write is in progress, registry.Remove(this), OnClosed if admitted, state Closed
```
In this task `ConnectPhaseAsync` reads until a complete CONNECT (first buffer 4,096 bytes, grown to at most 16,384), calls `OnConnectAsync` and stops there: Task 4 continues it. `BeginClose` moves the state to Closing with `Interlocked.CompareExchange`, stamps the time, and in this task only closes the WebSocket; Task 4 adds the DISCONNECT, written through its inline send, and Task 8 adds the rule for a write in progress. `CheckDeadlines` aborts an Accepted connection at the connect deadline and any Closing connection at the close deadline.

- [ ] **Step 4: Run them to see them pass.**

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire.Server tests/MqttWire.Tests/Server
git commit -m "Add the connection registry, the sweep and the accepted state."
```

### Task 4: CONNECT, `OnConnectAsync` and CONNACK

**Files:**
- Create: `MqttWireConnection.Connect.cs`; modify `MqttWireConnection.Send.cs` (create), `MqttWireConnection.Run.cs`
- Test: `tests/MqttWire.Tests/Server/ServerConnectTests.cs`

**Interfaces:**
- Consumes: `ConnectCodec.TryDecode`, `ConnectPacket`, `PropertyBlock.TryGetInteger`/`Contains`/`UserProperties`, `ConnAckCodec.GetSize`/`Write`, `ConnAckFields`.
- Produces:
  ```csharp
  // MqttWireConnection.Send.cs: the inline half of the send path; Task 8 adds the waiting half
  private void SendConnAck(in ConnAckFields fields);      // encodes into a rented buffer, writes, returns the buffer when the write has ended
  private void SendDisconnect(MqttReasonCode code, string? reasonString);   // the same path; BeginClose uses it from this task on
  // fields set by the connect phase
  private ushort _keepAliveSeconds;        // what applies: the client's, or 60
  private int _clientWindow;               // min(the client's Receive Maximum, 256); 256 when the client sent none
  private int _clientMaxPacketSize;        // the client's Maximum Packet Size; int.MaxValue when it sent none
  private bool _problemInformation;        // false when Request Problem Information was 0
  ```
- The checks, in this order, each answered with a CONNACK and the close (decision 7): the decoder's code (`0x81`, `0x82`, `0x84`); Authentication Method or Data present → `0x8C`; the Will flag → `0x87` with "Will messages are not supported."; Receive Maximum 0 or Maximum Packet Size 0 → `0x82` (if the codec has not already said so); Maximum Packet Size under 4,096 → `0x95`; a client id that is empty or over 256 bytes → `0x85`.
- `OnConnectAsync` runs with a `CancellationTokenSource` linked to `HttpContext.RequestAborted`, created for this call and disposed after it. The sweep cancels it at the authentication deadline. The connect phase awaits the handler's task with `WaitAsync(token)`; when the token wins, the connection answers CONNACK `0x88`, closes, and attaches a continuation to the handler's task that observes its result or exception and logs it (Task 12).
- CONNACK on success: session present 0, Receive Maximum 256, Subscription Identifiers Available 0, the host's user properties; Maximum QoS 1 only when `MaxQoS` is `AtLeastOnce`; Session Expiry Interval 0 only when the client asked for more than 0; Server Keep Alive 60 only when the client asked for 0 or for more than 60.

- [ ] **Step 1: Write the failing tests** in `ServerConnectTests.cs` (the handler's `Admit` accepts; admission itself is Task 5)

```csharp
[Fact] Connect_ValidConnect_AnswersConnAckWithTheFixedProperties()         // Connect → ConnAck
[Fact] Connect_KeepAliveZero_AnnouncesServerKeepAlive60()                  // ConnectKeepAlive0 → ConnAckKeepAlive60
[Fact] Connect_KeepAlive61_AnnouncesServerKeepAlive60()
[Fact] Connect_MaxQoS1_AnnouncesMaximumQoS1()                              // options MaxQoS AtLeastOnce → ConnAckMaxQoS1
[Fact] Connect_SessionExpiryAsked_AnswersSessionExpiry0()                  // ConnectSessionExpiry60 → ConnAckSessionExpiry0
[Fact] Connect_HostReturnsUserProperties_CarriesThemInConnAck()            // ("gw-player", "p1") is in the CONNACK
[Fact] Connect_UserNameAndPassword_ReachTheHostAsCopies()                  // request.UserName "u", Password 70; still readable after the buffer is back
[Fact] Connect_RequestCarriesTheHttpContext()                             // ReferenceEquals(request.HttpContext, the DefaultHttpContext given)
[Theory] Connect_NotAcceptable_IsRefusedWithoutCallingTheHost(byte[] connect, byte[] connAck)
         // (ConnectVersion4, 84) (ConnectEmptyClientId, 85) (ConnectWithAuthMethod, 8C) (ConnectWithWill, ConnAckWillRefused)
         // (client id of 257 bytes, 85) (Maximum Packet Size 4,095, 95) (Receive Maximum 0, 82) (a CONNECT with a bad UTF-8 client id, 81)
         // (a header "10 81 80 01", Remaining Length 16,385, 95: answered from the header alone, Pool.LargestRent <= 16,384)
         // each: Handler.Calls is empty; Link.ClosedWith == NormalClosure; Pool.Outstanding == 0
[Fact] Connect_HostRefuses_AnswersItsCodeAndReasonString()                 // Refuse(0x86, "no") → 20 08 00 86 05 1F 00 02 6E 6F; no OnAdmit, no OnClosed
[Fact] Connect_HostThrows_AnswersConnAck0x83AndLogsAnError()
[Fact] Connect_HostDoesNotAnswerInTenSeconds_AnswersConnAck0x88()          // the token given to the host is cancelled; Run completes although the host's task never does
[Fact] Connect_HostIgnoresCancellation_LateResultIsDiscarded()             // complete the host's task after the timeout: no OnAdmit, no OnClosed, no exception
[Fact] Connect_SocketClosesWhileTheHostDecides_CancelsTheHostsToken()      // RequestAborted.Cancel() → the token fires before the deadline
[Fact] Connect_WhileTheHostDecides_HoldsNoBuffer()                         // Pool.Outstanding == 0 inside OnConnectAsync
[Fact] Connect_PacketsBehindTheConnect_AreHandledAfterTheConnAck()         // Connect + Subscribe + PingReq in one Deliver → written: ConnAck, SubAckGrantedQoS1, PingResp, in that order (Review Focus 2; passes fully after Task 7)
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~ServerConnectTests"`.

- [ ] **Step 3: Implement the connect phase.** `MqttConnectRequest` copies the client id, user name, password and user properties out of the buffer; when nothing lies behind the CONNECT the buffer is returned before `OnConnectAsync` is called (decision 8).

- [ ] **Step 4: Run them to see them pass** (`Connect_PacketsBehindTheConnect…` is skipped with a reason until Task 7 un-skips it).

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire.Server tests/MqttWire.Tests/Server
git commit -m "Add the connect phase with its checks, deadline and CONNACK."
```

### Task 5: Admission, takeover and exactly one `OnClosed`

**Files:**
- Modify: `MqttWireConnection.Connect.cs`, `MqttWireConnection.Close.cs`, `MqttWireServer.cs`, `ServerHarness.cs` (`Connect`)
- Test: `tests/MqttWire.Tests/Server/AdmissionTests.cs`, `IdleCostTests.cs`

**Interfaces:**
- Produces: `internal MqttAdmitResult Admit(MqttWireConnection connection, out MqttWireConnection? replaced)` on `MqttWireServer`. Under `Registry.Gate`: refuse with `0x8B` when the server is stopping or the connection is no longer Authenticating; `existing = Find(clientId)`; `OnAdmit(connection, existing)`; on acceptance `SetCurrent(connection)`, mark it admitted and Active. After the lock is released, and before the CONNACK is handed to the link, the replaced connection gets `BeginClose(TakenOver, 0x8E, null)`.
- `MqttWireServer.ConnectionCount` is `Registry.ActiveCount`; `TryGetConnection` is `Registry.TryGet`.
- `FinishAsync` calls `OnClosed` once for an admitted connection, after `Registry.Remove(this)`, with the reason of the first `BeginClose`.

- [ ] **Step 1: Write the failing tests**

`AdmissionTests.cs`:
```csharp
[Fact] Admit_NoOtherConnection_PassesNullAsReplaces()                     // Calls: "OnConnectAsync c1", "OnAdmit c1 replaces=-"; ConnectionCount 1; TryGetConnection("c1") gives it
[Fact] Admit_SecondConnectionAccepted_TakesOverTheFirst()                 // first: written E0 01 8E, OnClosed TakenOver; TryGetConnection gives the second; OnAdmit's replaces is the first object
[Fact] Admit_TakeoverRefused_LeavesTheFirstUntouched()                    // second gets CONNACK with the host's code; first stays Active; no OnClosed for the second
[Fact] Admit_TwoConnectsFinishTogether_GoThroughOneAfterTheOther()        // both OnConnectAsync complete from one continuation: exactly one OnAdmit has replaces set
[Fact] Admit_NewConnectionFailsRightAfter_GetsOnClosed()                  // Link.FailWrites before the CONNACK → OnClosed ConnectionLost for the new one; the replaced one stays taken over
[Fact] Admit_HostThrows_AnswersConnAck0x83AndCallsNoOnClosed()            // the table is unchanged
[Fact] Admit_ReplacedConnectionAlreadyClosing_KeepsItsOwnReason()         // first is held in Closing by a held write; second connects: replaces is the first; first's OnClosed keeps its reason; one OnClosed each (Review Focus 3)
[Fact] OnClosed_OldConnectionEndsAfterTakeover_DoesNotRemoveItsReplacement()
[Fact] OnClosed_EveryAcceptedAdmit_IsMatchedByExactlyOne()                // 50 connect, takeover and disconnect cycles over 5 client ids: count("OnAdmit" accepted) == count("OnClosed")
[Fact] OnClosed_HostCallsTrySendAndClose_GetsNotOpenAndNothingHappens()   // Review Focus 1
[Fact] OnClosed_HostThrows_IsLoggedAndTheConnectionStillEnds()
[Fact] Disconnect_FromClient_EndsWithClientDisconnected()                 // E0 00 → OnClosed ClientDisconnected
```
`IdleCostTests.cs`:
```csharp
[Fact] Connect_TwoThousandIdleConnections_KeepAtMost768BytesEach()
// measure GC.GetTotalMemory(true) with 2,000 MemoryLinks alone, then with 2,000 more that are connected and idle;
// (second − first) / 2,000 <= 768. The state object the handler returns is null.
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~AdmissionTests|FullyQualifiedName~IdleCostTests"`.

- [ ] **Step 3: Implement admission, takeover and the end of a connection.**

- [ ] **Step 4: Run them to see them pass.** If the idle cost is over 768 bytes, find what holds it before going on (the usual suspects: a second parked state machine, a `CancellationTokenSource` that was not dropped, the read buffer struct's array): later tasks only add to it.

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire.Server tests/MqttWire.Tests/Server
git commit -m "Add admission, takeover and the exactly-once OnClosed."
```

### Task 6: Reading with no buffer while idle, and the three PUBLISH bands

**Files:**
- Create: `Internal/PublishSkip.cs`; modify `MqttWireConnection.Read.cs`, `MqttWireConnection.Close.cs`
- Test: `tests/MqttWire.Tests/Server/ReadTests.cs`, `PublishBandTests.cs`

**Interfaces:**
- Consumes: `PacketHeader.TryRead`, `PublishCodec.TryDecode(packet, PacketSource.Client, out PublishPacket)`.
- Produces:
  ```csharp
  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
  private async ValueTask<bool> HandleAvailableAsync();     // rents, reads, handles every complete packet, reads on while one is incomplete, returns the buffer; false ends the loop
  private bool HandlePacket(in PacketHeader header, ReadOnlySpan<byte> packet);   // Task 7 fills it; false when the connection is closing

  internal struct PublishSkip      // a field of the connection, in use only while a packet is being skipped
  {
      public void Start(in PacketHeader header);                          // QoS from the flags; remaining = RemainingLength
      public bool IsActive { get; }
      public int Feed(ReadOnlySpan<byte> bytes, out MqttReasonCode error); // consumes up to what the packet still has; returns the count consumed
      public bool IsComplete { get; }  public MqttQoS QoS { get; }  public ushort PacketIdentifier { get; }
  }
  ```
- Limits by type: a PUBLISH with Remaining Length up to `MaxPublishPayloadBytes + 5,120` is read whole; above that and up to `16 × MaxPublishPayloadBytes` it is skipped; above that DISCONNECT `0x95`. Any other packet over 16,384 bytes in total: DISCONNECT `0x95`.
- In band 1 a payload over `MaxPublishPayloadBytes` is refused with `0x97` and "The message is too large." in the form of its QoS (PUBACK, failing PUBREC with nothing remembered, nothing at QoS 0) and `OnPublish` is not called. A skipped packet is refused the same way once exactly its length has been consumed.
- The sweep closes with `0x82` (`PartialPacketTimedOut`) 10 s after the first byte of a packet that is not complete, and with `0x8D` (`KeepAliveTimedOut`) when 1.5 times the keep-alive has passed since the last complete packet. While the state is Closing, what arrives is read and thrown away until the link ends.

- [ ] **Step 1: Write the failing tests** (options `MaxPublishPayloadBytes = 1,024`: band 1 up to 6,144, the skip band up to 16,384)

`ReadTests.cs`:
```csharp
[Fact] Read_Idle_HoldsNoBufferAndWaitsInTheZeroByteWait()               // after Connect: Pool.Outstanding == 0, GetCounts().HoldsReadBuffer false
[Fact] Read_SeveralPacketsInOneMessage_HandlesThemInOrder()             // PingReq + PublishQoS0 + PingReq → Calls has one OnPublish; written D0 00 D0 00
[Fact] Read_PacketCutAtEveryPosition_IsHandledOnce()                    // PublishQoS1 cut at 1..11 → one OnPublish, one PubAck1 each time; Pool.Outstanding == 0 after each
[Fact] Read_AfterEveryPacket_ReturnsTheBuffer()
[Fact] Read_PartialPacketForTenSeconds_ClosesWith0x82()                 // "32 0A 00" then Advance(10 s) → E0 01 82, OnClosed PartialPacketTimedOut, buffer back
[Fact] Read_NoPacketFor45SecondsAtKeepAlive30_ClosesWith0x8D()          // Advance(44 s) → open; Advance(2 s) → E0 01 8D, OnClosed KeepAliveTimedOut
[Fact] Read_PingReqEvery30Seconds_KeepsTheConnectionOpen()              // five rounds
[Fact] Read_KeepAliveZero_UsesSixtySeconds()                            // closes at the sweep at 90 s, not before
[Fact] Read_OtherPacketOver16KiB_ClosesWith0x95()                       // a SUBSCRIBE header with Remaining Length 16,385 → E0 01 95, OnClosed ProtocolError; at most 4,096 bytes were rented
[Fact] Read_PeerEnds_EndsWithConnectionLost()
[Fact] Read_ReadFails_EndsWithConnectionLost()                          // Link.FailReads(new WebSocketException())
[Fact] Read_WhileClosing_DiscardsWhatArrives()                          // after a Close from the host, a PublishQoS0 reaches no handler
```
`PublishBandTests.cs`:
```csharp
[Theory] (QoS 0, 1, 2) Publish_PayloadAtTheLimit_ReachesTheHost()                       // 1,024 bytes
[Theory] (QoS 0, 1, 2) Publish_PayloadOverTheLimitInBandOne_IsRefusedWithoutOnPublish() // 1,025 bytes → nothing / TooLargePubAck1 / TooLargePubRec1; a later PubRel1 → PUBCOMP 0x92
[Theory] (QoS 0, 1, 2) Publish_LargeTopicSmallPayloadInBandOne_ReachesTheHost()         // topic of 5,000 bytes, payload "hi"
[Theory] (QoS 0, 1, 2) Publish_LargePropertiesSmallPayloadInBandOne_ReachesTheHost()    // a user property of 5,000 bytes
[Theory] (QoS 0, 1, 2) Publish_InTheSkipBand_IsRefusedAndNeverBuffered()                // Remaining Length 16,384 → Pool.LargestRent <= 4,096; refusal in the QoS's form; connection stays open
[Theory] (QoS 0, 1, 2) Publish_LargeTopicInTheSkipBand_FindsTheIdentifier()             // topic of 8,000 bytes, identifier 7 → the refusal carries identifier 7
[Theory] (QoS 0, 1, 2) Publish_AboveTheSkipBand_ClosesWith0x95()                        // Remaining Length 16,385
[Theory] (band 1, skip) × (QoS 0, 1, 2) Publish_CutAtEveryPosition_GivesTheSameResult() // every cut position of the whole packet; the packet after it (PingReq) is answered
[Fact] Publish_SkipBandTopicLengthRunsPastThePacket_ClosesWith0x81()
[Fact] Publish_SkipBandEndsBeforeItsIdentifier_ClosesWith0x81()
[Fact] Publish_SkippedPacketStallsForTenSeconds_ClosesWith0x82()
[Fact] Publish_SkipBandEmptyWithASmallLimit_GoesStraightTo0x95()                        // MaxPublishPayloadBytes 16: Remaining Length 5,137 → E0 01 95
[Fact] Publish_NoPayloadRequestProblemInformationZero_RefusalHasNoReasonString()        // ConnectNoProblemInfo; too large at QoS 1 → 40 03 00 01 97
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~ReadTests|FullyQualifiedName~PublishBandTests"`.

- [ ] **Step 3: Implement the read path.** For this task `HandlePacket` knows PUBLISH at QoS 0 and 1 (calling `OnPublish`, answering PUBACK through the inline send of Task 4), the failing PUBREC of a refused QoS 2 publish, which needs no record, PINGREQ and DISCONNECT; the rest is Task 7. The QoS 2 rows in which the host receives the publish are written now and skipped with the reason "Task 9".

- [ ] **Step 4: Run them to see them pass.** If `Publish_CutAtEveryPosition…` takes more than ten seconds, reuse one connection per theory row for the QoS 0 and 1 cases; every position is still covered.

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire.Server tests/MqttWire.Tests/Server
git commit -m "Add the read path with no buffer while idle and the three PUBLISH bands."
```

### Task 7: What each packet does

**Files:**
- Create: `MqttWireConnection.Packets.cs`
- Test: `tests/MqttWire.Tests/Server/PacketTests.cs`

**Interfaces:**
- Consumes: `SubscribeCodec.TryDecode`, `SubscribePacket.GetSubscriptions()`, `Topics.IsValidFilter`, `SubAckCodec.GetSize`/`Write`, `UnsubscribeCodec.TryDecode`, `UnsubAckCodec`, `PingCodec`, `DisconnectCodec`, `ReasonStrings.Cut`.
- Produces: the answers every later task uses, all in `MqttWireConnection.Send.cs`: `SendAck(PacketType type, ushort packetIdentifier, MqttReasonCode code, string? reasonString)`, `SendSubAck(ushort packetIdentifier, ReadOnlySpan<MqttReasonCode> codes, string? reasonString)`, `SendUnsubAck(ushort packetIdentifier, int count)`, `SendPingResp()`. In this task they take the inline path of Task 4; Task 8 adds waiting. Each drops the reason string when `_problemInformation` is false, cuts it to 256 bytes, and leaves it out when the packet would pass `_clientMaxPacketSize`.
- Behaviour, per design 6.6 "What each packet does" and 6.7:

  | Packet | Behaviour |
  |---|---|
  | PUBLISH | A decoder failure closes with the decoder's code (`0x81`, `0x82`, `0x90`). A topic alias: DISCONNECT `0x94`. A QoS above `MaxQoS`: DISCONNECT `0x9B`. Otherwise `OnPublish`; QoS 1 is answered with PUBACK carrying success or the host's refusal. |
  | SUBSCRIBE | A subscription identifier: DISCONNECT `0xA1`. `OnSubscribe` once per filter, in order, with the filter as a string and the QoS the client asked for; a granted QoS above `MaxQoS` is lowered. An invalid filter is `0x8F` without calling the host. One SUBACK, one code per filter, the reason string of the first refusal. |
  | UNSUBSCRIBE | `OnUnsubscribe` once per filter; UNSUBACK with success for each. |
  | PINGREQ | PINGRESP. |
  | DISCONNECT | Close, reason `ClientDisconnected`; nothing is written back. |
  | A second CONNECT, AUTH | DISCONNECT `0x82`. |
  | A packet type the server never reads (CONNACK, SUBACK, UNSUBACK, PINGRESP, type 0) | DISCONNECT `0x82`. |
  | Malformed | DISCONNECT with the decoder's code, `0x81` unless MQTT names another. |
- The reason codes for the SUBACK are collected in a `stackalloc` span of up to 256 codes, and in a rented buffer above that.
- A SUBACK or UNSUBACK that would pass `_clientMaxPacketSize` even without its reason string (thousands of filters against a client that announced 4,096 bytes) cannot be sent: DISCONNECT `0x95`, reason `ProtocolError`, and the host is not called for that packet. Design 6.6 says every control packet fits; this is the one that does not **(spec)**.

- [ ] **Step 1: Write the failing tests** in `PacketTests.cs`

```csharp
[Fact] Subscribe_OneFilterGranted_AnswersSubAckAtTheGrantedQoS()             // Subscribe → SubAckGrantedQoS1; Calls: "OnSubscribe c1 a/b q1"
[Fact] Subscribe_ThreeFilters_CallsTheHostOncePerFilterInOrder()            // codes 00, 87, 02 → 90 06 00 01 00 00 87 02
[Fact] Subscribe_GrantedAboveMaxQoS_IsLowered()                             // MaxQoS 1, host grants 2 → code 01
[Fact] Subscribe_RefusedWithAReasonString_CarriesTheFirstRefusalsString()
[Fact] Subscribe_InvalidFilter_Answers0x8FWithoutCallingTheHost()           // "a#"
[Fact] Subscribe_SharedFilter_IsPassedToTheHost()                           // "$share/g/a"
[Fact] Subscribe_WithSubscriptionIdentifier_ClosesWith0xA1()
[Fact] Subscribe_Options_AreIgnored()                                       // no-local, retain-as-published, retain handling 2: same call, same SUBACK
[Fact] Subscribe_ThreeHundredFilters_AnswersThreeHundredCodes()
[Fact] Subscribe_SubAckWouldPassTheClientsMaximumPacketSize_ClosesWith0x95()   // ConnectMaxPacketSize4096; 4,092 filters of one byte → E0 01 95; no OnSubscribe call
[Fact] Unsubscribe_TwoFilters_CallsTheHostTwiceAndAnswersSuccess()
[Fact] PingReq_Always_AnswersPingResp()
[Fact] Disconnect_WithAReasonAndProperties_EndsWithClientDisconnected()
[Fact] Connect_Second_ClosesWith0x82()                                      // E0 01 82, OnClosed ProtocolError
[Fact] Auth_Always_ClosesWith0x82()
[Fact] ConnAck_FromAClient_ClosesWith0x82()
[Fact] Publish_TopicNotUtf8_ClosesWith0x81()                                // ClientPackets.PublishTopicNotUtf8 → E0 01 81, OnClosed MalformedPacket
[Fact] Publish_WithTopicAlias_ClosesWith0x94()                              // ClientPackets.PublishWithTopicAlias
[Fact] Publish_WithSubscriptionIdentifier_ClosesWith0x82()
[Fact] Publish_WildcardInTheTopic_ClosesWith0x90()
[Fact] Publish_QoS2WithMaxQoS1_ClosesWith0x9B()
[Fact] Publish_QoS0_ReachesTheHostWithTheRetainFlag()                       // 31 08 … → "OnPublish c1 a/b hi q0 retain=1"; nothing written
[Fact] Publish_QoS1Refused_AnswersPubAckWithTheHostsCodeAndString()         // Refuse(0x87, "no") → 40 09 00 01 87 05 1F 00 02 6E 6F
[Fact] Publish_Payload_IsAViewIntoTheReadBuffer()                           // inside OnPublish: Pool.IsViewIntoRentedBuffer(publish.Payload)
[Fact] Publish_IgnoredProperties_AreValidatedAndWithoutEffect()             // content type, message expiry, response topic, correlation data, user property
[Theory] (OnSubscribe, OnUnsubscribe, OnPublish) Handler_Throws_ClosesWith0x83AndLogsAnError()   // E0 01 83, OnClosed HandlerThrew
[Fact] Acks_RequestProblemInformationZero_CarryNoReasonString()             // PUBACK, SUBACK and UNSUBACK after a refusal with a string
[Fact] Acks_ReasonStringOf600Bytes_IsCutTo256BytesAtACharacterBoundary()    // 300 × "é" → 128 characters; 100 × "€" → 85 (Review Focus 4)
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~PacketTests"`.

- [ ] **Step 3: Implement `HandlePacket` for every packet type.** Remove the skip from `Connect_PacketsBehindTheConnect_AreHandledAfterTheConnAck` (Task 4).

- [ ] **Step 4: Run them to see them pass.**

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire.Server tests/MqttWire.Tests/Server
git commit -m "Add what each packet from a client does."
```

### Task 8: Sending: taken at once, written when the socket allows

**Files:**
- Create: `Internal/PendingDelivery.cs`, `Internal/WaitingDeliveries.cs`, `Internal/ControlBuffer.cs`, `Internal/ServerPools.cs`
- Modify: `MqttWireConnection.Send.cs`, `MqttWireConnection.Close.cs`, `MqttWireConnection.cs`
- Test: `tests/MqttWire.Tests/Server/SendTests.cs`, `SendInterleavingTests.cs`, `ServerCloseTests.cs`

**Interfaces:**
- Consumes: `PublishCodec.GetHeaderSize`/`WriteHeader`/`GetSize`/`Write`, `PublishFields`, `DisconnectCodec.Write`, `IMqttLink.WriteAsync`/`CloseAsync`/`Abort`.
- Produces:
  ```csharp
  internal readonly record struct PendingDelivery(MqttTopic Topic, ReadOnlyMemory<byte> Payload, MqttQoS QoS);
  internal sealed class WaitingDeliveries     // a ring of PendingDelivery, pooled per server
  {
      public int Count { get; }  public long PayloadBytes { get; }
      public void Enqueue(in PendingDelivery delivery);  public ref readonly PendingDelivery Peek();  public void Dequeue();
      public void Clear();                    // drops every payload and topic reference
  }
  internal struct ControlBuffer               // a field of the connection; holds a rented array only while control packets wait
  {
      public int PacketCount { get; }  public int ByteCount { get; }  public bool HasPingResp { get; }
      public Span<byte> Reserve(ArrayPool<byte> pool, int size);        // grows by renting, copying and returning the old array
      public void Commit(int size, bool isPingResp);
      public byte[] Detach(out int length);                             // hands the array to the writer and forgets it
      public void Release(ArrayPool<byte> pool);                        // returns an array that was never handed to the link
  }
  internal sealed class ServerPools           // one per server
  {
      public ObjectPool<WaitingDeliveries> Waiting { get; }             // Task 9 adds Records
  }
  ```
- State of the send path, guarded by the connection's one `Lock`, never held across a write: `_writing` (a write is in progress or a caller is about to start one), `_waiting` (`WaitingDeliveries?`), `_control` (`ControlBuffer`), `_outgoingBytes`, `_writeStartedAt`.
- **The rule that makes "only while there is work" safe:** whoever finishes a write takes the lock once and, in that one hold, either picks the next thing to write (the control buffer first, then the first waiting delivery if its QoS has room in the window) or clears `_writing`. A caller that finds `_writing` clear sets it in the same hold in which it decided to write. There is no moment at which the drain has found nothing and `_writing` is still set.
- `TrySend`, in order: `ArgumentException` for `default(MqttTopic)` or a QoS that is not 0, 1 or 2; lower the QoS to `MaxQoS`; then under the lock: not Active → `NotOpen`; the packet is larger than `_clientMaxPacketSize`, or the payload larger than `MaxOutgoingBytes` → close with `0x95` (`MessageTooLargeToSend`), `TooLarge`; nothing in progress and nothing waiting → write it now, `Taken`; otherwise, if one more would pass `MaxOutgoingMessages` or `MaxOutgoingBytes` → close with `0x97` (`OutgoingLimitPassed`), `Evicted`; otherwise enqueue, `Taken`.
- A write that completes at once returns its buffer and drains on the caller's thread. One that does not is handed to `private async Task CompleteWriteAsync(ValueTask write, byte[]? buffer)`, which awaits it, returns the buffer, and drains; a write that fails is `BeginClose(ConnectionLost)`.
- `BeginClose` now follows design 6.6 "Closing, bounded": state to Closing once; **a write in progress → `link.Abort()` now and nothing else**; no write in progress → take the send path, write DISCONNECT with the code (not for `ClientDisconnected` or `ConnectionLost`), then `link.CloseAsync(NormalClosure)`, without waiting for the peer. When the send path next goes idle in Closing, what still waits is dropped: `WaitingDeliveries.Clear()` and back to the pool, `ControlBuffer.Release`.
- The sweep aborts the link when one piece has been with the link for 15 s (`WriteTimedOut`), and at the close deadline.
- `MqttWireConnection.Close(reason, reasonString)` is `BeginClose(ClosedByHost, reason, ReasonStrings.Cut(reasonString))`.

- [ ] **Step 1: Write the failing tests**

`SendTests.cs`:
```csharp
[Fact] TrySend_NothingInProgress_WritesAtOnceAndReturnsTheBuffer()            // Taken; written 30 08 00 03 61 2F 62 00 68 69; Pool.Outstanding 0; GetCounts() all zero
[Fact] TrySend_WriteHeld_ReturnsAtOnceAndWritesInOrderWhenReleased()          // HoldWrites; m1, m2, m3 → Taken ×3, WriteCount 1, WaitingDeliveries 2; CompleteHeldWrites → m1 m2 m3; counts zero
[Fact] TrySend_QueueEmptiedAgain_GoesBackToThePool()                          // the same WaitingDeliveries instance serves a second connection
[Fact] TrySend_ControlPacketWaitsBehindAWrite_IsWrittenBeforeWaitingDeliveries()   // m1 held, m2 waits, PingReq arrives → order m1, D0 00, m2
[Fact] TrySend_TwoPingReqsWhileAWriteIsHeld_OnePingRespWaits()
[Fact] TrySend_WaitingControlPackets_AreWrittenTogetherInProducedOrder()      // Unsubscribe ids 2, 3, 4 behind a held write → one write holding the three UNSUBACKs
[Fact] TrySend_PacketOf16384Bytes_IsCopiedIntoOneBuffer()                     // one write; the host's array is not what the link was given
[Fact] TrySend_PayloadOf200000Bytes_IsWrittenFromTheHostsMemoryInPieces()     // header, then 65,536 + 65,536 + 65,536 + 3,392, each piece inside the host's array; Pool.LargestRent <= 65,536
[Fact] TrySend_OnePayloadToTenConnections_IsNeverCopied()                     // 100,000 bytes: every payload piece on every link points into the one array
[Fact] TrySend_EmptyPayload_IsDelivered()                                     // 30 06 00 03 61 2F 62 00 (Review Focus 5)
[Fact] TrySend_DefaultTopic_ThrowsAndWritesNothing()                          // ArgumentException (Review Focus 5)
[Fact] TrySend_QoS2WithMaxQoS1_GoesOutAtQoS1()                                // first byte 32 (Review Focus 5; un-skipped in Task 10)
[Fact] TrySend_OneMoreThanMaxOutgoingMessages_EvictsWith0x97()                // MaxOutgoingMessages 3; held m1, waiting m2..m4, m5 → Evicted; IsAborted; no DISCONNECT; OnClosed OutgoingLimitPassed after the held write ends
[Fact] TrySend_WaitingBytesPassMaxOutgoingBytes_EvictsWith0x97()              // MaxOutgoingBytes 65,536: 40,000 held, 20,000 waits, 10,000 → Evicted
[Fact] TrySend_BurstWithinBothLimits_IsDeliveredInFull()                      // 1,000 messages behind one held write, then released: all written, in order
[Fact] TrySend_SingleMessageOverMaxOutgoingBytes_ReturnsTooLargeAndClosesWith0x95()   // written E0 01 95; OnClosed MessageTooLargeToSend
[Fact] TrySend_PacketOverTheClientsMaximumPacketSize_ReturnsTooLargeAndClosesWith0x95()   // ConnectMaxPacketSize4096, payload 5,000
[Fact] TrySend_ConnectionClosed_ReturnsNotOpen()
[Fact] TrySend_FromInsideOnPublishToTheSameConnection_SendsTheAckAndTheEcho()  // QoS 1 in, echo at QoS 0 → PubAck1 and the echo, no deadlock (Review Focus 1)
[Fact] TrySend_1025ControlPacketsWaiting_Evicts()                             // a held write, then 1,025 UNSUBSCRIBEs → aborted, OnClosed OutgoingLimitPassed
[Fact] TrySend_ManyValidPacketsFromAClientThatReadsNothing_StaysWithinTheBounds()   // held write; PINGREQs and QoS 0 publishes: one PINGRESP waits; AssertBounds after each
```
`SendInterleavingTests.cs` (design 6.13 item 3, deterministic):
```csharp
[Fact] Drain_MessageAddedBeforeTheWriteIsReleased_IsWrittenByTheDrain()
[Fact] Drain_MessageAddedWhileTheBufferIsReturned_IsWrittenOnceAndInOrder()   // TrySend from CountingPool.Returning: after the write ended, before the drain has looked
[Fact] Drain_MessageAddedAfterTheDrainWentIdle_IsWrittenByItsCaller()
[Fact] Drain_ControlPacketAddedWhileTheBufferIsReturned_GoesBeforeWaitingDeliveries()
// each: every message written exactly once, in order; GetCounts().IsWriting false and nothing waiting at the end
```
`ServerCloseTests.cs`:
```csharp
[Fact] Close_NoWriteInProgress_WritesDisconnectThenClosesTheWebSocket()        // Close(0x98) → E0 01 98, ClosedWith NormalClosure; OnClosed ClosedByHost once the peer ends
[Fact] Close_WithAReasonString_CarriesItCut()                                 // 300 × "é" → 256 bytes in the DISCONNECT
[Fact] Close_WriteInProgress_AbortsAtOnceAndWritesNoDisconnect()
[Fact] Close_CalledTwiceFromTwoThreads_ClosesOnce()
[Fact] Close_PeerNeverAnswers_AbortsAtTheCloseDeadline()                      // Advance(2 s) → IsAborted; OnClosed once
[Fact] Close_Abort_DoesNotReleaseTheHeldWritesBuffer()                        // Pool.Outstanding stays 1 until CompleteHeldWrites(exception), then 0 (6.8 rules 3 and 4)
[Fact] Write_StalledForFifteenSeconds_AbortsWithoutADisconnect()              // held write; Advance(14 s) → open; Advance(2 s) → IsAborted; nothing more written; OnClosed WriteTimedOut; what waited is released
[Fact] Write_Fails_EndsWithConnectionLostAndReturnsTheBuffer()
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~SendTests|FullyQualifiedName~SendInterleavingTests|FullyQualifiedName~ServerCloseTests"`.

- [ ] **Step 3: Implement the send path and the bounded close.** `TrySend` at QoS 1 and 2 is completed in Task 10; `TrySend_QoS2WithMaxQoS1_GoesOutAtQoS1` is written now and skipped with the reason "Task 10".

- [ ] **Step 4: Run them to see them pass**, then the whole suite: the earlier tasks' answers now go through the full send path.

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire.Server tests/MqttWire.Tests/Server
git commit -m "Add the send path, the outgoing limits and the bounded close."
```

### Task 9: QoS 1 and 2 from the client

**Files:**
- Create: `Internal/ExchangeRecord.cs`, `Internal/ExchangePhase.cs`, `Internal/ExchangeRecords.cs`, `MqttWireConnection.QoS.cs`
- Modify: `Internal/ServerPools.cs` (`public ObjectPool<ExchangeRecords> Records { get; }`)
- Test: `tests/MqttWire.Tests/Server/IncomingQoSTests.cs`

**Interfaces:**
- Consumes: `AckCodec.TryDecode`/`Write`, `ServerPools.Records`, `SendAck` (Task 7).
- Produces:
  ```csharp
  internal enum ExchangePhase : byte { IncomingAwaitingPubRel, DeliveryAwaitingPubAck, DeliveryAwaitingPubRec, DeliveryAwaitingPubComp }
  internal struct ExchangeRecord { public ushort PacketIdentifier; public ExchangePhase Phase; public long EnteredAt; }
  internal sealed class ExchangeRecords          // pooled per server; taken with the first exchange, returned with the last
  {
      public int IncomingCount { get; }  public int DeliveryCount { get; }  public bool IsEmpty { get; }
      public bool ContainsIncoming(ushort packetIdentifier);
      public void AddIncoming(ushort packetIdentifier, long now);
      public bool RemoveIncoming(ushort packetIdentifier);
      public bool TryFindDelivery(ushort packetIdentifier, out int index);
      public ref ExchangeRecord DeliveryAt(int index);
      public ushort AddDelivery(ExchangePhase phase, long now);    // the next free identifier: 1 to 65535, wrapping, skipping those in use
      public void RemoveDeliveryAt(int index);
      public long OldestEnteredAt { get; }                          // 0 when empty
      public void Clear();
  }
  ```
- The exchange, per design 6.6 "Publishes from the client at QoS 2": a new identifier → `OnPublish`; accepted → remembered, PUBREC success; refused → PUBREC with the refusal, not remembered. A remembered identifier, whatever its DUP flag → no `OnPublish`, PUBREC success again. PUBREL → forgotten, PUBCOMP; unknown → PUBCOMP `0x92`.
- The count of decision 13 goes up when a QoS 1 or 2 PUBLISH is taken for handling and down when its last answer (PUBACK, PUBCOMP, or a failing PUBREC) has been written. A PUBLISH that would make it 257 closes with `0x93` (`ReceiveMaximumPassed`) before `OnPublish`.
- The sweep closes with `0x82` (`AcknowledgementTimedOut`) when `OldestEnteredAt` is 30 s old.

- [ ] **Step 1: Write the failing tests** in `IncomingQoSTests.cs`

```csharp
[Fact] PublishQoS2_FullExchange_CallsTheHostOnceAndEndsWithNoRecord()        // PublishQoS2 → PubRec1; PubRel1 → PubComp1; IncomingRecords 0; the records object is back in the pool
[Fact] PublishQoS2_Refused_AnswersAFailingPubRecAndRemembersNothing()       // Refuse(0x87) → 50 03 00 01 87; then PubRel1 → PUBCOMP 0x92
[Fact] PublishQoS2_RepeatedIdentifierDup0_IsNotHandedOnAgain()              // PublishQoS2Id7 twice → one OnPublish, two PubRec7
[Fact] PublishQoS2_RepeatedIdentifierDup1_IsNotHandedOnAgain()              // PublishQoS2Id7 then PublishQoS2Id7Dup
[Fact] PublishQoS2_AfterPubRel_TheIdentifierIsNewAgain()                    // … PubRel7, PublishQoS2Id7 → a second OnPublish
[Fact] PubRel_UnknownIdentifier_AnswersPubComp0x92()                        // PubRel7 → PubCompNotFound7
[Fact] PubRel_NeverComes_ClosesWith0x82AfterThirtySeconds()                 // PINGREQs keep the keep-alive happy; Advance(30 s) → E0 01 82, OnClosed AcknowledgementTimedOut
[Fact] PublishQoS2_256Unreleased_AreAllAccepted()                           // identifiers 1..256, IncomingRecords 256
[Fact] PublishQoS2_The257th_ClosesWith0x93()                                // E0 01 93, OnClosed ReceiveMaximumPassed; OnPublish was called 256 times
[Fact] PublishQoS1_257UnwrittenPubAcks_ClosesWith0x93()                     // a held write; 257 QoS 1 publishes
[Fact] PublishQoS1_AnsweredAtOnce_NeverCountsUp()                           // 1,000 QoS 1 publishes on an open socket: the count is 0 after each
[Fact] PubAck_UnknownIdentifierFromTheClient_IsIgnored()                    // the connection stays open, nothing written
[Fact] PublishQoS2_SkippedAndRefused_RemembersNothing()                     // the skip band at QoS 2, then PubRel → PUBCOMP 0x92
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~IncomingQoSTests"`.

- [ ] **Step 3: Implement the records and the incoming exchanges.** Remove the skip from the accepting QoS 2 rows of `PublishBandTests` (Task 6).

- [ ] **Step 4: Run them to see them pass.**

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire.Server tests/MqttWire.Tests/Server
git commit -m "Add QoS 1 and 2 for what a client publishes."
```

### Task 10: Deliveries at QoS 1 and 2, and the window

**Files:**
- Modify: `MqttWireConnection.QoS.cs`, `MqttWireConnection.Send.cs`
- Test: `tests/MqttWire.Tests/Server/DeliveryQoSTests.cs`, `WindowDeadlockTests.cs`

**Interfaces:**
- Consumes: `ExchangeRecords` (Task 9), the drain of Task 8.
- Behaviour, per design 6.6 "Deliveries at QoS 1 or 2":
  - A delivery at QoS 1 or 2 takes an identifier and a record **under the lock, before its packet is handed to the link**. The window is `_clientWindow` (the client's Receive Maximum or 256, whichever is smaller). With the window full the delivery waits, and so does every delivery behind it, of any QoS; control packets do not.
  - PUBACK for a `DeliveryAwaitingPubAck` record: removed. PUBREC below `0x80` for `DeliveryAwaitingPubRec`: phase to `DeliveryAwaitingPubComp`, `EnteredAt` now, PUBREL. PUBREC of `0x80` or more: removed. PUBCOMP for `DeliveryAwaitingPubComp`: removed. Every removal frees a place and starts the drain if deliveries wait and nothing is being written.
  - One repetition is harmless: a PUBREC for a record already `DeliveryAwaitingPubComp` is answered with PUBREL again and changes nothing.
  - Any other acknowledgement for a known identifier (a PUBACK for a QoS 2 delivery, a PUBCOMP before the PUBREC, a PUBREC for a QoS 1 delivery): DISCONNECT `0x82`.
  - An acknowledgement for an identifier with no record is ignored, except a PUBREC, which is answered with PUBREL `0x92`.

- [ ] **Step 1: Write the failing tests**

`DeliveryQoSTests.cs`:
```csharp
[Fact] TrySendQoS1_FullExchange_EndsWithNoRecord()                           // written 32 0A 00 03 61 2F 62 00 01 00 68 69; DeliveryRecords 1; PubAck1 → 0, records back in the pool
[Fact] TrySendQoS2_FullExchange_EndsWithNoRecord()                           // 34 …; PubRec1 → PubRel1 written; PubComp1 → DeliveryRecords 0
[Fact] TrySendQoS2_PubRecWithAFailureCode_EndsTheExchange()                  // 50 03 00 01 87 → no PUBREL, DeliveryRecords 0, the place is free
[Fact] TrySendQoS1_WindowOfTwo_NeverHasThreeInFlight()                       // Receive Maximum 2; five sends: 2 written, 3 wait; each PUBACK lets one more out; AssertBounds after each
[Fact] TrySendQoS0_BehindAWaitingQoS1_KeepsItsPlaceInTheOrder()              // window 1: q1 a, q1 b (waits), q0 c (waits) → after PUBACK for a: b then c
[Fact] TrySendQoS1_WaitingForTheWindow_CountsTowardsBothLimits()             // window 1, MaxOutgoingMessages 3: the 5th send → Evicted, DISCONNECT 0x97 written (no write is in progress)
[Fact] TrySendQoS1_PubAckNeverComes_ClosesWith0x82AfterThirtySeconds()       // OnClosed AcknowledgementTimedOut
[Fact] TrySendQoS2_PubCompNeverComes_DeadlineRestartsAtThePubRec()           // PUBREC at 20 s; open at 48 s; closed at the sweep at 50 s
[Fact] PubAck_UnknownIdentifier_IsIgnored()
[Fact] PubComp_UnknownIdentifier_IsIgnored()
[Fact] PubRec_UnknownIdentifier_AnswersPubRel0x92()                          // PubRec7 → PubRelNotFound7
[Fact] PubRec_Repeated_AnswersPubRelAgainAndChangesNothing()
[Theory] (PUBACK for a QoS 2 delivery) (PUBCOMP before PUBREC) (PUBREC for a QoS 1 delivery) Ack_WrongKindForAKnownIdentifier_ClosesWith0x82()
[Fact] PubAck_ArrivesWhileItsPublishIsStillBeingWritten_CompletesTheExchange()   // HoldWrites; TrySend q1; Deliver PubAck1; CompleteHeldWrites → DeliveryRecords 0, no error
[Fact] TrySendQoS1_IdentifiersAcrossTheWrap_SkipThoseInUse()                 // start the counter at 65,534 with identifier 1 still in flight: 65534, 65535, 2
[Fact] TrySendQoS1_AfterTheLastAcknowledgement_TheConnectionIsIdleAgain()    // GetCounts() all zero; Pool.Outstanding 0
[Fact] TrySendQoS1_ClientUnsubscribesMeanwhile_IsStillDelivered()
```
`WindowDeadlockTests.cs` (design 6.13 item 2, step by step; `ConnectReceiveMaximum1`; `AssertBounds` after every step):
```csharp
[Fact] Window_Full_PubRelIsWrittenAlthoughADeliveryWaits()
// 1 TrySend A q2 → written;  2 TrySend B q2 → Taken, waits;  3 PubRec(A) → PUBREL(A) is written now, B still waits;
// 4 PubComp(A) → B's PUBLISH is written;  5 PubRec(B), PubComp(B) → idle
[Fact] Window_Full_PingReqIsAnsweredAlthoughADeliveryWaits()                 // between steps 2 and 3: PingReq → PingResp at once
[Fact] Window_Full_AClientsQoS1PublishIsAcknowledgedAlthoughADeliveryWaits() // between steps 2 and 3: PublishQoS1 → OnPublish, PubAck1 at once
[Fact] Window_FullAndAWriteHeld_ControlPacketsStillGoFirstWhenItIsReleased()
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~DeliveryQoSTests|FullyQualifiedName~WindowDeadlockTests"`.

- [ ] **Step 3: Implement the deliveries' exchanges and the window.** Remove the skip from `TrySend_QoS2WithMaxQoS1_GoesOutAtQoS1` (Task 8).

- [ ] **Step 4: Run them to see them pass.**

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire.Server tests/MqttWire.Tests/Server
git commit -m "Add deliveries at QoS 1 and 2 with the client's window."
```

### Task 11: The host stopping, registration and mapping

**Files:**
- Create: `MqttWireServiceCollectionExtensions.cs`, `MqttWireEndpointRouteBuilderExtensions.cs`, `Internal/MqttWireServerLifetime.cs`
- Modify: `MqttWireServer.cs`, `MqttWireConnection.Run.cs`
- Test: `tests/MqttWire.Tests/Server/StopTests.cs`, `HostingTests.cs`

**Interfaces:**
- Produces:
  ```csharp
  public static class MqttWireServiceCollectionExtensions
  {
      public static IServiceCollection AddMqttWireServer<THandler>(
          this IServiceCollection services, Action<MqttWireServerOptions>? configure = null)
          where THandler : class, IMqttWireHandler;
  }
  public static class MqttWireEndpointRouteBuilderExtensions
  {
      public static IEndpointConventionBuilder MapMqttWire(this IEndpointRouteBuilder endpoints, [StringSyntax("Route")] string pattern);
  }
  internal sealed class MqttWireServerLifetime(MqttWireServer server) : IHostedLifecycleService
  // MqttWireServer
  internal bool IsStopping { get; }
  internal Task StopAsync();     // design 6.6 "When the host stops"
  ```
- `AddMqttWireServer` registers: `THandler` as the singleton `IMqttWireHandler`, the options with the validator and `ValidateOnStart`, `MqttWireServer` as a singleton built by a factory with `TimeProvider.System` and `ArrayPool<byte>.Shared`, and the lifetime service. Calling it twice throws `InvalidOperationException`.
- The endpoint, in order: no `IHttpWebSocketFeature` → `InvalidOperationException` naming `UseWebSockets()`; not a WebSocket request, or `mqtt` not among the requested sub-protocols → HTTP 400; the server is stopping → HTTP 503; otherwise return `server.RunConnectionAsync(httpContext, link: null)`.
- `StopAsync`: set `IsStopping` under `Registry.Gate`, so no admission can follow; for every slot, `BeginClose(ServerStopping, 0x8B, null)` (an Accepted connection is aborted; an Authenticating one has its token cancelled and gets CONNACK `0x8B`); await `Registry.WhenEmptyAsync()`, during which the sweep keeps running and aborts what passes the close deadline; then dispose the sweep timer.
- `MqttWireServerLifetime`: `StartAsync` calls `server.Start()`; `StoppingAsync` awaits `server.StopAsync()`; the other four methods do nothing.

- [ ] **Step 1: Write the failing tests**

`StopTests.cs`:
```csharp
[Fact] StopAsync_ActiveConnections_AreClosedWith0x8B()                       // three connections: each written E0 01 8B and closed; OnClosed ServerStopping ×3
[Fact] StopAsync_PeersNeverAnswer_CompletesAfterTheCloseDeadline()           // not complete before Advance(2 s); complete after; every link aborted
[Fact] StopAsync_Completed_EveryBufferIsBackAndEveryOnClosedWasCalled()      // Pool.Outstanding 0; one OnClosed per accepted OnAdmit
[Fact] StopAsync_Completed_TheSweepTimerIsDisposed()
[Fact] StopAsync_AConnectionWithAHeldWrite_IsAbortedAndWaitedFor()           // completes only after CompleteHeldWrites(exception)
[Fact] StopAsync_WhileATakeoverIsUnderWay_EndsBothConnectionsOnce()          // the second is inside OnConnectAsync when the stop begins: it gets CONNACK 0x8B, no OnAdmit; the first OnClosed ServerStopping
[Fact] StopAsync_AnAcceptedConnectionWithoutConnect_IsAborted()
[Fact] StopAsync_NewConnectAfterTheStopBegan_IsRefusedWith0x8B()
[Fact] StopAsync_CalledTwice_ReturnsTheSameCompletion()
```
`HostingTests.cs` (the generic host, no web server, no network):
```csharp
[Fact] StartAsync_InvalidOption_ThrowsAndTheHostDoesNotStart()               // MaxOutgoingBytes = 1 → OptionsValidationException from host.StartAsync()
[Fact] AddMqttWireServer_Resolved_GivesOneServerAndTheHandler()
[Fact] AddMqttWireServer_CalledTwice_Throws()
[Fact] StopAsync_Host_StopsTheServerBeforeAnyHostedServiceIsStopped()        // a second hosted service records server.IsStopping in its StopAsync: true
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~StopTests|FullyQualifiedName~HostingTests"`.

- [ ] **Step 3: Implement the stop, the registration, the mapping and the lifetime service.**

- [ ] **Step 4: Run them to see them pass.**

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire.Server tests/MqttWire.Tests/Server
git commit -m "Add the bounded stop, the registration and the endpoint."
```

### Task 12: Logging why a connection ended

**Files:**
- Create: `Internal/ServerLog.cs`; modify the call sites in `MqttWireConnection.*.cs`
- Test: `tests/MqttWire.Tests/Server/LoggingTests.cs`

**Interfaces:**
- Produces: `internal static partial class ServerLog` with one `[LoggerMessage]` method per event. The logger's category is `MqttWire.Server.MqttWireServer`.

  | Id | Name | Level | Fields |
  |---|---|---|---|
  | 1 | MalformedPacket | Debug | client id, reason code |
  | 2 | ProtocolError | Debug | client id, reason code |
  | 3 | RefusedByHost | Debug | client id, reason code |
  | 4 | AuthenticationTimedOut | Error | client id |
  | 5 | ConnectTimedOut | Debug | remote end point |
  | 6 | KeepAliveTimedOut | Debug | client id |
  | 7 | PartialPacketTimedOut | Debug | client id |
  | 8 | WriteTimedOut | Debug | client id |
  | 9 | AcknowledgementTimedOut | Debug | client id |
  | 10 | OutgoingLimitPassed | Debug | client id |
  | 11 | ReceiveMaximumPassed | Debug | client id |
  | 12 | MessageTooLargeToSend | Debug | client id, payload length |
  | 13 | TakenOver | Debug | client id |
  | 14 | ClosedByHost | Debug | client id, reason code |
  | 15 | HandlerThrew | Error | client id, the handler method's name, the exception |
  | 16 | ServerStopping | Debug | client id |
  | 17 | ClientDisconnected | Debug | client id, reason code |
  | 18 | ConnectionLost | Debug | client id |
  | 19 | LateConnectResult | Debug, or Error when it threw | client id |
- Every end is logged once, where `BeginClose` wins the move to Closing (and at a refusal before admission). Reason strings from the host are not logged: a host may put anything into them.

- [ ] **Step 1: Write the failing tests** in `LoggingTests.cs`

```csharp
[Theory] End_EachReason_IsLoggedOnceWithItsEventId(…)        // one row per id 1 to 18, driving the scenario of the earlier task that ends that way
[Fact] End_HandlerThrew_IsAnErrorWithTheException()
[Fact] End_AuthenticationTimedOut_IsAnError()
[Fact] End_OrdinaryEnds_AreDebug()
[Fact] Connect_LateResultAfterTheDeadline_IsLoggedAndDiscarded()   // id 19; Error when the late task threw
[Fact] Log_AnyLine_ContainsNoPasswordAndNoPayload()
// password "s3cret-PASSWORD", payload "PAYLOAD-MARKER" in a publish, a delivery, a refused publish and a handler that throws with the payload still in hand:
// no entry's message contains either marker
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~LoggingTests"`.

- [ ] **Step 3: Implement `ServerLog` and call it.**

- [ ] **Step 4: Run them to see them pass.**

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire.Server tests/MqttWire.Tests/Server
git commit -m "Log why each connection ended."
```

### Task 13: Buffers, bounds, the stress test and the allocation tests

**Files:**
- Modify: `tests/MqttWire.Tests/Server/ServerHarness.cs` (`AssertBounds`)
- Test: `tests/MqttWire.Tests/Server/ServerBufferTests.cs`, `ServerStressTests.cs`, `ServerAllocationTests.cs`

**Interfaces:**
- Consumes: `CountingPool` (`Outstanding`, `FaultyReturns`, `LargestRent`, `Returning`), `OverwritingPool` (`Filler`, `IsRented`, `Outstanding`, `FaultyReturns`), `MemoryLink` (`HoldWrites`, `HeldWriteBuffer`, `PendingReadBuffer`, `HoldReadsOnAbort`, `CompleteHeldWrites`), `MqttWireConnection.GetCounts()`.
- `ServerHarness.AssertBounds()` exists from Task 3 and gains a check with each task that adds a count; this task completes it. Called by `TestConnection` after every delivery and by the tests after every send and every advance of the clock, it asserts for every connection: `IncomingRecords <= 256`; `DeliveryRecords <= min(the client's Receive Maximum, 256)`; `WaitingDeliveries <= MaxOutgoingMessages`; `WaitingControlPackets <= 1,024`; `OutgoingBytes <= MaxOutgoingBytes`; and for the pool: `FaultyReturns == 0`, `LargestRent <= max(65,536, MaxPublishPayloadBytes + 5,120 + 5)`, `Outstanding <= 3 × the number of connections` (a read buffer, an encoding buffer, a control buffer).

- [ ] **Step 1: Write the tests**

`ServerBufferTests.cs` (design 6.13 item 4):
```csharp
[Theory] WayOut_Each_ReturnsEveryBufferExactlyOnce(…)
// rows: normal disconnect · refused CONNECT · host refusal · authentication timeout · connect timeout · partial-packet timeout ·
// keep-alive timeout · malformed packet · a PUBLISH above the skip band · eviction with a held write · "too large" · takeover ·
// handler throws · write fails · read fails · write deadline · server stop.  Each ends with Outstanding == 0 and FaultyReturns == 0.
[Fact] Abort_ReadHeldOpen_TheReadNeverSeesReusedMemory()            // OverwritingPool; HoldReadsOnAbort; abort; another connection rents: the first read's buffer is still rented until it ends
[Fact] Abort_WriteHeldOpen_TheWriteNeverSeesReusedMemory()          // HeldWriteBuffer still holds the packet's bytes, not 0xEE, until CompleteHeldWrites
[Fact] Abort_AtTheSameMomentAsTheWritesCompletion_ReturnsOnce()     // abort from CountingPool.Returning and from just before CompleteHeldWrites: FaultyReturns 0 both ways
[Fact] Abort_ControlBufferStillWaiting_IsReturnedOnce()
[Fact] Close_WaitingDeliveries_DropEveryPayloadReference()          // a WeakReference to a queued payload dies after the close and a collection
[Fact] Cycles_ThousandConnectWorkDisconnect_AccumulateNothing()     // Outstanding 0, every slot free, both object pools hold at most 1,024, live bytes after == before within 64 KiB
```
`ServerStressTests.cs` (design 6.13 item 3, stress):
```csharp
[Fact] TrySend_EightThreadsOnOneConnection_WritesEverythingOnceAndInOrder()
// 8 threads × 5,000 QoS 0 sends, payload = (thread, sequence); a ninth thread toggles HoldWrites and CompleteHeldWrites;
// limits raised so nothing is evicted. Afterwards: 40,000 packets written, each once; per thread the sequences ascend;
// IsWriting false, nothing waiting, Outstanding 0.
[Fact] TrySendAndClose_Racing_NeverWriteAfterTheCloseAndReturnEveryBuffer()   // 200 rounds: 4 senders, one closer
```
`ServerAllocationTests.cs` (the pattern of `ClientAllocationTests`: warm up, then `GC.GetAllocatedBytesForCurrentThread()` around 1,000 iterations, Release only):
```csharp
[Fact] Receive_PingReq_AllocatesNothing()
[Fact] Receive_PublishQoS0ToTheHost_AllocatesNothing()
[Fact] Receive_PublishQoS1AndPubAck_AllocatesNothing()
[Fact] Receive_PublishQoS2Exchange_AllocatesNothing()                // the records object comes from and goes back to the pool
[Fact] TrySend_QoS0OnAnOpenSocket_AllocatesNothing()
[Fact] TrySend_QoS1Exchange_AllocatesNothing()
[Fact] TrySend_LargePayloadInPieces_AllocatesNothing()               // 200,000 bytes, writes completing at once
[Fact] TrySend_BehindAHeldWrite_AllocatesNothingOnceTheQueueExists()
[Fact] Sweep_ThousandIdleConnections_AllocatesNothing()
```

- [ ] **Step 2: Run them.** Run: `dotnet test -c Release --filter "FullyQualifiedName~ServerBufferTests|FullyQualifiedName~ServerStressTests|FullyQualifiedName~ServerAllocationTests"`. These test code that exists, so some pass at once; fix the server where one does not.

- [ ] **Step 3: See each buffer test fail on a planted fault** (the "Done when" of #6). One at a time, plant and revert: (a) skip the return of the read buffer on the malformed-packet path; (b) return the encoding buffer before awaiting a held write; (c) return the control buffer in `BeginClose` as well as in the drain; (d) have the sweep's abort return the held write's buffer. Expected: (a) `WayOut_Each…` fails on its row; (b) `Abort_WriteHeldOpen…` fails; (c) and (d) `FaultyReturns` is not 0. Note in the pull request which test caught which fault. `git diff --stat src` is empty afterwards.

- [ ] **Step 4: Run the whole suite**, Debug and Release.

- [ ] **Step 5: Commit**

```bash
git add tests/MqttWire.Tests/Server
git commit -m "Add the server's buffer, stress and allocation tests."
```

### Task 14: The test host, the .NET interops and the endpoint over Kestrel

**Files:**
- Create: `tests/MqttWire.TestHost/MqttWire.TestHost.csproj`, `ExactTopicHandler.cs`, `Program.cs`
- Create: `tests/MqttWire.Interop.Tests/ServerHost.cs`, `ServerInteropTests.cs`, `ClientServerInteropTests.cs`, `EndpointTests.cs`
- Modify: `tests/MqttWire.Interop.Tests/MqttWire.Interop.Tests.csproj` (reference `MqttWire.Server` and the test host), `MqttWire.slnx`

**Interfaces:**
- Produces:
  ```csharp
  // tests/MqttWire.TestHost: a web project, also referenced as a library
  public sealed class ExactTopicHandler : IMqttWireHandler
  // Accepts every CONNECT except user name "refuse-me" (0x86). OnSubscribe grants the QoS asked for and remembers (filter, QoS)
  // in the connection's State; filters are exact topics, a wildcard is refused with 0xA2. OnPublish copies the payload once and
  // sends it to every connection subscribed to that exact topic, the sender included, at the lower of the two QoS.
  // OnUnsubscribe and OnClosed clean the table.
  // Program: AddMqttWireServer<ExactTopicHandler>(), UseWebSockets(), MapMqttWire("/mqtt"); it prints "LISTENING <port>" once started.

  // tests/MqttWire.Interop.Tests
  internal sealed class ServerHost : IAsyncDisposable     // Kestrel on 127.0.0.1, port 0
  {
      public static Task<ServerHost> StartAsync(Action<MqttWireServerOptions>? configure = null);
      public Uri WebSocketUri { get; }  public MqttWireServer Server { get; }  public Task StopAsync();
  }
  ```

- [ ] **Step 1: Write the failing tests**

`ServerInteropTests.cs` (MQTTnet's client against MqttWire's server):
```csharp
[Theory] (QoS 0, 1, 2) Publish_FromMqttNetClient_ReachesASubscriberAtThatQoS()      // two MQTTnet clients on "t/1"; the payload arrives once, at the QoS
[Theory] (QoS 0, 1, 2) Subscribe_MqttNetClient_IsGrantedTheQoSItAskedFor()
[Fact] Connect_MqttNetClient_SeesTheConnAckProperties()                             // Receive Maximum 256; no Maximum QoS; Server Keep Alive absent at keep-alive 15
[Fact] Connect_RefusedUserName_MqttNetClientSeesBadUserNameOrPassword()
[Fact] Connect_SameClientIdTwice_TheFirstIsDisconnectedWithSessionTakenOver()
[Fact] Publish_OverMaxPublishPayloadBytes_IsRefusedWithQuotaExceeded()              // QoS 1, 70,000 bytes → the publish result's reason code is 0x97; the connection stays open
[Fact] Publish_TwoHundredQoS2InARow_AllArriveOnce()
```
`ClientServerInteropTests.cs` (MqttWire's client against MqttWire's server):
```csharp
[Theory] (QoS 0, 1, 2) PublishAsync_ToASubscriber_ArrivesAtThatQoS()
[Fact] ConnectAsync_Refused_ReportsTheReasonCode()
[Fact] PublishAsync_PayloadOf200000Bytes_ArrivesWhole()                             // MaxPublishPayloadBytes raised to 262,144; the pieces of decision 10 over a real socket
[Fact] ReconnectingClient_ServerClosesTheConnection_ComesBackAndSubscribesAgain()   // connection.Close(0x98) from the host
```
`EndpointTests.cs`:
```csharp
[Fact] Request_WithoutTheMqttSubProtocol_IsAnsweredWith400()
[Fact] Request_NotAWebSocket_IsAnsweredWith400()
[Fact] Request_WhileTheServerStops_IsAnsweredWith503()
[Fact] TextMessage_Always_ClosesTheWebSocketWith1003()
[Fact] Accept_Always_AnswersWithTheMqttSubProtocol()
[Fact] StopAsync_HostWithOpenConnections_EndsWithinFiveSeconds()                    // ten connected clients; each sees DISCONNECT 0x8B
[Fact] MapMqttWire_WithoutUseWebSockets_FailsTheRequestWithAClearError()
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test tests/MqttWire.Interop.Tests`.

- [ ] **Step 3: Implement the test host and `ServerHost`.**

- [ ] **Step 4: Run them to see them pass**, three times in a row: an interop test that passes two times in three is a bug in the server until shown otherwise.

- [ ] **Step 5: Commit**

```bash
git add MqttWire.slnx tests/MqttWire.TestHost tests/MqttWire.Interop.Tests
git commit -m "Add the test host and the interops with MQTTnet's client and MqttWire's client."
```

### Task 15: The design, the README, and the first pull request

**Files:**
- Modify: `docs/specs/2026-11-19-mqttwire-design.md`, `README.md`

- [ ] **Step 1: Update the design** where this plan's decisions marked **(spec)** change its text: the states table of 6.6 (decision 8); admission, "a connection stays in the table until it is Closed" (decision 4); the 65,526-byte limit of `MqttTopic` in 6.2 and 6.6 (decision 10); 6.12, the two further reasons and the reason code in the log (decision 15); 6.6 "A client's maximum packet size", the SUBACK that cannot fit (Task 7); 6.1, the test host and the interop project (decision 18); section 11, what the builder decided (`State` is `object?`, registration, mapping, shutdown); 6.14, the target is no longer provisional. Add anything that turned out differently while building, each as one sentence where the design states the old behaviour.

- [ ] **Step 2: Update the README:** the handler in twenty lines, registration and mapping, what MqttWire is not.

- [ ] **Step 3: Verify.** Run: `dotnet build -c Release` (0 warnings), `dotnet test -c Release` (all green).

- [ ] **Step 4: Commit and open the pull request**

```bash
git add docs README.md
git commit -m "Bring the design and the README up to the server as built."
git push -u origin feature/6-server-and-dotnet-interops
gh pr create --title "#6 - Step 3: the server, its tests and the .NET interops." --body "<what was built, the decisions of the plan, which planted fault each buffer test caught, 'Part of #6'>"
```

---

# Part B: MQTT.js, the sample and the benchmark

Part B starts when part A is merged: `git switch main && git pull && git switch -c feature/6-mqttjs-sample-and-benchmark`.

### Task 16: The interop with MQTT.js, and its CI job

**Files:**
- Create: `tests/interop-js/package.json`, `package-lock.json`, `host.mjs`, `interop.test.mjs`, `README.md`, `.gitignore` (`node_modules/`)
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `tests/MqttWire.TestHost` (Task 14), which prints `LISTENING <port>`.
- Produces:
  - `package.json`: `"private": true`, `"type": "module"`, `"scripts": { "test": "node --test" }`, `"engines": { "node": ">=24" }`, and one dependency, `mqtt`, at an exact version (no `^`). Install it with `npm install --save-exact mqtt@5`; the version it resolves is the pinned one, recorded in `README.md`, and raising it is a deliberate change (design 6.15).
  - `host.mjs`: `export async function startHost()` starts `dotnet run --project ../MqttWire.TestHost -c Release --no-build -- --urls http://127.0.0.1:0`, waits for the `LISTENING` line, and returns `{ url: "ws://127.0.0.1:<port>/mqtt", stop }`; `stop` ends the process and waits for it.
  - A second CI job, `interop-js`, beside `dotnet`: checkout, `actions/setup-dotnet@v4` (10.0.x), `actions/setup-node@v4` (`node-version: 24`, `cache: npm`, `cache-dependency-path: tests/interop-js/package-lock.json`), `dotnet build tests/MqttWire.TestHost -c Release`, then in `tests/interop-js`: `npm ci` and `npm test`. `timeout-minutes: 10`.

- [ ] **Step 1: Write the failing tests** in `interop.test.mjs` (Node's own test runner; every client is created with `protocolVersion: 5` and ended in `after`)

```js
test("connects and reads the CONNACK properties")                         // connack.properties.receiveMaximum === 256
for (const qos of [0, 1, 2]) {
  test(`a publish at QoS ${qos} from MQTT.js reaches a subscriber once`)  // two clients on "t/js"; payload and packet.qos match
  test(`MQTT.js is granted QoS ${qos} when it asks for it`)               // granted[0].qos === qos
}
test("a refused user name is reported as 0x86")                           // the error's code is 134
test("a second client with the same id takes the first one over")         // the first sees a disconnect packet with reasonCode 142
test("a publish over the limit is refused with 0x97 and the connection stays open")   // QoS 1, 70,000 bytes
test("a publish of 200,000 bytes from the host arrives whole")            // the host is started with MaxPublishPayloadBytes 262,144
test("two hundred QoS 2 publishes in a row all arrive once")
test("the client is told 0x8B when the host stops")                       // stop() while connected
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet build tests/MqttWire.TestHost -c Release`, then in `tests/interop-js`: `npm test`. Expected: failures, because `host.mjs` does not exist.

- [ ] **Step 3: Implement `host.mjs`**, and give `Program.cs` of the test host a `--max-publish-payload-bytes` argument.

- [ ] **Step 4: Run them to see them pass**, three times in a row.

- [ ] **Step 5: Add the CI job and commit**

```bash
git add tests/interop-js .github/workflows/ci.yml tests/MqttWire.TestHost
git commit -m "Add the interop tests with MQTT.js and their CI job."
```

### Task 17: Shared topics: the test and the `GroupChat` sample

**Files:**
- Create: `tests/MqttWire.Tests/Server/SharedTopicTests.cs`; modify `tests/MqttWire.Tests/MqttWire.Tests.csproj` (reference the test host)
- Create: `samples/GroupChat/GroupChat.Server/GroupChat.Server.csproj`, `Program.cs`, `ChatHandler.cs`, `ChatMember.cs`, `GroupTable.cs`
- Create: `samples/GroupChat/GroupChat.Client/GroupChat.Client.csproj`, `Program.cs`, `App.razor`, `Pages/Chat.razor`, `ChatSession.cs`, `wwwroot/index.html`, `wwwroot/app.css`
- Create: `samples/GroupChat/README.md`; modify `MqttWire.slnx`, `Directory.Packages.props`, `README.md`

**Interfaces:**
- Consumes: `ExactTopicHandler` (Task 14) for the test; `MqttWireReconnectingClient` and `MqttWireReconnectOptions` for the client.
- Packages, pinned centrally at the version of the installed .NET 10 SDK's ASP.NET Core: `Microsoft.AspNetCore.Components.WebAssembly` (client), `Microsoft.AspNetCore.Components.WebAssembly.Server` (server, to serve the client). They are referenced by the sample only.
- `GroupChat.Server`: `AddMqttWireServer<ChatHandler>()`, `UseWebSockets()`, `UseBlazorFrameworkFiles()`, `UseStaticFiles()`, `MapMqttWire("/mqtt")`, `MapFallbackToFile("index.html")`. `ChatHandler` is the code of design 6.10: `OnConnectAsync` accepts a user name that is not empty and puts a `ChatMember` (name, the groups it joined) into the state; `OnSubscribe` grants a filter of the form `groups/<name>` with no wildcard and refuses every other with `MqttSubscribeResult.NotAuthorized`; `OnPublish` refuses a topic the sender has not joined (`0x87`), otherwise copies the payload once and sends it to every member of the group, the sender included, at the lower of the publish's QoS and the member's granted QoS; `OnUnsubscribe` and `OnClosed` clean `GroupTable`. `GroupTable` is a `Dictionary<string, List<(MqttWireConnection, MqttQoS)>>` under one lock; `OnPublish` looks a group up with `publish.GetTopic()`, one string per publish, which a sample can afford and its comment says.
- `GroupChat.Client` (Blazor WebAssembly): one page with a name, a group, a message box and the list of messages. `ChatSession` wraps one `MqttWireReconnectingClient`: `ClientOptions` builds the options from the page's own origin (`ws` or `wss`, path `/mqtt`), the name as user name and a client id of the name plus four random characters; `OnConnected` and `OnDisconnected` drive a "connected / reconnecting" line; joining is `SubscribeAsync("groups/<name>", MqttQoS.AtLeastOnce)`; sending is `PublishAsync` at QoS 1 with a UTF-8 payload of `name: text`; `OnMessage` copies the payload into a string and adds it to the list. The client's code says in a comment that the browser supports no request headers and no keep-alive option (design 6.3).

- [ ] **Step 1: Write the failing test** in `SharedTopicTests.cs` (design 6.13 item 8, over in-memory links)

```csharp
[Fact] Publish_TenConnectionsOnOneTopic_ReachesAll()
// ten connections subscribe to "g/1" at QoS 1 through ExactTopicHandler; the first publishes "hi" at QoS 1:
// each of the ten links has one PUBLISH with the payload; the publisher also has its PUBACK
[Fact] Publish_OneMemberStopsReading_TheOthersKeepReceivingAndItIsDisconnected()
// member 10's link holds its writes. 2,000 publishes of 1 KiB at QoS 0: after every publish the other nine have received it
// (no clock advance, so nothing waited for member 10); member 10 is evicted (OnClosed OutgoingLimitPassed) by the publish
// that would make 1,001 wait, and the nine go on receiving after that
[Fact] Publish_LowerOfTheTwoQoS_IsUsed()                                    // a member granted QoS 0 gets a QoS 2 publish at QoS 0
[Fact] Closed_Member_IsRemovedFromTheTable()                                // a later publish is not sent to it; no exception
```

- [ ] **Step 2: Run it to see it fail, implement what is missing in `ExactTopicHandler`, run it to see it pass.** Run: `dotnet test --filter "FullyQualifiedName~SharedTopicTests"`.

- [ ] **Step 3: Build the sample.** `dotnet build -c Release` must stay at 0 warnings with both sample projects in the solution; every type and member of the sample is documented, Razor code blocks included.

- [ ] **Step 4: Run the sample once in the browser by hand** (design 6.13 item 9). Run: `dotnet run --project samples/GroupChat/GroupChat.Server`, open it in two browser tabs.
  1. Two names join the group `demo`; a message from each appears in both tabs.
  2. Stop the server: both tabs show "reconnecting". Start it again: both show "connected" without a reload, and a new message appears in both, which shows the subscription was made again.
  3. The browser's console shows no error.
  Write the date, the browser and its version, and the three results into `samples/GroupChat/README.md` under "Checked in a browser", and into the pull request.

- [ ] **Step 5: Add the sample to the README** (design 6.15: "the group chat sample") and commit

```bash
git add MqttWire.slnx Directory.Packages.props README.md samples tests/MqttWire.Tests
git commit -m "Add the shared-topics test and the GroupChat sample."
```

### Task 18: The idle-memory benchmark

**Files:**
- Create: `bench/MqttWire.Bench/MqttWire.Bench.csproj`, `Program.cs`, `ServerRole.cs`, `RunRole.cs`, `IdleClients.cs`, `Figures.cs`, `NothingHandler.cs`
- Create: `docs/bench/2026-11-idle-memory.md`
- Modify: `MqttWire.slnx`, `src/MqttWire.Server/MqttWireServer.cs` (`internal long LastSweepTicks`)

**Interfaces:**
- Produces a console program, `dotnet run -c Release --project bench/MqttWire.Bench -- <role> [options]`:
  - `server --port <p> [--tls]`: Kestrel on 127.0.0.1 with two endpoints. `/floor` accepts a WebSocket with sub-protocol `mqtt` and keep-alive off, and waits in a zero-byte `ReceiveAsync` until the socket ends (design 6.14 item 1). `/mqtt` is `MapMqttWire` with `NothingHandler`, which accepts everything, returns a null state and does nothing. `GET /measure` forces two full blocking collections with finalizers run between them and returns JSON: `liveManagedBytes` (`GC.GetTotalMemory(true)`), `workingSetBytes`, `connections` (`MqttWireServer.ConnectionCount`), `lastSweepMicroseconds`. `--tls` uses a self-signed certificate made in memory.
  - `run [--small 3000] [--large 12000] [--tls]`: for each target (`floor`, `mqttwire`) and each count, starts a fresh `server` process, opens the connections from this process, and measures through `/measure`. It prints the table below and the same as Markdown.
  - `IdleClients`: opens *n* `ClientWebSocket`s (sub-protocol `mqtt`, keep-alive off) in batches of 200; for `mqttwire` each sends a CONNECT with client id `b<i>` and keep-alive 0 and reads its CONNACK. `ActivityAsync`: every connection publishes one message at QoS 0 and one at QoS 2 (the full exchange), and `POST /burst?count=<n>` on the server sends every connection ten messages of 256 bytes at QoS 1, which each client acknowledges; it finds the connections with `TryGetConnection("b<i>")`, so the handler keeps no table that would be measured. `ChurnAsync`: closes all, waits until `connections` is 0, opens *n* again.
- The states measured, each at both counts:

  | State | What |
  |---|---|
  | floor | idle on `/floor` |
  | 1 idle | connected on `/mqtt`, nothing else |
  | 2 after activity | `ActivityAsync`, then quiet for two sweeps |
  | 3 after churn | `ChurnAsync`, then quiet for two sweeps |
- `Figures`: "per connection" is the slope, `(bytes at the large count − bytes at the small count) ÷ (large − small)`; "fixed" is the line's value at no connections. The report gives, per state: live managed bytes per connection (**the figure that decides**), fixed bytes, working set per connection (for information). Then: **pool retention**, the fixed part after activity minus the fixed part idle, with the statement whether the slope after activity equals the idle slope within 16 bytes; the time of one sweep at the large count; and one run with `--tls`, reported and not decisive.
- **The target:** `1 idle`, `2 after activity` and `3 after churn` each cost at most **the floor + 1,024 bytes** per connection.

- [ ] **Step 1: Write the program.** No test project: the program's own check is step 2.

- [ ] **Step 2: Check that it measures what it says.** Run: `dotnet run -c Release --project bench/MqttWire.Bench -- run --small 300 --large 1200`. Expected: it ends in under two minutes; the floor is between 11,000 and 13,000 bytes per connection (step 0 measured about 11,900); `connections` matched every count; no server process is left running.

- [ ] **Step 3: Run it in full, three times.** Run: `dotnet run -c Release --project bench/MqttWire.Bench -- run`, then once with `--tls`.

- [ ] **Step 4: Write `docs/bench/2026-11-idle-memory.md`:** the machine and the .NET version, the three runs' tables, the figures above, and one line saying whether the target is met in each of the three states. **If it is not met, stop here and take the result to the owner** before anything else (the "Done when" of #6); a heap dump of the server in state 1, attributed by type as in GrainWire's step 0 report, goes with it.

- [ ] **Step 5: Commit**

```bash
git add MqttWire.slnx bench/MqttWire.Bench docs/bench src/MqttWire.Server
git commit -m "Add the idle-memory benchmark and its first report."
```

### Task 19: The roadmap, and the second pull request

**Files:**
- Modify: `docs/roadmap.md`, `docs/specs/2026-11-19-mqttwire-design.md`, `README.md`

- [ ] **Step 1: Update the documents.** In `docs/roadmap.md`: step 3 is done, with this plan, #6 and both pull requests linked; step 4 (#7) is next. In the design: section 12 says what has now been run and measured, with a link to the benchmark's report; 6.14 gets the measured floor and the measured cost. In the README: the measured idle cost, in one sentence, with the link.

- [ ] **Step 2: Verify everything #6 calls done.**

```bash
dotnet build -c Release          # 0 warnings
dotnet test -c Release           # all green
cd tests/interop-js && npm ci && npm test
```
Then go through "Done when" of #6 line by line and write beside each the test class or file that shows it.

- [ ] **Step 3: Commit and open the pull request**

```bash
git add docs README.md
git commit -m "Mark step 3 of the roadmap done and record what was measured."
git push -u origin feature/6-mqttjs-sample-and-benchmark
gh pr create --title "#6 - Step 3: the MQTT.js interop, the sample and the benchmark." --body "<what was built, the browser check, the benchmark's three figures, 'Closes #6'>"
```

---

## What the plan leaves to the builder

- The private members of `MqttWireConnection` beyond those named in an Interfaces block, and how its fields are packed. The idle-cost test of Task 5 and the benchmark of Task 18 judge the result.
- Whether the drain hands over to the thread pool after a number of writes completed at once on a caller's thread. The stress test must pass either way.
- The exact wording of log messages and exception messages.
- The sample's markup and styling.
