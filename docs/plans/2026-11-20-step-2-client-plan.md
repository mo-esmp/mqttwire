# Step 2: the client and the reconnecting client — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `MqttWireClient`, a general MQTT 5 client over WebSockets with one connection per instance and QoS 0, 1 and 2 in both directions, and `MqttWireReconnectingClient`, the opt-in wrapper that keeps a connection up. Prove both over an in-memory link and against MQTTnet's broker, and put a first package in a local feed for GrainWire's step 3.

**Architecture:** The client sits on the internal link of design 6.5 (`IMqttLink`, five operations), which has a `WebSocket` implementation and an in-memory test double. `MqttWireClient` holds one link, one read loop that frames packets with step 1's `PacketHeader` and dispatches them to the codec, one send lock that writes packets in the order they were produced, a table of outgoing records keyed by packet identifier, and one keep-alive timer. Request outcomes (publish, subscribe, unsubscribe) are results, not exceptions, completed through pooled `IValueTaskSource`s. The reconnecting client wraps a new `MqttWireClient` per attempt and owns back-off, re-subscribing and stopping.

**Tech Stack:** .NET 10, C# 14, `System.Net.WebSockets.ClientWebSocket`, `TimeProvider`, xUnit 2.9.3 + Shouldly 4.3.0, MQTTnet 5.2.0.1603 (tests only), MQTTnet.Server and MQTTnet.AspNetCore 5.2.0.1603 (interop tests only), Microsoft.Extensions.TimeProvider.Testing (tests only), BenchmarkDotNet 0.15.8 (`bench/` only).

**Spec:** [docs/specs/2026-11-19-mqttwire-design.md](../specs/2026-11-19-mqttwire-design.md), sections 6.3, 6.5, 6.7 (CONNACK read by the client), 6.8, 6.9, 6.13 (items 4 to 7) and 7 (step 2). Step 2 of section 7 in [GrainWire on MqttWire](../specs/2026-11-19-grainwire-on-mqttwire-design.md), whose section 5.6 is what GrainWire's step 3 needs from this client. Tracked in mo-esmp/mqttwire#3.

## Global Constraints

- .NET 10 (`net10.0` only), C# 14, warnings are errors. `AGENTS.md` and `.editorconfig` are binding: no `var`, target-typed `new`, file-scoped namespaces matching the folder, `s_` for private static fields, braces always, XML documentation in the multi-line form on every type and member in `src`, `tests` and `bench`.
- `MqttWire` keeps **no package dependencies**. Test-only packages are pinned in `Directory.Packages.props`.
- **The client must work in Blazor WebAssembly** (design section 4): no blocking waits, no thread of its own, no `.Result`, `.Wait()` or `Thread.Sleep`. A short `lock` around in-memory state is fine; it is never held across an await.
- `ConfigureAwait(false)` on every await in `src`. `ValueTask` on the read and write paths. `CancellationToken` last.
- **The read loop and the write path allocate nothing in steady state** (AGENTS "Hot Paths"): no LINQ, closures, boxing, string building or `params`. Buffers come from an `ArrayPool<byte>` the client is given (the shared pool by default).
- **Malformed input from the server is an error result and the end of the connection, never an exception to the caller.** Exceptions are only for caller bugs: invalid options, an invalid topic, calling `ConnectAsync` twice.
- **Nothing of a broken connection survives into the next one** (6.3): no re-sending, no stored session, a new instance per connection.
- Public types live in the `MqttWire` namespace (beside `MqttQoS`); the link in `MqttWire.Link`; the client's internals in `MqttWire.Client`.
- Tests: `MethodName_StateUnderTest_ExpectedBehavior`, `// Arrange`, `// Act`, `// Assert` in every test. Unit tests in `tests/MqttWire.Tests` use the in-memory link only: no network, no sockets, no broker. They never wait in real time: every delay goes through a `TimeProvider` the tests replace.
- Every commit: `dotnet build -c Release` with 0 warnings and `dotnet test` green. Commit messages are one imperative sentence ending with a period, with no co-author trailer.

## Decisions this plan makes

The design leaves these open or is silent on them. Each is the plan's ruling; the owner can overturn any of them at review. Decisions 1 and 2 change design text, which Task 12 updates in the same pull request (AGENTS "Build & Docs").

1. **The client announces a Maximum Packet Size** in CONNECT, from a new option `MaxIncomingPacketBytes` (default 1 MiB, at least 4,096, at most 268,435,460). Design 6.9 says "no maximum packet size", which leaves a client accepting up to 256 MiB from a server it cannot bound. A conformant server then never sends more; a packet over it is refused as soon as its header is read, before it is buffered (the step 1 hand-over), with DISCONNECT `0x95`. The codec gains `ConnectFields.MaximumPacketSize` (`uint?`), written after Receive Maximum.
2. **Request outcomes are a result value.** `PublishAsync`, `SubscribeAsync` and `UnsubscribeAsync` return `ValueTask<MqttRequestResult>`, which says whether the exchange completed (with the server's reason code), failed at once without sending (not connected, receive maximum reached, packet too large, QoS not supported, no free identifier), or ended with the connection ("may or may not have arrived"). Design 6.3 shows `MqttQoS granted = await client.SubscribeAsync(…)`; a bare QoS cannot say "refused" or "connection lost", which the reconnecting client needs. `MqttRequestResult.GrantedQoS` gives the QoS.
3. **`ConnectAsync` returns `MqttConnectAck` for every outcome that is not a caller bug or a cancellation**: connected; refused by the server (its CONNACK reason code); or no valid CONNACK (unreachable, timed out, malformed, protocol error), with `Failure` saying which. Only an exception from `ConfigureWebSocket`, invalid use, or the caller's own cancellation throws. `OnClosed` is called only for a connection that `ConnectAsync` reported as connected.
4. **`Password` is a `string?`, sent as UTF-8**, as design 6.3's `Password = token` implies. Binary passwords wait until someone needs them.
5. **`PublishAsync` encodes the whole packet into a rented buffer before it returns**, so the caller may reuse its payload at once, and the packet keeps its place in the order of calls. It takes no `CancellationToken` (it never waits before handing over) and no retain flag (YAGNI). `SubscribeAsync` and `UnsubscribeAsync` take a token that cancels only the wait for the acknowledgement; the identifier stays in use until the acknowledgement or the end of the connection.
6. **Completions run their continuations asynchronously**, so application code never runs on the read loop. The completion sources are pooled: one returns to the pool in `GetResult`; one that is never awaited (GrainWire does not await) is simply dropped to the garbage collector, so a late `await` can never observe a recycled source.
7. **An `OnMessage` that throws ends the connection** with DISCONNECT `0x83` and reason `MessageHandlerThrew`, as a throwing handler does on the server (design 10.9). An exception from `OnClosed` is swallowed.
8. **Keep-alive uses one timer that ticks every half interval.** On a tick: a PINGREQ if nothing was sent for at least half the interval (so the interval between packets never exceeds Keep Alive, as MQTT requires); the end if a PINGREQ has waited a full interval; the end if the oldest record is older than twice the interval (6.9). The interval is the server's Server Keep Alive when CONNACK has one.
9. **What the client sends when it ends a connection.** For a fault it found in the server's packets it sends DISCONNECT with the code (`0x81`, `0x82`, `0x93`, `0x94`, `0x95`), then closes the link with 1000. On `DisposeAsync` it sends DISCONNECT `0x00`. Each close is bounded by an internal close timeout (2 s), then the link is aborted. Timeouts and failed reads or writes abort the link without a DISCONNECT.
10. **Internal constants:** receive maximum 1,024 (design 10.8), connect timeout 10 s, close timeout 2 s, first read buffer 4 KiB, at most 1,024 remembered incoming QoS 2 identifiers.
11. **`TimeProvider` for every delay and timestamp**, injected through internal constructors. The tests use `FakeTimeProvider` from `Microsoft.Extensions.TimeProvider.Testing` (tests only).
12. **The interop tests are a separate project, `tests/MqttWire.Interop.Tests`,** hosting MQTTnet's broker with `MQTTnet.Server` and `MQTTnet.AspNetCore` 5.2.0.1603 in Kestrel on 127.0.0.1. Keeping them out of `MqttWire.Tests` keeps AGENTS' "unit tests use no network" literally true. CI runs both.
13. **The reconnecting client has no logger**, because `MqttWire` takes no dependency on `Microsoft.Extensions.Logging`. A throwing callback is reported to `OnCallbackError` (an `Action<Exception>?`) and the wrapper carries on. An exception from the `ClientOptions` callback or from creating the client counts as a failed attempt and is retried. Back-off: attempt *n* waits `min(MaxDelay, InitialDelay × 2^(n−1))` times a random factor in [0.8, 1.2], capped at `MaxDelay`. It stops for good on a CONNACK refusal with `0x84`, `0x85`, `0x86`, `0x87` or `0x8A`. `StartAsync` returns the `MqttConnectAck` of the first connection or of the refusal that stopped it; cancelling its token stops the wrapper.
14. **The local package is `0.1.0-dev.N`**, packed into `artifacts/packages/` (already ignored by git), so that each re-pack gets a new version and GrainWire never restores a stale copy from the NuGet cache. Step 2 ships `0.1.0-dev.1`.
15. **Chapter 4 of the standard is still unread.** The OASIS HTML page is cut off for automated readers before section 4.2. Task 5 begins by reading sections 4.3, 4.4, 4.6, 4.9 and 4.13 in the PDF or DOCX. What chapter 3 already confirms: a sender counts a QoS 1 or 2 PUBLISH against Receive Maximum until PUBACK, PUBCOMP or a PUBREC of `0x80` or more [MQTT-3.3.4-7]; packets other than PUBLISH are never delayed by it [MQTT-3.3.4-8]; a receiver given more than its Receive Maximum disconnects with `0x93`; a Topic Alias above the receiver's maximum is `0x94`.

Not in this step: the server (step 3), `docs/mqtt-support.md` (with the preview package), WebSocket framing of our own, a .NET Standard build.

## Review Focus

The five inputs most likely to bite a user that the design implies but its test list does not name. Each has a test in the task that owns the code.

1. **PUBREL for an identifier the client does not remember** is answered with PUBCOMP `0x92`, and after a PUBREL a PUBLISH that reuses the identifier is handed on again. Tests in Task 6.
2. **A server that passes the client's limits:** more than 1,024 unreleased QoS 2 PUBLISHes ends with DISCONNECT `0x93`; a PUBLISH with a Topic Alias ends with `0x94`; a packet over `MaxIncomingPacketBytes` ends with `0x95` with no buffer of that size rented. Tests in Tasks 3 and 6.
3. **The connection ending while requests are in flight from many threads:** every task completes exactly once (completed or `ConnectionLost`), `OnClosed` comes once and after the last `OnMessage`, and the pool balances. Tests in Tasks 8 and 9.
4. **Re-entrant callbacks:** `OnMessage` calling `PublishAsync` at QoS 1 (the read loop must not wait on its own acknowledgement), and `OnClosed` calling `DisposeAsync` (no deadlock, still one `OnClosed`). Tests in Tasks 6 and 8.
5. **A topic the caller cannot send:** an empty topic, one with `+`, `#` or U+0000, one with an unpaired surrogate, one over 65,535 UTF-8 bytes. Expected: `ArgumentException`, nothing written, no identifier used. Tests in Task 5.

---

## File structure

```
src/MqttWire/
  MqttWireClient.cs                 public: the client
  MqttWireClientOptions.cs          public: its options and their checks
  MqttMessageHandler.cs             public delegate: void (in MqttPublish message)
  MqttPublish.cs                    public readonly ref struct: a received message, a view
  MqttConnectAck.cs                 public: what ConnectAsync reports
  MqttRequestResult.cs              public readonly record struct + MqttRequestStatus enum (two files)
  MqttRequestStatus.cs
  MqttClientCloseReason.cs          public enum: why a connection ended
  MqttClientCloseInfo.cs            public readonly record struct: reason, code, string, exception
  MqttWireReconnectingClient.cs     public: the opt-in wrapper
  MqttWireReconnectOptions.cs       public: its options
  MqttConnectedInfo.cs              public readonly record struct: IsReconnect, Ack
  Link/
    IMqttLink.cs  WebSocketLink.cs  IMqttLinkConnector.cs  LinkConnectResult.cs  ClientWebSocketConnector.cs
  Client/
    ClientLimits.cs                 the internal constants of decision 10
    OutgoingRecords.cs  OutgoingRecord.cs  RecordKind.cs
    IncomingQoS2.cs
    RequestCompletion.cs            pooled IValueTaskSource<MqttRequestResult>
    ReceiveBuffer.cs                rented, grows to one packet, empty when idle
    ReconnectBackoff.cs
  Codec/ConnectCodec.cs, Codec/ConnectFields.cs   modified: Maximum Packet Size
tests/MqttWire.Tests/
  Link/      MemoryLink.cs  MemoryLinkConnector.cs  WebSocketPair.cs  WebSocketLinkTests.cs  MemoryLinkTests.cs
  Client/    ClientHarness.cs  ClientPackets.cs  CountingPool.cs  OverwritingPool.cs
             MqttWireClientOptionsTests.cs  ConnectTests.cs  OutgoingRecordsTests.cs  PublishTests.cs
             ReceiveTests.cs  SubscribeTests.cs  KeepAliveTests.cs  CloseTests.cs  ClientBufferTests.cs
             ClientAllocationTests.cs  ReconnectBackoffTests.cs  ReconnectingClientTests.cs
tests/MqttWire.Interop.Tests/
  MqttWire.Interop.Tests.csproj  MqttNetBroker.cs  ClientInteropTests.cs
bench/MqttWire.Microbench/
  LoopbackLink.cs  ClientBenchmarks.cs
```

---

### Task 1: The link

**Files:**
- Create: `src/MqttWire/Link/IMqttLink.cs`, `src/MqttWire/Link/WebSocketLink.cs`
- Create: `tests/MqttWire.Tests/Link/WebSocketPair.cs`, `tests/MqttWire.Tests/Link/WebSocketLinkTests.cs`, `tests/MqttWire.Tests/Link/MemoryLink.cs`, `tests/MqttWire.Tests/Link/MemoryLinkTests.cs`

**Interfaces:**
- Produces:
  ```csharp
  namespace MqttWire.Link;
  internal interface IMqttLink
  {
      ValueTask WaitForDataAsync(CancellationToken cancellationToken);      // completes when a read would not wait; holds no buffer
      ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken); // 0 = the link has ended
      ValueTask WriteAsync(ReadOnlyMemory<byte> packet, CancellationToken cancellationToken); // one binary message
      ValueTask CloseAsync(WebSocketCloseStatus status, CancellationToken cancellationToken);
      void Abort();
  }
  internal sealed class WebSocketLink(WebSocket webSocket) : IMqttLink
  ```
  `ReadAsync` returns 0 when the peer closed, or when a text message arrived, in which case the link has already closed with `WebSocketCloseStatus.InvalidMessageType` (1003). Read and write failures surface as the exceptions `WebSocket` throws; the caller (Task 3) turns them into a reason.
- Test double, `tests/MqttWire.Tests/Link/MemoryLink.cs` (`internal sealed class MemoryLink : IMqttLink`). Pending operations complete **synchronously, on the calling thread** (continuations run inline), so a test drives the client deterministically and allocation tests can measure the current thread. It is safe to write to from several threads at once. In steady state it allocates nothing.
  - `void Deliver(ReadOnlySpan<byte> bytes)` — makes bytes readable; a pending wait or read completes now. `void DeliverInChunks(ReadOnlySpan<byte> bytes, int chunkSize)`.
  - `void EndFromPeer()` — the next read returns 0. `void FailReads(Exception exception)`.
  - `byte[] TakeWritten()` — everything written since the last call, concatenated. `int WriteCount`.
  - `void HoldWrites()` — later writes stay pending; `void CompleteHeldWrites(Exception? exception = null)`; `ReadOnlyMemory<byte> HeldWriteBuffer` — the buffer of the oldest held write.
  - `void FailWrites(Exception exception)`.
  - `WebSocketCloseStatus? ClosedWith`, `bool IsAborted`, `bool IsReadPending`. `Abort()` records the abort and ends a pending wait or read with `OperationCanceledException`, as `WebSocket.Abort` does; held writes stay pending until `CompleteHeldWrites`, so tests can see a buffer outlive an abort (6.8 rule 3).
- `WebSocketPair.Create()` returns `(WebSocket Client, WebSocket Server)` built with `WebSocket.CreateFromStream` over two `System.IO.Pipelines.Pipe`s, sub-protocol `mqtt`, keep-alive off. No sockets.

- [ ] **Step 1: Branch from `main` and commit this plan**

```bash
git switch main && git pull
git switch -c feature/3-client-and-reconnecting-client
git add docs/plans/2026-11-20-step-2-client-plan.md
git commit -m "Add the plan for step 2, the client."
```

- [ ] **Step 2: Write the failing tests** in `WebSocketLinkTests.cs` (each over `WebSocketPair`)

```csharp
[Fact] ReadAsync_OneBinaryMessage_ReturnsItsBytes()                      // server sends "C0 00" → link reads C0 00
[Fact] ReadAsync_TwoPacketsInOneMessage_ReturnsBothAsOneStream()           // "D0 00 D0 00" in one message → 4 bytes, in order
[Fact] ReadAsync_PacketSplitAcrossMessages_ReturnsTheBytesInOrder()        // "32 0A 00" then "03 61 2F 62 00 01 00 68 69" → the 12 bytes
[Fact] ReadAsync_TextMessage_ClosesWith1003AndReturnsZero()                // server sends text "x" → 0; server sees close status 1003
[Fact] ReadAsync_PeerCloses_ReturnsZero()
[Fact] WaitForDataAsync_DataArrives_CompletesWithoutConsumingIt()          // then ReadAsync returns the bytes
[Fact] WriteAsync_Packet_ArrivesAsOneBinaryMessage()                       // server receives MessageType.Binary, EndOfMessage true
[Fact] CloseAsync_NormalClosure_PeerSeesStatus1000()
[Fact] Abort_ReadPending_ReadEndsWithAnException()
```

And in `MemoryLinkTests.cs`, the double's own contract: `Deliver_ReadPending_CompletesTheReadInline`, `HoldWrites_WriteIssued_StaysPendingUntilCompleted`, `Abort_WriteHeld_WriteStaysPendingUntilCompleted`, `DeliverInChunks_SevenBytesChunksOfThree_ReadsThreeThreeOne`.

- [ ] **Step 3: Run them to see them fail**

Run: `dotnet test --filter "FullyQualifiedName~MqttWire.Tests.Link"`
Expected: build errors (`IMqttLink` does not exist).

- [ ] **Step 4: Implement `IMqttLink`, `WebSocketLink` and `MemoryLink`**

`WebSocketLink.WaitForDataAsync` is a zero-byte `ReceiveAsync(Memory<byte>.Empty)` (the server relies on this in step 3 to hold no buffer while idle). `ReadAsync` loops past empty frames; on `WebSocketMessageType.Close` it returns 0; on `Text` it calls `CloseOutputAsync(InvalidMessageType)` and returns 0. `WriteAsync` sends one binary message with `endOfMessage: true`. `MemoryLink` keeps a preallocated ring of incoming bytes and completes pending operations through `ManualResetValueTaskSourceCore` with `RunContinuationsAsynchronously = false`.

- [ ] **Step 5: Run them to see them pass**

Run: the command of step 3. Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/MqttWire/Link tests/MqttWire.Tests/Link
git commit -m "Add the link below the MQTT code, over a WebSocket and in memory."
```

---

### Task 2: The client's public types and option checks

**Files:**
- Create: `src/MqttWire/MqttWireClientOptions.cs`, `MqttMessageHandler.cs`, `MqttPublish.cs`, `MqttConnectAck.cs`, `MqttRequestResult.cs`, `MqttRequestStatus.cs`, `MqttClientCloseReason.cs`, `MqttClientCloseInfo.cs`, `src/MqttWire/Client/ClientLimits.cs`
- Create: `src/MqttWire/MqttWireClient.cs` (its public constructor and the option checks only; Tasks 3 to 8 add the rest)
- Test: `tests/MqttWire.Tests/Client/MqttWireClientOptionsTests.cs`

**Interfaces:**
- Produces:
  ```csharp
  public sealed class MqttWireClientOptions
  {
      public required Uri Uri { get; init; }                         // absolute, ws or wss
      public required string ClientId { get; init; }                 // not empty; at most 65,535 UTF-8 bytes; no U+0000
      public string? UserName { get; init; }
      public string? Password { get; init; }                         // sent as UTF-8 (decision 4)
      public TimeSpan KeepAlive { get; init; } = TimeSpan.FromSeconds(15);   // 1 s to 65,535 s, rounded up to whole seconds
      public int MaxIncomingPacketBytes { get; init; } = 1_048_576;  // 4,096 to 268,435,460 (decision 1)
      public Action<ClientWebSocketOptions>? ConfigureWebSocket { get; init; }
      public MqttMessageHandler? OnMessage { get; init; }
      public Action<MqttClientCloseInfo>? OnClosed { get; init; }
  }
  public delegate void MqttMessageHandler(in MqttPublish message);
  public readonly ref struct MqttPublish   // internal init; also used by the server in step 3
  {
      public ReadOnlySpan<byte> Topic { get; }     // UTF-8
      public ReadOnlySpan<byte> Payload { get; }
      public MqttQoS QoS { get; }
      public bool Retain { get; }
      public string GetTopic();                    // allocates a string
  }
  public sealed class MqttConnectAck
  {
      public bool IsConnected { get; }                                   // Failure is null
      public MqttClientCloseReason? Failure { get; }                     // ConnectRefused, Unreachable, ConnectTimedOut, ProtocolError
      public MqttReasonCode ReasonCode { get; }                          // the CONNACK's; UnspecifiedError when none arrived
      public string? ReasonString { get; }
      public IReadOnlyList<MqttWireUserProperty> UserProperties { get; } // empty when none
      public Exception? Exception { get; }                               // the network error behind Unreachable, if any
  }
  public enum MqttRequestStatus { Completed, NotConnected, ConnectionLost, ReceiveMaximumReached, PacketTooLarge, QoSNotSupported, NoFreePacketIdentifier }
  public readonly record struct MqttRequestResult(MqttRequestStatus Status, MqttReasonCode ReasonCode)
  {
      public bool IsSuccess { get; }     // Status is Completed and ReasonCode < 0x80
      public MqttQoS? GrantedQoS { get; }  // for a subscribe: the QoS when IsSuccess, else null
  }
  public enum MqttClientCloseReason { Disposed, ServerDisconnected, ConnectionLost, WriteFailed, Unreachable, ConnectTimedOut, ConnectRefused, ProtocolError, KeepAliveTimedOut, AcknowledgementTimedOut, MessageHandlerThrew }
  public readonly record struct MqttClientCloseInfo(MqttClientCloseReason Reason, MqttReasonCode ReasonCode, string? ReasonString, Exception? Exception);
  ```
  `ReasonCode` in `MqttClientCloseInfo` is the code of the DISCONNECT received (`ServerDisconnected`) or sent (`ProtocolError` with `0x81`, `0x82`, `0x93`, `0x94` or `0x95`; `MessageHandlerThrew` with `0x83`; `Disposed` with `0x00`), else `Success`.
- `internal static class ClientLimits` (namespace `MqttWire.Client`): `ReceiveMaximum = 1024`, `ConnectTimeout = 10 s`, `CloseTimeout = 2 s`, `FirstReadBufferBytes = 4096`, `MaxIncomingQoS2 = 1024`, `MinIncomingPacketBytes = 4096`, `MaxIncomingPacketBytes = 268_435_460`.
- `public sealed class MqttWireClient : IAsyncDisposable` with `public MqttWireClient(MqttWireClientOptions options)`, which checks the options and throws `ArgumentNullException` / `ArgumentException` / `ArgumentOutOfRangeException` (using the throw helpers) before anything else happens. `internal TimeSpan KeepAlive` exposes the rounded value for tests.

- [ ] **Step 1: Write the failing tests** in `MqttWireClientOptionsTests.cs`

```csharp
[Fact] Constructor_ValidOptions_DoesNotThrow()
[Theory] Constructor_UriNotWebSocket_ThrowsArgumentException()          // "http://x/mqtt", "tcp://x", relative "mqtt"
[Fact] Constructor_EmptyClientId_ThrowsArgumentException()
[Fact] Constructor_ClientIdOver65535Utf8Bytes_ThrowsArgumentException()  // 32,768 × "é" (65,536 bytes)
[Fact] Constructor_ClientIdWithNullCharacter_ThrowsArgumentException()
[Theory] Constructor_KeepAliveOutOfRange_ThrowsArgumentOutOfRangeException()  // 0, 999 ms, 65,536 s, negative
[Theory] Constructor_KeepAliveFraction_RoundsUpToWholeSeconds()          // 1.001 s → 2 s; 15 s → 15 s; 65,534.5 s → 65,535 s
[Theory] Constructor_MaxIncomingPacketBytesOutOfRange_ThrowsArgumentOutOfRangeException()  // 4,095; 268,435,461
[Fact] Constructor_NullOptions_ThrowsArgumentNullException()
[Fact] GrantedQoS_SubscribeGrantedQoS1_IsAtLeastOnce()                    // new(Completed, GrantedQoS1).GrantedQoS == AtLeastOnce
[Fact] IsSuccess_CompletedWithRefusal_IsFalse()                           // new(Completed, NotAuthorized)
```

- [ ] **Step 2: Run them to see them fail**

Run: `dotnet test --filter "FullyQualifiedName~MqttWireClientOptionsTests"`
Expected: build errors.

- [ ] **Step 3: Implement the types and the constructor checks**

- [ ] **Step 4: Run them to see them pass**, then `dotnet build -c Release` (0 warnings).

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire tests/MqttWire.Tests/Client
git commit -m "Add the client's public types and check its options."
```

---

### Task 3: Connecting, the read loop and closing

**Files:**
- Modify: `src/MqttWire/Codec/ConnectFields.cs`, `src/MqttWire/Codec/ConnectCodec.cs` (Maximum Packet Size), `tests/MqttWire.Tests/Codec/ConnectCodecTests.cs`, `tests/MqttWire.Tests/Codec/KnownPackets.cs`
- Create: `src/MqttWire/Link/IMqttLinkConnector.cs`, `LinkConnectResult.cs`, `ClientWebSocketConnector.cs`, `src/MqttWire/Client/ReceiveBuffer.cs`
- Modify: `src/MqttWire/MqttWireClient.cs`
- Create: `tests/MqttWire.Tests/Link/MemoryLinkConnector.cs`, `tests/MqttWire.Tests/Client/ClientHarness.cs`, `tests/MqttWire.Tests/Client/ClientPackets.cs`, `tests/MqttWire.Tests/Client/CountingPool.cs`, `tests/MqttWire.Tests/Client/ConnectTests.cs`, `tests/MqttWire.Tests/Client/CloseTests.cs`
- Modify: `Directory.Packages.props`, `tests/MqttWire.Tests/MqttWire.Tests.csproj` (add `Microsoft.Extensions.TimeProvider.Testing`)

**Interfaces:**
- Consumes: `IMqttLink`, `MemoryLink` (Task 1); the public types and `ClientLimits` (Task 2); `ConnectCodec`, `ConnAckCodec`, `DisconnectCodec`, `PingCodec`, `PacketHeader.TryReadPacket` (step 1).
- Produces:
  - `ConnectFields.MaximumPacketSize` (`uint?`, omitted when null; 0 throws `ArgumentOutOfRangeException`).
  - `internal interface IMqttLinkConnector { ValueTask<LinkConnectResult> ConnectAsync(MqttWireClientOptions options, CancellationToken cancellationToken); }` — an exception from `ConfigureWebSocket` propagates; a network failure is returned. `internal readonly record struct LinkConnectResult(IMqttLink? Link, Exception? Error)`.
  - `ClientWebSocketConnector.Instance`: creates a `ClientWebSocket`, runs `ConfigureWebSocket`, then adds sub-protocol `mqtt` and sets `KeepAliveInterval = TimeSpan.Zero`, connects, and refuses (closes, returns an error) a server that did not answer with `mqtt`.
  - `internal MqttWireClient(MqttWireClientOptions options, MqttMessageHandler? onMessage, Action<MqttClientCloseInfo>? onClosed, IMqttLinkConnector connector, TimeProvider timeProvider, ArrayPool<byte> pool)`; the public constructor passes `options.OnMessage`, `options.OnClosed`, `ClientWebSocketConnector.Instance`, `TimeProvider.System`, `ArrayPool<byte>.Shared`.
  - `public Task<MqttConnectAck> ConnectAsync(CancellationToken cancellationToken = default)` (a second call throws `InvalidOperationException`), `public ValueTask DisposeAsync()`, `public bool IsConnected { get; }`.
  - `internal sealed class ReceiveBuffer(ArrayPool<byte> pool, int maxPacketBytes)`: holds no array while empty; rents `FirstReadBufferBytes` on the first read; grows to a packet's `TotalLength` (rent, copy, return the old one); compacts after each dispatch; returns its array when it holds no bytes. Returns `PacketTooLarge` from its framing call when a header's `TotalLength` exceeds the maximum, before renting for it.
  - Test helpers: `CountingPool : ArrayPool<byte>` (wraps the shared pool; `int Outstanding`; `int LargestRent`; throws on a buffer returned twice or one it never rented); `MemoryLinkConnector` (hands out a new `MemoryLink` per call, or a scripted failure; `IReadOnlyList<MemoryLink> Links`); `ClientPackets` (the client-side known bytes below); `ClientHarness` (`static ClientHarness Create(TimeSpan? keepAlive = null, ArrayPool<byte>? pool = null, MqttMessageHandler? onMessage = null, Action<MqttClientCloseInfo>? onClosed = null)` with `MemoryLink Link`, `FakeTimeProvider Time`, `MqttWireClient Client`, `List<MqttClientCloseInfo> Closed`, `Task<MqttConnectAck> ConnectAsync(byte[] connAck)`, and `byte[] NextPacket()` which frames the next packet out of `Link.TakeWritten()`). The harness options are `ws://test/mqtt`, client id `c1`, user `u`, password `p`, keep-alive 15 s.

Known bytes (`ClientPackets`):

| Name | Bytes |
|---|---|
| `Connect` (harness defaults) | `10 1D 00 04 4D 51 54 54 05 C2 00 0F 08 21 04 00 27 00 10 00 00 00 02 63 31 00 01 75 00 01 70` |
| `ConnAckAccepted` | `20 03 00 00 00` |
| `ConnAckBadUserNameOrPassword` | `20 03 00 86 00` |
| `ConnAckSessionPresent` | `20 03 01 00 00` |
| `ConnAckWithReason` (reason string "x", user property a=b) | `20 0E 00 00 0B 1F 00 01 78 26 00 01 61 00 01 62` |

- [ ] **Step 1: Write the failing codec tests** in `ConnectCodecTests.cs`

```csharp
[Fact] Write_MaximumPacketSize_WritesItAfterReceiveMaximum()   // ClientId "c1", user "u", password "p", keep-alive 15, RM 1024, MPS 1,048,576 → ClientPackets.Connect bytes
[Fact] TryDecode_ConnectWithMaximumPacketSize_ExposesIt()       // Properties.TryGetInteger(MaximumPacketSize) → 1,048,576
[Fact] Write_MaximumPacketSizeZero_ThrowsAndWritesNothing()
[Fact] Write_MaximumPacketSize_MatchesMqttNet()                 // MqttNetFormatter.Decode gives MaximumPacketSize 1,048,576
```

- [ ] **Step 2: Run them to see them fail, implement the field, run them to see them pass.**

Run: `dotnet test --filter "FullyQualifiedName~ConnectCodecTests"`

- [ ] **Step 3: Write the failing client tests** in `ConnectTests.cs` and `CloseTests.cs`

```csharp
// ConnectTests
[Fact] ConnectAsync_ServerAccepts_WritesConnectAndReturnsConnected()       // NextPacket() == ClientPackets.Connect; ack.IsConnected; ack.ReasonCode == Success
[Fact] ConnectAsync_AckWithReasonAndUserProperty_ReturnsThem()             // ReasonString "x"; UserProperties == [("a","b")]
[Fact] ConnectAsync_ServerRefuses_ReturnsRefusalAndClosesTheLink()         // Failure ConnectRefused; ReasonCode BadUserNameOrPassword; Link.ClosedWith not null; no OnClosed
[Fact] ConnectAsync_SessionPresent_ReturnsProtocolErrorAndSendsDisconnect82() // Failure ProtocolError; the client wrote DISCONNECT 0x82
[Fact] ConnectAsync_NoConnAckWithinTenSeconds_ReturnsConnectTimedOut()     // Time.Advance(10 s) → Failure ConnectTimedOut; Link.IsAborted
[Fact] ConnectAsync_ConnectorFails_ReturnsUnreachableWithTheException()
[Fact] ConnectAsync_ConfigureWebSocketThrows_Throws()                      // over ClientWebSocketConnector with a Uri never dialled: the exception propagates
[Fact] ConnectAsync_MalformedConnAck_ReturnsProtocolErrorAndSendsDisconnect81() // "20 02 00 00" (no property length, step 1 decision 7)
[Fact] ConnectAsync_FirstPacketIsNotConnAck_ReturnsProtocolError()         // PINGRESP first
[Fact] ConnectAsync_CalledTwice_ThrowsInvalidOperationException()
[Fact] ConnectAsync_CallerCancels_ThrowsOperationCanceledAndAbortsTheLink()
[Fact] ConnectAsync_ConnAckSplitAcrossReads_Connects()                     // DeliverInChunks(ConnAckWithReason, 1)
// CloseTests
[Fact] DisposeAsync_Connected_SendsDisconnect00ThenClosesWith1000()        // Closed == [Disposed, Success]
[Fact] DisposeAsync_PeerNeverAcceptsTheClose_AbortsAfterTwoSeconds()       // HoldWrites; Time.Advance(2 s) → IsAborted
[Fact] Read_ServerDisconnects_ReportsServerDisconnectedOnce()              // "E0 01 8E" → [ServerDisconnected, SessionTakenOver]
[Fact] Read_PeerEndsTheLink_ReportsConnectionLost()
[Fact] Read_ReadFails_ReportsConnectionLostWithTheException()
[Fact] Read_PacketOverMaxIncomingPacketBytes_SendsDisconnect95BeforeBuffering() // header "30 FF FF 7F" with MaxIncomingPacketBytes 4,096 → [ProtocolError, PacketTooLarge]; CountingPool never rented ≥ 4,097 bytes
[Fact] Read_InvalidRemainingLength_SendsDisconnect81()                     // "30 FF FF FF FF"
[Theory] Read_PacketAClientNeverReceives_SendsDisconnect82()               // CONNECT, SUBSCRIBE, UNSUBSCRIBE, PINGREQ, AUTH "F0 00", a second CONNACK
[Fact] Read_EveryWayOut_ReportsOnClosedExactlyOnce()                       // a DISCONNECT, then the peer ends the link, then DisposeAsync → one entry
[Fact] OnClosed_Throws_IsSwallowed()
```

- [ ] **Step 4: Run them to see them fail**

Run: `dotnet test --filter "FullyQualifiedName~ConnectTests|FullyQualifiedName~CloseTests"`

- [ ] **Step 5: Implement connecting, the read loop and closing**

The read loop starts right after the link opens and is the only reader. Each turn: `WaitForDataAsync`, then `ReadAsync` into the `ReceiveBuffer`, then frame and dispatch every complete packet. Before CONNACK it accepts only CONNACK, and completes `ConnectAsync`; after it, it dispatches by `PacketType` (Tasks 5 to 8 add the cases). The connect deadline is a `CancellationTokenSource(ClientLimits.ConnectTimeout, timeProvider)` linked with the caller's token.

The end of a connection: the first reason recorded wins (`Interlocked.CompareExchange`); whoever records it sends the DISCONNECT of decision 9 where one is due, then closes or aborts the link; the read loop, once it has stopped, completes every in-flight request with `ConnectionLost`, returns its buffer, and then calls `OnClosed` once. So `OnClosed` never overlaps `OnMessage` and always comes last.

- [ ] **Step 6: Run them to see them pass**, then `dotnet build -c Release`.

- [ ] **Step 7: Commit**

```bash
git add Directory.Packages.props src tests
git commit -m "Connect the client, read packets from the server and end the connection once."
```

---

### Task 4: Packet identifiers and outgoing records

**Files:**
- Create: `src/MqttWire/Client/OutgoingRecords.cs`, `OutgoingRecord.cs`, `RecordKind.cs`, `RequestCompletion.cs`
- Test: `tests/MqttWire.Tests/Client/OutgoingRecordsTests.cs`

**Interfaces:**
- Produces (namespace `MqttWire.Client`; not thread-safe, the client guards it with its state lock):
  ```csharp
  internal enum RecordKind : byte { Subscribe, Unsubscribe, PublishQoS1, PublishQoS2AwaitingPubRec, PublishQoS2AwaitingPubComp }
  internal sealed class OutgoingRecord { ushort PacketIdentifier; RecordKind Kind; long PhaseStartedAt; RequestCompletion Completion; }
  internal sealed class OutgoingRecords(int publishLimit)          // publishLimit = min(server Receive Maximum, 1,024)
  {
      bool TryAdd(RecordKind kind, long now, RequestCompletion completion, out OutgoingRecord record, out MqttRequestStatus failure);
          // ReceiveMaximumReached when a publish would pass publishLimit; NoFreePacketIdentifier when all 65,535 are in use
      bool TryGet(ushort packetIdentifier, out OutgoingRecord record);
      void Advance(OutgoingRecord record, RecordKind next, long now);   // PUBREC → AwaitingPubComp
      void Remove(OutgoingRecord record);                              // frees the identifier
      int PublishesInFlight { get; }
      long? OldestPhaseStartedAt { get; }
      void DrainAll(Action<OutgoingRecord> each);                      // used once, at the end of the connection
  }
  internal sealed class RequestCompletion : IValueTaskSource<MqttRequestResult>
  {
      ValueTask<MqttRequestResult> Task { get; }
      void Complete(MqttRequestResult result);  // exactly once
  }
  // on OutgoingRecords: RequestCompletion RentCompletion();  // from this client's pool, `new` when empty
  ```
  Identifiers: the next one after the last given, skipping any in use, wrapping from 65,535 to 1. `RequestCompletion` uses `ManualResetValueTaskSourceCore<MqttRequestResult>` with `RunContinuationsAsynchronously = true` and returns itself to the pool it came from in `GetResult` (decision 6). That pool is guarded by its own lock, because `GetResult` runs on the caller's thread. Records are pooled the same way; the table never allocates per packet once warm.

- [ ] **Step 1: Write the failing tests**

```csharp
[Fact] TryAdd_First_GivesIdentifierOne()
[Fact] TryAdd_AfterRemove_GivesTheNextNotTheFreedOne()                 // 1, 2; remove 1; next is 3
[Fact] TryAdd_At65535_WrapsToOneSkippingThoseInUse()                   // 1 and 2 in use, last given 65,535 → 3
[Fact] TryAdd_PublishLimitReached_FailsWithReceiveMaximumReached()     // publishLimit 2: two QoS 1 → third fails; a Subscribe still succeeds
[Fact] TryAdd_PublishLimitReached_DoesNotUseAnIdentifier()
[Fact] TryAdd_AllIdentifiersInUse_FailsWithNoFreePacketIdentifier()   // 65,535 Subscribe records
[Fact] Advance_PubRecReceived_KeepsCountingAgainstTheLimit()            // a QoS 2 record still counts until removed
[Fact] OldestPhaseStartedAt_AfterAdvance_IsTheAdvanceTime()
[Fact] Remove_Record_FreesTheIdentifierAndTheSlot()
[Fact] Complete_NeverAwaited_DoesNotReturnTheSourceToThePool()
[Fact] Complete_Awaited_ReturnsTheSourceToThePoolAfterGetResult()
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~OutgoingRecordsTests"`

- [ ] **Step 3: Implement `OutgoingRecords`, `OutgoingRecord`, `RecordKind` and `RequestCompletion`**

- [ ] **Step 4: Run them to see them pass.**

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire/Client tests/MqttWire.Tests/Client
git commit -m "Add the client's packet identifiers and outgoing records."
```

---

### Task 5: Publishing at QoS 0, 1 and 2

**Files:**
- Modify: `src/MqttWire/MqttWireClient.cs`
- Test: `tests/MqttWire.Tests/Client/PublishTests.cs`; add the publish bytes to `ClientPackets.cs`

**Interfaces:**
- Consumes: `OutgoingRecords`, `RequestCompletion` (Task 4); the read loop and the end of a connection (Task 3); `PublishCodec`, `AckCodec` (step 1).
- Produces: `public ValueTask<MqttRequestResult> PublishAsync(string topic, ReadOnlyMemory<byte> payload, MqttQoS qos)`.
  - Throws `ArgumentNullException` for a null topic, `ArgumentException` for a topic that is empty, holds `+`, `#` or U+0000, has an unpaired surrogate or is over 65,535 UTF-8 bytes, and `ArgumentOutOfRangeException` for a QoS above 2 — before anything is written or any identifier is used.
  - Fails at once, sending nothing: `NotConnected`; `QoSNotSupported` above the CONNACK's Maximum QoS; `PacketTooLarge` over the CONNACK's Maximum Packet Size; `ReceiveMaximumReached`; `NoFreePacketIdentifier`.
  - QoS 0 completes at once with `Completed, Success`. QoS 1 completes on PUBACK with its reason code. QoS 2 completes on PUBCOMP, or on a PUBREC of `0x80` or more with that code; a PUBREC under `0x80` is answered with PUBREL and the record advances.
  - Acknowledgements: for an identifier not in use, PUBACK and PUBCOMP are ignored and PUBREC is answered with PUBREL `0x92`; an acknowledgement of the wrong kind for a known identifier ends the connection with DISCONNECT `0x82`.
  - Sending: the packet is encoded into a rented buffer with `PublishCodec.Write` inside `PublishAsync` (decision 5); the record is in place before the packet is handed to the link (6.9); writes take the send lock (`SemaphoreSlim(1, 1)`, FIFO) and go out in the order they were produced; the buffer goes back to the pool after the write's task has ended (6.8); the time of the last send is kept for keep-alive. A failed write ends the connection with `WriteFailed`.

Known bytes (`ClientPackets`), topic `a/b`, payload `hi`, identifier 1:

| Name | Bytes |
|---|---|
| `PublishQoS0` | `30 08 00 03 61 2F 62 00 68 69` |
| `PublishQoS1` | `32 0A 00 03 61 2F 62 00 01 00 68 69` |
| `PublishQoS2` | `34 0A 00 03 61 2F 62 00 01 00 68 69` |
| `PubAck1` / `PubRec1` / `PubRel1` / `PubComp1` | `40 02 00 01` / `50 02 00 01` / `62 02 00 01` / `70 02 00 01` |
| `PubRelNotFound7` | `62 03 00 07 92` |
| `ConnAckReceiveMaximum2` | `20 06 00 00 03 21 00 02` |
| `ConnAckMaximumQoS1` | `20 05 00 00 02 24 01` |
| `ConnAckMaximumPacketSize32` | `20 08 00 00 05 27 00 00 00 20` |

- [ ] **Step 1: Read chapter 4 of the standard** (decision 15): sections 4.3, 4.4, 4.6, 4.9 and 4.13 of the PDF or DOCX at docs.oasis-open.org/mqtt/mqtt/v5.0/os/. Check each rule of design 6.9 and this task against it, in particular: when a sender frees an identifier at QoS 1 and 2; what a sender does with a PUBREC for an unknown identifier; the ordering of outgoing PUBLISH and acknowledgements (4.6); what 4.13 asks of a client that receives a malformed packet. Where the standard differs, stop and report to the owner before writing code; once agreed, the difference is fixed in design 6.9 (Task 12).

- [ ] **Step 2: Write the failing tests**

```csharp
[Fact] PublishAsync_QoS0_WritesThePacketAndCompletesAtOnce()             // NextPacket() == PublishQoS0; result (Completed, Success)
[Fact] PublishAsync_QoS1_CompletesOnPubAck()                              // pending until PubAck1 delivered
[Fact] PublishAsync_QoS1Refused_CompletesWithTheServersCode()            // "40 03 00 01 97" → (Completed, QuotaExceeded); IsSuccess false
[Fact] PublishAsync_QoS2_SendsPubRelOnPubRecAndCompletesOnPubComp()       // writes PublishQoS2, then PubRel1; completes on PubComp1
[Fact] PublishAsync_QoS2RefusedAtPubRec_CompletesWithoutPubRel()          // "50 03 00 01 97" → completed; nothing more written
[Fact] PublishAsync_ReceiveMaximumOfTwoReached_FailsAtOnceAndSendsNothing() // ConnAckReceiveMaximum2; third QoS 1 → ReceiveMaximumReached; a QoS 0 still goes out
[Fact] PublishAsync_ReceiveMaximumOver1024_Uses1024()                     // CONNACK RM 65,535: the 1,025th QoS 1 fails at once
[Fact] PublishAsync_AboveMaximumQoS_FailsWithQoSNotSupported()            // ConnAckMaximumQoS1; QoS 2
[Fact] PublishAsync_OverMaximumPacketSize_FailsWithPacketTooLarge()       // ConnAckMaximumPacketSize32; 23-byte payload makes 33 bytes
[Fact] PublishAsync_NotConnected_FailsWithNotConnected()
[Theory] PublishAsync_TopicCannotBeSent_ThrowsAndWritesNothing()          // "", "a/+", "a/#", "a\0b", "a\uD800", 65,536 × "a"; then a valid QoS 1 publish uses identifier 1
[Fact] PublishAsync_PayloadChangedAfterTheCall_SendsTheOriginalBytes()
[Fact] PublishAsync_ThreeCallsWhileAWriteIsHeld_WritesInCallOrder()       // HoldWrites; three QoS 0; CompleteHeldWrites → three packets, in order
[Fact] PublishAsync_ConnectionLostInFlight_CompletesWithConnectionLost()
[Fact] PublishAsync_WriteFails_EndsWithWriteFailed()
[Fact] Read_PubAckForUnknownIdentifier_IsIgnored()
[Fact] Read_PubRecForUnknownIdentifier_AnswersPubRelNotFound()            // "50 02 00 07" → PubRelNotFound7
[Fact] Read_PubAckForQoS2Publish_SendsDisconnect82()
[Fact] Read_PubCompBeforePubRec_SendsDisconnect82()
```

- [ ] **Step 3: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~PublishTests"`

- [ ] **Step 4: Implement `PublishAsync`, the send lock and the acknowledgement cases of the read loop**

- [ ] **Step 5: Run them to see them pass**, then the whole suite.

- [ ] **Step 6: Commit**

```bash
git add src tests
git commit -m "Publish from the client at QoS 0, 1 and 2."
```

---

### Task 6: Receiving at QoS 0, 1 and 2

**Files:**
- Create: `src/MqttWire/Client/IncomingQoS2.cs`
- Modify: `src/MqttWire/MqttWireClient.cs`
- Test: `tests/MqttWire.Tests/Client/ReceiveTests.cs`

**Interfaces:**
- Consumes: the read loop and the send path (Tasks 3, 5); `PublishCodec.TryDecode(packet, PacketSource.Server, …)`.
- Produces: `internal sealed class IncomingQoS2` — the identifiers handed on at QoS 2 and not yet released: `bool TryAdd(ushort id)` (false when already present), `bool Remove(ushort id)`, `int Count`; at most `ClientLimits.MaxIncomingQoS2`; no allocation per message (a fixed array allocated on the first QoS 2 delivery).
  - A PUBLISH is handed to `OnMessage` as an `MqttPublish` whose `Topic` and `Payload` are views into the receive buffer, valid during the call only.
  - QoS 1: handed on, then PUBACK `0x00`. QoS 2: if the identifier is new, handed on and remembered; in both cases answered with PUBREC `0x00`. PUBREL: if remembered, forgotten and answered with PUBCOMP `0x00`; otherwise PUBCOMP `0x92`.
  - A PUBLISH with a Topic Alias ends the connection with DISCONNECT `0x94` (the client announces no alias maximum). A QoS 2 PUBLISH that would make `Count` pass 1,024 ends it with `0x93`. A PUBLISH the codec rejects ends it with the codec's code (`0x81`, `0x82` or `0x90`).
  - An `OnMessage` that throws ends the connection with DISCONNECT `0x83` and `MessageHandlerThrew` carrying the exception (decision 7); the PUBACK or PUBREC for that message is not sent.

Known bytes (server to client): `PublishQoS2Id7` `34 0A 00 03 61 2F 62 00 07 00 68 69`; `PubRec7` `50 02 00 07`; `PubRel7` `62 02 00 07`; `PubComp7` `70 02 00 07`; `PubCompNotFound7` `70 03 00 07 92`; `PublishWithTopicAlias` `30 0B 00 03 61 2F 62 03 23 00 01 68 69`.

- [ ] **Step 1: Write the failing tests**

```csharp
[Fact] Read_PublishQoS0_HandsOnTopicAndPayload()                          // KnownPackets.PublishQoS0 → topic "t", empty payload, QoS 0; nothing written
[Fact] Read_PublishQoS1_HandsOnThenSendsPubAck()                          // KnownPackets.PublishQoS1 → "a/b","hi"; then "40 02 00 0A"
[Fact] Read_PublishQoS2_HandsOnOnceAndSendsPubRec()                       // PublishQoS2Id7 → PubRec7
[Fact] Read_PublishQoS2Repeated_AnswersPubRecWithoutHandingOnAgain()      // twice, second with DUP 1 ("3C …"), and a third with DUP 0 → one OnMessage, three PUBRECs
[Fact] Read_PubRelForRemembered_SendsPubCompAndForgets()                  // then PublishQoS2Id7 again is handed on again
[Fact] Read_PubRelForUnknown_SendsPubCompNotFound()                       // PubRel7 alone → PubCompNotFound7
[Fact] Read_PublishWithTopicAlias_SendsDisconnect94()
[Fact] Read_1025UnreleasedQoS2_SendsDisconnect93()                        // identifiers 1..1,025 without PUBREL
[Fact] Read_PublishSplitAtEveryByte_HandsOnTheSameMessage()               // KnownPackets.PublishTwoByteLength cut at each position 1..n-1
[Fact] Read_SeveralPacketsInOneRead_HandsOnEachInOrder()
[Fact] Read_PayloadInOneBuffer_IsAViewNotACopy()                          // the payload span overlaps the CountingPool's rented array
[Fact] Read_OnMessageThrows_SendsDisconnect83AndNoPubAck()
[Fact] Read_OnMessagePublishesQoS1_DoesNotDeadlock()                      // OnMessage calls PublishAsync(QoS 1) and does not await; the PUBACK for it completes it
[Fact] Read_TopicNotValidUtf8_SendsDisconnect81()                         // "30 04 00 01 C3 00" (a cut two-byte sequence)
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~ReceiveTests"`

- [ ] **Step 3: Implement `IncomingQoS2` and the PUBLISH and PUBREL cases of the read loop**

- [ ] **Step 4: Run them to see them pass.**

- [ ] **Step 5: Commit**

```bash
git add src tests
git commit -m "Receive messages in the client at QoS 0, 1 and 2."
```

---

### Task 7: Subscribing and unsubscribing

**Files:**
- Modify: `src/MqttWire/MqttWireClient.cs`
- Test: `tests/MqttWire.Tests/Client/SubscribeTests.cs`

**Interfaces:**
- Consumes: `OutgoingRecords` (Task 4); the send path (Task 5); `SubscribeCodec.Write`, `SubAckCodec.TryDecode`, `UnsubscribeCodec.Write`, `UnsubAckCodec.TryDecode`, `Topics.IsValidFilter`.
- Produces:
  - `public ValueTask<MqttRequestResult> SubscribeAsync(string topicFilter, MqttQoS qos, CancellationToken cancellationToken = default)` — one filter per SUBSCRIBE; completes on SUBACK with its reason code (`GrantedQoS` gives the QoS).
  - `public ValueTask<MqttRequestResult> UnsubscribeAsync(string topicFilter, CancellationToken cancellationToken = default)` — completes on UNSUBACK.
  - Both throw `ArgumentException` for a filter that `Topics.IsValidFilter` rejects or that cannot be encoded, before anything is written. A SUBACK or UNSUBACK with other than one reason code ends the connection with `0x82`. Cancellation ends the wait with `OperationCanceledException`; the identifier stays in use until the acknowledgement (decision 5).

- [ ] **Step 1: Write the failing tests**

```csharp
[Fact] SubscribeAsync_Granted_WritesSubscribeAndReturnsGrantedQoS()       // "a/+", QoS 1 → KnownPackets.Subscribe; KnownPackets.SubAck → GrantedQoS AtLeastOnce
[Fact] SubscribeAsync_GrantedLess_ReturnsWhatWasGranted()                 // asks 2, SUBACK 0x00 → AtMostOnce
[Fact] SubscribeAsync_Refused_ReturnsTheCode()                            // "90 04 00 01 00 87" → (Completed, NotAuthorized); GrantedQoS null
[Theory] SubscribeAsync_InvalidFilter_ThrowsAndWritesNothing()            // "", "a/#/b", "a+", "a\0"
[Fact] SubscribeAsync_SubAckWithTwoCodes_SendsDisconnect82()
[Fact] SubscribeAsync_Cancelled_KeepsTheIdentifierUntilSubAck()           // cancel; next request gets identifier 2; late SUBACK for 1 is accepted
[Fact] UnsubscribeAsync_Acknowledged_WritesUnsubscribeAndCompletes()      // after one subscribe: KnownPackets.Unsubscribe; KnownPackets.UnsubAck → (Completed, Success)
[Fact] Read_SubAckForAPublishIdentifier_SendsDisconnect82()
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~SubscribeTests"`

- [ ] **Step 3: Implement `SubscribeAsync`, `UnsubscribeAsync` and the SUBACK and UNSUBACK cases**

- [ ] **Step 4: Run them to see them pass.**

- [ ] **Step 5: Commit**

```bash
git add src tests
git commit -m "Subscribe and unsubscribe from the client."
```

---

### Task 8: Keep-alive and the acknowledgement deadline

**Files:**
- Modify: `src/MqttWire/MqttWireClient.cs`
- Test: `tests/MqttWire.Tests/Client/KeepAliveTests.cs`; add to `CloseTests.cs`

**Interfaces:**
- Consumes: `TimeProvider` (Task 3); `OutgoingRecords.OldestPhaseStartedAt` (Task 4); `PingCodec`.
- Produces: one `ITimer` per connected client from `timeProvider.CreateTimer`, period half the keep-alive interval, disposed at the end of the connection (decision 8). The interval is the CONNACK's Server Keep Alive when present (`ConnAckServerKeepAlive5` = `20 06 00 00 03 13 00 05`), else the option.

- [ ] **Step 1: Write the failing tests**

```csharp
[Fact] Tick_NothingSentForHalfTheInterval_SendsPingReq()                  // keep-alive 10 s; Advance(5 s) → "C0 00"
[Fact] Tick_PublishSentRecently_SendsNoPingReq()                          // publish at 4 s; Advance to 5 s → nothing; at 10 s → PINGREQ
[Fact] Tick_PingRespArrives_KeepsTheConnection()
[Fact] Tick_NoPingRespWithinTheInterval_EndsWithKeepAliveTimedOut()       // PINGREQ at 5 s, none back; at 15 s → KeepAliveTimedOut; aborted, no DISCONNECT
[Fact] Tick_ServerKeepAlive_UsesTheServersInterval()                      // option 15 s, CONNACK 5 s → PINGREQ at 2.5 s
[Fact] Tick_OldestRecordOlderThanTwiceTheInterval_EndsWithAcknowledgementTimedOut() // QoS 1 never acknowledged, PINGRESPs keep coming; at 20 s → ended
[Fact] Tick_AfterTheEnd_TimerIsDisposed()                                 // Time has no active timers
// CloseTests
[Fact] DisposeAsync_FromOnClosed_DoesNotDeadlock()                        // OnClosed starts client.DisposeAsync() without waiting; that task completes; OnClosed called once
[Fact] End_ManyThreadsPublishing_EveryRequestCompletesExactlyOnce()       // CountingPool; 8 threads × 200 QoS 1 publishes; acknowledge some, then EndFromPeer; every task Completed or ConnectionLost; no identifier left; OnClosed once and after the last OnMessage; Outstanding 0
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~KeepAliveTests|FullyQualifiedName~CloseTests"`

- [ ] **Step 3: Implement the timer and its three checks**

- [ ] **Step 4: Run them to see them pass.**

- [ ] **Step 5: Commit**

```bash
git add src tests
git commit -m "Keep the client's connection alive and end it when acknowledgements stop."
```

---

### Task 9: Buffers, allocations and benchmarks

**Files:**
- Create: `tests/MqttWire.Tests/Client/OverwritingPool.cs`, `ClientBufferTests.cs`, `ClientAllocationTests.cs`
- Create: `bench/MqttWire.Microbench/LoopbackLink.cs`, `bench/MqttWire.Microbench/ClientBenchmarks.cs`
- Modify: `src/MqttWire` wherever a test finds a leak

**Interfaces:**
- Consumes: `ClientHarness` with a pool, `CountingPool` (Task 3).
- Produces:
  - `OverwritingPool : ArrayPool<byte>` — fills every returned array with `0xEE` and hands it out again on the next `Rent` of a fitting size, so memory used after its return is seen at once.
  - `[MemoryDiagnoser] public class ClientBenchmarks` over `LoopbackLink` (answers PUBACK to every QoS 1 PUBLISH, and plays back a prepared server PUBLISH): `PublishQoS0`, `PublishQoS1AwaitAck`, `ReceiveQoS0`, `ReceiveQoS1`.

- [ ] **Step 1: Write the failing tests**

```csharp
// ClientBufferTests — after every step, not only at the end (6.13 item 5): Outstanding is 0 whenever the client is idle
[Theory] EveryWayOut_ReturnsEveryBufferOnce()                // DISCONNECT from server; peer ends; read fails; write fails; keep-alive timeout; ack timeout; protocol error; OnMessage throws; DisposeAsync; connect timeout; refused CONNACK → Outstanding 0, no double return
[Fact] Idle_AfterAPacket_HoldsNoReadBuffer()                 // after a PUBLISH is dispatched, Outstanding 0
[Fact] Abort_WriteHeld_BufferNotReturnedUntilTheWriteEnds()  // HoldWrites; publish; DisposeAsync aborts; Outstanding 1 until CompleteHeldWrites(exception); then 0
[Fact] Abort_WriteHeld_InFlightBufferIsNeverOverwritten()    // OverwritingPool; the held write's buffer still holds PublishQoS1 bytes after abort and after other rents
[Fact] Abort_ReadPending_ReadBufferNotReusedUntilTheReadEnds()
[Fact] ConnectWorkDisconnect_HundredCycles_NothingAccumulates() // new harness per cycle on one CountingPool; Outstanding 0; GC-collected client count via WeakReference
// ClientAllocationTests — [Collection] not run in parallel; one warm-up pass, then 1,000 iterations; bytes measured with GC.GetAllocatedBytesForCurrentThread on the test thread (MemoryLink runs the loop inline)
[Fact] Receive_PublishQoS0_AllocatesNothing()
[Fact] Receive_PublishQoS1AndSendPubAck_AllocatesNothing()
[Fact] Receive_PublishQoS2Exchange_AllocatesNothing()
[Fact] PublishAsync_QoS0_AllocatesNothing()
[Fact] KeepAlive_PingReqAndPingResp_AllocatesNothing()
```

Each must be seen to fail on a planted fault (skip one `pool.Return`; copy the payload into a new array before `OnMessage`), then the fault is removed.

- [ ] **Step 2: Run them; fix what they find.** Run: `dotnet test --filter "FullyQualifiedName~ClientBufferTests|FullyQualifiedName~ClientAllocationTests"`. Expected after the fixes: all pass.

- [ ] **Step 3: Plant each fault, see its test fail, remove it.**

- [ ] **Step 4: Write the benchmarks and run them once**

Run: `dotnet run -c Release --project bench/MqttWire.Microbench -- --filter "*ClientBenchmarks*" --job short`
Expected: four rows; `ReceiveQoS0`, `ReceiveQoS1` and `PublishQoS0` show `-` allocated. `PublishQoS1AwaitAck` shows what the asynchronous continuation costs; put the table in the pull request description.

- [ ] **Step 5: Commit**

```bash
git add src tests bench
git commit -m "Prove the client returns every buffer and allocates nothing on its hot paths."
```

---

### Task 10: Interop with MQTTnet's broker

**Files:**
- Create: `tests/MqttWire.Interop.Tests/MqttWire.Interop.Tests.csproj`, `MqttNetBroker.cs`, `ClientInteropTests.cs`
- Modify: `Directory.Packages.props` (`MQTTnet.Server`, `MQTTnet.AspNetCore` 5.2.0.1603), `MqttWire.slnx`

**Interfaces:**
- Consumes: the public client only.
- Produces: `internal sealed class MqttNetBroker : IAsyncDisposable` — `static Task<MqttNetBroker> StartAsync(Func<string, string, bool>? validate = null)` hosts MQTTnet's server in Kestrel on `http://127.0.0.1:0` at `/mqtt` (sub-protocol `mqtt`), exposes `Uri WebSocketUri`, and can create MQTTnet clients connected to it. The project is a test project with `<FrameworkReference Include="Microsoft.AspNetCore.App" />` and references `src/MqttWire` and MQTTnet.

- [ ] **Step 1: Write the tests**

```csharp
[Fact] ConnectAsync_MqttNetBroker_Connects()                              // IsConnected; ReasonCode Success
[Fact] ConnectAsync_BadPassword_ReturnsBadUserNameOrPassword()           // validate rejects → Failure ConnectRefused, 0x86
[Theory] Publish_MqttWireToMqttNet_Arrives(MqttQoS qos)                  // 0, 1, 2: MQTTnet client subscribed at that QoS receives "a/b" "hi"; our result IsSuccess
[Theory] Publish_MqttNetToMqttWire_Arrives(MqttQoS qos)                  // 0, 1, 2: our OnMessage sees "a/b" "hi" at that QoS
[Theory] Publish_MqttWireToItself_ArrivesOnce(MqttQoS qos)                // subscribe "a/#" then publish; exactly one OnMessage
[Fact] UnsubscribeAsync_MqttNetBroker_StopsDelivery()
[Fact] DisposeAsync_MqttNetBroker_BrokerSeesNormalDisconnect()
[Fact] KeepAlive_OneSecond_ConnectionStaysUpForThreeSeconds()             // the only test that waits in real time
```

- [ ] **Step 2: Run them.** Run: `dotnet test tests/MqttWire.Interop.Tests`. Expected: all pass. A failure here that the unit tests did not catch gets a unit test of its own in the task that owns the code, before it is fixed.

- [ ] **Step 3: Check CI runs the project** (`dotnet test` at the root runs every test project in `MqttWire.slnx`), then commit.

```bash
git add Directory.Packages.props MqttWire.slnx tests/MqttWire.Interop.Tests
git commit -m "Test the client against MQTTnet's broker at every QoS in both directions."
```

---

### Task 11: The reconnecting client

**Files:**
- Create: `src/MqttWire/MqttWireReconnectingClient.cs`, `MqttWireReconnectOptions.cs`, `MqttConnectedInfo.cs`, `src/MqttWire/Client/ReconnectBackoff.cs`
- Test: `tests/MqttWire.Tests/Client/ReconnectBackoffTests.cs`, `tests/MqttWire.Tests/Client/ReconnectingClientTests.cs`

**Interfaces:**
- Consumes: the internal `MqttWireClient` constructor (Task 3); `MemoryLinkConnector`, `FakeTimeProvider`.
- Produces:
  ```csharp
  public sealed class MqttWireReconnectOptions
  {
      public required Func<CancellationToken, ValueTask<MqttWireClientOptions>> ClientOptions { get; init; } // its OnMessage and OnClosed must be null
      public TimeSpan InitialDelay { get; init; } = TimeSpan.FromMilliseconds(500);  // > 0
      public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(15);            // ≥ InitialDelay
      public Action<MqttConnectedInfo>? OnConnected { get; init; }
      public Action<MqttClientCloseInfo>? OnDisconnected { get; init; }
      public Action<string, MqttRequestResult>? OnSubscriptionRefused { get; init; }
      public Action<MqttConnectAck>? OnStopped { get; init; }
      public MqttMessageHandler? OnMessage { get; init; }
      public Action<Exception>? OnCallbackError { get; init; }
  }
  public readonly record struct MqttConnectedInfo(bool IsReconnect, MqttConnectAck Ack);
  public sealed class MqttWireReconnectingClient(MqttWireReconnectOptions options) : IAsyncDisposable
  {
      public Task<MqttConnectAck> StartAsync(CancellationToken cancellationToken = default);
      public ValueTask<MqttRequestResult> SubscribeAsync(string topicFilter, MqttQoS qos, CancellationToken cancellationToken = default); // remembered when granted
      public ValueTask<MqttRequestResult> UnsubscribeAsync(string topicFilter, CancellationToken cancellationToken = default);          // forgotten
      public ValueTask<MqttRequestResult> PublishAsync(string topic, ReadOnlyMemory<byte> payload, MqttQoS qos);                      // NotConnected while down
      public ValueTask DisposeAsync();
  }
  internal static class ReconnectBackoff { static TimeSpan GetDelay(int attempt, TimeSpan initial, TimeSpan max, double random01); } // decision 13
  ```
  Internal constructor for tests: `(MqttWireReconnectOptions options, IMqttLinkConnector connector, TimeProvider timeProvider, Func<double> random)`; each attempt builds `new MqttWireClient(options, onMessage, onClosed, connector, timeProvider, ArrayPool<byte>.Shared)`.
  - Every attempt: call `ClientOptions`, create a new client, connect; on success re-subscribe everything remembered, in the order it was first subscribed, before `OnConnected`; a refused one is forgotten and reported to `OnSubscriptionRefused`.
  - All callbacks run one at a time and in order, from one internal sequence; every `OnConnected` is followed by exactly one `OnDisconnected` before the next `OnConnected`; nothing is called after `DisposeAsync` returns.

- [ ] **Step 1: Write the failing tests**

```csharp
// ReconnectBackoffTests
[Theory] GetDelay_AttemptN_DoublesFromInitialUpToMax()                    // random 0.5 (factor 1.0): 1 → 500 ms, 2 → 1 s, 3 → 2 s, 6 → 15 s, 50 → 15 s
[Theory] GetDelay_RandomBounds_StaysWithinTwentyPercentAndUnderMax()      // random 0 → ×0.8; random 1 → ×1.2 but ≤ 15 s
// ReconnectingClientTests (FakeTimeProvider; no real waiting)
[Fact] StartAsync_FirstAttemptConnects_CompletesWithTheAck()
[Fact] StartAsync_ServerUnreachable_RetriesUntilConnected()               // two failures: attempts at 0, 500 ms, +1 s
[Fact] ConnectionLost_ReconnectsAtTheDocumentedDelays()                   // EndFromPeer → OnDisconnected; attempts at 500 ms, then 1 s after a failure; success resets to 500 ms
[Fact] EveryAttempt_CallsClientOptionsAndUsesANewClient()                 // connector Links.Count == attempts; options callback count == attempts
[Fact] Reconnect_ResubscribesBeforeOnConnected()                          // SUBSCRIBE for each remembered filter written before OnConnected(IsReconnect: true)
[Fact] Reconnect_SubscriptionRefused_ReportedAndForgotten()               // next reconnect does not send it
[Fact] Unsubscribe_ForgetsTheFilter()
[Theory] ConnAckRefusal_ThatRetryingCannotFix_Stops(byte code)            // 0x84, 0x85, 0x86, 0x87, 0x8A → OnStopped once; no more attempts
[Fact] StartAsync_FirstAttemptBadUserNameOrPassword_ReturnsTheRefusal()
[Theory] ConnAckRefusal_ServerUnavailableOrTimeout_Retries()              // 0x88; connect timeout
[Fact] PublishAsync_WhileDisconnected_FailsAtOnceWithNotConnected()
[Fact] PublishAsync_InFlightWhenLost_CompletesWithConnectionLost()
[Fact] Callbacks_ComeOneAtATimeInOrder()                                  // OnConnected, OnDisconnected, OnConnected …; never two at once
[Fact] Callback_Throws_ReportedToOnCallbackErrorAndCarriesOn()
[Fact] ClientOptions_Throws_CountsAsAFailedAttempt()
[Fact] ClientOptions_SetsOnMessage_CountsAsAFailedAttempt()
[Theory] DisposeAsync_DuringBackoffAttemptOrConnection_NoCallbackAfter()   // three states
[Fact] StartAsync_Cancelled_StopsTheWrapper()
```

- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~Reconnect"`

- [ ] **Step 3: Implement `ReconnectBackoff`, the options checks and `MqttWireReconnectingClient`**

Delays wait on `Task.Delay(delay, timeProvider, cancellationToken)`. Callbacks are serialised by running the wrapper's whole connection cycle as one async loop that is the only caller of `OnConnected`, `OnDisconnected`, `OnSubscriptionRefused` and `OnStopped`; `OnMessage` is the inner client's, which already runs one at a time and before that client's `OnClosed`.

- [ ] **Step 4: Run them to see them pass**, then the whole suite and the Release build.

- [ ] **Step 5: Commit**

```bash
git add src tests
git commit -m "Add the opt-in reconnecting client."
```

---

### Task 12: Documents and the local package

**Files:**
- Modify: `docs/specs/2026-11-19-mqttwire-design.md` (6.3 and 6.9, for decisions 1 and 2, and anything Task 5 step 1 found), `README.md`, `docs/roadmap.md`

- [ ] **Step 1: Update the design** in 6.3 (`SubscribeAsync` and `PublishAsync` return `MqttRequestResult`; `MaxIncomingPacketBytes`; `ConnectAsync` reports every outcome in `MqttConnectAck`) and 6.9 (the client announces Maximum Packet Size; the keep-alive tick of decision 8; PUBREL for an unknown identifier is answered with PUBCOMP `0x92`).

- [ ] **Step 2: Add a "Client" section to `README.md`**: the 6.3 example as it now compiles, what `PublishAsync`'s result means, that `MqttPublish` is valid only during `OnMessage`, the reconnecting client and what it does not do (6.3), and that in Blazor WebAssembly `ConfigureWebSocket` can set only what the browser allows (no request headers, no TLS or proxy settings). Add "Building a local package" with the command of step 3.

- [ ] **Step 3: Pack**

Run: `dotnet pack src/MqttWire -c Release -o artifacts/packages -p:Version=0.1.0-dev.1`
Expected: `artifacts/packages/MqttWire.0.1.0-dev.1.nupkg` and its `.snupkg`; no warnings. Open the package and check it holds `lib/net10.0/MqttWire.dll`, its XML documentation and `README.md`, and no dependency.

- [ ] **Step 4: Update `docs/roadmap.md`**: step 1 done (#1); step 2 tracked in #3, with this plan.

- [ ] **Step 5: Final check**

Run: `dotnet build -c Release` then `dotnet test`
Expected: 0 warnings; every test in both test projects passes.

- [ ] **Step 6: Commit**

```bash
git add docs README.md
git commit -m "Document the client and how to build a local package."
```

---

## Done when

- `dotnet build -c Release` has 0 warnings, and `dotnet test` passes, locally and in CI, in both test projects.
- Every client case of design 6.13 item 6 has a test, including the reconnecting client's, and none of the unit tests waits in real time.
- The buffer tests of 6.13 item 4 and the allocation tests pass for the client, and each has been seen to fail on a planted fault.
- The client talks to MQTTnet's broker at QoS 0, 1 and 2 in both directions.
- The Review Focus cases each have a passing test.
- `MqttWire.0.1.0-dev.1.nupkg` is in `artifacts/packages/`, ready for GrainWire's step 3.
- The design text matches what was built (decisions 1 and 2, and Task 5 step 1's findings).
