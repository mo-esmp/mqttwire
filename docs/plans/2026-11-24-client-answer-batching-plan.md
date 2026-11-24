# The client writes its answers when it runs out of input — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The client's read loop stops waiting for the write of each PUBACK, PUBREC, PUBREL and PUBCOMP. It records them in one pooled buffer and writes them as one WebSocket message just before it would wait for input, or before the sixteenth packet after the oldest one waiting. Nothing new is allocated per message.

**Architecture:** A new internal type, `MqttWire.Client.PendingAnswers`, owns a buffer rented from the client's pool, the encoded answers in arrival order, and the count of packets handled since the oldest answer. The read loop keeps deciding answers as today (`RecordReply` only notes one, also under `_gate`), then encodes each into `PendingAnswers` instead of writing it. It starts the link's wait and its read without awaiting them. If either did not complete at once and answers wait, it writes them through `SendAsync` and then awaits what it started, exactly once. The connection's end drops what waits and returns the buffer in `FinishConnection`.

**Tech Stack:** .NET 10, C# 14, xUnit + Shouldly over the in-memory `MemoryLink`, BenchmarkDotNet 0.15.8 with `[MemoryDiagnoser]`, MQTTnet 5.2.0.1603 (bench and tests only).

**Spec:** [docs/specs/2026-11-24-client-acknowledgement-batching-design.md](../specs/2026-11-24-client-acknowledgement-batching-design.md) (the rule, section 5; tests, section 9; measurement, section 10), amending section 6.9 of [the design](../specs/2026-11-19-mqttwire-design.md). Its [review](../specs/2026-11-24-client-acknowledgement-batching-review.md) holds the failure cases the tests pin.

## Global Constraints

- `AGENTS.md` and `.editorconfig` bind every line: no `var`, target-typed `new`, `s_` statics, braces always, multi-line XML documentation on every type and member in `src`, `tests` and `bench`, `internal` and `sealed` by default.
- **Hot path:** "No allocation, no closure, no `ToArray`; buffers come from the client's pool and go back exactly once, on every way out."
- **Order:** "Answers go out in the order their packets arrived (MQTT 5, section 4.6). That covers PUBRELs too: they follow the order of their PUBRECs."
- **No blocked thread** (Blazor WebAssembly), and **no second thread or work item for writing** (design anti-goal).
- **One write at a time:** every write still goes through `SendAsync` and its send lock.
- **"Nothing is rented or encoded under the client's lock."** `RecordReply` inside `lock (_gate)` (the PUBREL in `HandlePublishAck`) still only notes the answer.
- **The bound is sixteen packets**, counted over packets of every kind and QoS, not over answers and not over time.
- The public API does not change. The receive-maximum rule does not change.
- Every commit: `dotnet build -c Release` with 0 warnings and `dotnet test` green. Commit messages are one imperative sentence ending with a period, **with no co-author trailer**.
- A performance claim needs a BenchmarkDotNet run with `[MemoryDiagnoser]`, before and after. On this VM it also needs the interleaved probe (Task 4).

## Decisions this plan makes

1. **The answer that a packet asks for is still noted in `_reply` by `RecordReply`.** `HandleBuffered` encodes it into `PendingAnswers` once the handler has returned and the client's lock is released. `EndStep.SendReply` becomes `EndStep.RecordAnswer`. A new `EndStep.WriteAnswers` means "the bound is reached: write before the next packet".
2. **Constants in `ClientLimits`:** `AnswerPacketBound = 16`; `MaxAnswerBytes = 5` (fixed header 2, packet identifier 2, reason code 1, no properties); `AnswerBufferBytes = AnswerPacketBound * MaxAnswerBytes` (80). The buffer is rented on the first answer and handed to `SendAsync` (which returns it) on each write, so a quiet connection holds none.
3. **The bound is checked at the top of each turn of `HandleBuffered`,** before framing. A write it causes when no packet follows is the write the loop would make before waiting anyway.
4. **Answers that wait when the connection ends are dropped,** and `FinishConnection` returns their buffer (design section 7, item 2).
5. **A publish that `OnMessage` makes can now go out ahead of answers that still wait.** MQTT orders answers among themselves and publishes among themselves, not the one against the other. Task 5 states this in design 6.9, whose "everything the client writes goes out in the order it was produced" no longer holds word for word.
6. **The benchmarks come first** (Task 1), so the "before" numbers are taken on the unchanged client with the same code that takes the "after" numbers.
7. **The issue and the branch.** The work needs an issue. Task 1 asks the owner for its number, or for a yes to open it with `gh`. The branch is `feature/<issue>-client-answer-batching`.

## Review Focus

The five inputs the design implies but its test list does not name. Each has a test in Task 3.

1. **`OnMessage` calls `PublishAsync` while answers wait.** No deadlock, and the publish completes on its own PUBACK. The waiting PUBACKs still go out in one write before the loop waits, in arrival order (decision 5).
2. **`DisposeAsync` while answers wait.** Nothing but the DISCONNECT is written after the end, and the pool gets every buffer back exactly once.
3. **Packets split across reads while answers wait.** Three QoS 1 PUBLISHes delivered byte by byte: every read completes at once, so the three PUBACKs go out in one write, and each message is handed on once.
4. **A PUBREL arriving in the same run of input as its own QoS 2 PUBLISH** (a fast or non-conforming server). One write carries PUBREC then PUBCOMP, and a later PUBLISH that reuses the identifier is handed on again.
5. **A server Maximum Packet Size between the two answer sizes** (4 bytes). A waiting 4-byte PUBACK, then a 5-byte PUBCOMP `0x92` ends the connection with `0x95`. The PUBACK is not written, and its buffer is returned.

---

## File structure

| File | Change | Responsibility |
|---|---|---|
| `src/MqttWire/Client/PendingAnswers.cs` | create | The pooled buffer of encoded answers and the sixteen-packet count. |
| `src/MqttWire/Client/ClientLimits.cs` | modify | `AnswerPacketBound`, `MaxAnswerBytes`, `AnswerBufferBytes`. |
| `src/MqttWire/MqttWireClient.ReadLoop.cs` | modify | Start the wait and the read, write the answers before awaiting one that pends, `EndStep.RecordAnswer` / `WriteAnswers`, `TryAddAnswer`, `WriteAnswersAsync` (replaces `SendReplyAsync`). |
| `src/MqttWire/MqttWireClient.End.cs` | modify | `FinishConnection` releases `PendingAnswers`. |
| `src/MqttWire/MqttWireClient.cs` | modify | The `_answers` field. |
| `tests/MqttWire.Tests/Client/PendingAnswersTests.cs` | create | The type on its own. |
| `tests/MqttWire.Tests/Client/AnswerBatchingTests.cs` | create | Design section 9 and the Review Focus, over `MemoryLink`. |
| `tests/MqttWire.Tests/Client/ClientAllocationTests.cs` | modify | A burst allocates nothing. |
| `bench/MqttWire.Microbench/ClientComparisonBenchmarks.cs` | modify (already untracked) | Awaiting round trip, burst of 50 blocking and awaiting, writes per burst. |
| `bench/MqttWire.Microbench/CountingLinkConnector.cs` | create | Wraps `ClientWebSocketConnector` and counts `WriteAsync` calls. |
| `docs/specs/2026-11-19-mqttwire-design.md` | modify | Section 6.9: the rule, and the order sentence. |
| `docs/specs/2026-11-24-client-acknowledgement-batching-design.md` | modify | Status. |
| `docs/bench/2026-11-qos1-latency.md` | modify | Cause confirmed, before/after, no temporary links. |

---

### Task 1: Branch, commit the comparison benchmarks, measure "before"

**Files:**
- Create: `bench/MqttWire.Microbench/CountingLinkConnector.cs`
- Modify: `bench/MqttWire.Microbench/ClientComparisonBenchmarks.cs`
- Commit as they are: `bench/MqttWire.Microbench/{BenchPackets,CodecComparisonBenchmarks,MqttNetBrokerHost}.cs`, the working-tree changes to `CodecBenchmarks.cs`, `MqttWire.Microbench.csproj` and `docs/specs/2026-11-19-mqttwire-design.md` (sections 4 and the tree), the batching design and review, and this plan.

**Interfaces:**
- Produces: `CountingLinkConnector : IMqttLinkConnector` with `long Writes { get; }`. The benchmark categories `RoundTripQoS1`, `RoundTripQoS1Awaiting`, `BurstQoS1`, `BurstQoS1Awaiting`, which Task 4 re-runs.

- [ ] **Step 1: Get the issue number.** Ask the owner. If there is none, with their yes run `gh issue create --title "Write the client's answers when the read loop runs out of input" --body "Implements docs/specs/2026-11-24-client-acknowledgement-batching-design.md."`.
- [ ] **Step 2: Branch from an up-to-date `main`, carrying the working tree.** Run `git switch main && git pull --ff-only && git switch -c feature/<issue>-client-answer-batching`. Expected: the untracked bench files and docs listed above are still there; `git status` shows no conflict.
- [ ] **Step 3: Write `CountingLinkConnector`.** It takes an `IMqttLinkConnector` (the bench passes `ClientWebSocketConnector.Instance`). Its `ConnectAsync` wraps the returned link in a private sealed `CountingLink : IMqttLink` that forwards every member and does `Interlocked.Increment` on each `WriteAsync`.
- [ ] **Step 4: Build MqttWire's client in `ClientComparisonBenchmarks.SetupAsync` with the internal constructor** `new MqttWireClient(options, onMessage, onClosed: null, connector, TimeProvider.System, ArrayPool<byte>.Shared)`. Pass the handler as the `onMessage` argument instead of `OnMessage` in the options. Keep the setup's first round trips.
- [ ] **Step 5: Add the benchmarks**, MqttWire as baseline in each category. Each category has the same method for MQTTnet.

| Category | Method (MqttWire / MQTTnet) | One invocation | `OperationsPerInvoke` |
|---|---|---|---|
| `RoundTripQoS1` | existing | one round trip; BDN blocks | 1 |
| `RoundTripQoS1Awaiting` | `RoundTripQoS1AwaitingMqttWireAsync` / `…MqttNetAsync` | `AwaitingRun = 100` round trips, each awaited, in a loop | 100 |
| `BurstQoS1` | `BurstQoS1MqttWireAsync` / `…MqttNetAsync` | `BurstSize = 50` QoS 1 publishes to the own echo topic, started without awaiting into a field array of tasks made in setup, then all 50 awaited, then 50 echo waits | 50 |
| `BurstQoS1Awaiting` | `BurstQoS1AwaitingMqttWireAsync` / `…MqttNetAsync` | `AwaitingBursts = 10` bursts as above, awaited one after another | 500 |

- [ ] **Step 6: Report writes per burst.** Count bursts in a field. At the end of `SetupAsync` keep `connector.Writes`. In `CleanupAsync`, when at least one burst ran, write `Console.WriteLine($"MqttWire writes per burst of {BurstSize}, publishes included: {(writes - setupWrites) / (double)bursts:F1}");`. Expected before the change: about 100.0.
- [ ] **Step 7: Build and run the "before".** Run `dotnet build -c Release` (0 warnings), then `dotnet run -c Release --project bench/MqttWire.Microbench -- --filter "*ClientComparison*" "*ClientBenchmarks.ReceiveQoS1*"`. Copy the summary tables and the writes-per-burst lines into `docs/bench/2026-11-qos1-latency.md` under a heading "Before: the client of `main`" with the commit hash.
- [ ] **Step 8: Commit.**

```bash
git add bench/MqttWire.Microbench docs/specs docs/plans/2026-11-24-client-answer-batching-plan.md docs/bench/2026-11-qos1-latency.md
git commit -m "Add the client comparison benchmarks with MQTTnet, the burst and the awaiting caller, and the plan of the answer batching."
```

---

### Task 2: `PendingAnswers`

**Files:**
- Create: `src/MqttWire/Client/PendingAnswers.cs`
- Modify: `src/MqttWire/Client/ClientLimits.cs`
- Test: `tests/MqttWire.Tests/Client/PendingAnswersTests.cs`

**Interfaces:**
- Consumes: `AckCodec.Write(Span<byte> destination, PacketType type, ushort packetIdentifier, MqttReasonCode reasonCode, string? reasonString)` with a `null` reason string, as `SendReplyAsync` calls it today, and `ArrayPool<byte>`.
- Produces:

```csharp
internal sealed class PendingAnswers(ArrayPool<byte> pool)
{
    public int Count { get; }                                // answers waiting
    public bool MustWriteBeforeNextPacket { get; }           // Count > 0 and AnswerPacketBound - 1 packets counted
    public void Add(PacketType type, ushort packetIdentifier, MqttReasonCode reasonCode);
    public void CountPacket();                               // before each packet is handled; counts only while Count > 0
    public byte[] Take(out int length);                      // hands the buffer over; Count and the packet count go to 0
    public void Release();                                   // returns a held buffer; idempotent
}
```

`ClientLimits`: `public const int AnswerPacketBound = 16;`, `public const int MaxAnswerBytes = 5;`, `public const int AnswerBufferBytes = AnswerPacketBound * MaxAnswerBytes;`.

- [ ] **Step 1: Write the failing tests** (each with a `CountingPool`):
  - `Add_FirstAnswer_RentsOneBufferOfAnswerBufferBytes`: `pool.Outstanding.ShouldBe(1)`, `pool.LargestRent.ShouldBe(80)`. After a second `Add`, it is still `1`.
  - `Take_ThreeAnswers_ReturnsThemEncodedInOrder`: add PUBACK 1, PUBREC 7, PUBCOMP 7 `0x92`. `Take(out length)` bytes `[..length]` equal `Hex.Parse("40 02 00 01 50 02 00 07 70 03 00 07 92")`. `Count.ShouldBe(0)`.
  - `Take_AfterwardsAdd_RentsAFreshBuffer`: return the taken buffer to the pool, add again, and `pool.Outstanding.ShouldBe(1)`.
  - `MustWriteBeforeNextPacket_FifteenPacketsAfterTheOldest_IsTrue`: `Add` once, then `CountPacket()` 14 times and it is `false`; once more and it is `true`.
  - `CountPacket_NothingWaiting_DoesNotCount`: 20 × `CountPacket()`, then `Add`, then 14 × `CountPacket()`, and `MustWriteBeforeNextPacket.ShouldBeFalse()`.
  - `Release_Twice_ReturnsTheBufferOnce`: `pool.Outstanding.ShouldBe(0)`, `pool.FaultyReturns.ShouldBe(0)`. `Release` with nothing held does nothing.
- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~PendingAnswersTests"`. Expected: compile errors, as `PendingAnswers` does not exist.
- [ ] **Step 3: Implement `PendingAnswers` and the constants.** `Add` with `Count == AnswerPacketBound` and `Take` with `Count == 0` are client bugs: `Debug.Assert` them, with no exception path. Document in XML what `Take` hands over (the caller owns the buffer and returns it through `SendAsync`) and when `MustWriteBeforeNextPacket` turns true.
- [ ] **Step 4: Run them to see them pass,** then `dotnet build -c Release`. Expected: 0 warnings.
- [ ] **Step 5: Commit.**

```bash
git add src/MqttWire/Client tests/MqttWire.Tests/Client/PendingAnswersTests.cs
git commit -m "Add the pooled buffer in which the read loop keeps its answers."
```

---

### Task 3: The read loop writes its answers when it runs out of input

**Files:**
- Modify: `src/MqttWire/MqttWireClient.ReadLoop.cs`, `src/MqttWire/MqttWireClient.End.cs`, `src/MqttWire/MqttWireClient.cs`
- Test: `tests/MqttWire.Tests/Client/AnswerBatchingTests.cs` (new), `tests/MqttWire.Tests/Client/ClientAllocationTests.cs`

**Interfaces:**
- Consumes: `PendingAnswers` (Task 2), `SendAsync(byte[], int, SendKind)`, `FitsServerMaximumPacketSize(int)`, `EndOverServerMaximumPacketSize()`, `AckCodec.GetSize(MqttReasonCode reasonCode, string? reasonString)` with `null`.
- Produces, all private: `PendingAnswers _answers` (made in the constructor with the client's pool); `EndStep.RecordAnswer` (renamed from `SendReply`) and `EndStep.WriteAnswers`; `bool TryAddAnswer()`, which encodes `_reply`, or ends over the server's Maximum Packet Size and returns `false`; `ValueTask<bool> WriteAnswersAsync()`, which does `Take` plus `SendAsync(…, SendKind.Other)` and replaces `SendReplyAsync`.

**Test helpers in `AnswerBatchingTests`:**
- `ConnectAsync(MqttMessageHandler? onMessage = null, ArrayPool<byte>? pool = null, byte[]? connAck = null)`, as in `ReceiveTests`.
- `List<byte[]> CaptureWrites(MemoryLink link)` sets `link.Writing = packet => writes.Add(packet.ToArray())` after the CONNECT. The list holds one entry per WebSocket message.
- `PublishQoS1(ushort id)` builds `32 0A 00 03 61 2F 62 00 <id hi> <id lo> 00 68 69`, and `PubAck(ushort id)` builds `40 02 <id hi> <id lo>`.

- [ ] **Step 1: Write the failing tests.** The rule, from design section 9:

```csharp
// Read_PublishQoS1OnQuietConnection_WritesPubAckBeforeWaiting
harness.Link.Deliver(PublishQoS1(1));
writes.ShouldBe([PubAck(1)]);
harness.Link.IsReadPending.ShouldBeTrue();

// Read_PublishQoS1AndOwnPubAckTogether_CompletesPublishWhilePubAckWriteIsHeld
ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
harness.NextPacket();
harness.Link.HoldWrites();
harness.Link.Deliver([.. PublishQoS1(1), .. PubAck(1)]);
(await publish.AsTask().Within("the publish")).Status.ShouldBe(MqttRequestStatus.Completed);
harness.Link.HeldWriteBuffer.ToArray().ShouldBe(PubAck(1));

// Read_WaitCompletesAndReadPends_WritesPubAckWhileReadIsPending, then handles the next packet once
harness.Link.SkipWaits();
harness.Link.Deliver(PublishQoS1(1));
writes.ShouldBe([PubAck(1)]);
harness.Link.PendingReadBuffer.IsEmpty.ShouldBeFalse();
harness.Link.Deliver(KnownPackets.PublishQoS0);
received.Count.ShouldBe(2);
```

  The bound:
  - `Read_SeventeenQoS1Together_WritesSixteenPubAcksThenOne`: `writes.ShouldBe([Concat(PubAck(1)…PubAck(16)), PubAck(17)])`.
  - `Read_QoS1ThenSixteenQoS0Together_WritesBeforeSixteenthQoS0`: one log list. `OnMessage` adds `"m"` and `Writing` adds `"w"`. Deliver `PublishQoS1(1)` and 16 × `KnownPackets.PublishQoS0` as one chunk. Expected: `log.IndexOf("w").ShouldBe(16)`, which is 1 QoS 1 and 15 QoS 0 messages before it, and `log.Count(e => e == "m").ShouldBe(17)`.

  Order across kinds:
  - `Read_ThreeQoS1Together_WritesThreePubAcksInOneWrite`.
  - `Read_NewQoS2AndPubRelOfOldTogether_WritesPubRecThenPubCompInOneWrite`. First deliver `KnownPackets.PublishQoS2` (id 1) alone and expect `[PubRec1]`. Then deliver `PublishQoS2Id7 + PubRel1` and expect one write `PubRec7 + PubComp1`.
  - `Read_TwoPubRecsTogether_WritesBothPubRelsInOneWriteAndEachCompletesOnItsPubComp`. Make two QoS 2 publishes (ids 1 and 2) and deliver `50 02 00 02 50 02 00 01`. Expect one write `62 02 00 02 62 02 00 01`, with neither publish complete. After `70 02 00 02`, publish 2 completes and publish 1 does not. After `PubComp1`, publish 1 completes.

  The ways out:
  - `Read_EndsWithAnswerWaiting_WritesOnlyTheDisconnectAndReturnsEveryBuffer`, a `[Theory]` over `PublishQoS1(1)` followed in the same chunk by: `ClientPackets.PublishTopicNotUtf8` (expect `ClientPackets.DisconnectMalformedPacket`), `ClientPackets.Disconnect` from the server (expect nothing written), and a second `PublishQoS1(2)` whose `OnMessage` throws (expect `DisconnectImplementationSpecific`). Assert `writes.ShouldBe(expected)` (the PUBACK is not written). After the link ends, `pool.Outstanding.ShouldBe(0)` and `pool.FaultyReturns.ShouldBe(0)`.
  - `Read_AnswerWriteFailsWhileReadPends_EndsWithWriteFailedAfterTheReadEnds`. Set `SkipWaits()`, `HoldReadsOnAbort()` and `FailWrites(new IOException())`, then deliver `PublishQoS1(1)`. Expected: `harness.Closed.ShouldBeEmpty()` and `harness.Link.PendingReadBuffer.IsEmpty.ShouldBeFalse()`, because the receive buffer is still lent. After `harness.Link.EndFromPeer()`, `harness.Closed.Single().Reason.ShouldBe(MqttClientCloseReason.WriteFailed)` and `pool.Outstanding.ShouldBe(0)`.
  - `Read_AnswerOverServerMaximumPacketSize_EndsAndReturnsTheWaitingAnswers` (Review Focus 5). The CONNACK is `20 08 00 00 05 27 00 00 00 04`; deliver `PublishQoS1(1) + PubRel7` (unknown, so PUBCOMP `0x92`, 5 bytes). Expected: `Closed.Single()` is `ProtocolError` with `PacketTooLarge`, `writes.ShouldBeEmpty()`, and `pool.Outstanding.ShouldBe(0)`.

  Review Focus:
  - `Read_OnMessagePublishesWhileAnswersWait_PublishesFirstThenWritesPubAcksInOrder`. Deliver `PublishQoS1(1) + PublishQoS1(2)`; each `OnMessage` calls `PublishAsync("x", s_hi, MqttQoS.AtMostOnce)`. Expected: `writes.ShouldBe([pub0, pub0, Concat(PubAck(1), PubAck(2))])`, where `pub0` is the QoS 0 PUBLISH of `x`.
  - `Read_DisposeWhileAnswersWait_WritesOnlyTheDisconnect`. `OnMessage` of `PublishQoS1(1)` starts `harness.Client.DisposeAsync()` and keeps the task. Expected: `writes.ShouldBe([ClientPackets.Disconnect])`, and after the dispose completes, `pool.Outstanding.ShouldBe(0)`.
  - `Read_ThreeQoS1SplitAtEveryByte_WritesThreePubAcksInOneWrite`. Use `DeliverInChunks(bytes, 1)`; expect three messages and `writes.ShouldBe([Concat(PubAck(1), PubAck(2), PubAck(3))])`.
  - `Read_PublishQoS2AndItsPubRelTogether_WritesPubRecThenPubCompAndForgets`. Deliver `PublishQoS2 + PubRel1` and expect one write `PubRec1 + PubComp1`. A later `PublishQoS2` is handed on again.

  Allocation, in `ClientAllocationTests`:
  - `Receive_BurstOfSeventeenQoS1_AllocatesNothing`. Build the 17-publish chunk once in Arrange and deliver it once to warm up. Act: deliver it `Iterations` times. Expected: `AssertNothingAllocated(...)` and `link.WriteCount` up by `2 * Iterations`.
- [ ] **Step 2: Run them to see them fail.** Run: `dotnet test --filter "FullyQualifiedName~AnswerBatchingTests|FullyQualifiedName~ClientAllocationTests"`. Expected: the batching, bound, held-write and ways-out tests fail, because today writes one answer per packet before handling the next. `Read_PublishQoS1OnQuietConnection…` may already pass.
- [ ] **Step 3: Implement.**
  - `HandleBuffered`: at the top of each turn, `if (_answers.MustWriteBeforeNextPacket) { disconnect = MqttReasonCode.Success; return EndStep.WriteAnswers; }`. Call `_answers.CountPacket()` just before a framed packet is handled. When the handler returns `RecordAnswer`, call `TryAddAnswer()` after `Consume` and carry on with the next packet. When it returns `false`, the end has begun, and the existing `_phase >= Phase.Closing` check stops the turn.
  - `ReadAndHandleAsync`: `ValueTask<int> reading = link.ReadAsync(...)`. Then `if (!reading.IsCompleted && _answers.Count > 0) { await WriteAnswersAsync().ConfigureAwait(false); }`, then `int read = await reading.ConfigureAwait(false);`. `EndStep.WriteAnswers` awaits `WriteAnswersAsync()` and continues the loop, where `SendReply` did. Keep `[AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]`.
  - `ReadLoopAsync`: the same shape for `link.WaitForDataAsync(...)`. Await the started `ValueTask` exactly once, whatever `WriteAnswersAsync` returned.
  - `FinishConnection`: `_answers.Release();` beside `_receive.Clear();`. `ReadLoopAsync`'s outer catch (the bug path) also calls it.
  - Update the XML documentation of everything touched: `EndStep`, `HandleBuffered`, `HandlePacket`, the handlers in `Receive.cs` and `Publish.cs` that name `SendReply`, and `_reply`.
- [ ] **Step 4: Run them to see them pass, then the whole suite.** Run: `dotnet test`. Existing tests that expected one write per answer change to the new rule and are not deleted; `NextPacket()` splits a batched write, so most need no change. Expected: all green, including `Receive_PublishQoS1AndSendPubAck_AllocatesNothing` and the receive-maximum tests unchanged.
- [ ] **Step 5: Build.** Run: `dotnet build -c Release`. Expected: 0 warnings.
- [ ] **Step 6: Commit.**

```bash
git add src/MqttWire tests/MqttWire.Tests/Client
git commit -m "Write the client's answers when the read loop runs out of input or every sixteen packets."
```

---

### Task 4: Measure "after"

**Files:** none committed but `docs/bench/2026-11-qos1-latency.md` (in Task 5). The probe lives in the session scratchpad.

- [ ] **Step 1: Re-run BenchmarkDotNet** with Task 1's command on the branch. Expected writes per burst: well under 100, and at least 54 (50 publishes, then 4 writes of answers).
- [ ] **Step 2: Build the interleaved probe.** In the scratchpad, run `git worktree add <scratch>/before <commit of Task 1>` and `dotnet build <scratch>/before/src/MqttWire/MqttWire.csproj -c Release -p:AssemblyName=MqttWire.Before -o <scratch>/before-bin`. Then create a console app that references:
  - `MqttWire.Before.dll` with `<Aliases>before</Aliases>`;
  - the branch's `src/MqttWire` project;
  - MQTTnet 5.2.0.1603;
  - a copy of `MqttNetBrokerHost.cs`.

  It runs the before client, the after client and MQTTnet in rotating order, over 30 rounds of 1,000 operations per scenario: round trip with a blocking caller (`GetAwaiter().GetResult()` per operation), round trip with an awaiting caller, and a burst of 50 with each caller. It prints the median per scenario and client.
- [ ] **Step 3: Run the probe three times.** Expected (design section 10, success): with a blocking caller, the round trip is faster than MQTTnet's and than before; with an awaiting caller, it is no slower than before; a burst costs less per message than before; allocated bytes per operation are not up in the BenchmarkDotNet table. If a criterion fails, stop and report the numbers to the owner before Task 5.
- [ ] **Step 4: Remove the worktree.** Run `git worktree remove <scratch>/before`.

---

### Task 5: The documents, and the pull request

**Files:**
- Modify: `docs/specs/2026-11-19-mqttwire-design.md` (6.9), `docs/specs/2026-11-24-client-acknowledgement-batching-design.md` (status), `docs/bench/2026-11-qos1-latency.md`

- [ ] **Step 1: Design 6.9.** After the "Receive:" bullet, add this bullet:

  > - **Answers** (PUBACK, PUBREC, PUBCOMP, and the PUBREL for the client's own QoS 2 publishes) are recorded in a pooled buffer and written as one WebSocket message when the read loop would wait for the link, its wait for data or its read, or before it handles the sixteenth packet after the one whose answer waits longest. They go out in the order their packets arrived. When the connection ends, answers still waiting are not sent. See [the client writes its answers when it runs out of input](2026-11-24-client-acknowledgement-batching-design.md).

  In the "cannot have the server's deadlock" bullet, replace "everything the client writes goes out in the order it was produced" with "the answers go out in the order their packets arrived, and the publishes in the order they were made; a publish made from `OnMessage` may go out ahead of answers that still wait".
- [ ] **Step 2: The batching design's status line** becomes: `Status: approved by the owner and implemented on <date>; see [the plan](../plans/2026-11-24-client-answer-batching-plan.md).`
- [ ] **Step 3: Rewrite `docs/bench/2026-11-qos1-latency.md`.**
  - The cause is confirmed: the read loop awaited the write of its own PUBACK. Moving that write off the read loop, with the number of writes unchanged, was 13 to 25 µs faster with a blocking caller in four runs of four.
  - Replace the two scratchpad links, and the sentence "the specific cause … remains unconfirmed", with the before and after tables (BenchmarkDotNet, and the probe's medians with how many runs agree) and the writes per burst.
- [ ] **Step 4: Verify.** Run `dotnet build -c Release` (0 warnings) and `dotnet test` (green). `grep -n "AppData\|scratchpad" docs/bench/2026-11-qos1-latency.md` must print nothing.
- [ ] **Step 5: Commit.**

```bash
git add docs
git commit -m "Describe the answer batching in the design and the QoS 1 latency report, with the measurements before and after."
```

- [ ] **Step 6: Push and open the pull request,** after asking the owner. Title: `#<issue> - <issue title>.`; the description says `Closes #<issue>` and gives the before/after medians.
