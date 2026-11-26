# The client's send path: nobody waits for the send lock — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the client's `SemaphoreSlim` send lock with a send path like the server's, so that a send that finds a write in progress allocates nothing and the read loop no longer waits for writes (up to a cap of 4,096 answers waiting).

**Architecture:** A flag (`_writing`) and a FIFO ring of `QueuedWrite` structs, both guarded by a new `Lock _sendGate`, replace `SendAsync`. Whoever finds the path free writes on their own thread for as long as writes complete at once, up to sixteen packets, then hands the path to the thread pool. A write that does not complete at once is carried on by one `SendPathContinuation` object that the client keeps. That object also serves as the thread-pool work item. The read loop hands its answers over and goes on. Past 4,096 answers waiting, it awaits a one-off completion. The DISCONNECT and the "every write has ended" marker each get a one-off completion.

**Tech Stack:** .NET 10, C# 14, xUnit + Shouldly over the in-memory `MemoryLink`, BenchmarkDotNet 0.15.8 with `[MemoryDiagnoser]`, MQTTnet 5.2.0.1603 (bench and tests only).

**Spec:** [docs/specs/2026-11-25-client-send-path-design.md](../specs/2026-11-25-client-send-path-design.md) (design, section 5; tests, section 10; measurement, section 11). It amends section 6.9 of [the design](../specs/2026-11-19-mqttwire-design.md). The server's model is `src/MqttWire.Server/MqttWireConnection.Send.cs` and `src/MqttWire.Server/Internal/PendingWrite.cs`.

## Global Constraints

- `AGENTS.md` and `.editorconfig` apply to every line: no `var`, target-typed `new`, `s_` statics, braces always, and multi-line XML documentation on every type and member in `src`, `tests` and `bench`. Types are `internal` and `sealed` by default.
- **Hot path:** "no allocation, closure, boxing or `ToArray`". What may allocate is "the first use of an object the client then keeps, and the growth of the queue".
- **One write at a time** reaches the link. "A write's buffer is the write's until it has ended" (design 6.8, rules 3 and 4).
- **"Nothing is rented or encoded under a lock."** "No lock is held across a write, a close, an abort or a call into the application."
- **No blocked thread** (Blazor WebAssembly). **No writer of its own**: no thread, long-running loop, or work item for every write.
- **Order:** "the order in which packets reach the link is the order in which they were handed to the send path".
- **Sixteen packets inline**, the caller's own included. **4,096 answers** is the cap.
- The public API does not change. `src/MqttWire/Link`, `src/MqttWire/Codec` and `MqttWire.Server` do not change.
- Every commit: `dotnet build -c Release` with 0 warnings and `dotnet test -c Release` green. Commit messages are one imperative sentence ending with a period, **with no co-author trailer**. Stage only the task's files; leave `bench/MqttWire.Bench/Properties/` and `samples/GroupChat/GroupChat.Client/Properties/` untracked.
- A performance claim needs a BenchmarkDotNet run with `[MemoryDiagnoser]`, before and after, and the interleaved probe (Task 5).

## Decisions this plan makes

1. **Names.** `QueuedWrite` (struct), `SendQueue` (struct ring, inline in the client like `PendingAnswers`), and `SendPathContinuation` (class, the object that waits for a write, and the `IThreadPoolWorkItem`; one class, since it never does both at once). All three live in `src/MqttWire/Client`. The send path's code stays in `MqttWireClient.Send.cs`.
2. **`SendKind` gains two values:** `Answers` (counted against the cap; replaces `Other` for the read loop's writes) and `WritesEnded` (the marker, with no buffer).
3. **The lock is a new `Lock _sendGate`**, not `_gate` (design section 9, Claude's pick).
4. **The one-off completions** are `TaskCompletionSource` objects with `RunContinuationsAsynchronously`, kept in fields: `_disconnectWritten` (`TaskCompletionSource<bool>`), `_writesEnded`, `_answerRoom`. The send path completes them outside `_sendGate`.
5. **The end releases the cap's wait.** `TryBeginEnd`, once it has recorded the end, completes `_answerRoom`. A wait for room cannot be ended by `FinishConnection`, because the read loop that runs it is the one waiting.
6. **`SendQueue`'s first capacity is 8**, and it doubles when full.
7. **The client is never a public `IThreadPoolWorkItem`.** Implementing the interface on the public `MqttWireClient` would put it in the public API, so `SendPathContinuation` is the work item.

## Review Focus

Five conditions the design implies but its test list doesn't name. Each has a test in the task named.

1. **`OnMessage` publishes while the path is held by the read loop's own answers.** The publish is queued behind those answers; there is no deadlock, and it completes on its PUBACK (Task 4).
2. **`DisposeAsync` while the read loop waits at the cap.** The end releases the wait, the read loop finishes, and `DisposeAsync` returns (Task 4).
3. **A write that fails while the read loop waits at the cap.** The queued answers are dropped and counted down, the wait ends, and the pool gets every buffer back once (Task 4).
4. **A PINGREQ that the end drops while it is queued.** It is not written, and its buffer goes back to the pool once, like any other dropped write (Task 3).
5. **A CONNECT whose write fails.** Its bytes are cleared before the buffer goes back to the pool, as for one that was written (Task 3).

---

## File structure

| File | Change | Responsibility |
|---|---|---|
| `src/MqttWire/Client/QueuedWrite.cs` | create | One waiting write: buffer, length, kind, answer count. |
| `src/MqttWire/Client/SendQueue.cs` | create | The FIFO ring of `QueuedWrite`, created on first use. |
| `src/MqttWire/Client/SendPathContinuation.cs` | create | Awaits a pending write without allocating; runs the path from the thread pool. |
| `src/MqttWire/Client/SendKind.cs` | modify | `Answers`, `WritesEnded`. |
| `src/MqttWire/Client/ClientLimits.cs` | modify | `InlineWrites = 16`, `MaxWaitingAnswers = 4_096`, `SendQueueFirstCapacity = 8`. |
| `src/MqttWire/MqttWireClient.cs` | modify | Remove `_sendLock`; add `_sendGate`, `_writing`, `_sendQueue`, `_waitingAnswers`, `_continuation`, the three completions. `WaitUntilEndedAsync` uses the marker. |
| `src/MqttWire/MqttWireClient.Send.cs` | modify | The send path (Task 3). |
| `src/MqttWire/MqttWireClient.ReadLoop.cs` | modify | `HandOverAnswersAsync` replaces `WriteAnswersAsync` (Task 4). |
| `src/MqttWire/MqttWireClient.{Publish,Subscribe,KeepAlive,Connect,End}.cs` | modify | Call `Send` / `SendDisconnectAsync`; `TryBeginEnd` releases `_answerRoom`. |
| `tests/MqttWire.Tests/Link/MemoryLink.cs` | modify | `RefuseWriteRegistration()`. |
| `tests/MqttWire.Tests/Client/SendQueueTests.cs` | create | The ring on its own. |
| `tests/MqttWire.Tests/Client/SendPathTests.cs` | create | Order, inline bound, the ways out, Review Focus 4 and 5. |
| `tests/MqttWire.Tests/Client/AnswerCapTests.cs` | create | The read loop no longer waits; the cap; Review Focus 1 to 3. |
| `tests/MqttWire.Tests/Client/ClientAllocationTests.cs` | modify | Waiting sends allocate nothing. |
| `bench/MqttWire.Probe/Program.cs` | modify | Bytes per operation. |
| `docs/specs/2026-11-19-mqttwire-design.md`, `docs/specs/2026-11-24-client-acknowledgement-batching-design.md`, `CHANGELOG.md`, `docs/mqtt-support.md`, `docs/bench/2026-11-qos1-latency.md` | modify | Tasks 4 and 5. |

---

### Task 1: The probe counts bytes per operation

**Files:**
- Modify: `bench/MqttWire.Probe/Program.cs`

**Interfaces:**
- Produces: the probe prints a second table, `B/op`, with the same rows, columns and ratios as the µs table.

- [ ] **Step 1:** In `RunAsync`, read `GC.GetTotalAllocatedBytes(precise: true)` before and after the timed operations, and return bytes per operation beside microseconds per operation (a small `readonly record struct ProbeSample(double Microseconds, double Bytes)`). `MeasureAsync` keeps both, and `Print` prints both tables. Update the class remarks: the count is process-wide, broker included, and the broker is the same for every contestant. Also change the `before` commit in the remarks' build commands from `9aea873` to a placeholder `<commit>`, and name de7af74 as the "before" of this issue's measurements.
- [ ] **Step 2:** `dotnet build -c Release bench/MqttWire.Probe/MqttWire.Probe.csproj`. Expected: 0 warnings.
- [ ] **Step 3:** Run `dotnet bench/MqttWire.Probe/bin/Release/net10.0/MqttWire.Probe.dll` once without `before`. Expected: both tables print, and `after`'s `rt-awaiting` B/op is in the range of the issue's "after #12" (about 2,369–2,417).
- [ ] **Step 4:** Commit: `git commit -m "Make the probe report the bytes allocated per operation."`

### Task 2: The queue

**Files:**
- Create: `src/MqttWire/Client/QueuedWrite.cs`, `src/MqttWire/Client/SendQueue.cs`, `tests/MqttWire.Tests/Client/SendQueueTests.cs`
- Modify: `src/MqttWire/Client/SendKind.cs`, `src/MqttWire/Client/ClientLimits.cs`

**Interfaces:**
- Produces:
  - `internal readonly struct QueuedWrite(byte[]? buffer, int length, SendKind kind, int answers)` with get-only `Buffer`, `Length`, `Kind`, `Answers`. `Buffer` is null only for `SendKind.WritesEnded`, and `Answers` is 0 unless `Kind == SendKind.Answers`.
  - `internal struct SendQueue` with `readonly int Count`, `void Enqueue(in QueuedWrite write)` and `bool TryDequeue(out QueuedWrite write)`. A default instance holds no array. The first `Enqueue` creates `ClientLimits.SendQueueFirstCapacity` slots, a full queue doubles, and a dequeued slot is cleared so it keeps no buffer reference (design 6.8, rule 6).
  - `SendKind.Answers`, `SendKind.WritesEnded`.
  - `ClientLimits.InlineWrites = 16`, `ClientLimits.MaxWaitingAnswers = 4_096`, `ClientLimits.SendQueueFirstCapacity = 8`.

- [ ] **Step 1: Write the failing tests** in `SendQueueTests`:
  - `TryDequeue_Empty_ReturnsFalse`: `new SendQueue().TryDequeue(out _)` is false, and `Count` is 0.
  - `TryDequeue_AfterEnqueues_ReturnsThemInOrder`: enqueue writes with lengths 1, 2, 3; they dequeue as 1, 2, 3.
  - `Enqueue_PastFirstCapacity_GrowsAndKeepsOrder`: enqueue 20 writes (lengths 0–19); they dequeue as 0–19, and `Count` goes 20 → 0.
  - `Enqueue_AfterWrapAround_KeepsOrder`: enqueue 6, dequeue 4, enqueue 6 (the ring wraps), dequeue all 8 in order; then enqueue 5 more, forcing growth from the wrapped state, and dequeue all in order.
- [ ] **Step 2:** Run `dotnet test tests/MqttWire.Tests -c Release --filter "FullyQualifiedName~SendQueueTests"`. Expected: build fails (the types don't exist yet).
- [ ] **Step 3:** Implement the types and constants listed above.
- [ ] **Step 4:** Run the same filter. Expected: PASS.
- [ ] **Step 5:** Commit: `git commit -m "Add the queue of the client's send path."`

### Task 3: The send path

**Files:**
- Create: `src/MqttWire/Client/SendPathContinuation.cs`, `tests/MqttWire.Tests/Client/SendPathTests.cs`
- Modify: `src/MqttWire/MqttWireClient.Send.cs`, `MqttWireClient.cs`, `MqttWireClient.Publish.cs`, `MqttWireClient.Subscribe.cs`, `MqttWireClient.KeepAlive.cs`, `MqttWireClient.Connect.cs`, `MqttWireClient.End.cs`, `MqttWireClient.ReadLoop.cs` (for now, `WriteAnswersAsync` only calls `SendAnswers` and returns a completed task; Task 4 renames it and adds the cap), `tests/MqttWire.Tests/Link/MemoryLink.cs`, `tests/MqttWire.Tests/Client/ClientAllocationTests.cs`, and the test summaries that mention "send lock"

**Interfaces:**
- Consumes: Task 2's types.
- Produces, on `MqttWireClient`:
  - `private void Send(byte[] packet, int length, SendKind kind)` hands a packet over and never throws. Once the end has begun, anything but a DISCONNECT is refused and given back (`GiveBack`).
  - `private void SendAnswers(byte[] packet, int length, int answers)` is the read loop's hand-over (Task 4 adds the cap).
  - `private Task<bool> SendDisconnectAsync(byte[] packet, int length)` completes with whether the link took it, once its write has ended or it was dropped.
  - `private Task WhenWritesEndedAsync()` hands over the marker and completes once every earlier write has ended.
  - `private void RunSendPath(QueuedWrite write, int inlineWrites)` is the holder's loop.
  - `internal void ContinueSendPath(in QueuedWrite ended, bool failed, bool insideTheRegistration, int inlineWrites)` is called by `SendPathContinuation` when a pending write ends.
  - `internal void RunHandedOverSendPath()` is called by `SendPathContinuation.Execute()`.
  - `private void EndWrite(in QueuedWrite write, bool succeeded)` does the work of today's `finally`. `private void GiveBack(in QueuedWrite write)` does the same for a write that was dropped.
- Produces, on `SendPathContinuation`:
  - `SendPathContinuation(MqttWireClient client)`
  - `void Await(ValueTask write, in QueuedWrite queued, int inlineWrites)`
  - `IThreadPoolWorkItem.Execute()`
- Produces, on `MemoryLink`:
  - `void RefuseWriteRegistration()`: the next write pends on a source whose `OnCompleted` throws `InvalidOperationException`.
  - `bool CompleteRefusedWrite()`: ends that write.

- [ ] **Step 1: Write the failing tests** in `SendPathTests`, each with a one-sentence summary of what it proves:
  - `PublishAsync_WhileWriteHeld_LinkGetsPublishesInCallOrder`: `HoldWrites()`, then 3 QoS 0 publishes with payloads "1", "2", "3" (each `PublishAsync` completes while the write is held), then `CompleteHeldWrites()` and `WhenWrittenAsync(3)`. The written packets are publishes "1", "2", "3", in order, and `OverlappingWrites` is 0.
  - `PublishAsync_SixteenWaitingWhenCallerTakesPath_CallerWritesSixteenAndPoolWritesRest`: a one-shot `Writing` hook, set before the first publish, makes 16 more QoS 0 publishes from inside the first write. Count the writes that happen on the test thread while the first `PublishAsync` call is running (a flag set around the call, plus a thread-id check). That count is exactly 16. `WhenWrittenAsync(17)` then completes, and all 17 are in call order.
  - `PublishAsync_WriteFailsWithWritesWaiting_EndsWithWriteFailedAndDropsThem`: `PendWrites()`, 3 QoS 0 publishes, then `CompletePendingWrite(new IOException())`. Closed with `WriteFailed`; `TakeWritten()` is empty, since the first write failed and the other two were dropped; `CountingPool` has every buffer back exactly once.
  - `DisposeAsync_WritesWaiting_WritesDisconnectAfterHeldWriteOnly`: `HoldWrites()`, 3 QoS 0 publishes, start `DisposeAsync`, `CompleteHeldWrites()`, end from the peer. Written: publish "1", then DISCONNECT 0x00, nothing else; `ClosedWith` is `NormalClosure`; the dispose task completes; the pool has every buffer back exactly once.
  - `DisposeAsync_WriteHeld_ReturnsOnlyAfterWriteEnds`: `HoldWrites()`, 1 publish, abort from the test (`FailReads`), start `DisposeAsync`. It isn't complete while the write is held; after `CompleteHeldWrites()` it completes, and the pool is balanced.
  - `PublishAsync_LinkRefusesRegistration_EndsAndReturnsBufferOnlyWhenWriteEnds`: `RefuseWriteRegistration()`, 1 publish. Closed with `WriteFailed`; the publish's buffer is not yet back in the pool; after `CompleteRefusedWrite()` it is back exactly once.
  - `Tick_PingReqDroppedByEnd_ReturnsItsBufferUnwritten` (Review Focus 4): `PendWrites()`, 1 publish, a tick that queues a PINGREQ, end the connection by `FailReads`, then `CompletePendingWrite()`. The PINGREQ is not written, and the counting pool has its buffer back exactly once.
  - `Connect_ConnectWriteFails_ClearsConnectBytes` (Review Focus 5): a client with a user name and password over a `CountingPool` whose `Returning` hook records the returned arrays (as `ClientBufferTests` does for the CONNECT), and a link that fails the first write (`FailWrites`). The CONNECT's array is returned exactly once, all zero over its written length.
- [ ] **Step 2: Add to `ClientAllocationTests`** (Release only, as the class already does):
  - `PublishAsync_QoS0WhileWritePending_AllocatesNothing`: `PendWrites()`. Each iteration does one publish that takes the path and pends, one more that queues, then `CompletePendingWrite()` twice. Warm up with one iteration, then measure `Iterations`. 0 bytes.
  - Update the class summary: there is no send lock any more, a send that waits is now covered, and a write that completes asynchronously allocates nothing once `SendPathContinuation` exists.
- [ ] **Step 3:** Run `dotnet test tests/MqttWire.Tests -c Release --filter "FullyQualifiedName~SendPathTests|FullyQualifiedName~ClientAllocationTests"`. Expected: the new tests fail.
- [ ] **Step 4: Implement the send path** as design section 5 describes ("The send path", "The object that waits for a write", "Writes made on someone else's thread", "The end of a connection"). Follow the server's `StartSending`, `CompleteWrite`, `EndPendingWrite` and `PendingWrite` for the loop, the inside-the-registration test (a `[ThreadStatic]` marker), and the refused registration. In particular:
  - Decide under `_sendGate`, in one hold: take the path or queue the write; take the next write or clear `_writing`. The work of a write's end (pool return, counters, completions) runs outside the hold.
  - When `_phase >= Phase.Closing`, the holder drops every write it takes that isn't a DISCONNECT: it calls `GiveBack`, counts it as one more inline write, and goes on. `WritesEnded` completes `_writesEnded`. A DISCONNECT that is dropped, or whose write fails, completes `_disconnectWritten` with false.
  - `RunSendPath` counts `inlineWrites` down. At 0 with more waiting, it creates `_continuation` if it doesn't exist yet, calls `ThreadPool.UnsafeQueueUserWorkItem(_continuation, preferLocal: false)`, and returns with `_writing` still set. A continuation that runs later passes `int.MaxValue`.
  - Callers: Publish, Subscribe and KeepAlive call `Send`. `HandshakeAsync` calls `Send(connect, length, SendKind.Connect)` without awaiting it. `CloseLinkAsync` awaits `SendDisconnectAsync`. `WaitUntilEndedAsync` awaits `WhenWritesEndedAsync()` in place of its `_sendLock` wait. Remove `SendAsync`, `_sendLock`, and the `<remarks>` that describes the lock's allocation.
- [ ] **Step 5:** Run `dotnet test -c Release`. Expected: all pass, the existing ones included (`Tick_WriteStalledAheadOfPingReq_NoTimeoutAndOnePingReq` and the #12 tests unchanged, apart from summaries that say "send lock").
- [ ] **Step 6:** `dotnet build -c Release`. Expected: 0 warnings.
- [ ] **Step 7:** Commit: `git commit -m "Replace the client's send lock with a send path that queues instead of waiting."`

### Task 4: The read loop hands its answers over, up to the cap

**Files:**
- Create: `tests/MqttWire.Tests/Client/AnswerCapTests.cs`
- Modify: `src/MqttWire/MqttWireClient.ReadLoop.cs`, `src/MqttWire/MqttWireClient.Send.cs`, `src/MqttWire/MqttWireClient.End.cs`, `tests/MqttWire.Tests/Client/ClientAllocationTests.cs`, `docs/specs/2026-11-19-mqttwire-design.md` (6.9), `docs/specs/2026-11-24-client-acknowledgement-batching-design.md` (section 5, "Writing": one line pointing to the new design), `CHANGELOG.md`, `docs/mqtt-support.md`

**Interfaces:**
- Consumes: `SendAnswers`, `_waitingAnswers`, `_answerRoom` from Task 3.
- Produces:
  - `private ValueTask HandOverAnswersAsync()` replaces `WriteAnswersAsync`. It returns a completed `ValueTask` (no state machine) unless the hand-over took `_waitingAnswers` past `ClientLimits.MaxWaitingAnswers`. Then it returns `new ValueTask(_answerRoom.Task)`.
  - `SendAnswers` adds the answers to `_waitingAnswers` under `_sendGate`. `EndWrite` and `GiveBack` subtract them under `_sendGate`, and when the count is at or below the cap, they complete `_answerRoom` outside the hold. `TryBeginEnd` completes `_answerRoom` after it has recorded the end.

- [ ] **Step 1: Write the failing tests** in `AnswerCapTests`:
  - `ReadLoop_CallerWriteHeld_DeliversMessagesAndWritesPubAcksAfter`: `HoldWrites()`, 1 QoS 0 publish by the caller, then deliver 3 QoS 1 PUBLISHes (ids 1, 2, 3) and 1 QoS 0 PUBLISH. All 4 reach `OnMessage` while the write is held. After `CompleteHeldWrites()`, the written packets are the publish, then PUBACK 1, 2, 3, in order.
  - `ReadLoop_OwnAnswersWriteHeld_HandlesNextPackets`: `HoldWrites()`, deliver 1 QoS 1 PUBLISH (its PUBACK write is held), then 2 QoS 1 PUBLISHes. All 3 reach `OnMessage`. After release, the written packets are PUBACK 1, 2, 3, in order.
  - `ReadLoop_FloodOfUnknownPubRelWhileWriteHeld_StopsAtCapUntilReleased`: `HoldWrites()`, 1 caller publish, deliver 4,112 `PubRel7` packets (257 batches of 16), then 1 QoS 0 PUBLISH. The QoS 0 message hasn't reached `OnMessage` (checked through a signal the test controls, not a delay). After `CompleteHeldWrites()`, it arrives; the written packets are the publish, then 4,112 × `PubCompNotFound7`; the client is still connected.
  - `DisposeAsync_ReadLoopWaitingAtCap_Returns` (Review Focus 2): as above up to the cap, then `DisposeAsync`, then `CompleteHeldWrites()` and end from the peer. The dispose task completes, and the pool is balanced.
  - `ReadLoop_WriteFailsWhileWaitingAtCap_EndsAndReturnsEveryBuffer` (Review Focus 3): as above up to the cap, then `CompleteHeldWrites(new IOException())`. Closed with `WriteFailed`; the read loop finishes; the pool has every buffer back exactly once.
  - `ReadLoop_ServerWithinWindowWhileWriteHeld_NeverWaits`: `HoldWrites()`, 1 caller publish, deliver 1,024 QoS 1 PUBLISHes and then 1 QoS 0. All 1,025 reach `OnMessage` while the write is held.
  - `OnMessage_PublishesWhileOwnAnswersHeld_CompletesOnItsPubAck` (Review Focus 1): `HoldWrites()`. `OnMessage` publishes at QoS 1 on the first message. Deliver 1 QoS 1 PUBLISH, release, then deliver the PUBACK for the client's publish. The publish task completes `Completed`; the written packets are the server message's PUBACK, then the client's publish.
- [ ] **Step 2: Add to `ClientAllocationTests`:** `Receive_PublishQoS1WhileCallerWritePending_AllocatesNothing`. `PendWrites()`. Each iteration does one caller publish that pends, delivers one QoS 1 PUBLISH (its PUBACK queues), then `CompletePendingWrite()` twice. 0 bytes.
- [ ] **Step 3:** Run `dotnet test tests/MqttWire.Tests -c Release --filter "FullyQualifiedName~AnswerCapTests|FullyQualifiedName~ClientAllocationTests"`. Expected: the new tests fail.
- [ ] **Step 4: Implement `HandOverAnswersAsync`** and the cap, as in the Interfaces block. The read loop's three call sites keep their places and their `await`. Update the summaries of `ReadAndHandleAsync` and `ReadLoopAsync`: the loop doesn't wait for writes, apart from the cap.
- [ ] **Step 5:** Run `dotnet test -c Release`. Expected: all pass. Then run `dotnet build -c Release`. Expected: 0 warnings.
- [ ] **Step 6: Docs.**
  - Design 6.9: the three changes in section 6 of the new design, with a link to it.
  - The #12 design: one line under section 5, "Writing".
  - `CHANGELOG.md`, under `## Unreleased`, `MqttWire`: "A slow write no longer holds up the messages that arrive behind it, and a send that waits for another write allocates nothing (#13)."
  - `docs/mqtt-support.md`: "at most 4,096 answers wait to be written; past that the client stops reading until they are" among the client's limits; remove any mention of the send lock.
  - Check with `grep -rn "send lock" src docs README.md`: the only hits left are in dated designs, reviews and bench reports that describe the past.
- [ ] **Step 7:** Run the MQTT.js interop: build `tests/MqttWire.TestHost`, then `npm ci` and `npm test` in `tests/interop-js`. Expected: pass.
- [ ] **Step 8:** Commit: `git commit -m "Hand the read loop's answers to the send path, waiting only past 4,096 answers."`

### Task 5: Measure before and after

**Files:**
- Modify: `docs/bench/2026-11-qos1-latency.md`

- [ ] **Step 1:** Build `before` from de7af74, as in the probe's remarks: worktree `../mqttwire-before`, assembly `MqttWire.Before`, then the probe with `-p:BeforeAssembly=<absolute path>`. Check by reflection that the `before` contestant's client type comes from `MqttWire.Before`.
- [ ] **Step 2:** Run the probe 3 times. Record µs/op and B/op, medians per run, for all four scenarios, for `after`, `before` and `mqttnet`. Success: `rt-awaiting` B/op is about 230 below `before`, near 2,141–2,187. No µs ratio `after`/`before` is above 1 beyond the spread between runs.
- [ ] **Step 3:** Run BenchmarkDotNet `ClientBenchmarks` and `ClientComparisonBenchmarks` with `[MemoryDiagnoser]` at de7af74 (the worktree) and at this branch, minutes apart. Success: no row allocates more, and none is slower beyond the noise the probe shows.
- [ ] **Step 4:** Take contended sends per operation from a scratch build only: a counter added in the worktree's copy of this branch, never committed. It confirms that the waiting sends still happen and now cost nothing.
- [ ] **Step 5:** If B/op does not come back near the bytes of before #12, find where the rest goes (a BenchmarkDotNet allocation run on the awaiting round trip, or `dotnet-counters`/an EventPipe allocation trace) and write it into the report before the pull request.
- [ ] **Step 6:** Write a section in `docs/bench/2026-11-qos1-latency.md` that names the commits, the machine, the method and the run counts, and keeps every number it relies on. Remove the worktree.
- [ ] **Step 7:** Commit: `git commit -m "Measure the client's send path against the send lock."`
