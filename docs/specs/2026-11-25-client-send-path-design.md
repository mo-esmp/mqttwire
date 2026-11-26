# The client's send path: nobody waits for the send lock

A change to the inside of `MqttWireClient`, for issue #13. It amends section 6.9 of
[the design](2026-11-19-mqttwire-design.md), and the rule for writing answers in
[the client writes its answers when it runs out of input](2026-11-24-client-acknowledgement-batching-design.md)
(its section 5, "Writing"). Both stay the authority for everything else.

This changes the client only. The server's send path (design 6.6, "Sending: taken at once,
written when the socket allows") is the model for this change, and it does not change. Nothing
in `src/MqttWire/Link` or `src/MqttWire/Codec` changes.

Status: written on 2026-11-25 and approved by the owner on 2026-11-26, after a builder's review
of the draft; see [the plan](../plans/2026-11-26-client-send-path-plan.md).

## 1. Intent

Every write of the client goes through `SendAsync`, which takes a `SemaphoreSlim(1, 1)`, the
send lock, and holds it across the write. That costs two things.

**A send that waits for the lock allocates.** `SendAsync` is a plain `async ValueTask<bool>`, so
a wait boxes its state machine, and `SemaphoreSlim.WaitAsync` allocates a waiter of its own.
Before #12 a send rarely waited. Since #12, a caller that awaits each QoS 1 round trip to its
own subscription meets the lock on almost every operation: the caller's next publish takes the
lock first, and the read loop's batched PUBACK waits behind it. Measured with an interleaved
probe (one in-process MQTTnet broker, 20 rounds of 2,000 operations, medians, three runs):

| client | B/op | contended sends/op | µs/op |
|---|---|---|---|
| before #12 | 2,141–2,187 | 0.000 | 72.4–76.2 |
| after #12 | 2,369–2,417 | 0.97–0.98 | 73.5–78.9 |
| after #12, `SendAsync` on `PoolingAsyncValueTaskMethodBuilder` | 2,313–2,325 | 0.97 | 74.6–78.0 |

About 230 B per waiting send. Pooling the builder removes only about 95 of them; the rest is most
likely the semaphore's waiter (inferred, not measured). Latency did not change within the
machine's noise. The numbers are in `docs/bench/2026-11-qos1-latency.md`.

**The read loop waits for the caller's writes.** While a caller's write holds the lock, the read
loop cannot write its answers, and it waits for them before it reads on. So a slow write, such
as a large publish on a slow link, holds up every delivery behind it. This was true before #12,
when each answer took the lock in turn, and #12 did not change it.

The owner's goal: MqttWire is the faster client in every case. Performance is the reason the
library exists, and memory comes first in this codebase.

## 2. Goals, non-goals, anti-goals

**Goals**

- A send that finds another write in progress allocates nothing, whoever sends it: a caller's
  publish, subscribe or unsubscribe, a PINGREQ, or the read loop's answers.
- The read loop never waits for a write while the server keeps to its window. A slow write does
  not hold up the packets that arrive behind it.
- What a faulty or hostile server can make the client hold stays bounded (section 5, "How many
  answers may wait").
- The awaiting round trip loses the roughly 230 B per operation that #12 added.

**Non-goals**

- The server. Its send path already works this way and stays as it is.
- The public API. Nothing in it changes.
- The link. `IMqttLink` and `WebSocketLink` keep their contract: one write at a time, and a
  buffer belongs to its write until the write has ended.
- What the client sends, and when the read loop decides to write its answers (the rule of #12:
  when the loop would wait, or before the sixteenth packet).
- The end of a connection. The DISCONNECT, the close and the wait for every write to end happen
  once in a client's life; they may allocate, as they do today.

**Anti-goals**

- A writer of its own: a thread, a long-running loop or a work item for every write. The #12
  design ruled it out, because every burst then pays a hand-off, and Blazor WebAssembly has one
  thread.
- A blocked thread, in any form (Blazor WebAssembly).
- A packet that goes out in a different order from today (section 5, "Order").

## 3. Constraints

- **Hot path.** The send path is a hot path (AGENTS.md): no allocation, closure, boxing or
  `ToArray`. What may allocate is the first use of an object the client then keeps, and the
  growth of the queue (section 5).
- **One write at a time** reaches the link, and a write's buffer is the write's until it has
  ended (design 6.8, rules 3 and 4).
- **Nothing is rented or encoded under a lock.** The packets are encoded before they are handed
  to the send path, as today.
- **No lock is held across a write, a close, an abort or a call into the application.**
- **The client is one-shot.** One `MqttWireClient` has at most one connection; the reconnecting
  client makes a new one for each. So the send path never outlives its connection's link.

## 4. Approaches

The owner chose to fix both costs, the allocation and the read loop's wait, and then chose A.

- **A. A send path like the server's** (chosen). A flag says who is writing, and a queue holds
  what waits. Whoever finds the path free writes; everyone else adds to the queue and returns.
  Whoever finishes a write takes the next one. One object kept by the client waits for a write
  that does not complete at once. It fixes both costs with one mechanism, and the server has
  shown that it allocates nothing in the steady state. It is the most code of the three, and it
  amends the line of design 6.9 that said a plain send lock is enough for the client.
- **B. A lock that allocates nothing, plus a slot for answers.** Callers keep taking a lock,
  rebuilt to reuse its waiter objects. The read loop never takes it: when it is held, the read
  loop leaves its answers in a slot, and the holder writes them after its own packet. It keeps
  today's shape for the callers, but it is two mechanisms, and "release the lock, or take the
  answers" needs a guard of its own to be free of races, which is most of A's machinery.
- **C. A writer loop that does every write.** Dropped: it is the anti-goal of section 2, and it
  adds a hand-off even to a write that could have gone out at once.

A second choice, made by Claude: **the client gets its own copy of the pattern**, not code
shared with the server. The server's send path is bound up with its delivery windows, its
charges and its eviction, and a shared core would change the server too.

## 5. The design

### The send path

One flag, **writing**, says whether someone holds the send path. It and a **queue** of waiting
writes are guarded by one lock of the send path's own, never held across a write.

**A waiting write** is a small struct: the rented buffer, its length, and its `SendKind`. The
queue is a ring of these structs in an array the client keeps. It is created on the first send
that has to wait, grows by doubling when it is full, and is never shrunk; a client has one
connection, so it is gone with the client.

**Handing a packet to the send path** (what `SendAsync` does today) returns at once and never
throws. In one hold of the lock:

- once the end has begun, anything but a DISCONNECT is refused: its buffer is given back as a
  write that ended would give it back (the PUBLISH's queued bytes, the PINGREQ's flag, the
  CONNECT's credentials cleared), outside the lock, and nothing is written;
- if nobody holds the path, the caller takes it and writes the packet itself, on its own
  thread, as today when the lock is free;
- otherwise the packet is added to the queue, and the caller returns.

**Whoever holds the path** writes, and when the write has ended does its work (below), gives
the buffer back, and in one hold of the lock either takes the next waiting write or clears the
flag. It goes on for as long as the writes complete at once. A write that does not complete at
once is handed to the object that waits for writes, and the holder returns: from then on, the
path is held by that write, and its completion carries it on, on the thread that completes it.

**When a write ends,** whatever the outcome, the holder does what `SendAsync`'s `finally` does
today: it clears a CONNECT's bytes, gives back a PUBLISH's queued bytes or clears the PINGREQ's
flag, records the time of the last send after a write that succeeded, and returns the buffer to
the client's pool. A PINGREQ's deadline starts when its write starts, as today. A write that
failed, or a link that threw when the write was started, ends the connection with
`MqttClientCloseReason.WriteFailed` and aborts the link, as today.

**Taking the next write once the end has begun:** a waiting write that is not a DISCONNECT is
dropped as it is taken, and its buffer is given back as above. Today such a write finds the end
recorded when its turn at the lock comes, and is dropped the same way.

### The object that waits for a write

A client has at most one write in progress, so **one object waits for every write that does not
complete at once**. It is created on the first such write and kept; it is not pooled. It holds
the write's awaiter, buffer and kind, and an `Action` made once, which it registers with
`UnsafeOnCompleted` on the awaiter of `ValueTask.ConfigureAwait(false)`, without the caller's
execution context. When the write has ended, the continuation reads the result (which also lets
the link reuse what backs the write's task), clears the object, and carries the send path on.

The server's `PendingWrite` (in `MqttWire.Server.Internal`) is the model, including how it tells
a continuation that runs inside its own registration, on the stack of the caller that started
the write, from one that runs later.

A link that refuses the registration is a fault of the link. The holder then ends the connection
as a failed write does and gives up the send path; the write's buffer is not returned, since the
write may still be using it (design 6.8, rule 4). The object stays armed for that write's
completion, if it ever comes, and returns the buffer then.

### Writes made on someone else's thread

A holder writes at most **sixteen packets inline**, its own included, while the writes complete
at once. With more waiting, it hands the path, still held, to the thread pool through a work
item the client keeps (`IThreadPoolWorkItem`, queued with `ThreadPool.UnsafeQueueUserWorkItem`),
and returns. A write that completed during the registration of its continuation counts as one
more of the caller's inline writes. So a caller's `PublishAsync`, or the read loop, writes at most
sixteen packets of other writers before it returns, and the stack cannot grow deeper than that.

This hand-off happens only when sixteen writes in a row complete at once while others keep
adding to the queue, not once per burst; the anti-goal of section 2 stands.

The bound is for a caller's own thread. A continuation that runs later, after the caller has
returned, carries the path on for as long as the writes complete at once, as on the server: it
runs on the thread that completed the write, which is nobody's call.

### The read loop

The read loop hands its answers to the send path and **does not wait** for them. Where it now
awaits `WriteAnswersAsync`, it takes the answers' buffer from `PendingAnswers` and hands it over,
then goes on to its wait for data, its read, or the next packet. The points where it hands them
over are those of #12, unchanged. When the path is free, the read loop writes the answers itself,
as a caller does; a write that does not complete at once is carried on by its completion, not
by the read loop.

The answers' buffer now leaves the read loop as soon as it is handed over, and the send path
returns it. `PendingAnswers` stays read-loop state only.

### How many answers may wait

The caller's waiting writes are already bounded: a PUBLISH by the 1 MiB of queued publish bytes
(`ClientLimits.MaxPendingPublishBytes`), a SUBSCRIBE or UNSUBSCRIBE by the packet identifiers in
use, a PINGREQ at one.

The answers are not bounded by anything the client enforces. A server that keeps to its window
sends no more than 1,024 QoS 1 or 2 messages before their answers arrive, so a few kilobytes of
answers wait at most. A faulty or hostile server is not held to that. The client counts only its
QoS 2 identifiers against its receive maximum, not its unanswered QoS 1 messages, and it answers
every unknown PUBREL with PUBCOMP `0x92`, and every unknown PUBREC below `0x80` with PUBREL `0x92`,
however many come. Today a stuck write stops the read loop, so the client stops reading and the
server is held back. Without the read loop's wait, nothing would hold it back, and the answers
would queue for as long as the write stays stuck. No deadline would end that: a PINGREQ's
deadline starts only when its write starts.

So **the answers waiting in the send path are capped at 4,096** (decided by the owner: cap, then
wait). Counted are the answers handed to the send path and not yet written, whether they wait in
the queue or are being written. The send path counts them down when their write ends, or when
they are dropped.

- **Below the cap,** the read loop hands its answers over and goes on, as above.
- **When handing answers over takes the count past the cap,** the read loop waits, after handing
  them over and before it handles the next packet or waits for the link, until the count is
  back at or below the cap. That is today's brake, and it applies only to a server that has
  broken its window, or that floods the client with unknown PUBRELs or PUBRECs.
- **The wait ends** when enough answers have been written, or when the connection ends: answers
  dropped at the end count down too, and a connection that ends releases the wait. The read loop
  then sees the end on its next turn.
- **The wait may allocate.** No server that keeps to its window gets there.

4,096 is Claude's number: four times the window of 1,024 messages that the client announces, so
a server within its window, with the client's own QoS 2 exchanges besides, never reaches it. At
most 4,096 answers in buffers of at most sixteen each keeps what waits to some tens of kilobytes
when every batch holds one answer.

### Order

The order in which packets reach the link is the order in which they were handed to the send
path, which is the order of today's lock: a caller that finds the path free writes at once, and
the queue is first in, first out.

- The answers keep the order of their packets (MQTT 5, section 4.6): the read loop hands them
  over in that order.
- A caller's packets keep the order of its calls.
- A publish made from `OnMessage` may go out ahead of answers that still wait in the read loop,
  as design 6.9 already says. An answer handed to the send path before that publish goes out
  ahead of it.

### The end of a connection

These happen once in a client's life and may allocate.

- **The DISCONNECT and the close.** `CloseLinkAsync` today awaits the DISCONNECT's write, then
  closes the link. The DISCONNECT now carries a completion, made when the end is recorded, which
  the send path completes when its write has ended (or when it was dropped), with whether the link
  took it. `CloseLinkAsync` awaits that, and then closes the link, as today.
- **Every write has ended.** `WaitUntilEndedAsync` today takes and releases the lock once, after
  the read loop has finished, so that every write handed over before then has ended and given its
  buffer back. Instead it hands the send path a **marker** with a completion: added to the queue
  like a write, completed by the holder when it takes it, or at once when nobody holds the path.
  Every write handed over before the marker has then ended. A write handed over after it finds the
  end recorded and is refused at once.
- **The CONNECT** is handed over without being awaited. `HandshakeAsync` awaits the CONNACK, as
  today; a failed write ends the connection, and the read loop then fails the connect.

`SendAsync` and the `SemaphoreSlim` are removed.

## 6. What changes in the design

Section 6.9 of the design, in the same pull request:

- The bullet on answers: they are handed to the send path, and the read loop does not wait for
  their write.
- The keep-alive bullet: "while one waits for the send lock" becomes "while one waits to be
  written".
- The last bullet, "A plain send lock or queue is fine: a client has one connection and does not
  need the server's 'only while there is work' machinery", becomes a description of the send path
  of section 5, with a link to this document.

The #12 design gets one line under its section 5, "Writing", pointing here. `CHANGELOG.md` gets an
entry under `## Unreleased` for `MqttWire`: a slow write no longer holds up the messages that
arrive behind it, and a send that waits for another write allocates nothing.
`docs/mqtt-support.md` gets the cap of 4,096 answers waiting to be written among the client's
limits, and loses any mention of the send lock.

## 7. Decided by the owner

1. The goal: MqttWire is the faster client in every case.
2. Fix both costs: the allocation of a waiting send, and the read loop's wait for writes.
3. Approach A, a send path like the server's.
4. The answers waiting in the send path are capped, and past the cap the read loop waits for
   them, as it does today, rather than ending the connection.

## 8. Proposed by Claude (owner to check)

1. **The client keeps its own copy of the pattern** rather than sharing code with the server
   (section 4).
2. **Sixteen packets inline,** then the thread pool, as on the server. Without a bound, one
   caller's `PublishAsync` could write other callers' packets for as long as they keep coming.
3. **One kept object for a pending write,** not a pool: a client has one write in progress at a
   time. It and the queue are created on first need, so a client that never meets a slow or
   contended write allocates neither, and a new client allocates no more than today.
4. **The end may allocate:** a completion for the DISCONNECT and one for the marker, once per
   client.
5. **The queue never shrinks.** A client lives for one connection.
6. **The cap is 4,096 answers** (section 5).

## 9. Left to the builder

- The names of the new types and members, and whether the object that waits for a write and the
  work item are one class or two. They live in `src/MqttWire/Client`, and the send path stays in
  `MqttWireClient.Send.cs`.
- The queue's first size, as long as it is created only when a send has to wait.
- Whether the lock is a new `Lock` or the client's `_gate`, as long as no hold of it rents,
  encodes, writes or calls into the application. Claude's pick is a new `Lock`: `_gate` guards
  the phase and is taken by every publish.
- How the work item and the object that waits share the code that carries the path on.

## 10. Tests

Over the in-memory link, named `MethodName_StateUnderTest_ExpectedBehavior`, with every wait
bounded and no outcome depending on the bound. Where a test is about order or waiting, the link
holds a write open and releases it on a signal.

**The read loop no longer waits**

- A caller's write is held open by the link. QoS 0 and QoS 1 PUBLISHes from the server reach
  `OnMessage` while it is held; their PUBACKs are written, in order, after it is released.
- The read loop's answers find the path free and the write is held open: the next packets are
  still handled, and their answers go out after it, in order.

**Nothing allocated while waiting** (in the allocation collection, with
`GC.GetAllocatedBytesForCurrentThread()`, after one warm-up)

- A QoS 0 publish handed over while another write is held open allocates nothing.
- The read loop's answers handed over while a caller's write is held open allocate nothing.
- A write that does not complete at once allocates nothing once the waiting object exists.
- The existing allocation tests pass as they are.

**Order**

- Publishes from several callers while a write is held: they reach the link in the order of the
  calls.
- Answers and a publish made from `OnMessage`: the order of section 5.

**Sixteen inline**

- A caller takes the free path while sixteen packets of others wait, and every write completes
  at once: the caller writes sixteen, its own and fifteen others, and returns; the last one is
  written from the thread pool.

**The ways out**

- A write that fails while others wait: the connection ends with `WriteFailed`, every waiting
  write is dropped, and the counting pool has every buffer back exactly once.
- The end with writes waiting, by each way out: only the DISCONNECT is written after the end, the
  close follows it, and `DisposeAsync` returns only after every write has ended.
- A link that refuses the registration of a continuation: the connection ends, and the write's
  buffer is returned only when that write ends.
- A PINGREQ waiting behind a held write: its deadline starts when its write starts.

**The cap**

- A caller's write is held open and the server sends unknown PUBRELs: the read loop handles
  packets until 4,096 answers wait, then handles no more; when the write is released, the answers
  go out in order and the read loop goes on.
- The same, and the connection ends while the read loop waits: the wait ends, and the counting
  pool has every buffer back exactly once.
- A server within its window (1,024 QoS 1 messages unanswered behind a held write) never makes
  the read loop wait.

Existing tests that name the send lock are changed to the send path, not deleted.

## 11. Measurement and success

Before is `main` at de7af74. After is this branch. Both are measured on the same machine in the
same session.

- **The interleaved probe** (`bench/MqttWire.Probe`), with its `before` contestant built from
  de7af74: the awaiting round trip should lose the roughly 230 B per operation, back near the
  2,141–2,187 B of before #12. Latency must not be worse: the blocking and awaiting round trips
  and the bursts of 50.
- **How the probe counts bytes** (Claude's call). The committed probe today measures time only.
  It gains bytes per operation from `GC.GetTotalAllocatedBytes(precise: true)` around each round.
  That count is process-wide and includes the in-process broker, but the broker is the same for
  both contestants, so the difference between them is the client's. This is how the issue's
  numbers were taken.
- **Contended sends per operation** are counted only in a scratch build, as for the issue. No
  counter goes into the shipped hot path.
- **BenchmarkDotNet with `[MemoryDiagnoser]`:** `ClientBenchmarks` (publishes at QoS 0 and 1 and
  `ReceiveQoS1`) and `ClientComparisonBenchmarks`, before and after. No row may allocate more, and
  none may be slower beyond the noise the probe shows.
- **What gates the merge** (Claude's call): the allocation tests of section 10, which show that a
  waiting send allocates nothing, and no benchmark row worse. The probe's bytes per operation are
  reported as found. The ~135 B the issue puts on `SemaphoreSlim` was inferred, not measured; if
  the result does not come back near the bytes of before #12, the report says where the rest goes
  before the pull request is opened.
- **A slow write is not benchmarked** (Claude's call). Against a broker in the same process on
  loopback no write is slow, and a slow link cannot be put under the probe's clients without
  opening the client's internals to it, in both builds, or a throttling proxy that MQTTnet would
  need too. That the read loop no longer waits is shown by the tests of section 10, where the link
  holds a write open.

The results go into `docs/bench/2026-11-qos1-latency.md`, which names the commits, the machine,
the method and the run counts, and keeps every number it relies on.
