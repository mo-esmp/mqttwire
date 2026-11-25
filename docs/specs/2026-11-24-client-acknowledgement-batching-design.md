# The client writes its answers when it runs out of input

A change to the inside of `MqttWireClient`. It amends section 6.9 of
[the design](2026-11-19-mqttwire-design.md), which stays the authority for everything else.

Status: approved by the owner and implemented on 2026-11-24; see
[the plan](../plans/2026-11-24-client-answer-batching-plan.md). It was revised the same day after
[its review](2026-11-24-client-acknowledgement-batching-review.md).
Section 11 says what the review changed.

## 1. Intent

The client's read loop answers some of the server's packets with a packet of its own. Today it
writes that answer and **waits for the write** before it handles the next packet. So a packet
that has already arrived waits behind a write that has nothing to do with it.

**An answer** in this document is one of the four packets the read loop writes on its own:

| The server sent | The client answers |
|---|---|
| PUBLISH at QoS 1 | PUBACK |
| PUBLISH at QoS 2 | PUBREC |
| PUBREL | PUBCOMP |
| PUBREC, for a QoS 2 publish of the client's own | PUBREL |

This showed up in the comparison with MQTTnet. A client that publishes at QoS 1 to a topic it is
subscribed to gets two packets back, its own message and the PUBACK for its publish. The PUBACK
waits behind the write of the client's own PUBACK for the message. With a caller that blocks,
as BenchmarkDotNet does, that made MqttWire slower than MQTTnet. It also costs one write for
every message received, however many arrive together.

The owner's goal: MqttWire is the faster client in every case. Performance is the reason the
library exists.

## 2. Goals, non-goals, anti-goals

**Goals**

- A packet that has arrived is never held up by the write of an answer.
- Answers that are ready at the same time go out in one write.
- The read loop still allocates nothing for a message it receives.

**Non-goals**

- The server. It already keeps waiting control packets in a buffer and writes them together.
- The public API. Nothing in it changes.
- The receive maximum. The rule that enforces it stays as it is (section 5).
- The send lock. With a caller that awaits, the write of the answers and the caller's next
  publish seem to compete for it (section 7). That is measured after this change, not solved in
  it.

**Anti-goals**

- A second thread or a work item for writing. It costs a hand-off for every burst, and Blazor
  WebAssembly has one thread.
- An answer that waits while the read loop is idle. Batching must never add a delay a quiet
  connection can see.

## 3. Constraints

- **Order.** Answers go out in the order their packets arrived (MQTT 5, section 4.6). That
  covers PUBRELs too: they follow the order of their PUBRECs.
- **Hot path.** No allocation, no closure, no `ToArray`; buffers come from the client's pool and
  go back exactly once, on every way out.
- **No blocked thread.** The client runs in Blazor WebAssembly.
- **One write at a time.** Every write still goes through the send lock.
- **Nothing is rented or encoded under the client's lock.** A PUBREL is decided on while the
  lock is held; today that only notes what to send, and it stays that way.
- **The server's deadline.** `MqttWire.Server` closes a connection whose acknowledgement is 30
  seconds late. The client must not make an answer much later than today.

## 4. Approaches

Measured on 23 and 24 November 2026 with throwaway patches on a copy of the client and a probe
that ran every variant in turn in one process, against the same MQTTnet broker on loopback.
Medians in microseconds over three runs for each kind of caller; one awaiting run that fell in a
slow phase of the machine is left out, and its order was the same.

| | Today | A | B | MQTTnet |
|---|---|---|---|---|
| Round trip, caller blocks | 117 to 125 | 97 to 103 | 102 to 108 | 111 to 119 |
| Round trip, caller awaits | 80 to 83 | 83 | 83 to 88 | 90 to 93 |
| Burst of 50, per message, caller blocks | 32 to 35 | 14 | 16 to 20 | 46 to 48 |
| Burst of 50, per message, caller awaits | 27 to 34 | 14 | 14 to 15 | 43 to 45 |

- **A. Write the answers when the read loop runs out of input** (chosen). The loop records each
  one in a small pooled buffer, goes on with the packets that have arrived, and writes what
  waits as one WebSocket message just before it would wait. One thread, no hand-off, and a burst
  costs a few writes.
- **B. Write them from a work item on the thread pool.** The read loop never waits, as in
  MQTTnet. It was slower than A everywhere and slower than today with a caller that awaits,
  because every burst pays a hand-off to another thread.
- **C. A, and B when the write would have to wait.** Not built. Nothing measured asks for it.

The owner chose A after the spike.

**What these numbers do and do not show:**

- **The round trip's cause is isolated.** An earlier patch only moved the write of the answer
  off the read loop and left the number of writes as it was. With a caller that blocks it was 13
  to 25 microseconds faster in four runs of four. So it is the waiting, not the write count,
  that costs the round trip.
- **The burst rows are not the design below.** The spike wrote when 256 bytes waited, about
  sixty answers, and did not count packets. The design writes after sixteen packets. It changes
  both how many writes there are and when they happen, and the spike cannot say how much each
  gives. Section 9 measures the design itself.
- **The machine was a virtual one with other work running,** so the numbers are rough. The order
  was the same in all six runs.

## 5. The design

**One rule: when the read loop waits, no answer is waiting.** "Waits" means either of the two
things the loop asks of the link: the wait for data, and the read.

- **Recording.** Where the read loop today writes an answer and waits, it encodes the answer
  behind the ones that already wait, in a buffer rented from the client's pool. The buffer is
  rented when the first answer is recorded and exists only while answers wait. Only the read
  loop touches it, so it needs no lock.
- **Writing.** What waits is written as one WebSocket message through `SendAsync`, which takes
  the send lock and returns the buffer to the pool, as it does for every packet. The read loop
  waits for that write, as it waits for a single answer today.
- **When it is written.** In two places:
  1. **The loop would wait.** The loop starts the link's wait for data, and later its read. If
     either does not complete at once and answers wait, they are written before the loop waits
     for it. A wait that completes at once does not mean the read will: a peer can send the
     start of a message and withhold the rest, and the link cannot tell the two apart. Whatever
     the loop started it awaits exactly once, also when the write of the answers fails and the
     connection ends; the memory a read was given stays valid until that read has finished.
  2. **Sixteen packets.** While an answer waits, the loop counts every packet it handles, of
     whatever kind and QoS. It writes before it handles the sixteenth packet after the one whose
     answer waits longest. So at most sixteen answers wait, and an answer goes out at the latest
     once fifteen more packets have been handled, also when none of them needs an answer.
- **The bound is in packets, not in time.** One `OnMessage` call or one stalled write can take
  as long as it takes, as today.
- **The server's Maximum Packet Size** is checked for each answer when it is recorded, as today:
  one over the limit ends the connection without it.
- **A failed write** ends the connection inside `SendAsync`, as today; the read loop sees the
  phase on its next turn.
- **When the connection ends,** answers that still wait are not sent, and their buffer goes back
  to the pool at the next write attempt once the end has begun, or when the read loop finishes the
  connection, whichever comes first. The read loop records no new answer once the end has begun.
  `SendAsync` already refuses everything but a DISCONNECT once the end is recorded.
- **The receive maximum is enforced as today:** a new QoS 1 or 2 PUBLISH ends the connection
  with `0x93` while 1,024 QoS 2 identifiers wait for their PUBREL. An answer that waits does not
  count as a place taken. The client could now see a server that sends one delivery too many
  before it has the answer that frees the place; it sees no more of that than today, when it
  handles the next packet without knowing whether the server has its answer. The rule exists to
  bound the table of QoS 2 identifiers, and what waits here is bounded at sixteen.

What changes for a server, and why it is acceptable:

- **Several answers may arrive in one WebSocket message.** MQTT over WebSockets allows it: a
  receiver may not assume that packets and messages line up (MQTT-6.0.0-2). MqttWire's server
  writes its own control packets the same way.
- **An answer can go out later than today,** by at most the handling of fifteen more packets,
  which is the caller's `OnMessage` up to fifteen times. Against the server's 30 seconds that
  matters only for a handler that takes about two seconds a message, which already stalls the
  read loop today.
- **Up to sixteen answers are lost when the connection ends on a later packet** in the same run
  of input: a malformed packet, a DISCONNECT from the server, or `OnMessage` throwing. Today
  they were written before that packet was handled. The client has no stored session, so nothing
  is redelivered to it because of this, and a write that fails loses them today too. A PUBREL
  among them belongs to a publish that completes with "connection lost", as every publish in
  flight does.

## 6. Decided by the owner

1. The goal: MqttWire is the faster client in every case.
2. Approach A, after the spike.
3. The receive maximum is left as it is, after the review proposed to hold a place until its
   answer is written.

## 7. Proposed by Claude (owner to check)

1. **The number sixteen,** counted in packets. Small enough for the bound of section 5, large
   enough that a burst of 50 messages costs four writes instead of 50 when they are all there to
   be read.
2. **Answers that wait are dropped at the end of a connection,** not written ahead of the
   DISCONNECT. Writing them would need a way past the rule that only a DISCONNECT is sent once
   the end is recorded, for no gain a client without a session can use.
3. **The send lock is left alone.** In the spike, with a caller that awaits, the client's own
   message reached `OnMessage` about 13 microseconds later under A than today, and the round
   trip took the same time. The likely reason is that the write of the answers and the caller's
   next publish meet at the send lock; this was not verified. It is measured once this change is
   in, and is a change of its own if it is real.
4. **The comparison benchmarks are committed with this work,** with one more for a burst of 50,
   and `docs/bench/2026-11-qos1-latency.md` is corrected: it says the cause is unconfirmed,
   leaves out the patch that isolates it, and links to temporary files.
5. **This document lives in `docs/specs`,** beside the design it amends, and section 6.9 of the
   design gets the rule of section 5 in the same pull request.

## 8. Left to the builder

- Whether the waiting answers are a small internal type in `MqttWire.Client` or fields of the
  client, and their names.
- The size of the rented buffer, as long as sixteen answers of the largest kind the client
  sends fit.
- How the loop starts the link's wait and its read without waiting for them, as long as each is
  awaited exactly once.

## 9. Tests

Over the in-memory link, named `MethodName_StateUnderTest_ExpectedBehavior`. Where a test is
about order, the link holds an operation open and releases it on a signal, so no test needs a
timeout to tell before from after.

**The rule**

- A single QoS 1 message on a quiet connection: its PUBACK is written before the loop waits.
- A QoS 1 PUBLISH and the PUBACK for the client's own publish arrive together: the publish
  completes while the client's PUBACK is not yet written.
- The link reports data at once and holds the read open. After a QoS 1 PUBLISH, its PUBACK is
  written while that read is still pending; when the read is released, the next packet is
  handled exactly once.

**The bound**

- Seventeen QoS 1 messages arrive together: one write carries sixteen PUBACKs and the next
  carries one, in order.
- One QoS 1 PUBLISH and at least sixteen QoS 0 messages arrive together: the PUBACK is written
  before the sixteenth of those is handed to `OnMessage`.

**Order across kinds**

- Three QoS 1 messages arrive together: one write carries three PUBACKs, in order.
- An incoming QoS 2 exchange A is under way and the server has its PUBREC. A new QoS 2 PUBLISH B
  and the PUBREL of A arrive together: one write carries the PUBREC of B, then the PUBCOMP of A.
- Two QoS 2 publishes of the client's own are in flight and both PUBRECs arrive together: one
  write carries both PUBRELs, with the identifiers in the order of the PUBRECs, and each publish
  completes only on its own PUBCOMP.

**The ways out**

- The connection ends with answers waiting, by each way out: nothing but the DISCONNECT is
  written after the end, and the counting pool has every buffer back exactly once.
- The write of the answers fails while a wait or a read the loop started is pending: the
  connection ends as a failed write does today, and what was started is still awaited once.
- An answer over the server's Maximum Packet Size: the connection ends as today, and what waited
  is returned.

**Unchanged**

- The receive maximum: the existing tests pass as they are.
- `Receive_PublishQoS1AndSendPubAck_AllocatesNothing` still passes, and a new test receives a
  burst and allocates nothing.

Existing tests that expect one write for each answer are changed to the new rule, not deleted.

## 10. Measurement and success

A performance claim needs a BenchmarkDotNet run with `[MemoryDiagnoser]`, before and after. What
is compared is the client as it is against **this design with its sixteen packets**, not the
spike:

- `ClientBenchmarks.ReceiveQoS1`, over the in-memory link.
- `ClientComparisonBenchmarks`: the round trip at QoS 1 and a new burst of 50, each with a
  caller that blocks and with one that awaits. BenchmarkDotNet blocks on every invocation, so
  the awaiting caller is one invocation that awaits many operations in a row.
- **Writes for each burst** are recorded beside time and allocation. Four writes for 50 messages
  is the least there can be, and only when the input is there to be read between two writes.

The machine is a virtual one whose speed changes for seconds at a time, and one BenchmarkDotNet
table was misleading here before. So the before and after are also taken with both builds in one
process, in turn, and the report gives medians and says how many runs agree.

**Success:**

- With a caller that blocks, the QoS 1 round trip is faster than MQTTnet's and faster than
  today's.
- With a caller that awaits, it is no slower than today's.
- A burst of 50 costs less per message than today's, with the writes counted; the spike's "less
  than half" is to be confirmed for sixteen, not assumed.
- No allocation is added to receiving a message, and every test passes.

## 11. What the review changed

[The review](2026-11-24-client-acknowledgement-batching-review.md) of the first version made
five points. Each was checked against the code on `main`.

| | The review said | What was done |
|---|---|---|
| 1 | A read can wait with answers waiting, because the link's wait completes when a message has only begun to arrive. | Right. The rule now covers the read as well as the wait (section 5). |
| 2 | QoS 0 messages behind one QoS 1 message never reach a count of answers, so its PUBACK can wait without end. | Right. The count is now of packets handled, of every kind (section 5). |
| 3 | A delivery should hold its place in the receive maximum until its answer is written. | Not taken. Both of the review's cases admit the same delivery on today's client, so the design changes nothing here; the owner left the rule as it is (section 6). |
| 4 | PUBREL for the client's own QoS 2 publishes goes the same way and was not named; and a new QoS 2 PUBLISH with its own PUBREL in one run of input is not something a correct server sends. | Right. PUBREL is one of the four answers (section 1), and the tests are the review's (section 9). |
| 5 | The measurements do not separate the number of writes from their timing, and the spike's threshold was not the design's. | Right for the burst; the design itself is measured, with writes counted (section 10). For the round trip the cause is isolated, by a patch the review could not know of (section 4). |
