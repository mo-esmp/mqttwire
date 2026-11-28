# What an idle connection costs MqttWire

Measured on 23 November 2026 with `bench/MqttWire.Bench`, as section 6.14 of
[the design](../specs/2026-11-19-mqttwire-design.md) asks. The target is that MqttWire adds no
more than 1 KB (1,024 bytes) to the floor for an idle connection, before and after activity
alike.

## 1. The result

- **Idle, the target is met in every run:** a connected, idle connection costs MqttWire
  **513 to 624 bytes** above the floor, in ten runs. The floor is about 10,700 to 10,950 bytes
  on this machine.
- **After activity, the figure against the idle floor is not steady:** 501 to 2,587 bytes above
  it in ten runs, over the target in four of them. After the churn that follows it is the same
  within 52 bytes.
- **What moves is not MqttWire's.** A bare WebSocket endpoint that was given the same traffic,
  the "traffic floor", moved with MqttWire in the same run: by 44, 777 and 2,077 bytes per
  connection where MqttWire moved by −12, 199 and 2,006. Against that endpoint MqttWire is 509
  to 549 bytes above it idle and −68 to 479 after activity (section 3).
- **MqttWire's own objects** come to about 590 bytes per connection in a heap dump and do not
  grow with activity; what grows is Kestrel's pool of 4 KB transport buffers (section 5). Over
  the in-memory link a connection costs MqttWire 667 bytes idle and 595 to 632 after a burst;
  the server's own pools keep a fixed 0.5 to 0.65 MB after one (section 4).

**The owner decided on 2026-11-23 that the target is judged on MqttWire's own state, which
meets it before and after activity** (section 8).

## 2. How it was measured

One program, two roles in two processes, so that the clients' memory is never measured:

- `server` holds the connections on Kestrel, 127.0.0.1, and measures itself: two full blocking
  collections with the finalizers run between them, then `GC.GetTotalMemory(true)`.
- `run` starts a fresh `server` for each endpoint and each connection count, opens the
  connections, and reads the measurement once the server has been quiet for two sweeps and two
  readings in a row agree. A reading taken while the heap was still moving makes the run
  inconclusive; none of the runs below had one.

**"Per connection" is a slope:** (bytes at 12,000 connections − bytes at 3,000) ÷ 9,000. This
leaves out everything that does not grow with connections; "fixed" is the line's value at no
connections. The counts are those of
[GrainWire's step 0](https://github.com/mo-esmp/grainwire/blob/main/docs/step-0-idle-memory.md).
The program refuses a large count below four times the small one.

| Endpoint | What it is |
|---|---|
| Floor | A bare ASP.NET Core WebSocket endpoint: sub-protocol `mqtt`, keep-alive off, parked in a zero-byte read. Its connections send nothing. |
| Traffic floor | The same endpoint with no MQTT code, made to play MqttWire's side of the activity from fixed bytes. It answers the publish at QoS 2 and the PUBREL with four bytes each, and sends every connection the burst's ten packets itself, byte for byte the ones MqttWire sends. The clients run the same code against both, so the bytes and their directions are the same. It keeps a table of its sockets, to reach them for the burst. |
| MqttWire | `MapMqttWire` with a handler that accepts everything, keeps no state and does nothing. |

MqttWire's states:

1. **Idle:** connected with a CONNECT, nothing else.
2. **After activity:** every connection published once at QoS 0 and once at QoS 2 (the full
   exchange); then the server sent every connection ten messages of 256 bytes at QoS 1, all at
   once, and each connection acknowledged them. Then quiet.
3. **After churn:** every connection closed with a DISCONNECT, then all opened again. This
   happens in the server that has already had the activity, so the state is "after activity,
   then churn", not churn alone.

Two things the design did not foresee:

- **The connections ping before every measurement.** A client that asks for no keep-alive is
  given 60 seconds by the server and dropped after 90 seconds of silence, so a connection that
  only sits there would be closed during the run. A connection is idle again as soon as its
  PINGRESP is written.
- **The sweep's time is read before the first forced collection.** A sweep that overlaps a
  blocking collection is stopped with the process and looks ten times slower than it is.

The machine: Windows 11 Pro 10.0.26200 on arm64, a virtual machine on Apple Silicon with 10
logical processors and 32 GB; .NET SDK 10.0.404, runtime and ASP.NET Core 10.0.15; a Release
build; server garbage collection, which is the Web SDK's default; console logging off.

## 3. The runs

Live managed bytes per connection, as bytes above the floor of the same run.

**Three runs of the final program,** with nothing else running on the machine:

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Floor, idle (bytes per connection) | 10,907 | 10,948 | 10,886 |
| Traffic floor, idle | +34 | +4 | +32 |
| Traffic floor, after activity | +810 | +48 | +2,109 |
| 1. MqttWire, idle | **+543** | **+513** | **+581** |
| 2. MqttWire, after activity | **+742** | **+501** | **+2,587** |
| 3. MqttWire, after activity, then churn | **+747** | **+501** | **+2,568** |
| MqttWire above the traffic floor, idle | 510 | 509 | 549 |
| MqttWire above the traffic floor, after activity | −68 | 453 | 479 |
| Kept after activity at 3,000 / at 12,000 (MB) | 1.0 / 2.7 | 1.9 / 1.8 | 2.7 / 19.9 |
| Working set per connection, MqttWire idle | 11,705 | 10,894 | 11,579 |
| One sweep at 12,000, idle / after activity (µs) | 1,458 / 759 | 1,077 / 1,748 | 1,217 / 840 |

**Seven earlier runs,** before the traffic floor existed; the floor and MqttWire were measured
by the same code. Run G also took the heap dumps of section 5 between its states; whether
taking a dump changes what the server keeps afterwards was not tested, and its own dumps say
that what activity added in it was the same pool blocks as everywhere else, so it is counted.

| | A | B | C | D | E | F | G |
|---|---|---|---|---|---|---|---|
| Floor, idle | 10,725 | 10,769 | 10,745 | 10,790 | 10,745 | 10,746 | 10,745 |
| 1. MqttWire, idle | +624 | +572 | +578 | +519 | +584 | +579 | +594 |
| 2. MqttWire, after activity | +841 | +1,025 | +859 | +1,057 | +968 | +945 | +1,251 |
| 3. After activity, then churn | +828 | +1,023 | +853 | +1,005 | +955 | +974 | +1,247 |

**The target against the idle floor, state by state, over all ten runs:**

| State | Above the idle floor | Floor + 1,024 |
|---|---|---|
| 1. Idle | 513 to 624 | **Met** in all ten. |
| 2. After activity | 501 to 2,587 | Over in four of ten (by 1, 33, 227 and 1,563 bytes). |
| 3. After activity, then churn | 501 to 2,568 | Over in two of ten. |

**What the figure after activity is made of:**

- **It is what was kept after the burst, and that is not steady.** At 12,000 connections the
  heap after activity was 1.8 to 19.9 MB larger than idle; at 3,000 it was 0.2 to 2.7 MB larger.
  As a slope that is −12 to 2,006 bytes per connection in the three final runs. So the second
  figure of 6.14, "what the buffer pool keeps after activity must not grow with connections",
  did not come out as wished: the burst reaches every connection in the same moment, and the
  more connections there are, the more buffers are out at once.
- **A bare endpoint with the same traffic keeps as much, in the same run.** The traffic floor's
  slope moved by 777, 44 and 2,077 bytes per connection where MqttWire's moved by 199, −12 and
  2,006. When one is large the other is. Against the traffic floor MqttWire is 509 to 549
  bytes above it idle and −68 to 479 after activity: no further above it after activity than
  before.
- **A two-point slope cannot pin down what a pool keeps;** it can only show that the amount
  varies from run to run and is not MqttWire's. Section 5 says what it is.
- **A busy machine keeps more.** In a set of runs during which other tests ran on the machine
  for part of the time, one read +2,718 after activity. That set is not among the figures.

**What churn itself does:** nothing measurable. Closing and opening every connection moved the
after-activity figure by −52 to +29 bytes per connection in the ten runs. So nothing piles up
in one cycle, and nothing "comes back" either: what the pool kept after the activity was still
there after the churn. Section 8 of the design asks for "live managed bytes coming back after
churn"; this benchmark does not show that, because its churn follows the activity in the same
server.

**Working set,** for information: about 10,000 to 12,000 bytes per connection idle, 19,000 to
24,000 after churn.

**One sweep at 12,000 connections** took 0.6 to 1.7 ms in the three final runs, and 0.5 to
6.8 ms over all the readings of the earlier ones. It runs every 2 seconds.

**With TLS in Kestrel** (one run of the final program, not decisive): the floor is 13,580
bytes per connection; MqttWire is +727 idle, +619 after activity and +804 after churn, and the
traffic floor moved by −191 bytes after activity. Two earlier TLS runs read floors of 13,480
and 13,525 and MqttWire at +556 and +511 idle, +623 and +523 after activity. What is kept
after activity grows far less with TLS, on both endpoints.

## 4. MqttWire's own share, over the in-memory link

`IdleCostTests` measures connections without Kestrel and without a WebSocket:

| Test | Measured | Bound |
|---|---|---|
| `Connect_TwoThousandIdleConnections_KeepAtMost768BytesEach` | 667 bytes per connection | 768 |
| `Activity_TwoThousandConnectionsHoldABurstAtOnceThenQuiet_KeepAtMost768BytesEach` | 595 to 632 bytes per connection | 768 |
| `Activity_FourThousandConnectionsHoldABurstAtOnce_ThePoolsKeepAtMostOneMebibyte` | 527 to 653 KB in all | 1 MiB |

- **A connection holds no more after activity than before it.** In the second test every
  connection holds its burst of ten unacknowledged messages at the same moment, as in the
  benchmark, and then acknowledges it.
- **The server's own pools keep a fixed amount.** They hold the exchange records and queues a
  busy connection borrows, at most 1,024 objects of each kind. After a burst that every
  connection held at once they are full: 0.5 to 0.65 MB, the same whether 2,000 or 4,096
  connections held it. The second test fills them before its first reading, so that their
  fixed share is not counted as the connections'; the third bounds it. Without that, 2,000
  connections read 982 bytes each, of which 314 were the pools'.
- The figures were read by lowering the tests' bounds for one run, which also showed that
  each test can fail.

An earlier version of the second test ran the activity one connection after the other, which a
single pooled object can serve; its figure, 612 to 668 bytes, said nothing about the pools.

## 5. Who owns the bytes

Four heap dumps of the server (`run --dumps <folder>`, `dotnet-gcdump` 10.0.750501): idle and
after activity, at 3,000 and at 12,000 connections, in run G. Per connection is the slope, by
type.

**What activity added** (625 bytes per connection in that run):

| Type | Bytes per connection | Objects per connection |
|---|---|---|
| `System.Byte[]` | 618 | 0.15 |
| `Microsoft.AspNetCore.MemoryPoolBlock` | 6 | 0.15 |
| `MqttWire.Server.Internal.ExchangeRecords`, their arrays and the pool's slots | 8 | |
| Everything else | −7 | |

The byte arrays are 4,120 bytes each and come one to a `MemoryPoolBlock`: they are the blocks of
Kestrel's memory pool, which its transport reads into and writes from. About one block for
every seven connections was still in the pool when the server was measured. MqttWire's 8 bytes
are its own pools between the two counts, both of which are past the pools' cap.

**What MqttWire holds for an idle connection** (the types that name MqttWire, 591 bytes):

| Type | Bytes |
|---|---|
| `MqttWireConnection` | 232 |
| The state machine of its run (`RunCoreAsync`) | 152 |
| The state machine of the link's parked wait (`WebSocketLink.WaitForDataAsync`) | 128 |
| `WebSocketLink` | 32 |
| The connection's entry in the client-id table | 36 |
| Its slot in the array the sweep walks | 11 |

This agrees with the benchmark's 513 to 624 bytes above the floor. The sizes are those the dump
reports; for arrays it reports them in size classes, so the array rows are approximate.
`dotnet-gcdump` warned that a dump of a .NET 10 process "may be incomplete on large heaps". The
dumps' totals agree with the server's own measurement in that run (11,317 bytes per connection
idle against 11,339), so little can be missing.

## 6. What this does not show

- **One machine,** a virtual one, on arm64. The floor is about 10,700 to 10,950 bytes here and
  was about 11,900 in GrainWire's step 0 on another machine; the two are not comparable byte
  for byte.
- **How long Kestrel's pool keeps its blocks.** Every state was measured 5 to 35 seconds after
  the traffic ended. Whether the blocks are released after minutes was not measured.
- **Why the amount kept varies so much from run to run** (1.8 to 19.9 MB at 12,000). It varies
  on the bare endpoint too; what decides it was not looked into.
- **A burst that does not reach every connection at once.** The activity is the hardest case
  for a pool. A host that sends to a few connections at a time was not measured.
- **Heap dumps of the traffic floor.** The by-type attribution is of MqttWire's server only.
- **Other platforms.** Linux was not measured; CI builds the benchmark and does not run it.
- **Many churn cycles,** and churn without activity before it.

## 7. Running it

```
dotnet run -c Release -f net10.0 --project bench/MqttWire.Bench -- run
dotnet run -c Release -f net10.0 --project bench/MqttWire.Bench -- run --tls
dotnet run -c Release -f net10.0 --project bench/MqttWire.Bench -- run --small 300 --large 1200
dotnet run -c Release -f net10.0 --project bench/MqttWire.Bench -- run --dumps <an absolute folder>
```

Since mo-esmp/mqttwire#47 the benchmark builds for .NET 10 and .NET 11, and `-f` chooses the
runtime of both of its processes. The numbers of this report are from .NET 10; the same run on
.NET 11 is in [.NET 11 against .NET 10](2026-11-net11.md).

A full run takes about two minutes and prints its table as Markdown. Run it with nothing else
at work on the machine. Counts below about 1,024 say little about states 2 and 3: the server's
own object pools hold at most 1,024 objects each, so they still grow with the count there.
`--dumps` needs `dotnet-gcdump` installed; a dump taken between two states may change the later
ones.

The program's verdict is for the idle state. It prints the other two states against the idle
floor for information, because there the target is judged on MqttWire's own state (section 8),
which a heap total cannot tell from what Kestrel's pool keeps. It exits with 1, and says
"inconclusive", when the floor or the idle state was measured while its heap was still moving.

## 8. The owner's decision

The design's target compares MqttWire with a floor that never sees traffic. Idle, that is a
fair comparison and the target is met with about 400 bytes to spare. After activity, the floor
has had none, and what is kept is Kestrel's. There were three ways to read it:

1. **The target is about MqttWire's own state,** which is what the design's goal 2 lists (no
   buffer, no queue storage, no timer, no cancellation source, no record). Then it is met before
   and after activity: 513 to 624 bytes idle in the benchmark; after activity, 595 to 632 in the
   unit tests, about 8 bytes of MqttWire's types added in the heap dumps, and no further above
   a bare endpoint with the same traffic than before it.
2. **The target stands as written,** against the idle floor. Then it is not reliably met after
   activity, and the next step is to look at what a host or MqttWire can do about Kestrel's
   pool, such as how long it keeps blocks and whether a burst can be spread.
3. **Measure more first:** the same states a few minutes later, to see whether the pool lets
   go, and a burst that reaches a part of the connections at a time.

**On 2026-11-23 the owner chose the first.** Section 6.14 of the design records it. The
decision was made on the seven earlier runs and the earlier unit-test figure; the three final
runs, the traffic floor and the corrected unit tests came after it, and are why this report
was rewritten. The owner confirmed the decision on the rewritten report the same day. What the
second and third would have looked at is still unmeasured (section 6).
