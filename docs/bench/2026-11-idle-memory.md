# What an idle connection costs MqttWire

Measured on 23 November 2026 with `bench/MqttWire.Bench`, as section 6.14 of
[the design](../specs/2026-11-19-mqttwire-design.md) asks. The target is that MqttWire adds no
more than 1 KB (1,024 bytes) to the floor for an idle connection, before and after activity
alike.

## 1. The result

- **Idle, the target is met in every run:** a connected, idle connection costs MqttWire
  **519 to 624 bytes** above the floor, which is about 10,750 bytes on this machine.
- **After activity, the target is not reliably met against the idle floor:** 841 to 1,251 bytes
  above it in seven runs, over the target in three of them (by 1, by 33 and by 227 bytes). After
  the churn that follows it is 828 to 1,247, over in one of the seven.
- **What is kept after activity is not MqttWire's.** MqttWire's own objects come to about 590
  bytes per connection and do not grow with activity (sections 4 and 5). What grows is Kestrel's
  pool of 4 KB transport buffers, which keeps blocks after a burst that reaches every connection
  at once. A bare WebSocket endpoint that echoed the same traffic kept between nothing and 3,000
  bytes per connection after it (section 3).

So the figure that decides, live managed bytes as a slope, says "met" for an idle connection
and "at the edge" for one that has been active, and the cause of the second is below MqttWire.
**The owner decided on 2026-11-23 that the target is judged on MqttWire's own state, which
meets it before and after activity** (section 8).

## 2. How it was measured

One program, two roles in two processes, so that the clients' memory is never measured:

- `server` holds the connections on Kestrel, 127.0.0.1, and measures itself: two full blocking
  collections with the finalizers run between them, then `GC.GetTotalMemory(true)`.
- `run` starts a fresh `server` for each endpoint and each connection count, opens the
  connections, and reads the measurement once the server has been quiet for two sweeps and two
  readings in a row agree.

**"Per connection" is a slope:** (bytes at 12,000 connections − bytes at 3,000) ÷ 9,000. This
leaves out everything that does not grow with connections; "fixed" is the line's value at no
connections. The counts are those of
[GrainWire's step 0](https://github.com/mo-esmp/grainwire/blob/main/docs/step-0-idle-memory.md).

| Endpoint | What it is |
|---|---|
| Floor | A bare ASP.NET Core WebSocket endpoint: sub-protocol `mqtt`, keep-alive off, parked in a zero-byte read. Its connections send nothing. |
| Echo floor | The same endpoint, except that it sends back what it receives. It is measured idle and after the bytes of MqttWire's activity went through it. |
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

**Three runs of the final program:**

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Floor, idle (bytes per connection) | 10,790 | 10,745 | 10,746 |
| Echo floor, idle | +12 | −6 | +2 |
| Echo floor, after activity | +965 | +3,047 | −187 |
| 1. MqttWire, idle | **+519** | **+584** | **+579** |
| 2. MqttWire, after activity | **+1,057** | **+968** | **+945** |
| 3. MqttWire, after churn | **+1,005** | **+955** | **+974** |
| Kept after activity at 3,000 / at 12,000 (MB) | 0.2 / 4.8 | 1.0 / 4.3 | 0.3 / 3.4 |
| Working set per connection, MqttWire idle | 11,032 | 10,274 | 12,058 |
| One sweep at 12,000, idle / after activity (µs) | 821 / 4,013 | 5,720 / 2,087 | 1,423 / 775 |

**Three earlier runs,** before the echo floor was added; the floor and MqttWire were measured by
the same code:

| | Run A | Run B | Run C |
|---|---|---|---|
| Floor, idle (bytes per connection) | 10,725 | 10,769 | 10,745 |
| 1. MqttWire, idle | +624 | +572 | +578 |
| 2. MqttWire, after activity | +841 | +1,025 | +859 |
| 3. MqttWire, after churn | +828 | +1,023 | +853 |
| Kept after activity at 3,000 / at 12,000 (MB) | 0.7 / 2.6 | 1.4 / 5.3 | 0.6 / 3.0 |

**A seventh run,** which also took the heap dumps of section 5 between its states: floor
10,745; idle +594; after activity +1,251; after churn +1,247; kept after activity 0.4 MB at
3,000 and 6.1 MB at 12,000. Whether taking a dump changes what the server keeps afterwards was
not tested. Its own dumps say that what activity added in that run was the same pool blocks as
everywhere else (section 5), so the run is counted.

**The target, state by state:**

| State | Above the idle floor, seven runs | Floor + 1,024 |
|---|---|---|
| 1. Idle | 519 to 624 | **Met** in all seven. |
| 2. After activity | 841 to 1,251 | **Not reliably met:** over in three of seven, by 1, 33 and 227 bytes. |
| 3. After activity, then churn | 828 to 1,247 | Over in one of seven, by 223 bytes; under by a single byte in another. |

**What churn itself does:** nothing measurable. Closing and opening every connection moved the
after-activity figure by −52 to +29 bytes per connection in the seven runs. So nothing piles up
in one cycle, and nothing "comes back" either: what the pool kept after the activity was still
there after the churn. Section 8 of the design asks for "live managed bytes coming back after
churn"; this benchmark does not show that, because its churn follows the activity in the same
server.

**The other two figures of 6.14:**

- **What the buffer pools keep after activity** was to be the part that does not grow with
  connections. It does grow here: 0.2 to 1.4 MB are kept at 3,000 connections and 2.6 to 6.1 MB
  at 12,000, which is 218 to 657 bytes per connection as a slope. The burst reaches every
  connection in the same moment, so the pool's peak grows with the count.
- **The echo floor shows the same thing without any MQTT code,** and shows how unsteady it is:
  after the same traffic its slope moved by +952, +3,053 and −189 bytes per connection in three
  runs. A two-point slope cannot pin down what a pool keeps; it can only bound it.
- **Working set,** for information: about 10,000 to 12,000 bytes per connection idle, 19,000 to
  23,500 after churn.

**One sweep at 12,000 connections** took 0.5 to 6.8 ms over the 21 readings of the seven runs
(three states each), on a virtual machine with other work running. It runs every 2 seconds.

**With TLS in Kestrel** (two runs, not decisive): the floor is 13,480 and 13,525 bytes per
connection. MqttWire is +556 and +511 idle, +623 and +523 after activity, +633 and +548 after
churn. What is kept after activity grows far less there: the slope after activity is 67 and 11
bytes from the idle slope.

## 4. MqttWire's own share, over the in-memory link

`IdleCostTests` measures a connection without Kestrel and without a WebSocket, 2,000 at a time:

| | Bytes per connection | Bound |
|---|---|---|
| Idle (`Connect_TwoThousandIdleConnections_KeepAtMost768BytesEach`) | 668 | 768 |
| Quiet after the benchmark's activity (`Activity_TwoThousandConnectionsThenQuiet_KeepAtMost768BytesEach`, new) | 612 to 668 | 768 |

A connection holds no more after activity than before it. The second test is new with this
report; lowering its bound to 600 made both fail, which is how the figures were read.

## 5. Who owns the bytes

Four heap dumps of the server (`run --dumps <folder>`, `dotnet-gcdump` 10.0.750501): idle and
after activity, at 3,000 and at 12,000 connections. Per connection is the slope, by type.

**What activity added** (625 bytes per connection in that run):

| Type | Bytes per connection | Objects per connection |
|---|---|---|
| `System.Byte[]` | 618 | 0.15 |
| `Microsoft.AspNetCore.MemoryPoolBlock` | 6 | 0.15 |
| `MqttWire.Server.Internal.ExchangeRecords`, their arrays and the pool's slots | 8 | |
| Everything else | −7 | |

The byte arrays are 4,120 bytes each and come one to a `MemoryPoolBlock`: they are the blocks of
Kestrel's memory pool, which its transport reads into and writes from. About one block for
every seven connections was still in the pool when the server was measured.

**What MqttWire holds for an idle connection** (the types that name MqttWire, 591 bytes):

| Type | Bytes |
|---|---|
| `MqttWireConnection` | 232 |
| The state machine of its run (`RunCoreAsync`) | 152 |
| The state machine of the link's parked wait (`WebSocketLink.WaitForDataAsync`) | 128 |
| `WebSocketLink` | 32 |
| The connection's entry in the client-id table | 36 |
| Its slot in the array the sweep walks | 11 |

This agrees with the benchmark's 519 to 624 bytes above the floor. The sizes are those the dump
reports; for arrays it reports them in size classes, so the array rows are approximate.
`dotnet-gcdump` warned that a dump of a .NET 10 process "may be incomplete on large heaps". The
dumps' totals agree with the server's own measurement in that run (11,317 bytes per connection
idle against 11,339), so little can be missing.

## 6. What this does not show

- **One machine,** a virtual one, on arm64. The floor is about 10,750 bytes here and was about
  11,900 in GrainWire's step 0 on another machine; the two are not comparable byte for byte.
- **How long Kestrel's pool keeps its blocks.** Every state was measured 5 to 35 seconds after
  the traffic ended. Whether the blocks are released after minutes was not measured.
- **A burst that does not reach every connection at once.** The activity is the hardest case
  for a pool. A host that sends to a few connections at a time was not measured.
- **Other platforms.** Linux was not measured; CI builds the benchmark and does not run it.
- **Many churn cycles.** One cycle cannot show whether anything piles up over many.

## 7. Running it

```
dotnet run -c Release --project bench/MqttWire.Bench -- run
dotnet run -c Release --project bench/MqttWire.Bench -- run --tls
dotnet run -c Release --project bench/MqttWire.Bench -- run --small 300 --large 1200
dotnet run -c Release --project bench/MqttWire.Bench -- run --dumps <an absolute folder>
```

A full run takes about two minutes and prints its table as Markdown. Counts below about 1,024
say little about states 2 and 3: the server's own object pools hold at most 1,024 objects each,
so they still grow with the count there. `--dumps` needs `dotnet-gcdump` installed; a dump taken
between two states may change the later ones.

The program's verdict is for the idle state. It prints the other two states against the idle
floor for information, because there the target is judged on MqttWire's own state (section 8),
which a heap total cannot tell from what Kestrel's pool keeps.

## 8. The owner's decision

The design's target compares MqttWire with a floor that never sees traffic. Idle, that is a
fair comparison and the target is met with about 400 bytes to spare. After activity, the floor
has had none, and what is kept is Kestrel's. There were three ways to read it:

1. **The target is about MqttWire's own state,** which is what the design's goal 2 lists (no
   buffer, no queue storage, no timer, no cancellation source, no record). Then it is met before
   and after activity: 519 to 624 bytes idle in the benchmark; after activity, 612 to 668 in the
   unit tests, and about 8 bytes of MqttWire's types added in the heap dumps.
2. **The target stands as written,** against the idle floor. Then it is not reliably met after
   activity, and the next step is to look at what a host or MqttWire can do about Kestrel's
   pool, such as how long it keeps blocks and whether a burst can be spread.
3. **Measure more first:** the same states a few minutes later, to see whether the pool lets
   go, and a burst that reaches a part of the connections at a time.

**On 2026-11-23 the owner chose the first.** Section 6.14 of the design records it. What the
second and third would have looked at is still unmeasured (section 6).
