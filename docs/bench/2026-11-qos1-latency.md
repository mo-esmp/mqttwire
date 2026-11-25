# QoS 1 latency in MqttWire and MQTTnet

The client's read loop used to wait for the write of its own PUBACK before it read the next packet. That wait was the cause of the blocking round trip's extra latency, which the 13 to 25 µs experiment below supports. The read loop now records its answers and writes them together when it would wait for the link (see [the design](../specs/2026-11-24-client-acknowledgement-batching-design.md)).

Measured at commit 8463064 against the client of commit 9aea873:

* **Blocking round trip:** 0.84 of its former time in BenchmarkDotNet and in the original probe, 0.81 in the committed probe, and 0.81 to 0.86 of MQTTnet's.
* **Bursts of 50 publishes:** 0.66 to 0.67 of the former time per message in BenchmarkDotNet, 0.62 to 0.64 in the original probe and 0.62 to 0.71 in the committed probe. Every instrument puts them at 0.43 to 0.52 of MQTTnet's. The link writes per burst fell from 100 to about 57.5.
* **Awaiting round trip:** no difference beyond this VM's noise, but about 222 B more per operation. [#13](https://github.com/mo-esmp/mqttwire/issues/13) tracks that allocation.
* **Plain QoS 0 and QoS 1 publishes:** no change.
* **Receiving one QoS 1 message on an in-memory link:** 9.5 ns slower (113.0 against 103.5 ns), still with no allocation.

## Cause

MqttWire invoked the callback on its read loop and then awaited the PUBACK write before it processed another packet. If the echoed PUBLISH arrived first, the broker's PUBACK for the original publish sat behind that write. MQTTnet 5.2.0.1603 queues the echoed PUBLISH for a separate worker, so its receive loop can dispatch the broker's PUBACK while the worker handles the echo. [MqttWire inbound publish handler](../../src/MqttWire/MqttWireClient.Receive.cs) · [MqttWire read loop](../../src/MqttWire/MqttWireClient.ReadLoop.cs) · [MQTTnet receive worker](https://github.com/dotnet/MQTTnet/blob/14463d1ef9f1f119514185c1832905b91bff827d/Source/MQTTnet/MqttClient.cs#L614-L630)

The cause is confirmed. Moving the PUBACK write off the read loop, with the number of writes unchanged, was 13 to 25 µs faster with a blocking caller, in four runs of four.

## What the first run showed

The first BenchmarkDotNet run had MqttWire's plain QoS 1 publish faster than MQTTnet's (86.198 against 93.669 µs). Its self-echo round trip was slower (108.264 against 100.700 µs, a gap of 7.564 µs). A follow-up probe did not reproduce that gap consistently, and neither does the "Before" BenchmarkDotNet table below (143.39 against 147.56 µs). So the gap exists on some runs only, and the experiment above is what ties it to the PUBACK write.

## Method

All numbers come from one noisy VM (Apple Silicon, 10 cores, .NET 10.0.15, BenchmarkDotNet 0.15.8, default job). Both clients talk over a loopback WebSocket to the same in-process MQTTnet broker, with a 64-byte payload. Each burst is 50 QoS 1 publishes started together. The awaiting categories repeat the round trip (100 times) or the burst (10 times) and await them. "Blocking" means the caller blocks on the round trip. Every publish result is checked, and a lost connection fails the benchmark instead of hanging it.

A QoS 0 operation is a batch of 100 publishes, timed until the broker has received all of them, and reported per publish. A QoS 0 publish completes once the client has handed the packet over, so timing only the calls would measure how fast the client fills its send queue.

"Before" is the client of commit 9aea873, measured with the benchmarks of commit 8463064. "After" is the client of commit 8463064. Allocations count the whole process, broker included.

## After: the client with the answer batching

### ClientBenchmarks.ReceiveQoS1 (in-memory link)

| Method      | Mean     | Error   | StdDev  | Allocated |
|------------ |---------:|--------:|--------:|----------:|
| ReceiveQoS1 | 113.0 ns | 1.33 ns | 1.18 ns |         - |

### Client comparison (loopback WebSocket, same MQTTnet broker)

| Method                             | Categories            | Mean      | Error    | StdDev   | Median    | Ratio | RatioSD | Gen0   | Gen1   | Allocated | Alloc Ratio |
|----------------------------------- |---------------------- |----------:|---------:|---------:|----------:|------:|--------:|-------:|-------:|----------:|------------:|
| BurstQoS1MqttWireAsync             | BurstQoS1             |  27.83 us | 0.677 us | 1.963 us |  27.74 us |  1.00 |    0.10 | 0.5078 |      - |    2155 B |        1.00 |
| BurstQoS1MqttNetAsync              | BurstQoS1             |  57.43 us | 1.257 us | 3.545 us |  57.14 us |  2.07 |    0.19 | 1.4063 | 0.2344 |    6011 B |        2.79 |
|                                    |                       |           |          |          |           |       |         |        |        |           |             |
| BurstQoS1AwaitingMqttWireAsync     | BurstQoS1Awaiting     |  26.59 us | 0.888 us | 2.603 us |  26.50 us |  1.01 |    0.14 | 0.5000 |      - |    2151 B |        1.00 |
| BurstQoS1AwaitingMqttNetAsync      | BurstQoS1Awaiting     |  57.20 us | 1.547 us | 4.514 us |  57.20 us |  2.17 |    0.27 | 1.3750 | 0.3750 |    6037 B |        2.81 |
|                                    |                       |           |          |          |           |       |         |        |        |           |             |
| PublishQoS0MqttWireAsync           | PublishQoS0           |  10.51 us | 0.207 us | 0.419 us |  10.49 us |  1.00 |    0.06 | 0.1953 |      - |     856 B |        1.00 |
| PublishQoS0MqttNetAsync            | PublishQoS0           |  21.07 us | 0.650 us | 1.853 us |  20.39 us |  2.01 |    0.19 | 0.2344 |      - |    1059 B |        1.24 |
|                                    |                       |           |          |          |           |       |         |        |        |           |             |
| PublishQoS1MqttWireAsync           | PublishQoS1           | 106.43 us | 1.986 us | 2.651 us | 106.06 us |  1.00 |    0.03 | 0.2441 |      - |    1432 B |        1.00 |
| PublishQoS1MqttNetAsync            | PublishQoS1           | 111.33 us | 3.272 us | 9.440 us | 111.55 us |  1.05 |    0.09 | 0.7324 |      - |    3457 B |        2.41 |
|                                    |                       |           |          |          |           |       |         |        |        |           |             |
| RoundTripQoS1MqttWireAsync         | RoundTripQoS1         | 121.05 us | 3.122 us | 8.650 us | 118.13 us |  1.00 |    0.10 | 0.4883 |      - |    2354 B |        1.00 |
| RoundTripQoS1MqttNetAsync          | RoundTripQoS1         | 148.67 us | 2.936 us | 4.656 us | 148.73 us |  1.23 |    0.09 | 1.4648 |      - |    6422 B |        2.73 |
|                                    |                       |           |          |          |           |       |         |        |        |           |             |
| RoundTripQoS1AwaitingMqttWireAsync | RoundTripQoS1Awaiting |  92.24 us | 1.837 us | 3.954 us |  93.05 us |  1.00 |    0.06 | 0.4688 |      - |    2500 B |        1.00 |
| RoundTripQoS1AwaitingMqttNetAsync  | RoundTripQoS1Awaiting | 105.61 us | 2.093 us | 4.768 us | 106.61 us |  1.15 |    0.07 | 1.5625 |      - |    6770 B |        2.71 |

BenchmarkDotNet flagged `BurstQoS1MqttWireAsync` as multimodal and `BurstQoS1AwaitingMqttNetAsync` as bimodal.

### Before and after, BenchmarkDotNet means

| Category              |    Before |     After | After/before | Allocated before | Allocated after | Change |
| --------------------- | --------: | --------: | -----------: | ---------------: | --------------: | -----: |
| RoundTripQoS1         | 143.39 µs | 121.05 µs |         0.84 |           2341 B |          2354 B |  +13 B |
| RoundTripQoS1Awaiting |  89.70 µs |  92.24 µs |         1.03 |           2278 B |          2500 B | +222 B |
| BurstQoS1             |  41.54 µs |  27.83 µs |         0.67 |           2121 B |          2155 B |  +34 B |
| BurstQoS1Awaiting     |  40.57 µs |  26.59 µs |         0.66 |           2184 B |          2151 B |  -33 B |
| PublishQoS1           | 106.89 µs | 106.43 µs |         1.00 |           1432 B |          1432 B |      0 |
| PublishQoS0           |  10.55 µs |  10.51 µs |         1.00 |            859 B |           856 B |   -3 B |

A QoS 0 publish sends no answer and receives none, so the batching cannot change it. It is in the table to show that the client's write path did not change either.

### The in-memory receive

`ReceiveQoS1` feeds one QoS 1 PUBLISH through an in-memory link. It takes 113.0 ns, against 103.5 ns before, and allocates nothing in both. The answer is now recorded and written later, instead of written at once to a link whose write completes at once. The extra 9.5 ns is the price of that bookkeeping where a write costs almost nothing. On a real socket the write it saves costs microseconds. An earlier run of the same benchmark gave 159.7 ns before, with a standard deviation of 43 ns. That figure was noise and is replaced here.

### Writes per burst

MqttWire made 100 link writes per burst of 50 before (one PUBLISH and one PUBACK each), in both benchmark processes. It now makes about 57.5: the 50 publishes and 7 to 8 writes for the answers. The two processes printed 57.8 and 57.5.

## Probes: before and after

BenchmarkDotNet runs each benchmark on its own, so the before and after clients are measured minutes apart on a VM whose speed drifts. An interleaved probe runs before, after and MQTTnet in one process, in an order that rotates every round: 30 rounds of 1,000 operations per scenario, after two warm-up passes, reporting the median of the rounds. Values are microseconds per operation; burst rows are per message.

There are two probes. The original probe, a single file outside the repository, gave the first measurement of the batching. The committed probe, [bench/MqttWire.Probe](../../bench/MqttWire.Probe/Program.cs), measures the same four scenarios. It also checks every result and fails instead of hanging. Its remarks give the commands that build it against the before client. The original probe ran over two days at commit 8463064. The committed probe ran on 2026-11-25, after the fix described [below](#why-the-first-runs-of-the-committed-probe-disagreed), at a commit whose client is the client of 8463064. The time columns are medians of the runs' medians, so they compare only within one table: the VM ran faster on 2026-11-25. The ratio columns give the median ratio and, in brackets, the lowest and highest run.

### Original probe (24 runs)

| Scenario            | Before |  After | MQTTnet | After/before      | After/MQTTnet     | Runs where after is faster |
| ------------------- | -----: | -----: | ------: | ----------------- | ----------------- | -------------------------: |
| Blocking round trip |  153.6 |  126.5 |   155.4 | 0.84 (0.73–1.01)  | 0.86 (0.61–0.92)  |                      23/24 |
| Awaiting round trip |   88.7 |   87.5 |    94.5 | 1.01 (0.73–1.23)  | 0.92 (0.70–1.31)  |                      11/24 |
| Blocking burst      |   37.8 |   23.0 |    51.0 | 0.62 (0.51–0.85)  | 0.44 (0.35–0.58)  |                      24/24 |
| Awaiting burst      |   37.4 |   22.9 |    52.5 | 0.64 (0.41–0.99)  | 0.43 (0.32–0.61)  |                      24/24 |

The same probe against the client of commit d7640a3, three runs, gave 0.84 for the blocking round trip and 0.65 and 0.67 for the bursts. The review fixes between d7640a3 and 8463064 cost nothing measurable.

### Committed probe (16 runs)

| Scenario            | Before |  After | MQTTnet | After/before      | After/MQTTnet     | Runs where after is faster |
| ------------------- | -----: | -----: | ------: | ----------------- | ----------------- | -------------------------: |
| Blocking round trip |  103.0 |   82.3 |    99.5 | 0.81 (0.77–0.92)  | 0.84 (0.78–0.93)  |                      16/16 |
| Awaiting round trip |   67.0 |   67.2 |    72.8 | 1.01 (0.94–1.03)  | 0.92 (0.87–0.94)  |                       7/16 |
| Blocking burst      |   34.0 |   23.5 |    46.0 | 0.71 (0.51–0.94)  | 0.52 (0.39–0.70)  |                      16/16 |
| Awaiting burst      |   31.9 |   19.6 |    44.2 | 0.62 (0.46–0.83)  | 0.44 (0.34–0.53)  |                      16/16 |

### Why the first runs of the committed probe disagreed

An earlier version of this report had 15 runs of the committed probe that put after against before at 0.88 to 1.04 for the bursts and 0.97 for the blocking round trip, against the original probe and BenchmarkDotNet. Its before contestant was running the branch's client, not the client of 9aea873. Its `using` aliases to the extern alias `before` stood outside the namespace `MqttWire.Probe`, and C# looks for a simple name such as `MqttWireClient` in the enclosing namespace `MqttWire`, which holds the branch's client, before it looks at aliases declared outside the namespace. The built probe confirmed it: the before contestant's client field had the type from `MqttWire`, not from `MqttWire.Before`. Six differences between the probes were tested before this was found, and none could have changed it.

The aliases now stand inside the namespace, and the built probe's before contestant holds the client of `MqttWire.Before`. The table above was measured with that fix.

## The awaiting round trip allocates more

`ReceiveQoS1` still allocates nothing. The awaiting round trip allocates about 222 B more per operation in BenchmarkDotNet (2,278 to 2,500 B). Plain publishes show no change.

This was investigated at commit d7640a3, whose client the review fixes after it did not change in this respect. The counts show that the caller's next publish usually holds the send lock first, and that the read loop's batched PUBACK then finds the lock held and waits for it asynchronously. The likely reason, which was not measured, is that the caller resumes and publishes again while the read loop is still flushing. Contended sends per operation went from 0.000 to about 0.98. The sends per operation did not change (2.000 both before and after). A counter-instrumented scratch build of both commits, driven by a probe of 20 rounds of 2,000 operations, took these counts. That probe also measured about 228 B more per operation. The blocking caller meets the lock far less (about 0.3 contended sends per operation) and allocates 13 to 33 B more.

Pooling the async builder of `SendAsync` removes only about 95 of the roughly 230 B and does not change the contention. The rest is probably the waiter that `SemaphoreSlim.WaitAsync` allocates on a contended wait; that part is inferred, not measured. The fix is left to [#13](https://github.com/mo-esmp/mqttwire/issues/13), "Make the client's send lock allocation-free". The send lock is outside the batching design (section 7, item 3).

## Before: the client of `main`

Measured on the client of commit 9aea873, before the answer batching, with the benchmarks of commit 8463064 and the setup above.

### ClientBenchmarks.ReceiveQoS1 (in-memory link)

| Method      | Mean     | Error   | StdDev  | Allocated |
|------------ |---------:|--------:|--------:|----------:|
| ReceiveQoS1 | 103.5 ns | 0.71 ns | 0.55 ns |         - |

### Client comparison (loopback WebSocket, same MQTTnet broker)

| Method                             | Categories            | Mean      | Error    | StdDev   | Median    | Ratio | RatioSD | Gen0   | Gen1   | Allocated | Alloc Ratio |
|----------------------------------- |---------------------- |----------:|---------:|---------:|----------:|------:|--------:|-------:|-------:|----------:|------------:|
| BurstQoS1MqttWireAsync             | BurstQoS1             |  41.54 us | 1.535 us | 4.501 us |  40.69 us |  1.01 |    0.15 | 0.4688 |      - |    2121 B |        1.00 |
| BurstQoS1MqttNetAsync              | BurstQoS1             |  58.82 us | 2.419 us | 6.500 us |  57.24 us |  1.43 |    0.22 | 1.4063 | 0.0781 |    6011 B |        2.83 |
|                                    |                       |           |          |          |           |       |         |        |        |           |             |
| BurstQoS1AwaitingMqttWireAsync     | BurstQoS1Awaiting     |  40.57 us | 0.966 us | 2.819 us |  40.25 us |  1.00 |    0.10 | 0.5000 |      - |    2184 B |        1.00 |
| BurstQoS1AwaitingMqttNetAsync      | BurstQoS1Awaiting     |  55.53 us | 2.015 us | 5.845 us |  54.93 us |  1.38 |    0.17 | 1.2500 |      - |    6134 B |        2.81 |
|                                    |                       |           |          |          |           |       |         |        |        |           |             |
| PublishQoS0MqttWireAsync           | PublishQoS0           |  10.55 us | 0.211 us | 0.517 us |  10.43 us |  1.00 |    0.07 | 0.1953 |      - |     859 B |        1.00 |
| PublishQoS0MqttNetAsync            | PublishQoS0           |  20.36 us | 0.575 us | 1.667 us |  20.16 us |  1.93 |    0.18 | 0.2344 |      - |    1059 B |        1.23 |
|                                    |                       |           |          |          |           |       |         |        |        |           |             |
| PublishQoS1MqttWireAsync           | PublishQoS1           | 106.89 us | 2.095 us | 5.840 us | 106.71 us |  1.00 |    0.08 | 0.2441 |      - |    1432 B |        1.00 |
| PublishQoS1MqttNetAsync            | PublishQoS1           | 109.05 us | 2.776 us | 8.098 us | 106.31 us |  1.02 |    0.09 | 0.7324 |      - |    3456 B |        2.41 |
|                                    |                       |           |          |          |           |       |         |        |        |           |             |
| RoundTripQoS1MqttWireAsync         | RoundTripQoS1         | 143.39 us | 2.841 us | 6.968 us | 143.16 us |  1.00 |    0.07 | 0.4883 |      - |    2341 B |        1.00 |
| RoundTripQoS1MqttNetAsync          | RoundTripQoS1         | 147.56 us | 2.942 us | 3.721 us | 148.18 us |  1.03 |    0.05 | 1.4648 |      - |    6411 B |        2.74 |
|                                    |                       |           |          |          |           |       |         |        |        |           |             |
| RoundTripQoS1AwaitingMqttWireAsync | RoundTripQoS1Awaiting |  89.70 us | 1.783 us | 5.174 us |  90.56 us |  1.00 |    0.08 | 0.4688 |      - |    2278 B |        1.00 |
| RoundTripQoS1AwaitingMqttNetAsync  | RoundTripQoS1Awaiting | 106.92 us | 2.036 us | 4.382 us | 107.53 us |  1.20 |    0.09 | 1.5625 |      - |    6770 B |        2.97 |

BenchmarkDotNet flagged `BurstQoS1MqttWireAsync`, `PublishQoS0MqttWireAsync` and `RoundTripQoS1MqttWireAsync` as multimodal and `PublishQoS0MqttNetAsync` as bimodal.

### Writes per burst

MqttWire made 100 link writes per burst of 50 (one PUBLISH and one PUBACK each), in both runs of the burst benchmarks:

```
MqttWire writes per burst of 50, publishes included: 100.0
MqttWire writes per burst of 50, publishes included: 100.0
```
