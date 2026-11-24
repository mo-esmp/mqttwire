# QoS 1 latency in MqttWire and MQTTnet

The client's read loop used to wait for the write of its own PUBACK before it read the next packet. That wait was the cause of the blocking round trip's extra latency, which the 13 to 25 µs experiment below supports. The read loop now records its answers and writes them together when it would wait for the link (see [the design](../specs/2026-11-24-client-acknowledgement-batching-design.md)). With a blocking caller, the round trip now takes 0.68 to 0.78 of its former time, and 0.69 to 0.87 of MQTTnet's. A burst of 50 publishes costs 0.61 to 0.76 of its former time per message. A caller that awaits its round trip sees no difference beyond this VM's noise, but allocates about 220 B more per operation. [#13](https://github.com/mo-esmp/mqttwire/issues/13) tracks that allocation.

## Cause

MqttWire invoked the callback on its read loop and then awaited the PUBACK write before it processed another packet. If the echoed PUBLISH arrived first, the broker's PUBACK for the original publish sat behind that write. MQTTnet 5.2.0.1603 queues the echoed PUBLISH for a separate worker, so its receive loop can dispatch the broker's PUBACK while the worker handles the echo. [MqttWire inbound publish handler](../../src/MqttWire/MqttWireClient.Receive.cs) · [MqttWire read loop](../../src/MqttWire/MqttWireClient.ReadLoop.cs) · [MQTTnet receive worker](https://github.com/dotnet/MQTTnet/blob/14463d1ef9f1f119514185c1832905b91bff827d/Source/MQTTnet/MqttClient.cs#L614-L630)

The cause is confirmed. Moving the PUBACK write off the read loop, with the number of writes unchanged, was 13 to 25 µs faster with a blocking caller, in four runs of four.

## What the first run showed

The first BenchmarkDotNet run had MqttWire's plain QoS 1 publish faster than MQTTnet's (86.198 against 93.669 µs). Its self-echo round trip was slower (108.264 against 100.700 µs, a gap of 7.564 µs). A follow-up probe did not reproduce that gap consistently. The probe that measured the batching shows the same mix for the blocking round trip before the change: MqttWire took 165.2, 165.0 and 167.2 µs against MQTTnet's 162.7, 147.9 and 152.9 µs, which is level in run 1 and slower in runs 2 and 3. The "Before" BenchmarkDotNet table below shows no gap (157.36 against 162.33 µs). So the gap exists on some runs only, and the experiment above is what ties it to the PUBACK write.

## Method

All numbers come from one noisy VM (Apple Silicon, 10 cores, .NET 10.0.15, BenchmarkDotNet 0.15.8, default job). Both clients talk over a loopback WebSocket to the same in-process MQTTnet broker, with a 64-byte payload. Each burst is 50 QoS 1 publishes started together. The awaiting categories repeat the round trip (100 times) or the burst (10 times) and await them. "Blocking" means the caller blocks on the round trip.

"Before" is the client of commit 9aea873, measured with the benchmarks added in commit 8d4328b. "After" is the client of commit d7640a3. Allocations count the whole process, broker included.

## After: the client with the answer batching

### ClientBenchmarks.ReceiveQoS1 (in-memory link)

| Method      | Mean     | Allocated |
|------------ |---------:|----------:|
| ReceiveQoS1 | 118.7 ns |         - |

Before: 159.7 ns, also with no allocation.

### Client comparison (loopback WebSocket, same MQTTnet broker)

| Method                             | Categories            | Mean      | Error    | StdDev    | Median    | Ratio | RatioSD | Gen0   | Gen1   | Allocated | Alloc Ratio |
|----------------------------------- |---------------------- |----------:|---------:|----------:|----------:|------:|--------:|-------:|-------:|----------:|------------:|
| BurstQoS1MqttWireAsync             | BurstQoS1             |  28.57 us | 1.087 us |  3.136 us |  28.40 us |  1.01 |    0.16 | 0.4688 |      - |    2161 B |        1.00 |
| BurstQoS1MqttNetAsync              | BurstQoS1             |  58.15 us | 1.210 us |  3.531 us |  57.94 us |  2.06 |    0.25 | 1.4063 |      - |    6033 B |        2.79 |
|                                    |                       |           |          |           |           |       |         |        |        |           |             |
| BurstQoS1AwaitingMqttWireAsync     | BurstQoS1Awaiting     |  26.45 us | 0.915 us |  2.670 us |  26.39 us |  1.01 |    0.14 | 0.5000 |      - |    2159 B |        1.00 |
| BurstQoS1AwaitingMqttNetAsync      | BurstQoS1Awaiting     |  60.81 us | 1.753 us |  5.059 us |  60.49 us |  2.32 |    0.30 | 1.3750 | 0.1250 |    6086 B |        2.82 |
|                                    |                       |           |          |           |           |       |         |        |        |           |             |
| PublishQoS0MqttWire                | PublishQoS0           |  11.44 us | 0.331 us |  0.954 us |  11.50 us |  1.01 |    0.12 | 0.1678 |      - |     696 B |        1.00 |
| PublishQoS0MqttNet                 | PublishQoS0           |  20.40 us | 0.406 us |  1.140 us |  20.21 us |  1.80 |    0.18 | 0.2136 |      - |     898 B |        1.29 |
|                                    |                       |           |          |           |           |       |         |        |        |           |             |
| PublishQoS1MqttWire                | PublishQoS1           | 110.89 us | 3.448 us |  9.781 us | 108.07 us |  1.01 |    0.12 | 0.2441 |      - |    1464 B |        1.00 |
| PublishQoS1MqttNet                 | PublishQoS1           | 116.03 us | 4.672 us | 13.177 us | 115.29 us |  1.05 |    0.15 | 0.7324 |      - |    3515 B |        2.40 |
|                                    |                       |           |          |           |           |       |         |        |        |           |             |
| RoundTripQoS1MqttWireAsync         | RoundTripQoS1         | 136.66 us | 7.091 us | 20.907 us | 129.20 us |  1.02 |    0.21 | 0.4883 |      - |    2357 B |        1.00 |
| RoundTripQoS1MqttNetAsync          | RoundTripQoS1         | 160.06 us | 6.013 us | 17.635 us | 152.07 us |  1.20 |    0.22 | 1.4648 |      - |    6422 B |        2.72 |
|                                    |                       |           |          |           |           |       |         |        |        |           |             |
| RoundTripQoS1AwaitingMqttWireAsync | RoundTripQoS1Awaiting |  94.03 us | 2.449 us |  6.866 us |  93.15 us |  1.01 |    0.10 | 0.4688 |      - |    2502 B |        1.00 |
| RoundTripQoS1AwaitingMqttNetAsync  | RoundTripQoS1Awaiting | 111.79 us | 3.082 us |  8.386 us | 109.96 us |  1.20 |    0.12 | 1.5625 |      - |    6770 B |        2.71 |

BenchmarkDotNet flagged two MqttWire distributions here as multimodal or bimodal: `BurstQoS1MqttWireAsync` and `BurstQoS1AwaitingMqttWireAsync`. The "before" table below was noisier still (standard deviations up to 27 µs), so the means of the two tables are not a reliable comparison. The probe below is the comparison to read.

### Allocation per operation, before and after

| Category              |  Before |   After | Change |
| --------------------- | ------: | ------: | -----: |
| RoundTripQoS1         | 2339 B  | 2357 B  |  +18 B |
| RoundTripQoS1Awaiting | 2280 B  | 2502 B  | +222 B |
| BurstQoS1             | 2211 B  | 2161 B  |  -50 B |
| BurstQoS1Awaiting     | 2157 B  | 2159 B  |   +2 B |
| PublishQoS1           | 1464 B  | 1464 B  |      0 |

### Writes per burst

MqttWire made 100 link writes per burst of 50 before (one PUBLISH and one PUBACK each). It now makes about 57.5: the 50 publishes and 7 to 8 writes for the answers. The two benchmark processes printed 57.8 and 57.5.

## Probe: medians, before and after

An interleaved probe ran before, after and MQTTnet in rotating order, 30 rounds of 1,000 operations per scenario, and took the median of the rounds. Values are microseconds per operation; burst rows are per message. Three runs.

| Scenario            | Run | Before |  After | MQTTnet | After/before | After/MQTTnet |
| ------------------- | --: | -----: | -----: | ------: | -----------: | ------------: |
| Blocking round trip |   1 |  165.2 |  112.1 |   162.7 |         0.68 |          0.69 |
|                     |   2 |  165.0 |  128.8 |   147.9 |         0.78 |          0.87 |
|                     |   3 |  167.2 |  124.1 |   152.9 |         0.74 |          0.81 |
| Awaiting round trip |   1 |   75.9 |   93.3 |   135.1 |         1.23 |          0.69 |
|                     |   2 |   84.5 |   82.1 |   105.0 |         0.97 |          0.78 |
|                     |   3 |  104.1 |  102.7 |    88.4 |         0.99 |          1.16 |
| Blocking burst      |   1 |   34.5 |   26.2 |    66.1 |         0.76 |          0.40 |
|                     |   2 |   43.8 |   27.2 |    55.4 |         0.62 |          0.49 |
|                     |   3 |   41.2 |   29.3 |    56.8 |         0.71 |          0.52 |
| Awaiting burst      |   1 |   37.7 |   24.3 |    59.0 |         0.65 |          0.41 |
|                     |   2 |   44.1 |   26.8 |    57.9 |         0.61 |          0.46 |
|                     |   3 |   38.3 |   26.2 |    62.6 |         0.68 |          0.42 |

What the probe shows:

* **Blocking round trip:** faster than before and than MQTTnet in all three runs.
* **Bursts:** 0.61 to 0.76 of before per message, in all three runs and for both caller styles. The writes fell from 100 to about 57.5 per burst, not to half.
* **Awaiting round trip:** mixed. Run 1 was 23% slower than before; runs 2 and 3 were equal. A second probe (20 rounds of 2,000 operations, three runs) gave after/before 1.01, 1.07 and 0.97. There is no latency difference beyond this VM's noise.

## The awaiting round trip allocates more

`ReceiveQoS1` still allocates nothing. The awaiting round trip allocates about 222 B more per operation in BenchmarkDotNet (2,280 to 2,502 B) and about 228 B more in the second probe. Plain publishes show no change.

The counts show that the caller's next publish usually holds the send lock first, and that the read loop's batched PUBACK then finds the lock held and waits for it asynchronously. The likely reason, which was not measured, is that the caller resumes and publishes again while the read loop is still flushing. Contended sends per operation went from 0.000 to about 0.98. The sends per operation did not change (2.000 both before and after). A counter-instrumented scratch build of both commits, driven by the second probe, took these counts. The blocking caller meets the lock far less (about 0.3 contended sends per operation) and allocates 18 to 33 B more.

Pooling the async builder of `SendAsync` removes only about 95 of the roughly 230 B and does not change the contention. The rest is probably the waiter that `SemaphoreSlim.WaitAsync` allocates on a contended wait; that part is inferred, not measured. The fix is left to [#13](https://github.com/mo-esmp/mqttwire/issues/13), "Make the client's send lock allocation-free". The send lock is outside the batching design (section 7, item 3).

## Before: the client of `main`

Measured on the client of commit 9aea873, before the answer batching, with the benchmarks added in commit 8d4328b and the setup above.

### ClientBenchmarks.ReceiveQoS1 (in-memory link)

| Method      | Mean     | Error    | StdDev   | Median   | Allocated |
|------------ |---------:|---------:|---------:|---------:|----------:|
| ReceiveQoS1 | 159.7 ns | 14.78 ns | 42.88 ns | 143.3 ns |         - |

### Client comparison (loopback WebSocket, same MQTTnet broker)

| Method                             | Categories            | Mean      | Error     | StdDev    | Median    | Ratio  | RatioSD | Gen0   | Gen1   | Allocated | Alloc Ratio |
|----------------------------------- |---------------------- |----------:|----------:|----------:|----------:|-------:|--------:|-------:|-------:|----------:|------------:|
| BurstQoS1MqttWireAsync             | BurstQoS1             |  61.77 us |  4.316 us | 12.521 us |  59.64 us |   1.04 |    0.30 | 0.4688 |      - |    2211 B |        1.00 |
| BurstQoS1MqttNetAsync              | BurstQoS1             |  85.75 us |  3.456 us | 10.026 us |  87.06 us |   1.44 |    0.33 | 1.4063 | 0.1563 |    6158 B |        2.79 |
|                                    |                       |           |           |           |           |        |         |        |        |           |             |
| BurstQoS1AwaitingMqttWireAsync     | BurstQoS1Awaiting     |  40.65 us |  5.001 us | 14.746 us |  34.60 us |   1.12 |    0.55 | 0.4000 |      - |    2157 B |        1.00 |
| BurstQoS1AwaitingMqttNetAsync      | BurstQoS1Awaiting     |  72.78 us |  3.570 us | 10.413 us |  71.96 us |   2.01 |    0.69 | 1.4000 |      - |    6059 B |        2.81 |
|                                    |                       |           |           |           |           |        |         |        |        |           |             |
| PublishQoS0MqttWire                | PublishQoS0           |  11.17 us |  1.967 us |  5.800 us |  13.29 us |  73.28 |  184.57 |      - |      - |       5 B |        1.00 |
| PublishQoS0MqttNet                 | PublishQoS0           |  26.78 us |  1.141 us |  3.365 us |  26.69 us | 175.60 |  388.26 | 0.1831 |      - |     901 B |      180.20 |
|                                    |                       |           |           |           |           |        |         |        |        |           |             |
| PublishQoS1MqttWire                | PublishQoS1           | 149.88 us |  8.899 us | 26.240 us | 153.10 us |   1.03 |    0.26 |      - |      - |    1464 B |        1.00 |
| PublishQoS1MqttNet                 | PublishQoS1           | 119.53 us |  2.693 us |  7.508 us | 119.63 us |   0.82 |    0.16 | 0.7324 |      - |    3518 B |        2.40 |
|                                    |                       |           |           |           |           |        |         |        |        |           |             |
| RoundTripQoS1MqttWireAsync         | RoundTripQoS1         | 157.36 us |  3.128 us |  7.496 us | 156.53 us |   1.00 |    0.07 | 0.4883 |      - |    2339 B |        1.00 |
| RoundTripQoS1MqttNetAsync          | RoundTripQoS1         | 162.33 us |  3.089 us |  8.083 us | 161.78 us |   1.03 |    0.07 | 1.4648 |      - |    6423 B |        2.75 |
|                                    |                       |           |           |           |           |        |         |        |        |           |             |
| RoundTripQoS1AwaitingMqttWireAsync | RoundTripQoS1Awaiting | 147.63 us |  9.114 us | 26.872 us | 149.63 us |   1.04 |    0.29 | 0.4688 |      - |    2280 B |        1.00 |
| RoundTripQoS1AwaitingMqttNetAsync  | RoundTripQoS1Awaiting | 201.29 us | 12.901 us | 38.038 us | 197.19 us |   1.42 |    0.40 | 1.2500 |      - |    6797 B |        2.98 |

BenchmarkDotNet warned about `MinIterationTime` for `PublishQoS0MqttWire`: its fastest iteration took 785.5 µs, and BenchmarkDotNet recommends at least 100 ms per iteration (the default minimum iteration time) because shorter iterations make the timing less precise. The categories are compared within the same run, so the warning does not change the comparison.

### Writes per burst

MqttWire made 100 link writes per burst of 50 (one PUBLISH and one PUBACK each), in both runs of the burst benchmarks:

```
MqttWire writes per burst of 50, publishes included: 100.0
MqttWire writes per burst of 50, publishes included: 100.0
```
