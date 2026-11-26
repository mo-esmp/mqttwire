# QoS 1 latency in MqttWire and MQTTnet

The client's read loop used to wait for the write of its own PUBACK before it read the next packet. That wait was the cause of the blocking round trip's extra latency, which the 13 to 25 µs experiment below supports. The read loop now records its answers and writes them together when it would wait for the link (see [the design](../specs/2026-11-24-client-acknowledgement-batching-design.md)).

Measured at commit 8463064 against the client of commit 9aea873:

* **Blocking round trip:** 0.84 of its former time in BenchmarkDotNet and in the original probe, 0.81 in the committed probe, and 0.81 to 0.86 of MQTTnet's.
* **Bursts of 50 publishes:** 0.66 to 0.67 of the former time per message in BenchmarkDotNet, 0.62 to 0.64 in the original probe and 0.62 to 0.71 in the committed probe. Every instrument puts them at 0.43 to 0.52 of MQTTnet's. The link writes per burst fell from 100 to about 57.5.
* **Awaiting round trip:** no difference beyond this VM's noise, but about 222 B more per operation. [#13](https://github.com/mo-esmp/mqttwire/issues/13) took that allocation away again; see [the send path](#the-send-path-of-13-before-and-after).
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

## The send path of #13: before and after

[#13](https://github.com/mo-esmp/mqttwire/issues/13) replaced the client's `SemaphoreSlim` send lock with a send path that queues a write which finds another in progress, and hands the read loop's answers over without waiting for their write ([the design](../specs/2026-11-25-client-send-path-design.md), section 11 gives the method).

* **Awaiting round trip:** 238 B less per operation in the probe (2,539.5 to 2,301.7 B) and 235 to 237 B less in BenchmarkDotNet (2,506–2,507 to 2,270–2,271 B). In the same probe the client of 9aea873, before #12, allocates 2,306.5–2,307.4 B, so the bytes #12 added are gone. Time: 0.98–0.99 of before in the probe, 0.96 and 0.98 in BenchmarkDotNet.
* **Bursts of 50:** 182 to 241 B less per message across both instruments, since 50 publishes started together no longer wait for the lock. Faster in every probe run and every BenchmarkDotNet run (0.35 to 0.90 of before in the probe).
* **Blocking round trip:** 57 to 94 B less per operation; time 0.97 to 1.02 of before in the probe, 0.98 and 1.00 in BenchmarkDotNet.
* **No row allocates more or is slower beyond the noise.** The worst rows are `ReceiveQoS0`, 2.9 ns slower in one pair of runs and 1.7 ns faster in the other, and the comparison's `PublishQoS1`, 1.006 and 0.98 of before.
* **The waits still happen and cost nothing.** A scratch build counts 0.987 sends per awaiting round trip that find a write in progress, 0.92 to 0.94 of them the read loop's answers queued behind the caller's next publish, against 0.97 to 0.98 waits on the lock before.
* **More link writes per burst:** 64.2 to 64.5 per burst of 50, against 57.5 to 57.8. The read loop no longer waits for its answers' write, so it hands them over in more, smaller writes: about 14 answer writes per burst, against about 7.5.

### Method

Before is the client of `main` at de7af74; after is the client of this branch at e5f4586. Both were measured on 2026-11-26, between 10:19 and 11:11, on one noisy VM: Windows 11 Pro on Parallels (Apple Silicon, 10 cores, 32 GB), .NET SDK 10.0.404, .NET 10.0.15, BenchmarkDotNet 0.15.8.

* **The probe** ([bench/MqttWire.Probe](../../bench/MqttWire.Probe/Program.cs) of e5f4586, unchanged), built as its remarks say with the before contestant from a worktree at de7af74 under the assembly name `MqttWire.Before`: 3 runs of 30 rounds of 1,000 operations per scenario. Bytes per operation are `GC.GetTotalAllocatedBytes(precise: true)` over each round's timed operations, process-wide, broker included; the broker is the same for every contestant, so the differences between the columns are the clients'. The same probe was built a second time with the client of 9aea873 as before, for 3 more runs. Reflection on each built probe showed the before contestant's `_client` field typed `MqttWire.MqttWireClient` from the assembly `MqttWire.Before`, with a `_sendLock` field and no `MqttWire.Client.SendQueue` type in it, and the after contestant's from `MqttWire`, with `SendQueue`. The 9aea873 build had no `MqttWire.Client.PendingAnswers` type, as expected before #12.
* **BenchmarkDotNet:** `ClientBenchmarks` and `ClientComparisonBenchmarks`, default job, `[MemoryDiagnoser]`, filtered with `--filter "MqttWire.Microbench.ClientBenchmarks.*" "MqttWire.Microbench.ClientComparisonBenchmarks.*"`. The benchmarks are the same at both commits. Two pairs of runs in the order before, after, before, after, each run 9 to 14 minutes long.
* **Contended sends:** a scratch copy of e5f4586, never committed, counting under the send path's lock the writes handed over, those that found a write in progress (queued), the answers writes among both, and the holds of the lock that count answers down. Its probe ran after and MQTTnet only, twice, and divided the counts by the operations of all its rounds, warm-up included.

### Probe, after against de7af74 (3 runs)

Microseconds per operation (per message for a burst). The first three columns are the medians of the three runs' medians; the ratio is the median ratio of the runs and, in brackets, the lowest and highest.

| Scenario            | Before |  After | MQTTnet | After/before     | After/MQTTnet    |
| ------------------- | -----: | -----: | ------: | ---------------- | ---------------- |
| Blocking round trip |   73.1 |   74.6 |    87.9 | 1.01 (0.97–1.02) | 0.85 (0.84–0.87) |
| Awaiting round trip |   61.2 |   60.7 |    67.2 | 0.99 (0.98–0.99) | 0.90 (0.89–0.90) |
| Blocking burst      |   31.1 |   16.3 |    68.8 | 0.54 (0.35–0.56) | 0.23 (0.23–0.25) |
| Awaiting burst      |   20.5 |   16.1 |    67.1 | 0.79 (0.47–0.90) | 0.24 (0.24–0.27) |

Bytes per operation, the runs' medians:

| Scenario            | Before (3 runs)        | After (3 runs)         | After − before        | MQTTnet |
| ------------------- | ---------------------- | ---------------------- | --------------------- | ------: |
| Blocking round trip | 2448.4, 2447.3, 2444.9 | 2390.4, 2385.4, 2388.0 | −58.0, −61.9, −56.9   |  6434.3 |
| Awaiting round trip | 2539.1, 2539.7, 2539.5 | 2301.6, 2301.8, 2301.7 | −237.5, −237.9, −237.8 |  6788.1 |
| Blocking burst      | 2180.1, 2186.8, 2177.3 | 1987.4, 1992.3, 1986.2 | −192.7, −194.5, −191.1 |  6092.1 |
| Awaiting burst      | 2208.5, 2209.9, 2206.4 | 1990.3, 1985.2, 1983.6 | −218.2, −224.7, −222.8 |  6133.9 |

The MQTTnet column is the median of the three runs.

The round trips are the only scenarios close enough to judge against noise: the blocking round trip's ratio runs 0.97 to 1.02, so 1.01 is within it, and the awaiting round trip is below 1 in every run. The burst ratios swing between 0.35 and 0.90 because the before client's burst times swing (29.4 to 44.6 µs blocking, 16.9 to 34.3 µs awaiting), while the after client's stay within 15.3 to 16.7 µs.

### Probe, after against 9aea873, before #12 (3 runs)

| Scenario            | 9aea873 µs | After µs | After/9aea873    | 9aea873 B/op (3 runs)  | After B/op (3 runs)    |
| ------------------- | ---------: | -------: | ---------------- | ---------------------- | ---------------------- |
| Blocking round trip |       99.7 |     82.2 | 0.82 (0.80–0.84) | 2439.2, 2439.7, 2439.5 | 2392.1, 2398.3, 2386.4 |
| Awaiting round trip |       60.7 |     60.5 | 1.00 (0.99–1.01) | 2307.2, 2306.5, 2307.4 | 2301.3, 2301.2, 2301.3 |
| Blocking burst      |       63.3 |     16.5 |  0.26 (0.18–0.27) | 2190.8, 2192.2, 2187.9 | 1991.6, 1989.0, 1988.0 |
| Awaiting burst      |       47.6 |     15.4 |  0.32 (0.30–0.37) | 2193.2, 2190.7, 2191.8 | 1985.0, 1981.9, 1978.3 |

The awaiting round trip allocates 5 to 6 B less than before #12 in the same probe, so the roughly 230 B that #12 added are gone. The absolute bytes are not those the design expected. Design section 11 put the bytes of before #12 at 2,141 to 2,187 B, taken with the scratch probe of #12 (20 rounds of 2,000 operations, with counters). After allocates 2,301 B here, 114 to 160 B above that range. Part of the gap is the process: the counting probe below, which runs two contestants instead of three, measured the same client at 2,261 to 2,262 B, 40 B below this probe. The remaining 75 to 120 B between the design's figures and either probe of this report is unexplained. It was not re-measured: the scratch probe of #12 was not rerun, and no allocation trace was taken. The conclusion rests on the comparison within one probe, where after and 9aea873 allocate the same bytes.

### BenchmarkDotNet, before and after (two pairs)

| Row                   | Before 1 | After 1 | Before 2 | After 2 | After/before | Allocated before | Allocated after |
| --------------------- | -------: | ------: | -------: | ------: | -----------: | ---------------: | --------------: |
| RoundTripQoS1         | 73.24 µs | 71.97 µs | 70.91 µs | 71.11 µs | 0.98, 1.00 | 2383, 2356 B | 2289, 2291 B |
| RoundTripQoS1Awaiting | 62.74 µs | 60.32 µs | 61.07 µs | 59.88 µs | 0.96, 0.98 | 2507, 2506 B | 2270, 2271 B |
| BurstQoS1             | 56.28 µs | 35.05 µs | 65.66 µs | 34.80 µs | 0.62, 0.53 | 2149, 2146 B | 1954, 1964 B |
| BurstQoS1Awaiting     | 55.65 µs | 31.02 µs | 55.56 µs | 32.62 µs | 0.56, 0.59 | 2180, 2183 B | 1948, 1942 B |
| PublishQoS1           | 74.26 µs | 74.72 µs | 74.12 µs | 72.80 µs | 1.006, 0.98 | 1432, 1432 B | 1432, 1432 B |
| PublishQoS0           |  6.79 µs |  6.34 µs |  6.51 µs |  6.32 µs | 0.93, 0.97 |  859, 859 B |  860, 859 B |
| ReceiveQoS1           | 104.59 ns | 96.29 ns | 105.03 ns | 97.05 ns | 0.92, 0.92 | - | - |
| ReceiveQoS0           |  68.34 ns | 71.27 ns |  61.32 ns | 59.63 ns | 1.04, 0.97 | - | - |
| PublishQoS0 (in-memory) | 62.91 ns | 53.70 ns | 62.39 ns | 62.57 ns | 0.85, 1.003 | - | - |
| PublishQoS1AwaitAck   | 2,278.54 ns | 2,194.05 ns | 2,114.98 ns | 2,022.64 ns | 0.96, 0.96 | 32, 32 B | 32, 32 B |

The first six rows are `ClientComparisonBenchmarks` (loopback WebSocket, the in-process MQTTnet broker, allocations process-wide); the last four are `ClientBenchmarks` (in-memory link). The 1 B of `PublishQoS0` in the first pair is the broker's share of a process-wide count, and the second pair has none. `ReceiveQoS0` writes nothing, so the send path is not in it; its 2.9 ns in the first pair turned into 1.7 ns faster in the second. The burst rows are this VM's noisiest: BenchmarkDotNet flagged `BurstQoS1MqttWireAsync` bimodal in both before runs and `BurstQoS1AwaitingMqttWireAsync` bimodal in the first, with standard deviations of 12.6 to 20.3 µs before and 7.9 to 8.8 µs after. The before bursts also ran slower than the 27.83 and 26.59 µs measured for the same client after #12 above, on another day.

MQTTnet's rows did not change beyond their own noise (for example the awaiting round trip 69.47 and 69.09 µs before, 68.19 and 68.31 µs after, 6,770 to 6,772 B in all four runs).

### The extra hold of the lock

Each answers write now takes the send path's lock once more when it ends, to count the waiting answers down. The only direct data are the counts in the counting probe: 1.0 holds per round trip and 0.27 to 0.28 per burst message. No cost is visible in the net result, but the count-down cannot be measured on its own: no instrument isolates it. `ReceiveQoS1`, which writes one answer per operation to an in-memory link, is 8.3 and 8.0 ns faster than before, and the round trips and bursts are faster or within noise. Those are the net effect of the whole branch, which includes the count-down.

### Contended sends (scratch build, 2 runs)

| Scenario            | Sends per op | Found a write in progress | Answers writes per op | Answers queued per op | Count-down holds per op |
| ------------------- | -----------: | ------------------------: | --------------------: | --------------------: | ----------------------: |
| Blocking round trip | 2.000, 2.000 |              0.220, 0.212 |          1.000, 1.000 |          0.012, 0.018 |            1.003, 1.003 |
| Awaiting round trip | 2.000, 2.000 |              0.987, 0.987 |          1.000, 1.000 |          0.920, 0.935 |            0.999, 0.999 |
| Blocking burst      | 1.272, 1.276 |              0.908, 0.825 |          0.272, 0.276 |          0.252, 0.253 |            0.269, 0.273 |
| Awaiting burst      | 1.279, 1.277 |              1.008, 1.047 |          0.279, 0.277 |          0.268, 0.268 |            0.274, 0.270 |

A burst operation is one message. The awaiting round trip still meets a write in progress on almost every operation, as it met the lock before (0.97 to 0.98 contended sends per operation, measured for #12), but in the same probe it allocates the bytes of 9aea873, where it met none. The waits are still there; they no longer cost anything.

### Writes per burst

BenchmarkDotNet's burst benchmarks print the link writes per burst of 50: 57.8 and 57.5 before in both runs, 64.5 and 64.5 after in the first, 64.5 and 64.2 in the second. The counting probe agrees: 0.27 to 0.28 answers writes per message, about 14 per burst. Before, the read loop waited for each answers write, and the answers that arrived meanwhile went into the next; now it hands them over and reads on, so each write carries fewer. The bursts are faster and allocate less all the same.

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
