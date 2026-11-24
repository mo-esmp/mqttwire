# QoS 1 latency in MqttWire and MQTTnet

MqttWire completed a QoS 1 publish **7.471 µs faster** than MQTTnet in the supplied BenchmarkDotNet run. The ordering reversed when each client published to its own subscription and waited for the message callback as well as the broker’s PUBACK. MqttWire took **7.564 µs longer** in that case. A follow-up probe reversed the ordering again, so the measurements do not establish a consistent round-trip disadvantage.

## Benchmark results

Both clients sent a 64-byte payload over a loopback WebSocket to the same MQTTnet broker implementation. The publish-only case used a topic with no subscribers. The self-echo case subscribed at QoS 1. Allocations include the client and the in-process broker. [Benchmark setup and methods](../../bench/MqttWire.Microbench/ClientComparisonBenchmarks.cs)

| Operation | MqttWire mean | MQTTnet mean | MqttWire allocated/op | MQTTnet allocated/op |
| --- | ---: | ---: | ---: | ---: |
| Publish and await the broker’s PUBACK | 86.198 µs | 93.669 µs | 1,464 B | 3,519 B |
| Publish and await both PUBACK and the echo callback | 108.264 µs | 100.700 µs | 2,341 B | 6,421 B |

BenchmarkDotNet flagged both MqttWire QoS 1 latency distributions as bimodal. The round-trip standard deviations were 7.643 µs for MqttWire and 5.943 µs for MQTTnet.

## Why the echo can change latency

A self-echo involves two acknowledgements. The broker acknowledges the original publish, and the subscribing client acknowledges the echoed publish. The benchmark ends once the original publish completes and the echo callback has signalled. The broker’s receipt of the client’s acknowledgement falls outside that completion condition.

MqttWire invokes the callback on its read loop and then awaits the acknowledgement write before processing another packet. If the echoed PUBLISH arrives first, the broker’s PUBACK for the original publish can sit behind that write. MQTTnet 5.2.0.1603 queues the echoed PUBLISH for a separate worker, which lets its receive loop dispatch the broker’s PUBACK while the worker handles the echo. This difference gives packet arrival order a way to affect the measured latency. [MqttWire inbound publish handler](../../src/MqttWire/MqttWireClient.Receive.cs) · [MqttWire read loop](../../src/MqttWire/MqttWireClient.ReadLoop.cs) · [MQTTnet receive worker](https://github.com/dotnet/MQTTnet/blob/14463d1ef9f1f119514185c1832905b91bff827d/Source/MQTTnet/MqttClient.cs#L614-L630)

## Follow-up timing probe

The probe rotated scenario order across 60 measured rounds of 1,000 sequential operations per scenario. All outgoing publishes used QoS 1.

| Scenario | MqttWire mean | MQTTnet mean |
| --- | ---: | ---: |
| Publish with no subscriber | 88.5 µs | 97.4 µs |
| Echo delivered at QoS 1 | 104.2 µs | 112.2 µs |
| Echo delivered at QoS 0 | 95.9 µs | 101.8 µs |

For the QoS 1 echo, MqttWire’s callback ran before the publishing caller resumed in 100% of samples, compared with 79.7% for MQTTnet. These application timestamps show a scheduling difference. They do not record packet arrival order, write duration, or lock contention, so the specific cause of the original 7.564 µs gap remains unconfirmed.

The original run and the probe source were in the local scratchpad when this report was written. [BenchmarkDotNet log](<C:/Users/mohsenesmailpour/AppData/Local/Temp/claude/25b444182b2a2d7f/3fc2315f-ba8d-4a53-8c30-a1d61c757d9b/scratchpad/bdn-full.log>) · [Probe source](<C:/Users/mohsenesmailpour/AppData/Local/Temp/claude/25b444182b2a2d7f/3fc2315f-ba8d-4a53-8c30-a1d61c757d9b/scratchpad/qos1probe/Program.cs>)

## Before: the client of `main`

Measured on commit 9aea873, before the answer batching, on a noisy VM (Apple Silicon, 10 cores, .NET 10.0.15, BenchmarkDotNet 0.15.8, default job). The burst and awaiting categories are new in this run; each burst is 50 QoS 1 publishes started together, and the awaiting categories repeat the round trip (100 times) or the burst (10 times).

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

// * Warnings *
MinIterationTime

### Writes per burst

MqttWire made 100 link writes per burst of 50 (one PUBLISH and one PUBACK each), in both runs of the burst benchmarks:

```
MqttWire writes per burst of 50, publishes included: 100.0
MqttWire writes per burst of 50, publishes included: 100.0
```
