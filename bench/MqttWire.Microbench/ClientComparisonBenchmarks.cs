using System.Buffers;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using MqttWire.Link;

namespace MqttWire.Microbench;

/// <summary>
/// Compares MqttWire's client with MQTTnet's client end to end: both connect over a real WebSocket on loopback to
/// the same MQTTnet broker, and each category times the same operation through both, with MqttWire as the
/// baseline, so the Ratio column reads how many times MqttWire's time MQTTnet takes. The broker, Kestrel and the
/// loopback network are in every number, on both sides alike, so the ratios understate the difference between the
/// clients themselves; <see cref="ClientBenchmarks"/> times MqttWire's client alone, over an in-memory link. Both
/// clients publish the same 64-byte payload; MQTTnet's messages are built once, in setup, as its cheapest use is.
/// Every publish's result is checked, and a failure throws, so that a refused or lost publish is never timed as a
/// finished one; a wait for a message also ends with an exception when the client's connection has ended, so that
/// a lost connection fails the benchmark instead of hanging it. The QoS 0 benchmarks time a batch of publishes to
/// the broker's receipt of the whole batch, the one completion point both clients share. The Allocated column counts
/// the benchmark process only, both clients and the broker, but the broker's share is the same on both sides. The
/// class is not sealed because BenchmarkDotNet generates a class that derives from it.
/// </summary>
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class ClientComparisonBenchmarks
{
    /// <summary>
    /// The category of publishing at QoS 0, timed to the broker's receipt.
    /// </summary>
    private const string PublishQoS0Category = "PublishQoS0";

    /// <summary>
    /// The category of publishing at QoS 1 and awaiting the PUBACK.
    /// </summary>
    private const string PublishQoS1Category = "PublishQoS1";

    /// <summary>
    /// The category of publishing at QoS 1 to a topic the client subscribes to, and awaiting the message back.
    /// </summary>
    private const string RoundTripQoS1Category = "RoundTripQoS1";

    /// <summary>
    /// The category of the same round trip, run in a loop of <see cref="AwaitingRun"/> by a caller that awaits each.
    /// </summary>
    private const string RoundTripQoS1AwaitingCategory = "RoundTripQoS1Awaiting";

    /// <summary>
    /// The category of <see cref="BurstSize"/> QoS 1 publishes started together and then awaited.
    /// </summary>
    private const string BurstQoS1Category = "BurstQoS1";

    /// <summary>
    /// The category of <see cref="AwaitingBursts"/> bursts, one after another.
    /// </summary>
    private const string BurstQoS1AwaitingCategory = "BurstQoS1Awaiting";

    /// <summary>
    /// The number of publishes in a burst.
    /// </summary>
    private const int BurstSize = 50;

    /// <summary>
    /// The number of QoS 0 publishes in one invocation of the QoS 0 benchmarks, which wait for the broker to have
    /// them all. Small enough that the client's outgoing limit is never near.
    /// </summary>
    private const int QoS0Batch = 100;

    /// <summary>
    /// The number of round trips in one invocation of the awaiting round-trip benchmarks.
    /// </summary>
    private const int AwaitingRun = 100;

    /// <summary>
    /// The number of bursts in one invocation of the awaiting burst benchmarks.
    /// </summary>
    private const int AwaitingBursts = 10;

    /// <summary>
    /// The topic of the plain publishes; nobody subscribes to it, so the broker forwards nothing.
    /// </summary>
    private const string SinkTopic = "bench/sink";

    /// <summary>
    /// The topic MqttWire's client subscribes to and publishes its round trips to.
    /// </summary>
    private const string WireEchoTopic = "bench/echo/wire";

    /// <summary>
    /// The topic MQTTnet's client subscribes to and publishes its round trips to.
    /// </summary>
    private const string NetEchoTopic = "bench/echo/net";

    /// <summary>
    /// The size of every payload, in bytes.
    /// </summary>
    private const int PayloadSize = 64;

    /// <summary>
    /// The keep-alive of both clients, MQTTnet's default; no benchmark iteration is long enough to see a PINGREQ.
    /// </summary>
    private static readonly TimeSpan s_keepAlive = TimeSpan.FromSeconds(15);

    /// <summary>
    /// The longest the setup waits for its first round trip; a working setup never waits this long.
    /// </summary>
    private static readonly TimeSpan s_failSafe = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Released once for every message MqttWire's client receives.
    /// </summary>
    private readonly SemaphoreSlim _wireEcho = new(0);

    /// <summary>
    /// Released once for every message MQTTnet's client receives.
    /// </summary>
    private readonly SemaphoreSlim _netEcho = new(0);

    /// <summary>
    /// The payload of every publish.
    /// </summary>
    private readonly byte[] _payload = new byte[PayloadSize];

    /// <summary>
    /// The pending publishes of an MqttWire burst.
    /// </summary>
    private readonly ValueTask<MqttRequestResult>[] _wireBurst = new ValueTask<MqttRequestResult>[BurstSize];

    /// <summary>
    /// The pending publishes of an MQTTnet burst.
    /// </summary>
    private readonly Task<MqttClientPublishResult>[] _netBurst = new Task<MqttClientPublishResult>[BurstSize];

    /// <summary>
    /// Whether MqttWire's client has lost its connection; a waiter that wakes and sees it throws.
    /// </summary>
    private volatile bool _wireClosed;

    /// <summary>
    /// Whether MQTTnet's client has lost its connection; a waiter that wakes and sees it throws.
    /// </summary>
    private volatile bool _netClosed;

    /// <summary>
    /// The connector that counts the writes of MqttWire's client.
    /// </summary>
    // Set in [GlobalSetup], before any benchmark runs, so it is never read as null.
    private CountingLinkConnector _connector = null!;

    /// <summary>
    /// The number of writes MqttWire's client had made when the setup ended.
    /// </summary>
    private long _setupWrites;

    /// <summary>
    /// The number of MqttWire bursts run so far.
    /// </summary>
    private long _bursts;

    /// <summary>
    /// The broker both clients connect to.
    /// </summary>
    // Set in [GlobalSetup], before any benchmark runs, so it is never read as null.
    private MqttNetBrokerHost _broker = null!;

    /// <summary>
    /// MqttWire's client, connected and subscribed to <see cref="WireEchoTopic"/>.
    /// </summary>
    // Set in [GlobalSetup], before any benchmark runs, so it is never read as null.
    private MqttWireClient _wire = null!;

    /// <summary>
    /// MQTTnet's client, connected and subscribed to <see cref="NetEchoTopic"/>.
    /// </summary>
    // Set in [GlobalSetup], before any benchmark runs, so it is never read as null.
    private IMqttClient _net = null!;

    /// <summary>
    /// MQTTnet's QoS 0 message to <see cref="SinkTopic"/>.
    /// </summary>
    // Set in [GlobalSetup], before any benchmark runs, so it is never read as null.
    private MqttApplicationMessage _netSinkQoS0 = null!;

    /// <summary>
    /// MQTTnet's QoS 1 message to <see cref="SinkTopic"/>.
    /// </summary>
    // Set in [GlobalSetup], before any benchmark runs, so it is never read as null.
    private MqttApplicationMessage _netSinkQoS1 = null!;

    /// <summary>
    /// MQTTnet's QoS 1 message to <see cref="NetEchoTopic"/>.
    /// </summary>
    // Set in [GlobalSetup], before any benchmark runs, so it is never read as null.
    private MqttApplicationMessage _netEchoQoS1 = null!;

    /// <summary>
    /// Starts the broker, connects both clients with the same keep-alive, subscribes each at QoS 1 to its own echo
    /// topic, and makes one round trip with each, so that a broken setup fails here rather than hanging a
    /// benchmark. The end of either client's connection is observed: it wakes that client's waits, which then
    /// throw.
    /// </summary>
    /// <returns>
    /// A task that completes when both clients are ready.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// A client could not connect or subscribe.
    /// </exception>
    /// <exception cref="TimeoutException">
    /// A first round trip did not come back within <see cref="s_failSafe"/>.
    /// </exception>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        new Random(1).NextBytes(_payload);
        _broker = await MqttNetBrokerHost.StartAsync().ConfigureAwait(false);

        _connector = new CountingLinkConnector(ClientWebSocketConnector.Instance);
        MqttWireClientOptions wireOptions = new()
        {
            Uri = _broker.WebSocketUri,
            ClientId = "bench-wire",
            KeepAlive = s_keepAlive,
        };
        _wire = new MqttWireClient(
            wireOptions,
            (in _) => _wireEcho.Release(),
            _ => OnWireClosed(),
            _connector,
            TimeProvider.System,
            ArrayPool<byte>.Shared);
        MqttConnectAck ack = await _wire.ConnectAsync().ConfigureAwait(false);
        if (!ack.IsConnected)
        {
            throw new InvalidOperationException($"MqttWire's client could not connect: {ack.Failure}.");
        }

        MqttRequestResult subscribed = await _wire.SubscribeAsync(WireEchoTopic, MqttQoS.AtLeastOnce).ConfigureAwait(false);
        if (!subscribed.IsSuccess)
        {
            throw new InvalidOperationException($"MqttWire's client could not subscribe: {subscribed.Status}.");
        }

        _net = new MqttClientFactory().CreateMqttClient();
        _net.ApplicationMessageReceivedAsync += _ =>
        {
            _netEcho.Release();
            return Task.CompletedTask;
        };
        _net.DisconnectedAsync += _ =>
        {
            OnNetClosed();
            return Task.CompletedTask;
        };
        MqttClientOptions netOptions = new MqttClientOptionsBuilder()
            .WithClientId("bench-net")
            .WithProtocolVersion(MqttProtocolVersion.V500)
            .WithKeepAlivePeriod(s_keepAlive)
            .WithWebSocketServer(options => options.WithUri(_broker.WebSocketUri.ToString()))
            .Build();
        await _net.ConnectAsync(netOptions).ConfigureAwait(false);
        await _net.SubscribeAsync(new MqttTopicFilterBuilder()
            .WithTopic(NetEchoTopic)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build()).ConfigureAwait(false);

        _netSinkQoS0 = CreateMessage(SinkTopic, MqttQualityOfServiceLevel.AtMostOnce);
        _netSinkQoS1 = CreateMessage(SinkTopic, MqttQualityOfServiceLevel.AtLeastOnce);
        _netEchoQoS1 = CreateMessage(NetEchoTopic, MqttQualityOfServiceLevel.AtLeastOnce);

        await RoundTripQoS1MqttWireAsync().AsTask().WaitAsync(s_failSafe).ConfigureAwait(false);
        await RoundTripQoS1MqttNetAsync().WaitAsync(s_failSafe).ConfigureAwait(false);
        _setupWrites = _connector.Writes;
    }

    /// <summary>
    /// Disconnects both clients and stops the broker.
    /// </summary>
    /// <returns>
    /// A task that completes when everything is closed.
    /// </returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_bursts > 0)
        {
            long writes = _connector.Writes;
            Console.WriteLine($"MqttWire writes per burst of {BurstSize}, publishes included: {(writes - _setupWrites) / (double)_bursts:F1}");
        }

        await _wire.DisposeAsync().ConfigureAwait(false);
        await _net.DisconnectAsync().ConfigureAwait(false);
        _net.Dispose();
        await _broker.DisposeAsync().ConfigureAwait(false);
        _wireEcho.Dispose();
        _netEcho.Dispose();
    }

    /// <summary>
    /// Publishes <see cref="QoS0Batch"/> times 64 bytes at QoS 0 with MqttWire's client, awaiting and checking each
    /// publish, and then waits until the broker has received them all.
    /// </summary>
    /// <returns>
    /// A task that completes when the broker has the whole batch.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// A publish failed, or the connection ended before the broker had the batch.
    /// </exception>
    [Benchmark(Baseline = true, OperationsPerInvoke = QoS0Batch)]
    [BenchmarkCategory(PublishQoS0Category)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask PublishQoS0MqttWireAsync()
    {
        _broker.ExpectPublishes(SinkTopic, QoS0Batch);
        for (int i = 0; i < QoS0Batch; i++)
        {
            Check(await _wire.PublishAsync(SinkTopic, _payload, MqttQoS.AtMostOnce).ConfigureAwait(false));
        }

        await _broker.WaitForExpectedPublishesAsync().ConfigureAwait(false);
        ThrowIfClosed(_wireClosed, "MqttWire's");
        ThrowIfBatchIncomplete(_broker.RemainingPublishes);
    }

    /// <summary>
    /// Publishes <see cref="QoS0Batch"/> times 64 bytes at QoS 0 with MQTTnet's client, awaiting and checking each
    /// publish, and then waits until the broker has received them all.
    /// </summary>
    /// <returns>
    /// A task that completes when the broker has the whole batch.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// A publish failed, or the connection ended before the broker had the batch.
    /// </exception>
    [Benchmark(OperationsPerInvoke = QoS0Batch)]
    [BenchmarkCategory(PublishQoS0Category)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask PublishQoS0MqttNetAsync()
    {
        _broker.ExpectPublishes(SinkTopic, QoS0Batch);
        for (int i = 0; i < QoS0Batch; i++)
        {
            Check(await _net.PublishAsync(_netSinkQoS0).ConfigureAwait(false));
        }

        await _broker.WaitForExpectedPublishesAsync().ConfigureAwait(false);
        ThrowIfClosed(_netClosed, "MQTTnet's");
        ThrowIfBatchIncomplete(_broker.RemainingPublishes);
    }

    /// <summary>
    /// Publishes 64 bytes at QoS 1 with MqttWire's client, awaits the broker's PUBACK and checks it. The pooling
    /// builder, used by its MQTTnet counterpart too, keeps the check from adding an allocation to either.
    /// </summary>
    /// <returns>
    /// A task that completes with the PUBACK.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The publish failed.
    /// </exception>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(PublishQoS1Category)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask PublishQoS1MqttWireAsync()
        => Check(await _wire.PublishAsync(SinkTopic, _payload, MqttQoS.AtLeastOnce).ConfigureAwait(false));

    /// <summary>
    /// Publishes 64 bytes at QoS 1 with MQTTnet's client, awaits the broker's PUBACK and checks it.
    /// </summary>
    /// <returns>
    /// A task that completes with the PUBACK.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The publish failed.
    /// </exception>
    [Benchmark]
    [BenchmarkCategory(PublishQoS1Category)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask PublishQoS1MqttNetAsync()
        => Check(await _net.PublishAsync(_netSinkQoS1).ConfigureAwait(false));

    /// <summary>
    /// Publishes 64 bytes at QoS 1 with MqttWire's client to its own echo topic, awaits and checks the PUBACK, and
    /// then awaits the broker's delivery of the same message back to the client, which acknowledges it.
    /// </summary>
    /// <returns>
    /// A task that completes when the message is back.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The publish failed, or the connection ended before the message was back.
    /// </exception>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(RoundTripQoS1Category)]
    public async ValueTask RoundTripQoS1MqttWireAsync()
    {
        Check(await _wire.PublishAsync(WireEchoTopic, _payload, MqttQoS.AtLeastOnce).ConfigureAwait(false));
        await _wireEcho.WaitAsync().ConfigureAwait(false);
        ThrowIfClosed(_wireClosed, "MqttWire's");
    }

    /// <summary>
    /// Publishes 64 bytes at QoS 1 with MQTTnet's client to its own echo topic, awaits and checks the PUBACK, and
    /// then awaits the broker's delivery of the same message back to the client, which acknowledges it.
    /// </summary>
    /// <returns>
    /// A task that completes when the message is back.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The publish failed, or the connection ended before the message was back.
    /// </exception>
    [Benchmark]
    [BenchmarkCategory(RoundTripQoS1Category)]
    public async Task RoundTripQoS1MqttNetAsync()
    {
        Check(await _net.PublishAsync(_netEchoQoS1).ConfigureAwait(false));
        await _netEcho.WaitAsync().ConfigureAwait(false);
        ThrowIfClosed(_netClosed, "MQTTnet's");
    }

    /// <summary>
    /// Runs <see cref="AwaitingRun"/> round trips with MqttWire's client, awaiting each before the next.
    /// </summary>
    /// <returns>
    /// A task that completes when the last message is back.
    /// </returns>
    [Benchmark(Baseline = true, OperationsPerInvoke = AwaitingRun)]
    [BenchmarkCategory(RoundTripQoS1AwaitingCategory)]
    public async ValueTask RoundTripQoS1AwaitingMqttWireAsync()
    {
        for (int i = 0; i < AwaitingRun; i++)
        {
            await RoundTripQoS1MqttWireAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Runs <see cref="AwaitingRun"/> round trips with MQTTnet's client, awaiting each before the next.
    /// </summary>
    /// <returns>
    /// A task that completes when the last message is back.
    /// </returns>
    [Benchmark(OperationsPerInvoke = AwaitingRun)]
    [BenchmarkCategory(RoundTripQoS1AwaitingCategory)]
    public async Task RoundTripQoS1AwaitingMqttNetAsync()
    {
        for (int i = 0; i < AwaitingRun; i++)
        {
            await RoundTripQoS1MqttNetAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Starts <see cref="BurstSize"/> QoS 1 publishes with MqttWire's client without awaiting, awaits them all, and
    /// then awaits the same number of echoes.
    /// </summary>
    /// <returns>
    /// A task that completes when every message is back.
    /// </returns>
    [Benchmark(Baseline = true, OperationsPerInvoke = BurstSize)]
    [BenchmarkCategory(BurstQoS1Category)]
    public ValueTask BurstQoS1MqttWireAsync() => BurstWireAsync();

    /// <summary>
    /// Starts <see cref="BurstSize"/> QoS 1 publishes with MQTTnet's client without awaiting, awaits them all, and
    /// then awaits the same number of echoes.
    /// </summary>
    /// <returns>
    /// A task that completes when every message is back.
    /// </returns>
    [Benchmark(OperationsPerInvoke = BurstSize)]
    [BenchmarkCategory(BurstQoS1Category)]
    public Task BurstQoS1MqttNetAsync() => BurstNetAsync();

    /// <summary>
    /// Runs <see cref="AwaitingBursts"/> bursts with MqttWire's client, one after another.
    /// </summary>
    /// <returns>
    /// A task that completes when the last burst is back.
    /// </returns>
    [Benchmark(Baseline = true, OperationsPerInvoke = AwaitingBursts * BurstSize)]
    [BenchmarkCategory(BurstQoS1AwaitingCategory)]
    public async ValueTask BurstQoS1AwaitingMqttWireAsync()
    {
        for (int i = 0; i < AwaitingBursts; i++)
        {
            await BurstWireAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Runs <see cref="AwaitingBursts"/> bursts with MQTTnet's client, one after another.
    /// </summary>
    /// <returns>
    /// A task that completes when the last burst is back.
    /// </returns>
    [Benchmark(OperationsPerInvoke = AwaitingBursts * BurstSize)]
    [BenchmarkCategory(BurstQoS1AwaitingCategory)]
    public async Task BurstQoS1AwaitingMqttNetAsync()
    {
        for (int i = 0; i < AwaitingBursts; i++)
        {
            await BurstNetAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Runs one burst with MqttWire's client.
    /// </summary>
    /// <returns>
    /// A task that completes when every publish is acknowledged and every echo has arrived.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// A publish failed, or the connection ended before every echo had arrived.
    /// </exception>
    private async ValueTask BurstWireAsync()
    {
        _bursts++;
        for (int i = 0; i < BurstSize; i++)
        {
            _wireBurst[i] = _wire.PublishAsync(WireEchoTopic, _payload, MqttQoS.AtLeastOnce);
        }

        for (int i = 0; i < BurstSize; i++)
        {
            Check(await _wireBurst[i].ConfigureAwait(false));
        }

        for (int i = 0; i < BurstSize; i++)
        {
            await _wireEcho.WaitAsync().ConfigureAwait(false);
            ThrowIfClosed(_wireClosed, "MqttWire's");
        }
    }

    /// <summary>
    /// Runs one burst with MQTTnet's client.
    /// </summary>
    /// <returns>
    /// A task that completes when every publish is acknowledged and every echo has arrived.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// A publish failed, or the connection ended before every echo had arrived.
    /// </exception>
    private async Task BurstNetAsync()
    {
        for (int i = 0; i < BurstSize; i++)
        {
            _netBurst[i] = _net.PublishAsync(_netEchoQoS1);
        }

        for (int i = 0; i < BurstSize; i++)
        {
            Check(await _netBurst[i].ConfigureAwait(false));
        }

        for (int i = 0; i < BurstSize; i++)
        {
            await _netEcho.WaitAsync().ConfigureAwait(false);
            ThrowIfClosed(_netClosed, "MQTTnet's");
        }
    }

    /// <summary>
    /// Checks the outcome of one of MqttWire's publishes.
    /// </summary>
    /// <param name="result">
    /// The outcome.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// The publish did not succeed: it was refused, as when the send queue is full, the connection was lost, or the
    /// broker answered with a failure.
    /// </exception>
    private static void Check(MqttRequestResult result)
    {
        if (!result.IsSuccess)
        {
            throw new InvalidOperationException($"MqttWire's publish failed: {result.Status}, {result.ReasonCode}.");
        }
    }

    /// <summary>
    /// Checks the outcome of one of MQTTnet's publishes.
    /// </summary>
    /// <param name="result">
    /// The outcome.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// The broker answered with a failure.
    /// </exception>
    private static void Check(MqttClientPublishResult result)
    {
        if (!result.IsSuccess)
        {
            throw new InvalidOperationException($"MQTTnet's publish failed: {result.ReasonCode}.");
        }
    }

    /// <summary>
    /// Fails a wait that was woken by the end of the client's connection rather than by what it waited for.
    /// </summary>
    /// <param name="closed">
    /// Whether the client's connection has ended.
    /// </param>
    /// <param name="client">
    /// Which client, for the message.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// The connection has ended.
    /// </exception>
    private static void ThrowIfClosed(bool closed, string client)
    {
        if (closed)
        {
            throw new InvalidOperationException($"{client} client lost its connection during the benchmark.");
        }
    }

    /// <summary>
    /// Fails a QoS 0 batch whose wait for the broker ended without the broker having received exactly the batch: the
    /// broker's waiter is shared, so the other client's lost connection, or a wake left over from it, can end the
    /// wait early.
    /// </summary>
    /// <param name="remaining">
    /// The publishes of the batch the broker has not received; 0 when the batch arrived whole.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="remaining"/> is not 0.
    /// </exception>
    private static void ThrowIfBatchIncomplete(int remaining)
    {
        if (remaining != 0)
        {
            throw new InvalidOperationException(
                $"The wait for the broker ended with {remaining} of {QoS0Batch} publishes not received.");
        }
    }

    /// <summary>
    /// Releases a semaphore once, so that a waiter wakes; does nothing once it is disposed, in cleanup.
    /// </summary>
    /// <param name="semaphore">
    /// The semaphore.
    /// </param>
    private static void Wake(SemaphoreSlim semaphore)
    {
        try
        {
            semaphore.Release();
        }
        catch (ObjectDisposedException)
        {
            // Cleanup has run: nobody waits any more.
        }
    }

    /// <summary>
    /// Records that MqttWire's client lost its connection and wakes whatever waits for it: an echo, or the broker's
    /// receipt of a QoS 0 batch. Called by the client once, when the connection ends, cleanup included.
    /// </summary>
    private void OnWireClosed()
    {
        _wireClosed = true;
        Wake(_wireEcho);
        _broker.WakePublishWaiter();
    }

    /// <summary>
    /// Records that MQTTnet's client lost its connection and wakes whatever waits for it: an echo, or the broker's
    /// receipt of a QoS 0 batch. Called by the client when the connection ends, cleanup included.
    /// </summary>
    private void OnNetClosed()
    {
        _netClosed = true;
        Wake(_netEcho);
        _broker.WakePublishWaiter();
    }

    /// <summary>
    /// Builds an MQTTnet message with the benchmark payload.
    /// </summary>
    /// <param name="topic">
    /// The topic.
    /// </param>
    /// <param name="qos">
    /// The QoS.
    /// </param>
    /// <returns>
    /// The message, reused for every publish.
    /// </returns>
    private MqttApplicationMessage CreateMessage(string topic, MqttQualityOfServiceLevel qos) => new MqttApplicationMessageBuilder()
        .WithTopic(topic)
        .WithPayload(_payload)
        .WithQualityOfServiceLevel(qos)
        .Build();
}
