using System.Buffers;
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
/// The Allocated column counts the benchmark process only, both clients and the broker, but the broker's share is
/// the same on both sides. The class is not sealed because BenchmarkDotNet generates a class that derives from it.
/// </summary>
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class ClientComparisonBenchmarks
{
    /// <summary>
    /// The category of publishing at QoS 0.
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
    /// The connector that counts the writes of MqttWire's client.
    /// </summary>
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
    private MqttNetBrokerHost _broker = null!;

    /// <summary>
    /// MqttWire's client, connected and subscribed to <see cref="WireEchoTopic"/>.
    /// </summary>
    private MqttWireClient _wire = null!;

    /// <summary>
    /// MQTTnet's client, connected and subscribed to <see cref="NetEchoTopic"/>.
    /// </summary>
    private IMqttClient _net = null!;

    /// <summary>
    /// MQTTnet's QoS 0 message to <see cref="SinkTopic"/>.
    /// </summary>
    private MqttApplicationMessage _netSinkQoS0 = null!;

    /// <summary>
    /// MQTTnet's QoS 1 message to <see cref="SinkTopic"/>.
    /// </summary>
    private MqttApplicationMessage _netSinkQoS1 = null!;

    /// <summary>
    /// MQTTnet's QoS 1 message to <see cref="NetEchoTopic"/>.
    /// </summary>
    private MqttApplicationMessage _netEchoQoS1 = null!;

    /// <summary>
    /// Starts the broker, connects both clients with the same keep-alive, subscribes each at QoS 1 to its own echo
    /// topic, and makes one round trip with each, so that a broken setup fails here rather than hanging a
    /// benchmark.
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
            onClosed: null,
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
    /// Publishes 64 bytes at QoS 0 with MqttWire's client; completes once the packet is written to the WebSocket.
    /// </summary>
    /// <returns>
    /// The outcome.
    /// </returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(PublishQoS0Category)]
    public ValueTask<MqttRequestResult> PublishQoS0MqttWire() => _wire.PublishAsync(SinkTopic, _payload, MqttQoS.AtMostOnce);

    /// <summary>
    /// Publishes 64 bytes at QoS 0 with MQTTnet's client; completes once the packet is written to the WebSocket.
    /// </summary>
    /// <returns>
    /// The outcome.
    /// </returns>
    [Benchmark]
    [BenchmarkCategory(PublishQoS0Category)]
    public Task<MqttClientPublishResult> PublishQoS0MqttNet() => _net.PublishAsync(_netSinkQoS0);

    /// <summary>
    /// Publishes 64 bytes at QoS 1 with MqttWire's client and awaits the broker's PUBACK.
    /// </summary>
    /// <returns>
    /// The outcome, completed by the PUBACK.
    /// </returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(PublishQoS1Category)]
    public ValueTask<MqttRequestResult> PublishQoS1MqttWire() => _wire.PublishAsync(SinkTopic, _payload, MqttQoS.AtLeastOnce);

    /// <summary>
    /// Publishes 64 bytes at QoS 1 with MQTTnet's client and awaits the broker's PUBACK.
    /// </summary>
    /// <returns>
    /// The outcome, completed by the PUBACK.
    /// </returns>
    [Benchmark]
    [BenchmarkCategory(PublishQoS1Category)]
    public Task<MqttClientPublishResult> PublishQoS1MqttNet() => _net.PublishAsync(_netSinkQoS1);

    /// <summary>
    /// Publishes 64 bytes at QoS 1 with MqttWire's client to its own echo topic, awaits the PUBACK, and then
    /// awaits the broker's delivery of the same message back to the client, which acknowledges it.
    /// </summary>
    /// <returns>
    /// A task that completes when the message is back.
    /// </returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(RoundTripQoS1Category)]
    public async ValueTask RoundTripQoS1MqttWireAsync()
    {
        await _wire.PublishAsync(WireEchoTopic, _payload, MqttQoS.AtLeastOnce).ConfigureAwait(false);
        await _wireEcho.WaitAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Publishes 64 bytes at QoS 1 with MQTTnet's client to its own echo topic, awaits the PUBACK, and then awaits
    /// the broker's delivery of the same message back to the client, which acknowledges it.
    /// </summary>
    /// <returns>
    /// A task that completes when the message is back.
    /// </returns>
    [Benchmark]
    [BenchmarkCategory(RoundTripQoS1Category)]
    public async Task RoundTripQoS1MqttNetAsync()
    {
        await _net.PublishAsync(_netEchoQoS1).ConfigureAwait(false);
        await _netEcho.WaitAsync().ConfigureAwait(false);
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
    private async ValueTask BurstWireAsync()
    {
        _bursts++;
        for (int i = 0; i < BurstSize; i++)
        {
            _wireBurst[i] = _wire.PublishAsync(WireEchoTopic, _payload, MqttQoS.AtLeastOnce);
        }

        for (int i = 0; i < BurstSize; i++)
        {
            await _wireBurst[i].ConfigureAwait(false);
        }

        for (int i = 0; i < BurstSize; i++)
        {
            await _wireEcho.WaitAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Runs one burst with MQTTnet's client.
    /// </summary>
    /// <returns>
    /// A task that completes when every publish is acknowledged and every echo has arrived.
    /// </returns>
    private async Task BurstNetAsync()
    {
        for (int i = 0; i < BurstSize; i++)
        {
            _netBurst[i] = _net.PublishAsync(_netEchoQoS1);
        }

        for (int i = 0; i < BurstSize; i++)
        {
            await _netBurst[i].ConfigureAwait(false);
        }

        for (int i = 0; i < BurstSize; i++)
        {
            await _netEcho.WaitAsync().ConfigureAwait(false);
        }
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
