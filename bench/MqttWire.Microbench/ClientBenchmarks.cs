using System.Buffers;
using BenchmarkDotNet.Attributes;

namespace MqttWire.Microbench;

/// <summary>
/// Measures the client's hot paths over a <see cref="LoopbackLink"/>: publishing at QoS 0, publishing at QoS 1 and
/// awaiting the PUBACK, and receiving a PUBLISH at QoS 0 and at QoS 1 (with the PUBACK the client writes). The
/// client is connected in <see cref="SetupAsync"/>, so only the publish or the receive is timed. The receives and
/// the QoS 0 publish run synchronously on the benchmark's thread; awaiting a PUBACK includes the asynchronous
/// continuation of the request's completion, which runs on the thread pool. The Allocated column is reported for
/// information; that the hot paths allocate nothing is asserted by the test project, not here. The class is not
/// sealed because BenchmarkDotNet generates a class that derives from it.
/// </summary>
[MemoryDiagnoser]
public class ClientBenchmarks
{
    /// <summary>
    /// The topic of every publish.
    /// </summary>
    private const string Topic = "bench/topic";

    /// <summary>
    /// A PUBLISH at QoS 0 to "a/b" with the payload "hi", as the server plays it back.
    /// </summary>
    private static readonly byte[] s_publishQoS0 = [0x30, 0x08, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x00, 0x68, 0x69];

    /// <summary>
    /// A PUBLISH at QoS 1 to "a/b" with the payload "hi" and packet identifier 1, as the server plays it back.
    /// </summary>
    private static readonly byte[] s_publishQoS1 = [0x32, 0x0A, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x00, 0x01, 0x00, 0x68, 0x69];

    /// <summary>
    /// The payload of every publish, 64 bytes.
    /// </summary>
    private readonly byte[] _payload = new byte[64];

    /// <summary>
    /// The server end of the client's link.
    /// </summary>
    private LoopbackLink _link = null!;

    /// <summary>
    /// The connected client.
    /// </summary>
    private MqttWireClient _client = null!;

    /// <summary>
    /// The number of messages the client handed to its handler; read so the handler does real work.
    /// </summary>
    private int _messages;

    /// <summary>
    /// Connects a client over a new loopback link, with the longest keep-alive, so that no PINGREQ runs during
    /// the measurement.
    /// </summary>
    /// <returns>
    /// A task that completes when the client is connected.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The loopback server did not accept the connection.
    /// </exception>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _link = new LoopbackLink();
        MqttWireClientOptions options = new()
        {
            Uri = new Uri("ws://loopback/mqtt"),
            ClientId = "bench",
            KeepAlive = TimeSpan.FromSeconds(ushort.MaxValue),
        };
        _client = new MqttWireClient(options, (in _) => _messages++, null, _link, TimeProvider.System, ArrayPool<byte>.Shared);
        MqttConnectAck ack = await _client.ConnectAsync().ConfigureAwait(false);
        if (!ack.IsConnected)
        {
            throw new InvalidOperationException($"The loopback server refused the connection: {ack.Failure}.");
        }
    }

    /// <summary>
    /// Disposes the client, which closes the link.
    /// </summary>
    /// <returns>
    /// A task that completes when the client is disposed.
    /// </returns>
    [GlobalCleanup]
    public async Task CleanupAsync() => await _client.DisposeAsync().ConfigureAwait(false);

    /// <summary>
    /// Publishes 64 bytes at QoS 0: the topic is checked and encoded, the packet encoded into a pooled buffer and
    /// written, and the buffer returned.
    /// </summary>
    /// <returns>
    /// The outcome, already completed.
    /// </returns>
    [Benchmark]
    public ValueTask<MqttRequestResult> PublishQoS0() => _client.PublishAsync(Topic, _payload, MqttQoS.AtMostOnce);

    /// <summary>
    /// Publishes 64 bytes at QoS 1 and waits for the PUBACK, which the loopback server hands to the read loop from
    /// the thread pool: the identifier is taken and freed, the read loop runs on a pool thread, and the request's
    /// completion continues on the thread pool.
    /// </summary>
    /// <returns>
    /// The outcome, completed by the PUBACK.
    /// </returns>
    [Benchmark]
    public ValueTask<MqttRequestResult> PublishQoS1AwaitAck() => _client.PublishAsync(Topic, _payload, MqttQoS.AtLeastOnce);

    /// <summary>
    /// Receives a QoS 0 PUBLISH: the read loop reads it into a pooled buffer, decodes it and hands it to the
    /// handler.
    /// </summary>
    /// <returns>
    /// The number of messages handled so far.
    /// </returns>
    [Benchmark]
    public int ReceiveQoS0()
    {
        _link.Deliver(s_publishQoS0);
        return _messages;
    }

    /// <summary>
    /// Receives a QoS 1 PUBLISH: as <see cref="ReceiveQoS0"/>, and the read loop then writes its PUBACK.
    /// </summary>
    /// <returns>
    /// The number of messages handled so far.
    /// </returns>
    [Benchmark]
    public int ReceiveQoS1()
    {
        _link.Deliver(s_publishQoS1);
        return _messages;
    }
}
