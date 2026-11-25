using System.Buffers;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using MqttWire.Link;

namespace MqttWire.Microbench;

/// <summary>
/// Measures the client's read path over a real <see cref="WebSocketLink"/>: a <see cref="WebSocket"/> pair over
/// in-memory pipes, so the read loop waits in the WebSocket's zero-byte receive and wakes on a thread-pool thread
/// when the server sends, as it does over a network. Each operation sends one QoS 0 PUBLISH from the server end and
/// spins until the client's handler has counted it. Allocated counts every thread, so it includes what the read loop
/// allocates per wake. The class is not sealed because BenchmarkDotNet generates a class that derives from it.
/// </summary>
[MemoryDiagnoser]
public class WebSocketClientBenchmarks
{
    /// <summary>
    /// An accepting CONNACK without properties.
    /// </summary>
    private static readonly byte[] s_connAck = [0x20, 0x03, 0x00, 0x00, 0x00];

    /// <summary>
    /// A PUBLISH at QoS 0 to "a/b" with the payload "hi".
    /// </summary>
    private static readonly byte[] s_publishQoS0 = [0x30, 0x08, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x00, 0x68, 0x69];

    /// <summary>
    /// The buffer the server end receives the client's packets into.
    /// </summary>
    private readonly byte[] _serverBuffer = new byte[256];

    /// <summary>
    /// The server end of the pair.
    /// </summary>
    // Set in [GlobalSetup], before any benchmark runs, so it is never read as null.
    private WebSocket _server = null!;

    /// <summary>
    /// The client end of the pair, under the client's link.
    /// </summary>
    // Set in [GlobalSetup], before any benchmark runs, so it is never read as null.
    private WebSocket _clientSocket = null!;

    /// <summary>
    /// The connected client.
    /// </summary>
    // Set in [GlobalSetup], before any benchmark runs, so it is never read as null.
    private MqttWireClient _client = null!;

    /// <summary>
    /// The number of messages the client handed to its handler.
    /// </summary>
    private int _messages;

    /// <summary>
    /// Connects a client over a new WebSocket pair, with the longest keep-alive, so that no PINGREQ runs during the
    /// measurement; the server end reads the CONNECT and answers with a CONNACK.
    /// </summary>
    /// <returns>
    /// A task that completes when the client is connected.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The connect did not succeed.
    /// </exception>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        (WebSocket client, WebSocket server) = PipeWebSocketPair.Create();
        _clientSocket = client;
        _server = server;
        MqttWireClientOptions options = new()
        {
            Uri = new Uri("ws://pipe/mqtt"),
            ClientId = "bench",
            KeepAlive = TimeSpan.FromSeconds(ushort.MaxValue),
        };
        _client = new MqttWireClient(
            options,
            (in _) => Interlocked.Increment(ref _messages),
            null,
            new PairConnector(client),
            TimeProvider.System,
            ArrayPool<byte>.Shared);
        Task<MqttConnectAck> connect = _client.ConnectAsync();
        await _server.ReceiveAsync(_serverBuffer.AsMemory(), CancellationToken.None).ConfigureAwait(false);
        await _server.SendAsync(s_connAck, WebSocketMessageType.Binary, true, CancellationToken.None).ConfigureAwait(false);
        MqttConnectAck ack = await connect.ConfigureAwait(false);
        if (!ack.IsConnected)
        {
            throw new InvalidOperationException($"The connect failed: {ack.Failure}.");
        }
    }

    /// <summary>
    /// Disposes the client, answering its close from the server end so that the dispose does not wait for the close
    /// deadline, then disposes both sockets.
    /// </summary>
    /// <returns>
    /// A task that completes when everything is disposed.
    /// </returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        ValueTask dispose = _client.DisposeAsync();
        while (true)
        {
            ValueWebSocketReceiveResult result = await _server.ReceiveAsync(_serverBuffer.AsMemory(), CancellationToken.None)
                .ConfigureAwait(false);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                break;
            }
        }

        await _server.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None).ConfigureAwait(false);
        await dispose.ConfigureAwait(false);
        _server.Dispose();
        _clientSocket.Dispose();
    }

    /// <summary>
    /// Receives a QoS 0 PUBLISH over the WebSocket: the server end sends it, the read loop wakes from its wait,
    /// reads it into a pooled buffer, decodes it and hands it to the handler, and this spins until it has.
    /// </summary>
    /// <returns>
    /// The number of messages handled so far.
    /// </returns>
    [Benchmark]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> ReceiveQoS0OverWebSocket()
    {
        int before = Volatile.Read(ref _messages);
        await _server.SendAsync(s_publishQoS0, WebSocketMessageType.Binary, true, CancellationToken.None).ConfigureAwait(false);
        SpinWait spinner = default;
        while (Volatile.Read(ref _messages) == before)
        {
            spinner.SpinOnce(sleep1Threshold: -1);
        }

        return before + 1;
    }

    /// <summary>
    /// A connector that hands out one <see cref="WebSocketLink"/> over the client end of the pair.
    /// </summary>
    /// <param name="socket">
    /// The client end of the pair.
    /// </param>
    private sealed class PairConnector(WebSocket socket) : IMqttLinkConnector
    {
        /// <summary>
        /// Hands out a new link over the socket, at once.
        /// </summary>
        /// <param name="options">
        /// Not used.
        /// </param>
        /// <param name="cancellationToken">
        /// Not used.
        /// </param>
        /// <returns>
        /// A completed task with the link.
        /// </returns>
        public ValueTask<LinkConnectResult> ConnectAsync(MqttWireClientOptions options, CancellationToken cancellationToken)
            => new(new LinkConnectResult(new WebSocketLink(socket, dropWritesAfterClose: true), null));
    }
}
