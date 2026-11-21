using System.Buffers;
using Microsoft.Extensions.Time.Testing;
using MqttWire.Codec;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Client;

/// <summary>
/// A client over in-memory links with a fake clock, and what it reported, for the client tests. The options are
/// <c>ws://test/mqtt</c>, client id <c>c1</c>, user <c>u</c>, password <c>p</c> and keep-alive 15 s unless
/// <see cref="Create"/> is told otherwise.
/// </summary>
internal sealed class ClientHarness
{
    private readonly List<byte> _written = [];

    /// <summary>
    /// Creates the harness around a client that is not yet connected.
    /// </summary>
    /// <param name="connector">
    /// The connector the client opens its links with.
    /// </param>
    /// <param name="time">
    /// The fake clock the client uses.
    /// </param>
    /// <param name="client">
    /// The client.
    /// </param>
    /// <param name="closed">
    /// The list the client's <c>OnClosed</c> adds to.
    /// </param>
    private ClientHarness(MemoryLinkConnector connector, FakeTimeProvider time, MqttWireClient client, List<MqttClientCloseInfo> closed)
    {
        Connector = connector;
        Time = time;
        Client = client;
        Closed = closed;
    }

    /// <summary>
    /// Gets the connector the client opens its links with.
    /// </summary>
    public MemoryLinkConnector Connector { get; }

    /// <summary>
    /// Gets the link of the latest connect.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// No link was opened yet.
    /// </exception>
    public MemoryLink Link => Connector.Links.Count > 0
        ? Connector.Links[^1]
        : throw new InvalidOperationException("No link was opened yet.");

    /// <summary>
    /// Gets the fake clock the client uses; nothing happens in real time.
    /// </summary>
    public FakeTimeProvider Time { get; }

    /// <summary>
    /// Gets the client.
    /// </summary>
    public MqttWireClient Client { get; }

    /// <summary>
    /// Gets every report the client made through <c>OnClosed</c>, oldest first.
    /// </summary>
    public List<MqttClientCloseInfo> Closed { get; }

    /// <summary>
    /// Creates a client over in-memory links.
    /// </summary>
    /// <param name="keepAlive">
    /// The keep-alive, or <see langword="null"/> for 15 s.
    /// </param>
    /// <param name="pool">
    /// The pool the client rents from, or <see langword="null"/> for the shared pool.
    /// </param>
    /// <param name="onMessage">
    /// The message handler, or <see langword="null"/> for none.
    /// </param>
    /// <param name="onClosed">
    /// A callback the client's <c>OnClosed</c> calls after adding to <see cref="Closed"/>, or <see langword="null"/>.
    /// </param>
    /// <param name="maxIncomingPacketBytes">
    /// The largest packet the client accepts, or <see langword="null"/> for the default.
    /// </param>
    /// <returns>
    /// The harness.
    /// </returns>
    public static ClientHarness Create(
        TimeSpan? keepAlive = null,
        ArrayPool<byte>? pool = null,
        MqttMessageHandler? onMessage = null,
        Action<MqttClientCloseInfo>? onClosed = null,
        int? maxIncomingPacketBytes = null)
    {
        MqttWireClientOptions options = new()
        {
            Uri = new Uri("ws://test/mqtt"),
            ClientId = "c1",
            UserName = "u",
            Password = "p",
            KeepAlive = keepAlive ?? TimeSpan.FromSeconds(15),
            MaxIncomingPacketBytes = maxIncomingPacketBytes ?? 1_048_576,
        };
        List<MqttClientCloseInfo> closed = [];
        MemoryLinkConnector connector = new();
        FakeTimeProvider time = new();
        MqttWireClient client = new(
            options,
            onMessage,
            info =>
            {
                closed.Add(info);
                onClosed?.Invoke(info);
            },
            connector,
            time,
            pool ?? ArrayPool<byte>.Shared);
        return new ClientHarness(connector, time, client, closed);
    }

    /// <summary>
    /// Starts a connect and delivers the server's CONNACK to the link it opened.
    /// </summary>
    /// <param name="connAck">
    /// The CONNACK bytes the server sends.
    /// </param>
    /// <returns>
    /// The connect's task.
    /// </returns>
    public Task<MqttConnectAck> ConnectAsync(byte[] connAck)
    {
        Task<MqttConnectAck> connect = Client.ConnectAsync();
        Link.Deliver(connAck);
        return connect;
    }

    /// <summary>
    /// Takes the next whole packet the client wrote.
    /// </summary>
    /// <returns>
    /// The packet's bytes, fixed header included.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The client has not written a whole packet that was not already taken.
    /// </exception>
    public byte[] NextPacket()
    {
        _written.AddRange(Link.TakeWritten());
        byte[] written = [.. _written];
        if (PacketHeader.TryReadPacket(written, out PacketHeader header) != OperationStatus.Done)
        {
            throw new InvalidOperationException("The client has not written a whole packet.");
        }

        _written.RemoveRange(0, header.TotalLength);
        return written.AsSpan(0, header.TotalLength).ToArray();
    }
}
