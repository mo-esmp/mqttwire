using Microsoft.AspNetCore.Http;
using MqttWire.Link;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <summary>
/// One client connected to the server, as the host sees it. Methods may be called from any thread.
/// </summary>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// The server the connection belongs to: its handler, options, clock, pool and registry.
    /// </summary>
    private readonly MqttWireServer _server;

    /// <summary>
    /// The link, or <see langword="null"/> until <see cref="RunAsync"/> has it; set before the connection takes its
    /// slot, so the sweep never sees it unset.
    /// </summary>
    private IMqttLink? _link;

    /// <summary>
    /// The read buffer; held only while bytes are being read and handled. A mutable struct changed in place, so the
    /// explicit empty start tells the analyzer it is meant to be assigned through its members.
    /// </summary>
    private PacketReadBuffer _readBuffer = default;

    /// <summary>
    /// When the current state was entered, a timestamp of the server's clock; 0 before the accept.
    /// </summary>
    private long _stateEnteredAt;

    /// <summary>
    /// The state; it moves forward only, by <see cref="Interlocked.CompareExchange{T}(ref T, T, T)"/> where two
    /// threads may race.
    /// </summary>
    private volatile ConnectionState _state;

    /// <summary>
    /// Creates a connection in the Accepted state, before its WebSocket is accepted.
    /// </summary>
    /// <param name="server">
    /// The server the connection belongs to.
    /// </param>
    /// <param name="httpContext">
    /// The context of the WebSocket request.
    /// </param>
    internal MqttWireConnection(MqttWireServer server, HttpContext httpContext)
    {
        _server = server;
        HttpContext = httpContext;
    }

    /// <summary>
    /// Gets the client identifier; empty until the CONNECT has been read.
    /// </summary>
    public string ClientId { get; internal set; } = string.Empty;

    /// <summary>
    /// Gets the object the host returned from <see cref="IMqttWireHandler.OnConnectAsync"/>; the server never
    /// looks at it.
    /// </summary>
    public object? State { get; }

    /// <summary>
    /// Gets the context of the WebSocket request.
    /// </summary>
    internal HttpContext HttpContext { get; }

    /// <summary>
    /// Gets the state now; it may change at any moment after it is read.
    /// </summary>
    internal ConnectionState StateNow => _state;

    /// <summary>
    /// Gets why the connection ended, recorded by the call that moved it to Closing; meaningless before.
    /// </summary>
    internal MqttCloseReason CloseReason { get; private set; }

    /// <summary>
    /// Gets or sets the index of the connection's slot in the registry; set by the registry, under its lock.
    /// </summary>
    internal int Slot { get; set; }

    /// <summary>
    /// Hands a message for this connection to the server and returns at once; it never waits for the network.
    /// Safe to call from any thread. The topic and the payload are not copied: the caller must not change them
    /// afterwards, and one payload may be handed to any number of connections.
    /// </summary>
    /// <param name="topic">
    /// The topic of the message.
    /// </param>
    /// <param name="payload">
    /// The payload; it is referenced, not copied.
    /// </param>
    /// <param name="qos">
    /// The QoS to deliver with; the server lowers it to what the client was granted if needed.
    /// </param>
    /// <returns>
    /// <see cref="MqttSendResult.Taken"/> when the server took the message; otherwise why it did not: the
    /// connection is not open, the message is too large, or the connection was just evicted. A message that was
    /// not taken is not queued and nothing changes.
    /// </returns>
#pragma warning disable S2325 // The shell sends nothing yet; the task that adds sending makes this an instance member in earnest.
    public MqttSendResult TrySend(MqttTopic topic, ReadOnlyMemory<byte> payload, MqttQoS qos) => MqttSendResult.NotOpen;
#pragma warning restore S2325

    /// <summary>
    /// Ends the connection. Safe from any thread and any number of times; a call after the first changes
    /// nothing.
    /// </summary>
    /// <param name="reason">
    /// The reason code to send to the client in DISCONNECT.
    /// </param>
    /// <param name="reasonString">
    /// A human-readable reason, or <see langword="null"/>; it is cut to 256 UTF-8 bytes.
    /// </param>
    public void Close(MqttReasonCode reason, string? reasonString = null)
    {
        // A shell: the task that lets the host close a connection fills it in.
    }

    /// <summary>
    /// Tells what the connection holds now, for the tests of its bounds. Counts that do not exist yet are 0.
    /// </summary>
    /// <returns>
    /// The counts; read without a lock, so each is a moment's value.
    /// </returns>
    internal ConnectionCounts GetCounts() =>
        new(IncomingRecords: 0, DeliveryRecords: 0, WaitingDeliveries: 0, WaitingControlPackets: 0,
            OutgoingBytes: 0, IsWriting: false, HoldsReadBuffer: _readBuffer.IsHeld);
}
