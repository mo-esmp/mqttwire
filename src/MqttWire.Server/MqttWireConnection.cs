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
    /// The skip of a PUBLISH too large to be read whole; active only while such a packet is being skipped, and
    /// otherwise its default value. Changed in place, like <see cref="_readBuffer"/>, by the code that reads alone.
    /// </summary>
    private PublishSkip _publishSkip = default;

    /// <summary>
    /// When the first byte of a packet that is not complete yet was read, a timestamp of the server's clock; 0 when
    /// no packet is incomplete. Written by the code that reads, read by the sweep for the partial-packet deadline;
    /// it covers a packet that is being skipped.
    /// </summary>
    private long _partialPacketAt;

    /// <summary>
    /// When the last complete packet from the client was read (until the first, the moment the connection became
    /// Active), a timestamp of the server's clock. Written by the code that reads, read by the sweep for the keep-alive deadline; a skipped
    /// packet counts once it has been consumed whole.
    /// </summary>
    private long _lastPacketAt;

    /// <summary>
    /// When the Accepted or the Authenticating state was entered, a timestamp of the server's clock; 0 before the
    /// accept. The move to Closing never writes it, so a stamp written by a move that then lost its race is harmless.
    /// </summary>
    private long _stateEnteredAt;

    /// <summary>
    /// When the connection entered Closing, a timestamp of the server's clock; 0 until then. Only the winner of
    /// the move to Closing writes it, after its compare-exchange, so the close deadline is never measured from an
    /// older state's stamp.
    /// </summary>
    private long _closingAt;

    /// <summary>
    /// The cancellation source of the host's <see cref="IMqttWireHandler.OnConnectAsync"/>; it exists only in the
    /// Authenticating state. Whoever takes it out of the field with <see cref="Interlocked.Exchange{T}(ref T, T)"/>
    /// disposes it: the connect phase when the host has answered, or the sweep at the authentication deadline.
    /// </summary>
    private CancellationTokenSource? _authentication;

    /// <summary>
    /// The keep-alive interval that applies, in seconds: the client's, or 60 when it asked for none or for more;
    /// set by the connect phase.
    /// </summary>
    private ushort _keepAliveSeconds;

    /// <summary>
    /// Whether the host's <see cref="IMqttWireHandler.OnAdmit"/> accepted the connection, so that its end calls
    /// <see cref="IMqttWireHandler.OnClosed"/>. Written once, under the registry's lock, by the admission step of the
    /// connection's own run, and read by the end of that run.
    /// </summary>
    private bool _admitted;

    /// <summary>
    /// How far the connection is with its CONNACK; it moves by <see cref="Interlocked.CompareExchange{T}(ref T, T, T)"/>
    /// between the connect phase, the admission step and <see cref="BeginClose"/>.
    /// </summary>
    private ConnAckProgress _connAck;

#pragma warning disable S4487 // Set by the connect phase now; read by the tasks that send deliveries and acknowledgements.

    /// <summary>
    /// How many QoS 1 and 2 deliveries may be unacknowledged at once: the client's Receive Maximum, at most 256;
    /// 256 when it sent none. Set by the connect phase.
    /// </summary>
    private int _clientWindow;
#pragma warning restore S4487

    /// <summary>
    /// The client's Maximum Packet Size, in bytes; <see cref="int.MaxValue"/> when it sent none or a larger one, and
    /// until the connect phase has read the CONNECT, so a CONNACK that refuses an unread CONNECT is not limited.
    /// </summary>
    private int _clientMaxPacketSize = int.MaxValue;

    /// <summary>
    /// Whether acknowledgements may carry a reason string and user properties; <see langword="false"/> when the
    /// client sent Request Problem Information 0. Set by the connect phase.
    /// </summary>
    private bool _problemInformation;

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
    /// looks at it. Set before <see cref="IMqttWireHandler.OnAdmit"/> is called.
    /// </summary>
    public object? State { get; private set; }

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
    /// <exception cref="ArgumentException">
    /// The reason string holds U+0000 or an unpaired surrogate, which no packet can carry; nothing changes.
    /// </exception>
#pragma warning disable S2325 // A shell beyond the check; the task that lets the host close a connection makes it an instance member in earnest.
    public void Close(MqttReasonCode reason, string? reasonString = null) =>
        ReasonStrings.ThrowIfNotEncodable(reasonString: reasonString, paramName: nameof(reasonString));
#pragma warning restore S2325

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
