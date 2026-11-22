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
    /// The lock of the send path: it guards <see cref="_writing"/>, <see cref="_waiting"/>, <see cref="_control"/>,
    /// <see cref="_outgoingBytes"/>, <see cref="_writingBytes"/>, <see cref="_closeStatus"/>,
    /// <see cref="_sendIdle"/>, <see cref="_records"/> and <see cref="_lastDeliveryIdentifier"/>, and every move to Closing is made under it. It is held for a few field updates and a
    /// copy of at most one control packet, never across a write, a close, an abort or a call of the host's handler,
    /// so a host may call <see cref="TrySend"/> and <see cref="Close"/> from inside its callbacks. It is taken inside
    /// the registry's lock (the admission step), never the other way round.
    /// </summary>
    private readonly Lock _sendLock = new();

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
    /// The deliveries that wait behind the write in progress, or, the first of them at QoS 1 or 2, for room in the
    /// client's window, in the order of the <see cref="TrySend"/> calls; taken from the server's pool with the first
    /// one and given back with the last, so <see langword="null"/> while none waits. With no write in progress, they
    /// wait only while the first has no room in the window. Guarded by <see cref="_sendLock"/>.
    /// </summary>
    private WaitingDeliveries? _waiting;

    /// <summary>
    /// The records of the QoS 1 and 2 exchanges in progress; taken from the server's pool with the first record and
    /// given back with the last, or by the end of the connection, so <see langword="null"/> while there is none.
    /// Changed under <see cref="_sendLock"/>, always with <see cref="Volatile.Write{T}(ref T, T)"/>, so that the sweep
    /// can read the reference without the lock and skip a connection that holds none.
    /// </summary>
    private ExchangeRecords? _records;

    /// <summary>
    /// The control packets that wait behind the write in progress, already encoded; it holds a rented array only
    /// while one waits. A mutable struct changed in place, so the explicit empty start tells the analyzer it is
    /// meant to be assigned through its members. Guarded by <see cref="_sendLock"/>.
    /// </summary>
    private ControlBuffer _control = default;

    /// <summary>
    /// What is left to write of the payload of the large delivery being written, a view of the host's memory; empty
    /// when no such delivery is in progress. Only whoever holds the send path reads and writes it.
    /// </summary>
    private ReadOnlyMemory<byte> _remainingPayload;

    /// <summary>
    /// Completes when the send path is given up; it exists only while the end of the run, or a test
    /// (<see cref="WhenSendPathIdleAsync"/>), waits for the holder to give it up. Guarded by <see cref="_sendLock"/>:
    /// set by whoever waits, taken by the hold that clears <see cref="_writing"/>, whether the connection is Closing
    /// or not.
    /// </summary>
    private TaskCompletionSource? _sendIdle;

    /// <summary>
    /// When the piece that is with the link was handed to it, a timestamp of the server's clock; 0 when no write is
    /// with the link, and for a write that completed at once. Written by whoever holds the send path, read by the
    /// sweep for the write deadline.
    /// </summary>
    private long _writeStartedAt;

    /// <summary>
    /// The bytes counted against <see cref="MqttWireServerOptions.MaxOutgoingBytes"/>: the payloads of the waiting
    /// deliveries and of the delivery being written, and the bytes of the waiting control packets. Guarded by
    /// <see cref="_sendLock"/>.
    /// </summary>
    private int _outgoingBytes;

    /// <summary>
    /// The payload bytes of the delivery being written, which leave <see cref="_outgoingBytes"/> when its last
    /// piece has been written; 0 when no delivery is being written. Guarded by <see cref="_sendLock"/>.
    /// </summary>
    private int _writingBytes;

    /// <summary>
    /// The WebSocket status of the close that the call that began the close still has to hand to the link, after
    /// its packet; 0 when there is none. Guarded by <see cref="_sendLock"/>.
    /// </summary>
    private ushort _closeStatus;

    /// <summary>
    /// The last packet identifier a delivery at QoS 1 or 2 was given, or 0 before the first (design 6.6, "Deliveries
    /// at QoS 1 or 2", item 2): the counter is the connection's, not the pooled records', so it goes on from where it
    /// was when the records go back to the pool with the last exchange, and an identifier whose exchange has just
    /// ended is not handed out again at once. Guarded by <see cref="_sendLock"/>.
    /// </summary>
    private ushort _lastDeliveryIdentifier;

    /// <summary>
    /// Whether the send path is taken: a write or a close is with the link, or a caller is about to start one, or
    /// the path is reserved for the CONNACK of a connection in or after its admission step. Whoever sets it writes;
    /// everyone else adds to what waits. It is set only in a hold of <see cref="_sendLock"/> that found it clear,
    /// and cleared only by its holder, in the hold in which that holder found nothing more to do.
    /// </summary>
    private bool _writing;

    /// <summary>
    /// How many QoS 1 and 2 deliveries may be unacknowledged at once: the client's Receive Maximum, at most 256;
    /// 256 when it sent none. Set by the connect phase.
    /// </summary>
    private int _clientWindow;

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
    /// Gets how many QoS 1 and 2 deliveries may be unacknowledged at once, for the tests of the connection's bounds:
    /// the client's Receive Maximum, at most 256; 0 until the connect phase has read the CONNECT.
    /// </summary>
    internal int ClientWindow => _clientWindow;

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
    /// not taken is not queued. "Not open" changes nothing; "too large" and "evicted" also end the connection,
    /// with <see cref="MqttCloseReason.MessageTooLargeToSend"/> and <see cref="MqttCloseReason.OutgoingLimitPassed"/>.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="topic"/> is the default value, which holds no topic, or <paramref name="qos"/> is not 0, 1 or
    /// 2 (the caller's bug); nothing changes.
    /// </exception>
    public MqttSendResult TrySend(MqttTopic topic, ReadOnlyMemory<byte> payload, MqttQoS qos)
    {
        if (topic.IsDefault)
        {
            throw new ArgumentException("The topic is the default value, which holds no topic.", nameof(topic));
        }

        if (qos > MqttQoS.ExactlyOnce)
        {
            throw new ArgumentException("The QoS is not 0, 1 or 2.", nameof(qos));
        }

        MqttQoS maxQoS = _server.Options.MaxQoS;
        return TakeDelivery(topic, payload, qos > maxQoS ? maxQoS : qos);
    }

    /// <summary>
    /// Ends the connection. Safe from any thread and any number of times; a call after the first changes
    /// nothing. It never waits for the network: when nothing is being written to the client, DISCONNECT with the
    /// reason code is handed to the socket, followed by the WebSocket close; when a write is in progress, the socket
    /// is aborted instead, so a client that is not reading is never waited for.
    /// <see cref="IMqttWireHandler.OnClosed"/> then reports <see cref="MqttCloseReason.ClosedByHost"/>.
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
    public void Close(MqttReasonCode reason, string? reasonString = null)
    {
        ReasonStrings.ThrowIfNotEncodable(reasonString: reasonString, paramName: nameof(reasonString));
        BeginClose(MqttCloseReason.ClosedByHost, reason, ReasonStrings.Cut(reasonString));
    }

    /// <summary>
    /// Sets the packet identifier counter of the deliveries as if this identifier had been the last one handed out,
    /// for the tests of the identifiers' wrap.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The identifier taken as the last one handed out, from 1 to 65,535.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="packetIdentifier"/> is 0 (the test's bug); nothing changes.
    /// </exception>
    internal void SetLastDeliveryIdentifier(ushort packetIdentifier)
    {
        ArgumentOutOfRangeException.ThrowIfZero(packetIdentifier);
        lock (_sendLock)
        {
            _lastDeliveryIdentifier = packetIdentifier;
        }
    }

    /// <summary>
    /// Waits until the send path is given up, for the tests that assert what is true only once the holder of the
    /// path has finished (a close that then finds the path idle and writes its DISCONNECT, counts that are idle). It
    /// uses the completion the end of the run waits on, so it adds no field and costs an idle connection nothing.
    /// </summary>
    /// <returns>
    /// A task that completes, never inline, in the hold that gives the send path up; at once when the path is not
    /// taken now. The path may be taken again by the time the caller resumes.
    /// </returns>
    internal Task WhenSendPathIdleAsync()
    {
        lock (_sendLock)
        {
            if (!_writing)
            {
                return Task.CompletedTask;
            }

            _sendIdle ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return _sendIdle.Task;
        }
    }

    /// <summary>
    /// Tells what the connection holds now, for the tests of its bounds.
    /// </summary>
    /// <returns>
    /// The counts. Those of the send path are read in one hold of its lock, so they belong to one moment; whether a
    /// read buffer is held is read without a lock.
    /// </returns>
    internal ConnectionCounts GetCounts()
    {
        lock (_sendLock)
        {
            int incomingRecords = _records?.IncomingCount ?? 0;
            return new ConnectionCounts(IncomingRecords: incomingRecords, DeliveryRecords: _records?.DeliveryCount ?? 0,
                IncomingInFlight: incomingRecords + _control.PublishAnswerCount, HoldsRecords: _records is not null,
                WaitingDeliveries: _waiting?.Count ?? 0, WaitingControlPackets: _control.PacketCount,
                OutgoingBytes: _outgoingBytes, IsWriting: _writing, HoldsReadBuffer: _readBuffer.IsHeld);
        }
    }
}
