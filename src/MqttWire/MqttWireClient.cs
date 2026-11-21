using System.Buffers;
using System.Text;
using MqttWire.Client;
using MqttWire.Codec;
using MqttWire.Link;

namespace MqttWire;

/// <summary>
/// An MQTT 5 client over a WebSocket. One instance serves one connection.
/// </summary>
/// <remarks>
/// <para>
/// One read loop, started when the link opens, is the only reader. Every packet is written through one send lock,
/// in the order the writers took it. The end of the connection has one path: the first reason recorded wins; whoever
/// records it sends the DISCONNECT that is due and closes the link, or aborts it; the read loop, once it has
/// stopped, completes what is in flight, returns its buffer and calls <c>OnClosed</c> once.
/// </para>
/// <para>
/// A close waits for the server to end the link, at most two seconds; then the link is aborted.
/// </para>
/// <para>
/// One keep-alive timer, from the accepting CONNACK until the connection is finished, ticks every half interval:
/// it sends PINGREQ when nothing was sent, and aborts the link when a PINGREQ waits a full interval or a request
/// waits twice the interval for its acknowledgement.
/// </para>
/// </remarks>
public sealed partial class MqttWireClient : IAsyncDisposable
{
    /// <summary>
    /// Guards the phase, the end's record and the close timer; never held across an await.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// Lets one write at a time reach the link, in the order the writers asked; held across the write.
    /// </summary>
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    /// <summary>
    /// The application's end-of-connection callback, or <see langword="null"/>.
    /// </summary>
    private readonly Action<MqttClientCloseInfo>? _onClosed;

    /// <summary>
    /// Opens the link.
    /// </summary>
    private readonly IMqttLinkConnector _connector;

    /// <summary>
    /// The clock of every deadline and timestamp.
    /// </summary>
    private readonly TimeProvider _time;

    /// <summary>
    /// The pool every buffer is rented from.
    /// </summary>
    private readonly ArrayPool<byte> _pool;

    /// <summary>
    /// The read loop's buffer.
    /// </summary>
    private readonly ReceiveBuffer _receive;

    /// <summary>
    /// The password as UTF-8, or <see langword="null"/>.
    /// </summary>
    private readonly byte[]? _password;

    /// <summary>
    /// Where the connection is; changed under <see cref="_gate"/>.
    /// </summary>
    private volatile Phase _phase;

    /// <summary>
    /// The link, set once when it opens.
    /// </summary>
    private volatile IMqttLink? _link;

    /// <summary>
    /// Whether <see cref="DisposeAsync"/> was called; under <see cref="_gate"/>.
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// Whether the connection was connected when its end was recorded, so <c>OnClosed</c> is due; under
    /// <see cref="_gate"/>.
    /// </summary>
    private bool _wasConnected;

    /// <summary>
    /// The first reason recorded for the end; under <see cref="_gate"/>.
    /// </summary>
    private MqttClientCloseInfo _closeInfo;

    /// <summary>
    /// The close deadline's timer, while a close is under way; under <see cref="_gate"/>.
    /// </summary>
    private ITimer? _closeTimer;

    /// <summary>
    /// The outcome of the connect; set once, before the read loop starts.
    /// </summary>
    private TaskCompletionSource<MqttConnectAck>? _connectResult;

    /// <summary>
    /// Completes when the read loop has finished the connection; set once, with the link.
    /// </summary>
    private TaskCompletionSource? _ended;

    /// <summary>
    /// Cancelled by <see cref="DisposeAsync"/> to stop a connect that is still opening the link; set once, when
    /// the connect starts. It holds no timer, so it needs no disposing.
    /// </summary>
    private CancellationTokenSource? _connectCanceledByDispose;

    /// <summary>
    /// Completes when <see cref="ConnectAsync"/> has returned or thrown; set once, when the connect starts.
    /// </summary>
    private TaskCompletionSource? _connectReturned;

    /// <summary>
    /// The refusing CONNACK's report, built by the read loop for the connect to return.
    /// </summary>
    private MqttConnectAck? _refusal;

    /// <summary>
    /// The <see cref="TimeProvider.GetTimestamp"/> of the last packet written.
    /// </summary>
    private long _lastSendTimestamp;

    /// <summary>
    /// The <see cref="TimeProvider.GetTimestamp"/> of the last PINGRESP received.
    /// </summary>
    private long _lastPingResponseTimestamp;

    /// <summary>
    /// The requests sent and not yet acknowledged; created by the read loop from the accepting CONNACK, before
    /// the connection is connected, and used only under <see cref="_gate"/>.
    /// </summary>
    private OutgoingRecords? _records;

    /// <summary>
    /// The highest QoS the server accepts, from the CONNACK's Maximum QoS; set with <see cref="_records"/>.
    /// </summary>
    private MqttQoS _serverMaximumQoS;

    /// <summary>
    /// The largest packet the server accepts, from the CONNACK's Maximum Packet Size, or
    /// <see cref="uint.MaxValue"/> when it sets none; set with <see cref="_records"/>.
    /// </summary>
    private uint _serverMaximumPacketSize;

    /// <summary>
    /// The acknowledgement the read loop sends after the packet it has just handled; read-loop state only.
    /// </summary>
    private Reply _reply;

    /// <summary>
    /// Creates a client. Nothing is connected until <see cref="ConnectAsync"/> is called.
    /// </summary>
    /// <param name="options">
    /// The options.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// The options, or a required option, are <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The URI is not an absolute <c>ws</c> or <c>wss</c> URI, or the client identifier, user name or password is
    /// something MQTT cannot carry: an empty client identifier, more than 65,535 UTF-8 bytes, U+0000 or an
    /// unpaired surrogate.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The keep-alive or the maximum incoming packet size is outside its range.
    /// </exception>
    public MqttWireClient(MqttWireClientOptions options)
        : this(
            options,
            options?.OnMessage,
            options?.OnClosed,
            ClientWebSocketConnector.Instance,
            TimeProvider.System,
            ArrayPool<byte>.Shared)
    {
    }

    /// <summary>
    /// Creates a client with its collaborators given, for tests and the reconnecting client.
    /// </summary>
    /// <param name="options">
    /// The options.
    /// </param>
    /// <param name="onMessage">
    /// The message handler, or <see langword="null"/>.
    /// </param>
    /// <param name="onClosed">
    /// The end-of-connection callback, or <see langword="null"/>.
    /// </param>
    /// <param name="connector">
    /// Opens the link.
    /// </param>
    /// <param name="timeProvider">
    /// The clock of every deadline and timestamp.
    /// </param>
    /// <param name="pool">
    /// The pool every buffer is rented from.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// The options, a required option, or a collaborator is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The URI or a text option cannot be used; see the public constructor.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The keep-alive or the maximum incoming packet size is outside its range.
    /// </exception>
    internal MqttWireClient(
        MqttWireClientOptions options,
        MqttMessageHandler? onMessage,
        Action<MqttClientCloseInfo>? onClosed,
        IMqttLinkConnector connector,
        TimeProvider timeProvider,
        ArrayPool<byte> pool)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(connector);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(pool);

        // Required members can still be null when the caller suppresses the compiler's warning.
        if (options.Uri is null || options.ClientId is null)
        {
            throw new ArgumentNullException(nameof(options), "The URI and the client identifier are required.");
        }

        if (!options.Uri.IsAbsoluteUri || (!string.Equals(options.Uri.Scheme, Uri.UriSchemeWs, StringComparison.Ordinal)
                && !string.Equals(options.Uri.Scheme, Uri.UriSchemeWss, StringComparison.Ordinal)))
        {
            throw new ArgumentException("The URI must be an absolute ws or wss URI.", nameof(options));
        }

        if (options.ClientId.Length == 0)
        {
            throw new ArgumentException("The client identifier must not be empty.", nameof(options));
        }

        CheckText(options.ClientId, "client identifier", nameof(options));
        if (options.UserName is not null)
        {
            CheckText(options.UserName, "user name", nameof(options));
        }

        if (options.Password is not null)
        {
            CheckText(options.Password, "password", nameof(options));
        }

        if (options.KeepAlive < TimeSpan.FromSeconds(1) || options.KeepAlive > TimeSpan.FromSeconds(ushort.MaxValue))
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.KeepAlive, "The keep-alive must be from 1 to 65,535 seconds.");
        }

        if (options.MaxIncomingPacketBytes is < ClientLimits.MinIncomingPacketBytes or > ClientLimits.MaxIncomingPacketBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.MaxIncomingPacketBytes, "The maximum incoming packet size must be from 4,096 to 268,435,460 bytes.");
        }

        Options = options;
        KeepAlive = TimeSpan.FromSeconds(Math.Ceiling(options.KeepAlive.TotalSeconds));
        MessageHandler = onMessage;
        _onClosed = onClosed;
        _connector = connector;
        _time = timeProvider;
        _pool = pool;
        _receive = new ReceiveBuffer(pool, options.MaxIncomingPacketBytes);
        _password = options.Password is null ? null : Encoding.UTF8.GetBytes(options.Password);
    }

    /// <summary>
    /// Where a client's connection is. The order matters: a later phase never goes back to an earlier one.
    /// </summary>
    private enum Phase
    {
        /// <summary>
        /// <see cref="ConnectAsync"/> has not been called.
        /// </summary>
        New,

        /// <summary>
        /// The link is opening, or the client is waiting for CONNACK.
        /// </summary>
        Connecting,

        /// <summary>
        /// An accepting CONNACK arrived and the end has not begun.
        /// </summary>
        Connected,

        /// <summary>
        /// The end's reason is recorded; the link is being closed or aborted.
        /// </summary>
        Closing,

        /// <summary>
        /// Nothing runs any more: the read loop has finished, or no link was ever opened.
        /// </summary>
        Closed,
    }

    /// <summary>
    /// Gets a value indicating whether the client is connected: from an accepting CONNACK until the end of the
    /// connection begins.
    /// </summary>
    public bool IsConnected => _phase == Phase.Connected;

    /// <summary>
    /// Gets the options the client was created with.
    /// </summary>
    internal MqttWireClientOptions Options { get; }

    /// <summary>
    /// Gets the keep-alive interval rounded up to whole seconds.
    /// </summary>
    internal TimeSpan KeepAlive { get; }

    /// <summary>
    /// Gets the application's message handler, or <see langword="null"/>; the read loop calls it for each PUBLISH
    /// (Task 6).
    /// </summary>
    internal MqttMessageHandler? MessageHandler { get; }

    /// <summary>
    /// Gets the <see cref="TimeProvider.GetTimestamp"/> of the last packet written, or 0 before the first.
    /// </summary>
    internal long LastSendTimestamp => Volatile.Read(ref _lastSendTimestamp);

    /// <summary>
    /// Gets the <see cref="TimeProvider.GetTimestamp"/> of the last PINGRESP received, or 0 before the first.
    /// </summary>
    internal long LastPingResponseTimestamp => Volatile.Read(ref _lastPingResponseTimestamp);

    /// <summary>
    /// Gets the number of requests sent and not yet acknowledged, each holding a packet identifier: 0 before the
    /// accepting CONNACK and once the end of the connection has completed them.
    /// </summary>
    internal int RequestsInFlight
    {
        get
        {
            lock (_gate)
            {
                return _records?.Count ?? 0;
            }
        }
    }

    /// <summary>
    /// Opens the link, sends CONNECT and waits for the server's CONNACK, at most ten seconds in all.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels the connect while it waits.
    /// </param>
    /// <returns>
    /// What happened: connected (<see cref="MqttConnectAck.IsConnected"/>); refused by the server
    /// (<see cref="MqttClientCloseReason.ConnectRefused"/> with the CONNACK's reason code); or no valid CONNACK,
    /// with <see cref="MqttConnectAck.Failure"/> saying why: <see cref="MqttClientCloseReason.Unreachable"/>
    /// (with the network error), <see cref="MqttClientCloseReason.ConnectTimedOut"/> or
    /// <see cref="MqttClientCloseReason.ProtocolError"/>. After a failure the client is finished; a new connection
    /// needs a new client.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// <see cref="ConnectAsync"/> was already called: a client serves one connection.
    /// </exception>
    /// <exception cref="ObjectDisposedException">
    /// The client was disposed before or during the connect.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// The token was cancelled before the connect was decided. The link is aborted.
    /// </exception>
    /// <remarks>
    /// An exception from <see cref="MqttWireClientOptions.ConfigureWebSocket"/> propagates, and the client is
    /// finished.
    /// </remarks>
    public async Task<MqttConnectAck> ConnectAsync(CancellationToken cancellationToken = default)
    {
        CancellationTokenSource disposing = new();
        TaskCompletionSource returned = new(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_phase != Phase.New)
            {
                throw new InvalidOperationException("ConnectAsync can be called once: a client serves one connection.");
            }

            _phase = Phase.Connecting;
            _connectCanceledByDispose = disposing;
            _connectReturned = returned;
        }

        try
        {
            return await ConnectCoreAsync(disposing.Token, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            returned.TrySetResult();
        }
    }

    /// <summary>
    /// Ends the connection and waits until it has ended. When connected: sends DISCONNECT 0x00, closes the link
    /// with 1000 and waits for the server to end it, at most two seconds, then aborts it. While connecting: cancels
    /// the opening of the link or aborts the link, and waits for <see cref="ConnectAsync"/> to throw
    /// <see cref="ObjectDisposedException"/>. Calling it again, or after the connection ended, only waits for that
    /// end.
    /// </summary>
    /// <returns>
    /// A task that completes when <see cref="ConnectAsync"/> has returned, the read loop has finished,
    /// <c>OnClosed</c>, if due, has returned, and every write handed to the send path before then has ended, so
    /// every buffer those writes held is back in the pool. A write still in flight when the link was aborted is
    /// waited for; the abort makes it end. A <see cref="PublishAsync"/> or keep-alive tick racing the dispose can
    /// rent a buffer after the dispose's turn on the send lock; that buffer comes back at once, because its send
    /// finds the connection ended. It does not throw, unless finishing the connection failed through a bug in the
    /// client.
    /// </returns>
    public async ValueTask DisposeAsync()
    {
        bool close = false;
        bool abort = false;
        IMqttLink? link;
        TaskCompletionSource? ended;
        TaskCompletionSource? connectReturned;
        CancellationTokenSource? connectCanceledByDispose;
        lock (_gate)
        {
            _disposed = true;
            Phase phase = _phase;
            connectCanceledByDispose = phase == Phase.Connecting ? _connectCanceledByDispose : null;
            if (phase == Phase.New)
            {
                _phase = Phase.Closed;
            }
            else
            {
                EndMode mode = phase == Phase.Connected ? EndMode.Close : EndMode.Abort;
                if (TryRecordEndLocked(new MqttClientCloseInfo(MqttClientCloseReason.Disposed, MqttReasonCode.Success, null, null), mode))
                {
                    close = mode == EndMode.Close;
                    abort = mode == EndMode.Abort;
                }
            }

            link = _link;
            ended = _ended;
            connectReturned = _connectReturned;
        }

        // Stops a link that is still opening; the connect then throws ObjectDisposedException.
        if (connectCanceledByDispose is not null)
        {
            await connectCanceledByDispose.CancelAsync().ConfigureAwait(false);
        }

        if (link is not null)
        {
            if (close)
            {
                await CloseLinkAsync(link, MqttReasonCode.Success).ConfigureAwait(false);
            }
            else if (abort)
            {
                link.Abort();
            }
        }

        await WaitUntilEndedAsync(connectReturned, ended).ConfigureAwait(false);
    }

    /// <summary>
    /// Waits until the connect has returned, the read loop has finished the connection, and every write handed to
    /// the send path before then has ended and returned its buffer. A write handed over after this wait's turn on
    /// the send lock finds the connection ended and returns its buffer at once.
    /// </summary>
    /// <param name="connectReturned">
    /// Completes when <see cref="ConnectAsync"/> has returned, or <see langword="null"/> when it was never called.
    /// </param>
    /// <param name="ended">
    /// Completes when the read loop has finished, or <see langword="null"/> when no link was opened.
    /// </param>
    /// <returns>
    /// A task that completes then. It throws only what finishing the connection failed with, a bug in the client.
    /// </returns>
    private async ValueTask WaitUntilEndedAsync(TaskCompletionSource? connectReturned, TaskCompletionSource? ended)
    {
        if (connectReturned is not null)
        {
            await connectReturned.Task.ConfigureAwait(false);
        }

        if (ended is null)
        {
            // No link, so no write.
            return;
        }

        await ended.Task.ConfigureAwait(false);

        // A write still in flight, or queued behind one, owns its buffer until it has ended (6.8 rule 3), and the
        // link may have been aborted under it. Each entered the send lock's queue before this wait, and the lock
        // serves its waiters in order, so when this turn comes each has returned its buffer. The read loop aborted
        // the link as it finished, so each write ends promptly; a later one finds the connection over and returns.
        await _sendLock.WaitAsync().ConfigureAwait(false);
        _sendLock.Release();
    }

    /// <summary>
    /// Checks that text can be carried by MQTT as a UTF-8 string.
    /// </summary>
    /// <param name="value">
    /// The text.
    /// </param>
    /// <param name="what">
    /// What the text is, for the message.
    /// </param>
    /// <param name="paramName">
    /// The caller's parameter that holds the text, for <see cref="ArgumentException.ParamName"/>.
    /// </param>
    /// <exception cref="ArgumentException">
    /// The text is over 65,535 UTF-8 bytes, contains U+0000 or has an unpaired surrogate.
    /// </exception>
    private static void CheckText(string value, string what, string paramName)
    {
        try
        {
            FieldSize.Utf8String(value);
        }
        catch (ArgumentException ex)
        {
            throw new ArgumentException($"The {what} cannot be sent: {ex.Message}", paramName, ex);
        }
    }

    /// <summary>
    /// Reads the Reason String property.
    /// </summary>
    /// <param name="properties">
    /// The validated properties.
    /// </param>
    /// <returns>
    /// The reason string, or <see langword="null"/> when there is none.
    /// </returns>
    private static string? ReadReasonString(in PropertyBlock properties)
        => properties.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> value) ? Encoding.UTF8.GetString(value) : null;

    /// <summary>
    /// Reads the User Property properties, in wire order.
    /// </summary>
    /// <param name="properties">
    /// The validated properties.
    /// </param>
    /// <returns>
    /// The user properties, or <see langword="null"/> when there are none.
    /// </returns>
    private static List<MqttWireUserProperty>? ReadUserProperties(in PropertyBlock properties)
    {
        if (!properties.Contains(PropertyId.UserProperty))
        {
            return null;
        }

        List<MqttWireUserProperty> list = [];
        foreach (Utf8Pair pair in properties.UserProperties)
        {
            list.Add(new MqttWireUserProperty(Encoding.UTF8.GetString(pair.Name), Encoding.UTF8.GetString(pair.Value)));
        }

        return list;
    }
}
