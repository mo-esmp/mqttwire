using System.Buffers;
using System.Net.WebSockets;
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
/// </remarks>
public sealed class MqttWireClient : IAsyncDisposable
{
    /// <summary>
    /// Ends a connect that is still waiting when its deadline or the caller's token fires. The state is always the
    /// client that registered it.
    /// </summary>
    private static readonly Action<object?> s_connectCanceled = static state => ((MqttWireClient)state!).OnConnectCanceled();

    /// <summary>
    /// Aborts the link when a close has taken too long. The state is always the client that created the timer.
    /// </summary>
    private static readonly TimerCallback s_closeTimedOut = static state => ((MqttWireClient)state!).AbortLink();

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

        CheckText(options.ClientId, "client identifier");
        if (options.UserName is not null)
        {
            CheckText(options.UserName, "user name");
        }

        if (options.Password is not null)
        {
            CheckText(options.Password, "password");
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
    /// How the one who records the end treats the link.
    /// </summary>
    private enum EndMode
    {
        /// <summary>
        /// Abort the link at once: for timeouts and failed reads or writes.
        /// </summary>
        Abort,

        /// <summary>
        /// Close the link, after a DISCONNECT where one is due, and abort it if the close takes too long.
        /// </summary>
        Close,
    }

    /// <summary>
    /// What the read loop does after handling the buffered packets.
    /// </summary>
    private enum EndStep
    {
        /// <summary>
        /// Carry on reading.
        /// </summary>
        None,

        /// <summary>
        /// This read loop recorded the end: close the link without a DISCONNECT.
        /// </summary>
        Close,

        /// <summary>
        /// This read loop recorded the end for a fault in the server's packets: send DISCONNECT with the fault's
        /// code, then close the link.
        /// </summary>
        DisconnectAndClose,
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
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_phase != Phase.New)
            {
                throw new InvalidOperationException("ConnectAsync can be called once: a client serves one connection.");
            }

            _phase = Phase.Connecting;
        }

        using CancellationTokenSource deadline = new(ClientLimits.ConnectTimeout, _time);
        using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
        LinkConnectResult opened;
        try
        {
            opened = await _connector.ConnectAsync(Options, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (linked.IsCancellationRequested)
        {
            SetClosed();
            cancellationToken.ThrowIfCancellationRequested();
            return new MqttConnectAck(MqttClientCloseReason.ConnectTimedOut, MqttReasonCode.UnspecifiedError, null, null, null);
        }
        catch
        {
            SetClosed();
            throw;
        }

        if (opened.Link is not IMqttLink link)
        {
            SetClosed();
            return new MqttConnectAck(MqttClientCloseReason.Unreachable, MqttReasonCode.UnspecifiedError, null, null, opened.Error);
        }

        MqttConnectAck ack = await HandshakeAsync(link, linked.Token).ConfigureAwait(false);
        if (ack.Failure == MqttClientCloseReason.ConnectTimedOut)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }

        return ack;
    }

    /// <summary>
    /// Ends the connection and waits until it has ended. When connected: sends DISCONNECT 0x00, closes the link
    /// with 1000 and waits for the server to end it, at most two seconds, then aborts it. While connecting: aborts
    /// the link. Calling it again, or after the connection ended, only waits for that end.
    /// </summary>
    /// <returns>
    /// A task that completes when the read loop has finished, every buffer it held is back in the pool, and
    /// <c>OnClosed</c>, if due, has returned. It does not throw.
    /// </returns>
    public async ValueTask DisposeAsync()
    {
        bool close = false;
        bool abort = false;
        IMqttLink? link;
        TaskCompletionSource? ended;
        lock (_gate)
        {
            _disposed = true;
            Phase phase = _phase;
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

        if (ended is not null)
        {
            await ended.Task.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Writes one packet through the send lock: one write at a time, in the order the writers asked.
    /// </summary>
    /// <param name="packet">
    /// The encoded packet in a buffer rented from the client's pool. This call owns it from now on and returns it
    /// once the write has ended, whatever the outcome.
    /// </param>
    /// <param name="length">
    /// The packet's length.
    /// </param>
    /// <param name="isDisconnect">
    /// <see langword="true"/> for the DISCONNECT that ends the connection, the only packet written after the end
    /// began.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the link took the packet. <see langword="false"/> when it was not written
    /// because the end had begun, or the write failed; a failed write ends the connection with
    /// <see cref="MqttClientCloseReason.WriteFailed"/> and aborts the link.
    /// </returns>
    internal async ValueTask<bool> SendAsync(byte[] packet, int length, bool isDisconnect)
    {
        await _sendLock.WaitAsync().ConfigureAwait(false);
        try
        {
            IMqttLink? link = _link;
            if (link is null || (!isDisconnect && _phase >= Phase.Closing))
            {
                return false;
            }

            await link.WriteAsync(packet.AsMemory(0, length), CancellationToken.None).ConfigureAwait(false);
            Volatile.Write(ref _lastSendTimestamp, _time.GetTimestamp());
            return true;
        }
        catch (Exception ex)
        {
            // Whatever the link throws, the write did not happen: the connection cannot go on.
            TryBeginEnd(new MqttClientCloseInfo(MqttClientCloseReason.WriteFailed, MqttReasonCode.Success, null, ex), EndMode.Abort);
            return false;
        }
        finally
        {
            // The write has ended, so the buffer is no longer in use (6.8).
            _pool.Return(packet);
            _sendLock.Release();
        }
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
    /// <exception cref="ArgumentException">
    /// The text is over 65,535 UTF-8 bytes, contains U+0000 or has an unpaired surrogate.
    /// </exception>
    private static void CheckText(string value, string what)
    {
        try
        {
            FieldSize.Utf8String(value);
        }
        catch (ArgumentException ex)
        {
            throw new ArgumentException($"The {what} cannot be sent: {ex.Message}", nameof(value), ex);
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

    /// <summary>
    /// Completes every request still in flight with <see cref="MqttRequestStatus.ConnectionLost"/>. Called once,
    /// by the read loop after it has stopped and before <c>OnClosed</c>.
    /// </summary>
    private static void CompleteInFlightRequests()
    {
        // Nothing is in flight yet: the outgoing records that this drains come with publishing (Task 5).
    }

    /// <summary>
    /// Takes the link for this connection, unless the client was disposed while it opened.
    /// </summary>
    /// <param name="link">
    /// The open link.
    /// </param>
    /// <param name="result">
    /// Completes with the outcome of the connect.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the client was disposed meanwhile; it is then finished and the caller aborts
    /// the link.
    /// </returns>
    private bool TryStart(IMqttLink link, TaskCompletionSource<MqttConnectAck> result)
    {
        lock (_gate)
        {
            if (_phase != Phase.Connecting)
            {
                _phase = Phase.Closed;
                return false;
            }

            _connectResult = result;
            _ended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _link = link;
            return true;
        }
    }

    /// <summary>
    /// Starts the read loop over the open link, sends CONNECT and waits for the read loop to decide the connect.
    /// </summary>
    /// <param name="link">
    /// The open link.
    /// </param>
    /// <param name="cancellationToken">
    /// The connect deadline and the caller's token together; when it fires before the connect is decided, the
    /// link is aborted and the connect times out.
    /// </param>
    /// <returns>
    /// The outcome of the connect.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    /// The client was disposed while the link opened or while it waited for CONNACK.
    /// </exception>
    private async Task<MqttConnectAck> HandshakeAsync(IMqttLink link, CancellationToken cancellationToken)
    {
        TaskCompletionSource<MqttConnectAck> result = new(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!TryStart(link, result))
        {
            link.Abort();
            throw new ObjectDisposedException(GetType().FullName);
        }

        CancellationTokenRegistration registration = cancellationToken.UnsafeRegister(s_connectCanceled, this);
        try
        {
            _ = ReadLoopAsync(link);
            byte[] connect = RentConnect(out int length);
            await SendAsync(connect, length, isDisconnect: false).ConfigureAwait(false);
            return await result.Task.ConfigureAwait(false);
        }
        finally
        {
            // Unregister rather than Dispose: it does not wait for a callback running on another thread.
            registration.Unregister();
        }
    }

    /// <summary>
    /// Marks a connect that never opened a link as finished.
    /// </summary>
    private void SetClosed()
    {
        lock (_gate)
        {
            _phase = Phase.Closed;
        }
    }

    /// <summary>
    /// Encodes the CONNECT into a rented buffer.
    /// </summary>
    /// <param name="length">
    /// The packet's length.
    /// </param>
    /// <returns>
    /// The rented buffer, which the caller hands to <see cref="SendAsync"/>.
    /// </returns>
    private byte[] RentConnect(out int length)
    {
        ConnectFields fields = new()
        {
            ClientId = Options.ClientId,
            UserName = Options.UserName,
            Password = _password,
            KeepAliveSeconds = (ushort)KeepAlive.TotalSeconds,
            ReceiveMaximum = ClientLimits.ReceiveMaximum,
            MaximumPacketSize = (uint)Options.MaxIncomingPacketBytes,
        };
        byte[] buffer = _pool.Rent(ConnectCodec.GetSize(fields));
        length = ConnectCodec.Write(buffer, fields);
        return buffer;
    }

    /// <summary>
    /// Encodes a DISCONNECT into a rented buffer.
    /// </summary>
    /// <param name="reasonCode">
    /// The DISCONNECT's reason code.
    /// </param>
    /// <param name="length">
    /// The packet's length.
    /// </param>
    /// <returns>
    /// The rented buffer, which the caller hands to <see cref="SendAsync"/>.
    /// </returns>
    private byte[] RentDisconnect(MqttReasonCode reasonCode, out int length)
    {
        byte[] buffer = _pool.Rent(DisconnectCodec.GetSize(reasonCode, null));
        length = DisconnectCodec.Write(buffer, reasonCode, null);
        return buffer;
    }

    /// <summary>
    /// Ends a connect that is still waiting, because its deadline or the caller's token fired: records
    /// <see cref="MqttClientCloseReason.ConnectTimedOut"/> and aborts the link. Does nothing once the connect is
    /// decided.
    /// </summary>
    private void OnConnectCanceled()
    {
        IMqttLink? link;
        lock (_gate)
        {
            if (_phase != Phase.Connecting
                || !TryRecordEndLocked(new MqttClientCloseInfo(MqttClientCloseReason.ConnectTimedOut, MqttReasonCode.Success, null, null), EndMode.Abort))
            {
                return;
            }

            link = _link;
        }

        link?.Abort();
    }

    /// <summary>
    /// Aborts the link, if one is open.
    /// </summary>
    private void AbortLink() => _link?.Abort();

    /// <summary>
    /// Records the end of the connection unless one is already recorded, and aborts the link if the mode says so.
    /// </summary>
    /// <param name="info">
    /// Why the connection ends.
    /// </param>
    /// <param name="mode">
    /// How the link is treated. For <see cref="EndMode.Close"/> the caller then runs <see cref="CloseLinkAsync"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when this call recorded the end; <see langword="false"/> when another reason was
    /// recorded first, or the connect never started, and nothing changed.
    /// </returns>
    private bool TryBeginEnd(in MqttClientCloseInfo info, EndMode mode)
    {
        IMqttLink? link;
        lock (_gate)
        {
            if (!TryRecordEndLocked(info, mode))
            {
                return false;
            }

            link = _link;
        }

        if (mode == EndMode.Abort)
        {
            link?.Abort();
        }

        return true;
    }

    /// <summary>
    /// Records the end of the connection unless one is already recorded; for a close, starts the close deadline.
    /// The caller holds <see cref="_gate"/>.
    /// </summary>
    /// <param name="info">
    /// Why the connection ends.
    /// </param>
    /// <param name="mode">
    /// How the link is treated.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when this call recorded the end; <see langword="false"/> when the connection is not
    /// connecting or connected, and nothing changed.
    /// </returns>
    private bool TryRecordEndLocked(in MqttClientCloseInfo info, EndMode mode)
    {
        if (_phase is not (Phase.Connecting or Phase.Connected))
        {
            return false;
        }

        _wasConnected = _phase == Phase.Connected;
        _phase = Phase.Closing;
        _closeInfo = info;
        if (mode == EndMode.Close && _link is not null)
        {
            _closeTimer = _time.CreateTimer(s_closeTimedOut, this, ClientLimits.CloseTimeout, Timeout.InfiniteTimeSpan);
        }

        return true;
    }

    /// <summary>
    /// Closes the link, after a DISCONNECT when one is given. Run by whoever recorded the end with
    /// <see cref="EndMode.Close"/>; the close deadline aborts the link if this or the server's answer takes too long.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code, or <see langword="null"/> to send none.
    /// </param>
    /// <returns>
    /// A task that completes when the close has been handed to the link, or has failed. It does not throw.
    /// </returns>
    private async ValueTask CloseLinkAsync(IMqttLink link, MqttReasonCode? disconnect)
    {
        if (disconnect is MqttReasonCode reasonCode)
        {
            byte[] packet = RentDisconnect(reasonCode, out int length);
            if (!await SendAsync(packet, length, isDisconnect: true).ConfigureAwait(false))
            {
                // The write failed and aborted the link; there is nothing left to close.
                return;
            }
        }

        try
        {
            await link.CloseAsync(WebSocketCloseStatus.NormalClosure, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The close failed: end the link now rather than at the close deadline. The read loop then sees it end.
            link.Abort();
        }
    }

    /// <summary>
    /// The read loop: waits for data holding no buffer, reads, handles every whole packet, until the link ends.
    /// Then it finishes the connection.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <returns>
    /// A task that completes when the connection is finished. It does not throw.
    /// </returns>
    private async Task ReadLoopAsync(IMqttLink link)
    {
        try
        {
            bool reading = true;
            while (reading)
            {
                await link.WaitForDataAsync(CancellationToken.None).ConfigureAwait(false);
                reading = await ReadAndHandleAsync(link).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            // A failed read, or the link aborted: the first reason recorded wins.
            TryBeginEnd(new MqttClientCloseInfo(MqttClientCloseReason.ConnectionLost, MqttReasonCode.Success, null, ex), EndMode.Abort);
        }

        FinishConnection(link);
    }

    /// <summary>
    /// Reads once into the receive buffer and handles every whole packet in it. Once the end has begun, what is
    /// read is dropped unhandled, until the server ends the link or the close deadline aborts it.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the link has ended; <see langword="true"/> to read again.
    /// </returns>
    /// <exception cref="Exception">
    /// The read failed; the read loop records it.
    /// </exception>
    private async ValueTask<bool> ReadAndHandleAsync(IMqttLink link)
    {
        int read = await link.ReadAsync(_receive.GetReadMemory(), CancellationToken.None).ConfigureAwait(false);
        if (read == 0)
        {
            _receive.Clear();
            if (TryBeginEnd(new MqttClientCloseInfo(MqttClientCloseReason.ConnectionLost, MqttReasonCode.Success, null, null), EndMode.Close))
            {
                // Answer the server's close.
                await CloseLinkAsync(link, null).ConfigureAwait(false);
            }

            return false;
        }

        _receive.Advance(read);
        if (_phase >= Phase.Closing)
        {
            _receive.Clear();
            return true;
        }

        EndStep step = HandleBuffered(out MqttReasonCode disconnect);
        if (step == EndStep.None)
        {
            _receive.Compact();
            return true;
        }

        _receive.Clear();
        await CloseLinkAsync(link, step == EndStep.DisconnectAndClose ? disconnect : null).ConfigureAwait(false);
        CompleteFailedConnect();
        return true;
    }

    /// <summary>
    /// Frames and handles every whole packet in the receive buffer, until one ends the connection.
    /// </summary>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code when the result is <see cref="EndStep.DisconnectAndClose"/>.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.None"/> to carry on; otherwise what this read loop, which recorded the end, does next.
    /// </returns>
    private EndStep HandleBuffered(out MqttReasonCode disconnect)
    {
        while (true)
        {
            FrameStatus status = _receive.TryFrame(out PacketHeader header);
            if (status == FrameStatus.NeedMoreData)
            {
                disconnect = MqttReasonCode.Success;
                return EndStep.None;
            }

            EndStep step;
            if (status == FrameStatus.Done)
            {
                ReadOnlySpan<byte> packet = _receive.GetPacket(header);
                step = _phase == Phase.Connecting
                    ? HandleConnAck(header, packet, out disconnect)
                    : HandlePacket(header, packet, out disconnect);
                _receive.Consume(header.TotalLength);
            }
            else
            {
                step = EndStep.DisconnectAndClose;
                disconnect = status == FrameStatus.PacketTooLarge ? MqttReasonCode.PacketTooLarge : MqttReasonCode.MalformedPacket;
            }

            if (step == EndStep.DisconnectAndClose
                && !TryBeginEnd(new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, disconnect, null, null), EndMode.Close))
            {
                // Another reason was recorded first; its recorder closes the link.
                return EndStep.None;
            }

            if (step != EndStep.None || _phase >= Phase.Closing)
            {
                return step;
            }
        }
    }

    /// <summary>
    /// Handles the first packet of the connection, which must be a CONNACK that accepts it or refuses it.
    /// </summary>
    /// <param name="header">
    /// The packet's fixed header.
    /// </param>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code when the result is <see cref="EndStep.DisconnectAndClose"/>.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.None"/> when the connection is accepted (the connect completes now), or when another
    /// reason ended it first; <see cref="EndStep.Close"/> for a refusal this call recorded;
    /// <see cref="EndStep.DisconnectAndClose"/> for a packet that is not CONNACK, a malformed CONNACK, or one with
    /// Session Present or a reason code that is neither success nor a refusal.
    /// </returns>
    private EndStep HandleConnAck(in PacketHeader header, ReadOnlySpan<byte> packet, out MqttReasonCode disconnect)
    {
        disconnect = MqttReasonCode.ProtocolError;
        if (header.Type != PacketType.ConnAck)
        {
            return EndStep.DisconnectAndClose;
        }

        MqttReasonCode result = ConnAckCodec.TryDecode(packet, out ConnAckPacket connAck);
        if (result != MqttReasonCode.Success)
        {
            disconnect = result;
            return EndStep.DisconnectAndClose;
        }

        if (connAck.ReasonCode >= MqttReasonCode.UnspecifiedError)
        {
            string? reasonString = ReadReasonString(connAck.Properties);
            _refusal = new MqttConnectAck(
                MqttClientCloseReason.ConnectRefused,
                connAck.ReasonCode,
                reasonString,
                ReadUserProperties(connAck.Properties),
                null);
            return TryBeginEnd(new MqttClientCloseInfo(MqttClientCloseReason.ConnectRefused, connAck.ReasonCode, reasonString, null), EndMode.Close)
                ? EndStep.Close
                : EndStep.None;
        }

        // The client asked for a clean start, so the server cannot have a session (MQTT 5, 3.2.2.1.1).
        if (connAck.SessionPresent || connAck.ReasonCode != MqttReasonCode.Success)
        {
            return EndStep.DisconnectAndClose;
        }

        // Tasks 5 and 8 read the server's Receive Maximum, Maximum Packet Size, Maximum QoS and Server Keep Alive here.
        MqttConnectAck ack = new(null, MqttReasonCode.Success, ReadReasonString(connAck.Properties), ReadUserProperties(connAck.Properties), null);
        lock (_gate)
        {
            if (_phase != Phase.Connecting)
            {
                return EndStep.None;
            }

            _phase = Phase.Connected;
        }

        // Set before the read loop started, and the read loop is the caller.
        _connectResult!.TrySetResult(ack);
        return EndStep.None;
    }

    /// <summary>
    /// Handles a packet after the CONNACK, by its type.
    /// </summary>
    /// <param name="header">
    /// The packet's fixed header.
    /// </param>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code when the result is <see cref="EndStep.DisconnectAndClose"/>.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.None"/> to carry on; <see cref="EndStep.Close"/> for a DISCONNECT from the server this
    /// call recorded; <see cref="EndStep.DisconnectAndClose"/> for a malformed packet or one the server may not send.
    /// </returns>
    private EndStep HandlePacket(in PacketHeader header, ReadOnlySpan<byte> packet, out MqttReasonCode disconnect)
    {
        switch (header.Type)
        {
            case PacketType.Disconnect:
                return HandleDisconnect(packet, out disconnect);

            case PacketType.PingResp:
                disconnect = PingCodec.TryDecode(packet);
                if (disconnect != MqttReasonCode.Success)
                {
                    return EndStep.DisconnectAndClose;
                }

                // Keep-alive (Task 8) reads when the last answer came.
                Volatile.Write(ref _lastPingResponseTimestamp, _time.GetTimestamp());
                return EndStep.None;

            case PacketType.Publish or PacketType.PubAck or PacketType.PubRec or PacketType.PubRel
                or PacketType.PubComp or PacketType.SubAck or PacketType.UnsubAck:
                // Tasks 5 to 7 handle these. Until then the client has nothing in flight and subscribes to nothing,
                // so a server that sends one breaks the protocol.
                disconnect = MqttReasonCode.ProtocolError;
                return EndStep.DisconnectAndClose;

            default:
                // CONNECT, a second CONNACK, SUBSCRIBE, UNSUBSCRIBE, PINGREQ and AUTH: a server never sends these to
                // this client.
                disconnect = MqttReasonCode.ProtocolError;
                return EndStep.DisconnectAndClose;
        }
    }

    /// <summary>
    /// Handles a DISCONNECT from the server: records the end with its reason code and reason string. The client
    /// sends no DISCONNECT back; it closes the link.
    /// </summary>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code when the result is <see cref="EndStep.DisconnectAndClose"/>.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.Close"/> when this call recorded the end; <see cref="EndStep.None"/> when another
    /// reason was first; <see cref="EndStep.DisconnectAndClose"/> for a malformed DISCONNECT.
    /// </returns>
    private EndStep HandleDisconnect(ReadOnlySpan<byte> packet, out MqttReasonCode disconnect)
    {
        disconnect = DisconnectCodec.TryDecode(packet, out DisconnectPacket received);
        if (disconnect != MqttReasonCode.Success)
        {
            return EndStep.DisconnectAndClose;
        }

        MqttClientCloseInfo info = new(MqttClientCloseReason.ServerDisconnected, received.ReasonCode, ReadReasonString(received.Properties), null);
        return TryBeginEnd(info, EndMode.Close) ? EndStep.Close : EndStep.None;
    }

    /// <summary>
    /// Completes the connect with its failure, once the end of a connection that never connected is recorded.
    /// Does nothing for a connection that connected, or a connect already completed.
    /// </summary>
    private void CompleteFailedConnect()
    {
        MqttClientCloseInfo info;
        lock (_gate)
        {
            if (_phase < Phase.Closing || _wasConnected)
            {
                return;
            }

            info = _closeInfo;
        }

        TaskCompletionSource<MqttConnectAck>? result = _connectResult;
        if (result is null || result.Task.IsCompleted)
        {
            return;
        }

        switch (info.Reason)
        {
            case MqttClientCloseReason.Disposed:
                result.TrySetException(new ObjectDisposedException(GetType().FullName));
                break;
            case MqttClientCloseReason.ConnectRefused:
                // The read loop builds the refusal before it records ConnectRefused.
                result.TrySetResult(_refusal!);
                break;
            case MqttClientCloseReason.ConnectTimedOut or MqttClientCloseReason.ProtocolError:
                result.TrySetResult(new MqttConnectAck(info.Reason, MqttReasonCode.UnspecifiedError, null, null, null));
                break;
            default:
                // The link ended, or a read or the CONNECT's write failed, before any CONNACK.
                result.TrySetResult(new MqttConnectAck(MqttClientCloseReason.Unreachable, MqttReasonCode.UnspecifiedError, null, null, info.Exception));
                break;
        }
    }

    /// <summary>
    /// Finishes the connection once the read loop has stopped: stops the close deadline, completes what is in
    /// flight, returns the read buffer, releases the link, and then reports the end once: to <c>OnClosed</c> for a
    /// connection that connected, to the connect otherwise.
    /// </summary>
    /// <param name="link">
    /// The link, which has ended.
    /// </param>
    private void FinishConnection(IMqttLink link)
    {
        ITimer? closeTimer;
        bool wasConnected;
        MqttClientCloseInfo info;
        lock (_gate)
        {
            closeTimer = _closeTimer;
            _closeTimer = null;
            wasConnected = _wasConnected;
            info = _closeInfo;
        }

        closeTimer?.Dispose();
        CompleteInFlightRequests();
        _receive.Clear();

        // The link has ended; aborting it now only releases the socket under it.
        link.Abort();

        if (wasConnected)
        {
            InvokeOnClosed(info);
        }
        else
        {
            CompleteFailedConnect();
        }

        SetClosed();

        // Set with the link, before the read loop started.
        _ended!.TrySetResult();
    }

    /// <summary>
    /// Calls <c>OnClosed</c>, swallowing what it throws (decision 7).
    /// </summary>
    /// <param name="info">
    /// Why the connection ended.
    /// </param>
    private void InvokeOnClosed(in MqttClientCloseInfo info)
    {
        try
        {
            _onClosed?.Invoke(info);
        }
#pragma warning disable RCS1075 // An empty catch of Exception is the point here.
        catch (Exception)
        {
            // Decision 7: what OnClosed throws is swallowed; the connection has ended and there is no one to tell.
        }
#pragma warning restore RCS1075
    }
}
