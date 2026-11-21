using System.Buffers;
using MqttWire.Client;
using MqttWire.Link;

namespace MqttWire;

/// <summary>
/// An opt-in wrapper around <see cref="MqttWireClient"/> that keeps a connection up: every attempt is a new client
/// with fresh options, failed attempts are retried after a growing delay, and what the application subscribed to is
/// subscribed again on every new connection. It does not bring back what was missed while it was away, does not
/// queue while disconnected, and sends nothing a second time.
/// </summary>
/// <remarks>
/// <para>
/// One connection cycle, a single asynchronous loop started by <see cref="StartAsync"/>, makes the attempts, waits
/// out the back-off and is the only caller of <c>OnConnected</c>, <c>OnDisconnected</c>,
/// <c>OnSubscriptionRefused</c>, <c>OnStopped</c> and <c>OnCallbackError</c>. <c>OnMessage</c> comes from the
/// current connection's read loop. Every callback runs under one callback lock, so no two ever run at the same
/// time; the lock is never held across an await, and since every callback is synchronous, a read loop waits for it
/// only while another callback runs. In a single-threaded runtime such as Blazor WebAssembly it never waits.
/// </para>
/// <para>
/// The first attempt is made at once. After a failed attempt the cycle waits <c>InitialDelay</c>, doubling with each
/// further failure up to <c>MaxDelay</c>, each with a random part of ±20 %; after a connection that was reported
/// with <c>OnConnected</c> ends, the first retry waits <c>InitialDelay</c> again.
/// </para>
/// </remarks>
public sealed class MqttWireReconnectingClient : IAsyncDisposable
{
    /// <summary>
    /// The longest delay a timer supports, 4,294,967,294 milliseconds.
    /// </summary>
    private static readonly TimeSpan s_maxSupportedDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>
    /// Stops the connection cycle when the start's token is cancelled before the start completed. The state is
    /// always the client that registered it.
    /// </summary>
    private static readonly Action<object?> s_startCanceled = static state => ((MqttWireReconnectingClient)state!).OnStartCanceled();

    /// <summary>
    /// The options, checked when the client was created.
    /// </summary>
    private readonly MqttWireReconnectOptions _options;

    /// <summary>
    /// Opens each connection's link.
    /// </summary>
    private readonly IMqttLinkConnector _connector;

    /// <summary>
    /// The clock of the back-off and of every connection.
    /// </summary>
    private readonly TimeProvider _time;

    /// <summary>
    /// Gives the random part of each delay, a number from 0 to 1.
    /// </summary>
    private readonly Func<double> _random;

    /// <summary>
    /// The handler each connection is given: <see cref="ForwardMessage"/> when the application has a message
    /// handler, otherwise <see langword="null"/>. Created once, so a message allocates nothing here.
    /// </summary>
    private readonly MqttMessageHandler? _forwardMessage;

    /// <summary>
    /// Guards the start, the remembered subscriptions and the dispose flag; never held across an await or around a
    /// callback.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// Held around every callback, so that no two run at the same time; never held across an await.
    /// </summary>
    private readonly Lock _callbackGate = new();

    /// <summary>
    /// Cancelled when the connection cycle must stop: by <see cref="DisposeAsync"/>, or by the start's token
    /// before the start completed. It holds no timer, so it needs no disposing.
    /// </summary>
#pragma warning disable S2930 // Holds no timer, and the start's token may still cancel it after the dispose.
    private readonly CancellationTokenSource _stop = new();
#pragma warning restore S2930

    /// <summary>
    /// The outcome of <see cref="StartAsync"/>.
    /// </summary>
    private readonly TaskCompletionSource<MqttConnectAck> _started = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Completes when the connection cycle has ended and every client it created is disposed.
    /// </summary>
    private readonly TaskCompletionSource _ended = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// The subscriptions to make again on each new connection, in the order they were first granted, with the QoS
    /// last asked for; under <see cref="_gate"/>.
    /// </summary>
    private readonly OrderedDictionary<string, MqttQoS> _subscriptions = new(StringComparer.Ordinal);

    /// <summary>
    /// For each filter with a subscribe in flight, how many are in flight and how often the filter was unsubscribed
    /// meanwhile; under <see cref="_gate"/>. An entry is removed when the last subscribe to its filter ends, so the
    /// table holds only filters with a subscribe in flight.
    /// </summary>
    private readonly Dictionary<string, SubscribesInFlight> _subscribesInFlight = new(StringComparer.Ordinal);

    /// <summary>
    /// The connection the application's requests go to, from just before <c>OnConnected</c> until the cycle sees
    /// the connection end; otherwise <see langword="null"/>.
    /// </summary>
    private volatile MqttWireClient? _current;

    /// <summary>
    /// Whether <see cref="StartAsync"/> was called; under <see cref="_gate"/>.
    /// </summary>
    private bool _startCalled;

    /// <summary>
    /// Whether the start's outcome is decided, so its token no longer matters; under <see cref="_gate"/>.
    /// </summary>
    private bool _startDecided;

    /// <summary>
    /// Whether the start's token stopped the cycle before the start was decided; under <see cref="_gate"/>.
    /// </summary>
    private bool _startCanceled;

    /// <summary>
    /// Whether <see cref="DisposeAsync"/> was called; under <see cref="_gate"/>.
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// The registration of <see cref="s_startCanceled"/> on the start's token; set once, before the cycle starts.
    /// </summary>
    private CancellationTokenRegistration _startRegistration;

    /// <summary>
    /// Creates a reconnecting client. Nothing is connected until <see cref="StartAsync"/> is called.
    /// </summary>
    /// <param name="options">
    /// The options.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> or its <see cref="MqttWireReconnectOptions.ClientOptions"/> is
    /// <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="MqttWireReconnectOptions.InitialDelay"/> is not greater than zero, or
    /// <see cref="MqttWireReconnectOptions.MaxDelay"/> is less than it or more than 4,294,967,294 milliseconds.
    /// </exception>
    public MqttWireReconnectingClient(MqttWireReconnectOptions options)
        : this(options, ClientWebSocketConnector.Instance, TimeProvider.System, Random.Shared.NextDouble)
    {
    }

    /// <summary>
    /// Creates a reconnecting client with its collaborators given, for tests.
    /// </summary>
    /// <param name="options">
    /// The options.
    /// </param>
    /// <param name="connector">
    /// Opens each connection's link.
    /// </param>
    /// <param name="timeProvider">
    /// The clock of the back-off and of every connection.
    /// </param>
    /// <param name="random">
    /// Gives the random part of each delay, a number from 0 to 1.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/>, its <see cref="MqttWireReconnectOptions.ClientOptions"/>, or a collaborator is
    /// <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// A delay is out of range; see the public constructor.
    /// </exception>
    internal MqttWireReconnectingClient(
        MqttWireReconnectOptions options,
        IMqttLinkConnector connector,
        TimeProvider timeProvider,
        Func<double> random)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(connector);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(random);

        // A required member can still be null when the caller suppresses the compiler's warning.
        if (options.ClientOptions is null)
        {
            throw new ArgumentNullException(nameof(options), "The ClientOptions callback is required.");
        }

        if (options.InitialDelay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.InitialDelay, "The initial delay must be greater than zero.");
        }

        if (options.MaxDelay < options.InitialDelay || options.MaxDelay > s_maxSupportedDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.MaxDelay, "The maximum delay must be at least the initial delay and at most 4,294,967,294 milliseconds.");
        }

        _options = options;
        _connector = connector;
        _time = timeProvider;
        _random = random;
        _forwardMessage = options.OnMessage is null ? null : ForwardMessage;
    }

    /// <summary>
    /// How an attempt ended.
    /// </summary>
    private enum AttemptEnd
    {
        /// <summary>
        /// No connection was reported: the options, the client or the connect failed, the server refused in a way
        /// that may pass, or the connection ended before <c>OnConnected</c>. The back-off grows.
        /// </summary>
        Failed,

        /// <summary>
        /// A connection was reported with <c>OnConnected</c> and has ended, with <c>OnDisconnected</c> called. The
        /// back-off starts again.
        /// </summary>
        Disconnected,

        /// <summary>
        /// The server refused in a way the same CONNECT would meet again. The cycle stops for good.
        /// </summary>
        Stopped,
    }

    /// <summary>
    /// Gets a task that completes when the connection cycle has ended, because it stopped, its start was
    /// cancelled or the client was disposed, and every client it created is disposed. It fails only through a bug
    /// in the cycle.
    /// </summary>
    internal Task Running => _ended.Task;

    /// <summary>
    /// Starts the connection cycle: the first attempt is made at once, and failures are retried after the
    /// back-off.
    /// </summary>
    /// <param name="cancellationToken">
    /// Stops the reconnecting client when it is cancelled before the start has completed: no further attempt is
    /// made, and the client can only be disposed. Once the start has completed the token is not used.
    /// </param>
    /// <returns>
    /// The CONNACK of the first connection, once the remembered subscriptions were made and <c>OnConnected</c> has
    /// returned; or the refusal that stopped the client before any connection (bad user name or password and the
    /// like, see <see cref="MqttWireReconnectOptions.OnStopped"/>), once <c>OnStopped</c> has returned. Every other
    /// failure is retried, so the task waits until a connection is made. It fails with
    /// <see cref="OperationCanceledException"/> when <paramref name="cancellationToken"/> stopped the client, and
    /// with <see cref="ObjectDisposedException"/> when the client was disposed before either outcome.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    /// The client was disposed.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// <see cref="StartAsync"/> was already called.
    /// </exception>
    public Task<MqttConnectAck> StartAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_startCalled)
            {
                throw new InvalidOperationException("StartAsync can be called once.");
            }

            _startCalled = true;
        }

        // A token already cancelled runs the callback now, so the cycle stops before its first attempt.
        _startRegistration = cancellationToken.UnsafeRegister(s_startCanceled, this);
        _ = RunAsync(cancellationToken);
        return _started.Task;
    }

    /// <summary>
    /// Subscribes to one topic filter on the current connection, and remembers it when the server grants it, so
    /// that every later connection subscribes to it again.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter, which may hold wildcards.
    /// </param>
    /// <param name="qos">
    /// The highest QoS asked for, now and on every later connection.
    /// </param>
    /// <param name="cancellationToken">
    /// Ends the wait for the SUBACK; see <see cref="MqttWireClient.SubscribeAsync"/>.
    /// </param>
    /// <returns>
    /// The outcome, as <see cref="MqttWireClient.SubscribeAsync"/> reports it. When there is no connection it
    /// fails at once with <see cref="MqttRequestStatus.NotConnected"/>, nothing is sent and nothing is remembered:
    /// there is no queue. Only a granted subscription (<see cref="MqttRequestResult.IsSuccess"/>) is remembered; a
    /// refused one leaves what was remembered as it was. A grant is not remembered when
    /// <see cref="UnsubscribeAsync"/> was called for the same filter after this call began: the server processes the
    /// UNSUBSCRIBE after this SUBSCRIBE, so the filter ends unsubscribed.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="topicFilter"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topicFilter"/> is not a valid topic filter, or cannot be encoded: it is empty, misplaces a
    /// wildcard, holds U+0000, has an unpaired surrogate, or is over 65,535 UTF-8 bytes. Thrown whether connected
    /// or not; nothing is sent.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="qos"/> is above 2.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="cancellationToken"/> was cancelled before the call (when connected, the task returned
    /// fails with it), or while the SUBACK was awaited; the subscription is then not remembered.
    /// </exception>
    public ValueTask<MqttRequestResult> SubscribeAsync(string topicFilter, MqttQoS qos, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topicFilter);
        if (qos > MqttQoS.ExactlyOnce)
        {
            // Not ThrowIfGreaterThan: an enum is not IComparable<T>, and a cast would name "(byte)qos".
            throw new ArgumentOutOfRangeException(nameof(qos), qos, "The QoS must be 0, 1 or 2.");
        }

        if (_current is not MqttWireClient client)
        {
            // Checked here too, so that a caller's bug does not depend on the network.
            TopicChecks.ThrowIfInvalidTopicFilter(topicFilter, ArrayPool<byte>.Shared);
            cancellationToken.ThrowIfCancellationRequested();
            return NotConnected();
        }

        int unsubscribes = BeginSubscribe(topicFilter);
        ValueTask<MqttRequestResult> request;
        try
        {
            request = client.SubscribeAsync(topicFilter, qos, cancellationToken);
        }
        catch
        {
            EndSubscribe(topicFilter, qos, unsubscribes, granted: false);
            throw;
        }

        return RememberWhenGrantedAsync(request, topicFilter, qos, unsubscribes);
    }

    /// <summary>
    /// Forgets a topic filter, so that no later connection subscribes to it again, and unsubscribes from it on the
    /// current connection.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter, exactly as it was subscribed.
    /// </param>
    /// <param name="cancellationToken">
    /// Ends the wait for the UNSUBACK; see <see cref="MqttWireClient.UnsubscribeAsync"/>.
    /// </param>
    /// <returns>
    /// The outcome, as <see cref="MqttWireClient.UnsubscribeAsync"/> reports it. When there is no connection it
    /// fails at once with <see cref="MqttRequestStatus.NotConnected"/> and nothing is sent. The filter is forgotten
    /// whatever the outcome, and a subscribe to it already in flight is not remembered when granted. A connection
    /// that is subscribing again to the filter at that moment unsubscribes from it before <c>OnConnected</c>.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="topicFilter"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topicFilter"/> is not a valid topic filter, or cannot be encoded, as
    /// <see cref="SubscribeAsync"/> checks it. Thrown whether connected or not; nothing is sent and nothing is
    /// forgotten.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="cancellationToken"/> was cancelled before the call (nothing is forgotten), or while the
    /// UNSUBACK was awaited.
    /// </exception>
    public ValueTask<MqttRequestResult> UnsubscribeAsync(string topicFilter, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topicFilter);
        TopicChecks.ThrowIfInvalidTopicFilter(topicFilter, ArrayPool<byte>.Shared);
        cancellationToken.ThrowIfCancellationRequested();
        MqttWireClient? client;
        lock (_gate)
        {
            _subscriptions.Remove(topicFilter);
            if (_subscribesInFlight.TryGetValue(topicFilter, out SubscribesInFlight? inFlight))
            {
                // A subscribe already in flight must not remember the filter when its SUBACK comes.
                inFlight.Unsubscribes++;
            }

            // Read under the lock: a new connection is published under it only once every filter forgotten during
            // its re-subscribe was unsubscribed there, so either that connection sees this filter forgotten, or this
            // call sees the connection.
            client = _current;
        }

        return client is not null ? client.UnsubscribeAsync(topicFilter, cancellationToken) : NotConnected();
    }

    /// <summary>
    /// Publishes a message on the current connection.
    /// </summary>
    /// <param name="topic">
    /// The topic name.
    /// </param>
    /// <param name="payload">
    /// The payload; copied before this returns.
    /// </param>
    /// <param name="qos">
    /// The quality of service.
    /// </param>
    /// <returns>
    /// The outcome, as <see cref="MqttWireClient.PublishAsync"/> reports it. When there is no connection it fails
    /// at once with <see cref="MqttRequestStatus.NotConnected"/> and nothing is sent: there is no queue. A publish
    /// in flight when the connection is lost completes with <see cref="MqttRequestStatus.ConnectionLost"/>: the
    /// server may or may not have it, and it is not sent again.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="topic"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topic"/> is empty, holds <c>+</c>, <c>#</c> or U+0000, has an unpaired surrogate, or is over
    /// 65,535 UTF-8 bytes. Thrown whether connected or not; nothing is sent.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="qos"/> is above 2.
    /// </exception>
    public ValueTask<MqttRequestResult> PublishAsync(string topic, ReadOnlyMemory<byte> payload, MqttQoS qos)
    {
        ArgumentNullException.ThrowIfNull(topic);
        if (qos > MqttQoS.ExactlyOnce)
        {
            // Not ThrowIfGreaterThan: an enum is not IComparable<T>, and a cast would name "(byte)qos".
            throw new ArgumentOutOfRangeException(nameof(qos), qos, "The QoS must be 0, 1 or 2.");
        }

        if (_current is MqttWireClient client)
        {
            return client.PublishAsync(topic, payload, qos);
        }

        // Checked here too, so that a caller's bug does not depend on the network.
        TopicChecks.ThrowIfInvalidTopicName(topic, ArrayPool<byte>.Shared);
        return NotConnected();
    }

    /// <summary>
    /// Stops everything at once, whether the client is waiting out a back-off, making an attempt or connected, and
    /// waits until it has stopped. A connection is ended as <see cref="MqttWireClient.DisposeAsync"/> ends it, and
    /// <c>OnDisconnected</c> reports it if <c>OnConnected</c> did. Calling it again only waits for that end.
    /// </summary>
    /// <returns>
    /// A task that completes when the connection cycle has ended and every client it created is disposed. No
    /// callback is called after it has completed. Do not wait for it inside a callback: the cycle cannot end until
    /// the callback returns. It does not throw, unless the cycle failed through a bug.
    /// </returns>
    public async ValueTask DisposeAsync()
    {
        bool started;
        lock (_gate)
        {
            _disposed = true;
            started = _startCalled;
        }

        await _stop.CancelAsync().ConfigureAwait(false);
        if (started)
        {
            await _ended.Task.ConfigureAwait(false);
        }
        else
        {
            _ended.TrySetResult();
        }
    }

    /// <summary>
    /// Tells whether a CONNACK refusal is one the same CONNECT would meet again, so that retrying cannot help.
    /// </summary>
    /// <param name="reasonCode">
    /// The CONNACK's reason code.
    /// </param>
    /// <returns>
    /// <see langword="true"/> for unsupported protocol version, client identifier not valid, bad user name or
    /// password, not authorized and banned.
    /// </returns>
    private static bool IsPermanentRefusal(MqttReasonCode reasonCode) => reasonCode
        is MqttReasonCode.UnsupportedProtocolVersion
        or MqttReasonCode.ClientIdentifierNotValid
        or MqttReasonCode.BadUserNameOrPassword
        or MqttReasonCode.NotAuthorized
        or MqttReasonCode.Banned;

    /// <summary>
    /// Checks the options the <c>ClientOptions</c> callback returned for one connection.
    /// </summary>
    /// <param name="clientOptions">
    /// The options.
    /// </param>
    /// <returns>
    /// The same options.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// The callback returned <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The options set <see cref="MqttWireClientOptions.OnMessage"/> or <see cref="MqttWireClientOptions.OnClosed"/>,
    /// which the reconnecting client must own.
    /// </exception>
    private static MqttWireClientOptions CheckClientOptions(MqttWireClientOptions? clientOptions)
    {
        ArgumentNullException.ThrowIfNull(clientOptions);
        if (clientOptions.OnMessage is not null || clientOptions.OnClosed is not null)
        {
            throw new ArgumentException(
                "The options of a reconnecting client's connection must leave OnMessage and OnClosed null; set them on MqttWireReconnectOptions.",
                nameof(clientOptions));
        }

        return clientOptions;
    }

    /// <summary>
    /// The outcome of a request made without a connection.
    /// </summary>
    /// <returns>
    /// A completed <see cref="MqttRequestStatus.NotConnected"/>.
    /// </returns>
    private static ValueTask<MqttRequestResult> NotConnected()
        => new(new MqttRequestResult(MqttRequestStatus.NotConnected, MqttReasonCode.UnspecifiedError));

    /// <summary>
    /// Waits for a subscribe and remembers the filter when the server granted it.
    /// </summary>
    /// <param name="request">
    /// The subscribe on the current connection.
    /// </param>
    /// <param name="topicFilter">
    /// The topic filter.
    /// </param>
    /// <param name="qos">
    /// The QoS asked for.
    /// </param>
    /// <param name="unsubscribes">
    /// The filter's unsubscribe count when the subscribe began, from <see cref="BeginSubscribe"/>.
    /// </param>
    /// <returns>
    /// The subscribe's outcome; it throws what the subscribe throws, and then nothing is remembered. A grant is
    /// not remembered when the filter was unsubscribed since the subscribe began.
    /// </returns>
    private async ValueTask<MqttRequestResult> RememberWhenGrantedAsync(
        ValueTask<MqttRequestResult> request,
        string topicFilter,
        MqttQoS qos,
        int unsubscribes)
    {
        bool granted = false;
        try
        {
            MqttRequestResult result = await request.ConfigureAwait(false);
            granted = result.IsSuccess;
            return result;
        }
        finally
        {
            EndSubscribe(topicFilter, qos, unsubscribes, granted);
        }
    }

    /// <summary>
    /// Counts a subscribe to a filter as in flight, so that an unsubscribe of the filter before its SUBACK keeps it
    /// from being remembered.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter.
    /// </param>
    /// <returns>
    /// The filter's unsubscribe count now, for <see cref="EndSubscribe"/> to compare.
    /// </returns>
    private int BeginSubscribe(string topicFilter)
    {
        lock (_gate)
        {
            if (!_subscribesInFlight.TryGetValue(topicFilter, out SubscribesInFlight? inFlight))
            {
                inFlight = new SubscribesInFlight();
                _subscribesInFlight.Add(topicFilter, inFlight);
            }

            inFlight.Count++;
            return inFlight.Unsubscribes;
        }
    }

    /// <summary>
    /// Ends a subscribe counted by <see cref="BeginSubscribe"/>: remembers the filter when the server granted it
    /// and it was not unsubscribed since, and drops the filter's entry when no other subscribe to it is in flight.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter.
    /// </param>
    /// <param name="qos">
    /// The QoS asked for.
    /// </param>
    /// <param name="unsubscribes">
    /// The filter's unsubscribe count when the subscribe began.
    /// </param>
    /// <param name="granted">
    /// Whether the server granted the subscription.
    /// </param>
    private void EndSubscribe(string topicFilter, MqttQoS qos, int unsubscribes, bool granted)
    {
        lock (_gate)
        {
            SubscribesInFlight inFlight = _subscribesInFlight[topicFilter];
            if (granted && inFlight.Unsubscribes == unsubscribes)
            {
                _subscriptions[topicFilter] = qos;
            }

            inFlight.Count--;
            if (inFlight.Count == 0)
            {
                _subscribesInFlight.Remove(topicFilter);
            }
        }
    }

    /// <summary>
    /// Stops the connection cycle because the start's token was cancelled, unless the start is already decided.
    /// </summary>
    private void OnStartCanceled()
    {
        lock (_gate)
        {
            if (_startDecided)
            {
                return;
            }

            _startCanceled = true;
        }

        _stop.Cancel();
    }

    /// <summary>
    /// Completes the start with an outcome, unless it is already decided or its token stopped the cycle.
    /// </summary>
    /// <param name="ack">
    /// The first connection's CONNACK, or the refusal that stopped the client.
    /// </param>
    private void CompleteStart(MqttConnectAck ack)
    {
        lock (_gate)
        {
            if (_startDecided || _startCanceled)
            {
                return;
            }

            _startDecided = true;
        }

        _startRegistration.Unregister();
        _started.TrySetResult(ack);
    }

    /// <summary>
    /// Fails a start that the cycle ended without deciding: cancelled when its token stopped the cycle, otherwise
    /// disposed, or the bug that ended the cycle.
    /// </summary>
    /// <param name="startToken">
    /// The start's token.
    /// </param>
    /// <param name="failure">
    /// The bug that ended the cycle, or <see langword="null"/>.
    /// </param>
    private void FinishStart(CancellationToken startToken, Exception? failure)
    {
        bool canceled;
        lock (_gate)
        {
            if (_startDecided)
            {
                return;
            }

            _startDecided = true;
            canceled = _startCanceled;
        }

        _startRegistration.Unregister();
        if (failure is not null)
        {
            _started.TrySetException(failure);
        }
        else if (canceled)
        {
            _started.TrySetCanceled(startToken);
        }
        else
        {
            _started.TrySetException(new ObjectDisposedException(GetType().FullName));
        }
    }

    /// <summary>
    /// The connection cycle: makes attempts until one is refused for good or the cycle is stopped, waiting out the
    /// back-off between them. It is the only caller of the callbacks other than <c>OnMessage</c>.
    /// </summary>
    /// <param name="startToken">
    /// The start's token, for the cancellation the start fails with.
    /// </param>
    /// <returns>
    /// A task that completes when the cycle has ended; it never fails. <see cref="Running"/> reports the end.
    /// </returns>
    private async Task RunAsync(CancellationToken startToken)
    {
        CancellationToken stop = _stop.Token;
        Exception? failure = null;
        try
        {
            int retry = 0;
            bool connectedBefore = false;
            while (true)
            {
                stop.ThrowIfCancellationRequested();
                if (retry > 0)
                {
                    TimeSpan delay = ReconnectBackoff.GetDelay(retry, _options.InitialDelay, _options.MaxDelay, _random());
                    await Task.Delay(delay, _time, stop).ConfigureAwait(false);
                }

                (AttemptEnd end, MqttConnectAck? refusal) = await AttemptAsync(connectedBefore, stop).ConfigureAwait(false);
                if (end == AttemptEnd.Stopped)
                {
                    // AttemptAsync returns the refusal with Stopped.
                    Invoke(_options.OnStopped, refusal!);
                    CompleteStart(refusal!);
                    return;
                }

                if (end == AttemptEnd.Disconnected)
                {
                    connectedBefore = true;
                    retry = 1;
                }
                else if (retry < int.MaxValue)
                {
                    retry++;
                }
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
            // Disposed, or the start's token stopped the cycle.
        }
        catch (Exception exception)
        {
            // A bug in the cycle: it ends, and the start and the dispose report it.
            failure = exception;
        }
        finally
        {
            FinishStart(startToken, failure);
            if (failure is null)
            {
                _ended.TrySetResult();
            }
            else
            {
                _ended.TrySetException(failure);
            }
        }
    }

    /// <summary>
    /// Makes one attempt: asks for options, creates a new client and connects it; when it connects, subscribes again
    /// to what is remembered, reports the connection, and waits for it to end. Every client it creates is disposed
    /// before it returns or throws.
    /// </summary>
    /// <param name="isReconnect">
    /// Whether a connection was reported before.
    /// </param>
    /// <param name="stop">
    /// Stops the attempt.
    /// </param>
    /// <returns>
    /// How the attempt ended, with the refusal when it is <see cref="AttemptEnd.Stopped"/>.
    /// </returns>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="stop"/> was cancelled. A connection that was reported is first reported as ended.
    /// </exception>
    private async Task<(AttemptEnd End, MqttConnectAck? Refusal)> AttemptAsync(bool isReconnect, CancellationToken stop)
    {
        TaskCompletionSource<MqttClientCloseInfo> closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        MqttWireClient client;
        try
        {
            MqttWireClientOptions clientOptions = CheckClientOptions(await _options.ClientOptions(stop).ConfigureAwait(false));
            client = new MqttWireClient(clientOptions, _forwardMessage, info => closed.TrySetResult(info), _connector, _time, ArrayPool<byte>.Shared);
        }
        catch (Exception exception)
        {
            stop.ThrowIfCancellationRequested();
            ReportError(exception);
            return (AttemptEnd.Failed, null);
        }

        try
        {
            MqttConnectAck ack;
            try
            {
                ack = await client.ConnectAsync(stop).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                // Stopped, or ConfigureWebSocket threw.
                stop.ThrowIfCancellationRequested();
                ReportError(exception);
                return (AttemptEnd.Failed, null);
            }

            if (!ack.IsConnected)
            {
                return ack.Failure == MqttClientCloseReason.ConnectRefused && IsPermanentRefusal(ack.ReasonCode)
                    ? (AttemptEnd.Stopped, ack)
                    : (AttemptEnd.Failed, null);
            }

            (bool resubscribed, List<string>? granted) = await ResubscribeAsync(client, stop).ConfigureAwait(false);
            if (!resubscribed)
            {
                return (AttemptEnd.Failed, null);
            }

            PublishConnection(client, granted);
            Invoke(_options.OnConnected, new MqttConnectedInfo(isReconnect, ack));
            CompleteStart(ack);
            await EndConnectionAsync(client, closed, stop).ConfigureAwait(false);
            return (AttemptEnd.Disconnected, null);
        }
        finally
        {
            // Waits until the client has finished, so no OnMessage comes after it; a second call only waits.
            await client.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Waits for a reported connection to end, or for the cycle to stop, then disposes its client and reports the
    /// end with <c>OnDisconnected</c>.
    /// </summary>
    /// <param name="client">
    /// The connection's client.
    /// </param>
    /// <param name="closed">
    /// Completes with the client's <c>OnClosed</c>.
    /// </param>
    /// <param name="stop">
    /// Stops the wait; the connection is then ended by disposing its client.
    /// </param>
    /// <returns>
    /// A task that completes once <c>OnDisconnected</c> has returned.
    /// </returns>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="stop"/> was cancelled; the end was reported first.
    /// </exception>
    private async Task EndConnectionAsync(MqttWireClient client, TaskCompletionSource<MqttClientCloseInfo> closed, CancellationToken stop)
    {
        bool stopping = false;
        try
        {
            await closed.Task.WaitAsync(stop).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
            stopping = true;
        }

        _current = null;
        await client.DisposeAsync().ConfigureAwait(false);

        // The client calls OnClosed for a connection that connected before its dispose completes; this only keeps
        // the wait from hanging should it not.
        closed.TrySetResult(new MqttClientCloseInfo(MqttClientCloseReason.Disposed, MqttReasonCode.Success, null, null));
        Invoke(_options.OnDisconnected, await closed.Task.ConfigureAwait(false));
        if (stopping)
        {
            throw new OperationCanceledException(stop);
        }
    }

    /// <summary>
    /// Subscribes a new connection to every remembered filter, one after the other in the order they were first
    /// granted. A filter the server refuses is forgotten and reported with <c>OnSubscriptionRefused</c>.
    /// </summary>
    /// <param name="client">
    /// The new connection's client.
    /// </param>
    /// <param name="stop">
    /// Stops the wait for a SUBACK.
    /// </param>
    /// <returns>
    /// Whether every filter was granted or refused (<see langword="false"/> when the connection ended first), and
    /// the filters the server granted, or <see langword="null"/> when there were none. What is remembered is changed
    /// only by refusals; a granted filter the application unsubscribed meanwhile is left to
    /// <see cref="PublishConnection"/>.
    /// </returns>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="stop"/> was cancelled.
    /// </exception>
    private async Task<(bool Completed, List<string>? Granted)> ResubscribeAsync(MqttWireClient client, CancellationToken stop)
    {
        KeyValuePair<string, MqttQoS>[] remembered;
        lock (_gate)
        {
            if (_subscriptions.Count == 0)
            {
                return (true, null);
            }

            remembered = [.. _subscriptions];
        }

        List<string>? granted = null;

        foreach ((string filter, MqttQoS qos) in remembered)
        {
            lock (_gate)
            {
                // The application may have unsubscribed meanwhile.
                if (!_subscriptions.ContainsKey(filter))
                {
                    continue;
                }
            }

            MqttRequestResult result = await client.SubscribeAsync(filter, qos, stop).ConfigureAwait(false);
            if (result.IsSuccess)
            {
                (granted ??= []).Add(filter);
                continue;
            }

            if (result.Status is MqttRequestStatus.NotConnected or MqttRequestStatus.ConnectionLost)
            {
                return (false, null);
            }

            lock (_gate)
            {
                _subscriptions.Remove(filter);
            }

            Invoke(_options.OnSubscriptionRefused, filter, result);
        }

        return (true, granted);
    }

    /// <summary>
    /// Makes a connection whose re-subscribe completed the one the application's requests go to. First, every
    /// granted filter the application unsubscribed during the re-subscribe is unsubscribed on the connection,
    /// without waiting for the UNSUBACK; the connection is published under <see cref="_gate"/> only once no such
    /// filter is left, so an <see cref="UnsubscribeAsync"/> either came before and is handled here, or sees the
    /// connection and unsubscribes there itself, and the application's own requests always follow these
    /// UNSUBSCRIBEs.
    /// </summary>
    /// <param name="client">
    /// The new connection's client.
    /// </param>
    /// <param name="granted">
    /// The filters the re-subscribe was granted, or <see langword="null"/>; the ones unsubscribed here are removed.
    /// </param>
    private void PublishConnection(MqttWireClient client, List<string>? granted)
    {
        while (true)
        {
            List<string>? forgotten = null;
            lock (_gate)
            {
                if (granted is not null)
                {
                    int kept = 0;
                    for (int i = 0; i < granted.Count; i++)
                    {
                        string filter = granted[i];
                        if (_subscriptions.ContainsKey(filter))
                        {
                            granted[kept++] = filter;
                        }
                        else
                        {
                            (forgotten ??= []).Add(filter);
                        }
                    }

                    granted.RemoveRange(kept, granted.Count - kept);
                }

                if (forgotten is null)
                {
                    _current = client;
                    return;
                }
            }

            foreach (string filter in forgotten)
            {
                // Not awaited: OnConnected need not wait for the UNSUBACK, and a request that is never awaited is
                // left to the garbage collector. A connection that has ended meanwhile sends nothing.
                _ = client.UnsubscribeAsync(filter, CancellationToken.None);
            }
        }
    }

    /// <summary>
    /// Calls the application's message handler under the callback lock; set as each connection's handler. An
    /// exception the handler throws passes to the connection, which ends with
    /// <see cref="MqttClientCloseReason.MessageHandlerThrew"/>.
    /// </summary>
    /// <param name="message">
    /// The message, valid only during the call.
    /// </param>
    private void ForwardMessage(in MqttPublish message)
    {
        lock (_callbackGate)
        {
            // Only set as a connection's handler when the application has one.
            _options.OnMessage!(in message);
        }
    }

    /// <summary>
    /// Calls a callback under the callback lock and reports what it throws.
    /// </summary>
    /// <typeparam name="T">
    /// The callback's argument.
    /// </typeparam>
    /// <param name="callback">
    /// The callback, or <see langword="null"/> to do nothing.
    /// </param>
    /// <param name="argument">
    /// Its argument.
    /// </param>
    private void Invoke<T>(Action<T>? callback, T argument)
    {
        if (callback is null)
        {
            return;
        }

        lock (_callbackGate)
        {
            try
            {
                callback(argument);
            }
            catch (Exception exception)
            {
                ReportErrorLocked(exception);
            }
        }
    }

    /// <summary>
    /// Calls a two-argument callback under the callback lock and reports what it throws.
    /// </summary>
    /// <param name="callback">
    /// The callback, or <see langword="null"/> to do nothing.
    /// </param>
    /// <param name="filter">
    /// The topic filter.
    /// </param>
    /// <param name="result">
    /// The outcome.
    /// </param>
    private void Invoke(Action<string, MqttRequestResult>? callback, string filter, MqttRequestResult result)
    {
        if (callback is null)
        {
            return;
        }

        lock (_callbackGate)
        {
            try
            {
                callback(filter, result);
            }
            catch (Exception exception)
            {
                ReportErrorLocked(exception);
            }
        }
    }

    /// <summary>
    /// Reports an exception to <c>OnCallbackError</c> under the callback lock.
    /// </summary>
    /// <param name="exception">
    /// The exception.
    /// </param>
    private void ReportError(Exception exception)
    {
        lock (_callbackGate)
        {
            ReportErrorLocked(exception);
        }
    }

    /// <summary>
    /// Reports an exception to <c>OnCallbackError</c>; the caller holds the callback lock. An exception that
    /// callback throws is ignored: there is nowhere left to report it.
    /// </summary>
    /// <param name="exception">
    /// The exception.
    /// </param>
    private void ReportErrorLocked(Exception exception)
    {
        if (_options.OnCallbackError is not Action<Exception> onError)
        {
            return;
        }

        try
        {
            onError(exception);
        }
#pragma warning disable RCS1075 // An empty catch of Exception is the point here.
        catch (Exception)
        {
            // OnCallbackError itself failed; there is nowhere left to report it, and the cycle carries on.
        }
#pragma warning restore RCS1075
    }

    /// <summary>
    /// The subscribes in flight to one filter, and how often the filter was unsubscribed while any was; under
    /// <see cref="_gate"/>.
    /// </summary>
    private sealed class SubscribesInFlight
    {
        /// <summary>
        /// Gets or sets the number of subscribes to the filter in flight.
        /// </summary>
        public int Count { get; set; }

        /// <summary>
        /// Gets or sets the number of unsubscribes of the filter since the entry was made; a subscribe remembers
        /// the filter only when this is what it was when the subscribe began.
        /// </summary>
        public int Unsubscribes { get; set; }
    }
}
