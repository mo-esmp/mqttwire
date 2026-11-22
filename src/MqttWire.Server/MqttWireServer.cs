using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MqttWire.Link;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <summary>
/// The running server: lets the host look up the connections it has admitted. It has no public constructor; the
/// host gets it from dependency injection, and its handler may take it in its constructor.
/// </summary>
public sealed class MqttWireServer
{
    /// <summary>
    /// The services the handler is resolved from when the server starts, or <see langword="null"/> for a server
    /// that was given its handler.
    /// </summary>
    private readonly IServiceProvider? _services;

    /// <summary>
    /// The host's handler; <see langword="null"/> until it is resolved from <see cref="_services"/>, by
    /// <see cref="Start"/> or by whoever needs it first.
    /// </summary>
    private IMqttWireHandler? _handler;

    /// <summary>
    /// The one timer that runs <see cref="Sweep"/>, or <see langword="null"/> before <see cref="Start"/>; written
    /// and read under <see cref="ConnectionRegistry.Gate"/>.
    /// </summary>
    private ITimer? _sweepTimer;

    /// <summary>
    /// Completes when the stop has ended, or <see langword="null"/> before the first call of
    /// <see cref="StopAsync"/>; set once, under <see cref="ConnectionRegistry.Gate"/>.
    /// </summary>
    private TaskCompletionSource? _stopped;

    /// <summary>
    /// Creates the server; it enforces no deadline until <see cref="Start"/>.
    /// </summary>
    /// <param name="handler">
    /// The host's handler, called for every connection.
    /// </param>
    /// <param name="options">
    /// The options, already validated.
    /// </param>
    /// <param name="logger">
    /// The logger.
    /// </param>
    /// <param name="time">
    /// The clock every deadline is measured on, and that runs the sweep's timer.
    /// </param>
    /// <param name="pool">
    /// The pool every buffer is rented from.
    /// </param>
    /// <param name="pools">
    /// The pools of the queues and exchange records, or <see langword="null"/> for the server's own; given only by
    /// the tests of a pool that fails.
    /// </param>
    internal MqttWireServer(IMqttWireHandler handler, MqttWireServerOptions options, ILogger<MqttWireServer> logger,
        TimeProvider time, ArrayPool<byte> pool, ServerPools? pools = null)
        : this(options, logger, time, pool, pools) =>
        _handler = handler;

    /// <summary>
    /// Creates the server the registration builds: without its handler, which is resolved from
    /// <paramref name="services"/> when the server starts. So the handler may itself take the server in its
    /// constructor, to look connections up, without the two waiting for each other.
    /// </summary>
    /// <param name="services">
    /// The services that hold the one <see cref="IMqttWireHandler"/>.
    /// </param>
    /// <param name="options">
    /// The options, already validated.
    /// </param>
    /// <param name="logger">
    /// The logger.
    /// </param>
    /// <param name="time">
    /// The clock every deadline is measured on, and that runs the sweep's timer.
    /// </param>
    /// <param name="pool">
    /// The pool every buffer is rented from.
    /// </param>
    internal MqttWireServer(IServiceProvider services, MqttWireServerOptions options, ILogger<MqttWireServer> logger,
        TimeProvider time, ArrayPool<byte> pool)
        : this(options, logger, time, pool, pools: null) =>
        _services = services;

    /// <summary>
    /// Creates the server but for its handler.
    /// </summary>
    /// <param name="options">
    /// The options, already validated.
    /// </param>
    /// <param name="logger">
    /// The logger.
    /// </param>
    /// <param name="time">
    /// The clock every deadline is measured on, and that runs the sweep's timer.
    /// </param>
    /// <param name="pool">
    /// The pool every buffer is rented from.
    /// </param>
    /// <param name="pools">
    /// The pools of the queues and exchange records, or <see langword="null"/> for the server's own.
    /// </param>
    private MqttWireServer(MqttWireServerOptions options, ILogger<MqttWireServer> logger, TimeProvider time,
        ArrayPool<byte> pool, ServerPools? pools)
    {
        Options = options;
        Logger = logger;
        Time = time;
        Pool = pool;
        Pools = pools ?? new ServerPools();
        ConnectDeadlineLength = ToTimestampLength(ServerLimits.ConnectDeadline, time);
        AuthenticationDeadlineLength = ToTimestampLength(ServerLimits.AuthenticationDeadline, time);
        CloseDeadlineLength = ToTimestampLength(ServerLimits.CloseDeadline, time);
        PartialPacketDeadlineLength = ToTimestampLength(ServerLimits.PartialPacketDeadline, time);
        WriteDeadlineLength = ToTimestampLength(ServerLimits.WriteDeadline, time);
        AcknowledgementDeadlineLength = ToTimestampLength(ServerLimits.AcknowledgementDeadline, time);
    }

    /// <summary>
    /// Gets the number of connections that have a client identifier's place: every connection from its admission
    /// until it has closed, one that is closing included, counted until just before its
    /// <see cref="IMqttWireHandler.OnClosed"/>.
    /// </summary>
    public int ConnectionCount => Registry.ActiveCount;

    /// <summary>
    /// Gets the host's handler. <see cref="Start"/> resolves it, and so does the first connection of a server that
    /// was not started, so every later read is that of a field.
    /// </summary>
    /// <exception cref="Exception">
    /// The handler was not resolved yet and could not be: its constructor threw, or it is not registered.
    /// </exception>
    internal IMqttWireHandler Handler => _handler ?? ResolveHandler();

    /// <summary>
    /// Gets the options.
    /// </summary>
    internal MqttWireServerOptions Options { get; }

    /// <summary>
    /// Gets the logger.
    /// </summary>
    internal ILogger<MqttWireServer> Logger { get; }

    /// <summary>
    /// Gets the clock every deadline is measured on.
    /// </summary>
    internal TimeProvider Time { get; }

    /// <summary>
    /// Gets the pool every buffer is rented from.
    /// </summary>
    internal ArrayPool<byte> Pool { get; }

    /// <summary>
    /// Gets the connections: the slots the sweep walks and the client-id table.
    /// </summary>
    internal ConnectionRegistry Registry { get; } = new();

    /// <summary>
    /// Gets the pools of the objects a busy connection needs; an idle connection holds none of them.
    /// </summary>
    internal ServerPools Pools { get; }

    /// <summary>
    /// Gets a value indicating whether the server is stopping: from the first call of <see cref="StopAsync"/> on, the
    /// admission step lets no connection in, the connect phase refuses a CONNECT with <c>0x8B</c> before asking the
    /// host, a connection that takes its slot is aborted at once, and the endpoint answers a new request with HTTP
    /// 503. It is set under <see cref="ConnectionRegistry.Gate"/>, so an admission and the taking of a slot, which run
    /// under the same lock, either precede it or see it; read without the lock, it may be a moment late.
    /// </summary>
    internal bool IsStopping => Registry.IsStopping;

    /// <summary>
    /// Gets <see cref="ServerLimits.ConnectDeadline"/> in units of <see cref="Time"/>'s timestamps.
    /// </summary>
    internal long ConnectDeadlineLength { get; }

    /// <summary>
    /// Gets <see cref="ServerLimits.AuthenticationDeadline"/> in units of <see cref="Time"/>'s timestamps.
    /// </summary>
    internal long AuthenticationDeadlineLength { get; }

    /// <summary>
    /// Gets <see cref="ServerLimits.CloseDeadline"/> in units of <see cref="Time"/>'s timestamps.
    /// </summary>
    internal long CloseDeadlineLength { get; }

    /// <summary>
    /// Gets <see cref="ServerLimits.PartialPacketDeadline"/> in units of <see cref="Time"/>'s timestamps.
    /// </summary>
    internal long PartialPacketDeadlineLength { get; }

    /// <summary>
    /// Gets <see cref="ServerLimits.WriteDeadline"/> in units of <see cref="Time"/>'s timestamps.
    /// </summary>
    internal long WriteDeadlineLength { get; }

    /// <summary>
    /// Gets <see cref="ServerLimits.AcknowledgementDeadline"/> in units of <see cref="Time"/>'s timestamps.
    /// </summary>
    internal long AcknowledgementDeadlineLength { get; }

    /// <summary>
    /// Looks up the connection that has a client identifier: the one admitted last for it, from its admission until
    /// it has closed. So it may be closing already, in which case its <see cref="MqttWireConnection.TrySend"/>
    /// answers <see cref="MqttSendResult.NotOpen"/>; it is found until just before its
    /// <see cref="IMqttWireHandler.OnClosed"/>.
    /// </summary>
    /// <param name="clientId">
    /// The client identifier.
    /// </param>
    /// <param name="connection">
    /// The connection when found; otherwise <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when a connection has this client identifier; <see langword="false"/> otherwise.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="clientId"/> is <see langword="null"/>.
    /// </exception>
    public bool TryGetConnection(string clientId, [NotNullWhen(true)] out MqttWireConnection? connection)
    {
        ArgumentNullException.ThrowIfNull(clientId);
        return Registry.TryGet(clientId, out connection);
    }

    /// <summary>
    /// Resolves the host's handler, when the server was not given it, and creates the one timer that runs the sweep
    /// every <see cref="ServerLimits.SweepInterval"/>, from <see cref="Time"/>. The host calls it before the web
    /// server accepts a connection, so the handler exists before the first connection needs it.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The server was already started, or has been stopped; nothing changes.
    /// </exception>
    /// <exception cref="Exception">
    /// The handler could not be resolved: its constructor threw, or it is not registered. The server is not started.
    /// </exception>
    internal void Start()
    {
        // Outside the lock: the handler's constructor is the host's code and may call the server.
        _ = Handler;
        lock (Registry.Gate)
        {
            if (_sweepTimer is not null)
            {
                throw new InvalidOperationException("The server was already started.");
            }

            if (Registry.IsStopping)
            {
                throw new InvalidOperationException("The server was stopped.");
            }

            // The state is always this server, passed just below.
            _sweepTimer = Time.CreateTimer(static state => ((MqttWireServer)state!).Sweep(), this,
                ServerLimits.SweepInterval, ServerLimits.SweepInterval);
        }
    }

    /// <summary>
    /// Stops the server (design 6.6, "When the host stops"). It sets <see cref="IsStopping"/> under
    /// <see cref="ConnectionRegistry.Gate"/>, so that no admission can follow; then ends every connection that has a
    /// slot, each as its state calls for (see <see cref="MqttWireConnection.BeginStop"/>): an Active one is closed
    /// with <c>0x8B</c>, an Accepted one is aborted, an Authenticating one has its host's token cancelled and is
    /// answered with CONNACK <c>0x8B</c>, and a Closing one is left to its own close. A connection that takes its
    /// slot after the flag was set learns it in the same hold of the lock (<see cref="ConnectionRegistry.Add"/>) and
    /// is aborted at once by its own run, so every connection is either found by the walk or aborts itself, and none
    /// needs the sweep to end; this holds also for one that takes its slot after the stop has completed. The stop
    /// then waits until no slot is taken, which is after every
    /// <see cref="IMqttWireHandler.OnClosed"/> has returned and every buffer is back; the sweep keeps running
    /// meanwhile and aborts every link still open at its close deadline, so the wait is bounded and never on a peer.
    /// Last, it disposes the sweep's timer: a sweep already running finishes harmlessly, and none starts afterwards.
    /// It never blocks a thread: the walk runs on the calling thread, and the rest is a continuation of the wait.
    /// </summary>
    /// <returns>
    /// A task that completes when the server has stopped; it does not fail. Every call returns the same task. A call
    /// before <see cref="Start"/> completes at once, with <see cref="IsStopping"/> set.
    /// </returns>
    internal Task StopAsync()
    {
        TaskCompletionSource stopped;
        ITimer? sweepTimer;
        lock (Registry.Gate)
        {
            if (_stopped is not null)
            {
                return _stopped.Task;
            }

            Registry.MarkStopping();
            _stopped = stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            sweepTimer = _sweepTimer;
        }

        if (sweepTimer is null)
        {
            stopped.SetResult();
            return stopped.Task;
        }

        MqttWireConnection?[] slots = Registry.Slots;
        for (int i = 0; i < slots.Length; i++)
        {
            Volatile.Read(ref slots[i])?.BeginStop();
        }

        _ = CompleteStopAsync(sweepTimer, stopped);
        return stopped.Task;
    }

    /// <summary>
    /// The end of the stop: waits until no connection holds a slot, then disposes the sweep's timer and completes the
    /// stop's task.
    /// </summary>
    /// <param name="sweepTimer">
    /// The sweep's timer.
    /// </param>
    /// <param name="stopped">
    /// The stop's completion.
    /// </param>
    /// <returns>
    /// A task that completes with the stop; it does not fail.
    /// </returns>
    private async Task CompleteStopAsync(ITimer sweepTimer, TaskCompletionSource stopped)
    {
        await Registry.WhenEmptyAsync().ConfigureAwait(false);
        sweepTimer.Dispose();
        stopped.SetResult();
    }

    /// <summary>
    /// Runs one connection from its accept until it is Closed.
    /// </summary>
    /// <param name="httpContext">
    /// The context of the WebSocket request.
    /// </param>
    /// <param name="link">
    /// The link to run over, or <see langword="null"/> to accept the WebSocket of <paramref name="httpContext"/>.
    /// </param>
    /// <returns>
    /// A task that completes when the connection is Closed. It does not fail for anything the client does; it
    /// fails only when the WebSocket cannot be accepted.
    /// </returns>
    /// <exception cref="Exception">
    /// The server was not started and its handler could not be resolved; no connection is created.
    /// </exception>
    internal Task RunConnectionAsync(HttpContext httpContext, IMqttLink? link)
    {
        // A server that was not started resolves its handler here, before the connection can reach any lock.
        _ = Handler;
        MqttWireConnection connection = new(this, httpContext);
        return connection.RunAsync(link);
    }

    /// <summary>
    /// Resolves the host's handler from the services the registration gave the server. The container creates the
    /// singleton once, whatever the number of threads that ask, so every caller gets the same handler and no lock is
    /// needed here.
    /// </summary>
    /// <returns>
    /// The handler.
    /// </returns>
    /// <exception cref="Exception">
    /// The handler's constructor threw, or no handler is registered; nothing is kept, and a later call tries again.
    /// </exception>
    private IMqttWireHandler ResolveHandler()
    {
        // A server without a handler was given the services to resolve it from: the two constructors set one or the other.
        IMqttWireHandler handler = _services!.GetRequiredService<IMqttWireHandler>();
        Volatile.Write(ref _handler, handler);
        return handler;
    }

    /// <summary>
    /// The admission step (design 6.6, "Admission and takeover"), one atomic step under
    /// <see cref="ConnectionRegistry.Gate"/>: finds the connection that has the client id now, asks the host's
    /// <see cref="IMqttWireHandler.OnAdmit"/>, and on acceptance makes the new connection the one in the table,
    /// admitted and Active, with its send path reserved for its CONNACK. Two connections for one client id go through
    /// it one after the other, and the second sees the first as the one it replaces. The caller closes the replaced
    /// connection once the lock is released.
    /// </summary>
    /// <param name="connection">
    /// The new connection, which the host's <see cref="IMqttWireHandler.OnConnectAsync"/> accepted.
    /// </param>
    /// <param name="replaced">
    /// The connection the new one took the client id from when the result accepts and there was one; otherwise
    /// <see langword="null"/>. It may be closing already.
    /// </param>
    /// <returns>
    /// The host's answer. A refusal with <see cref="MqttReasonCode.ServerShuttingDown"/>, without asking the host,
    /// when the server is stopping or the connection is no longer Authenticating (it has begun to close). A refusal
    /// changes nothing.
    /// </returns>
    /// <exception cref="Exception">
    /// <see cref="IMqttWireHandler.OnAdmit"/> threw (the host's bug), or the clock or the table did; the lock is
    /// released, the send path is no longer reserved, and the connection was not admitted.
    /// </exception>
    internal MqttAdmitResult Admit(MqttWireConnection connection, out MqttWireConnection? replaced)
    {
        replaced = null;
        lock (Registry.Gate)
        {
            // Entering the step reserves the connection's send path for its CONNACK; a refusal gives it up again.
            if (IsStopping || !connection.TryEnterAdmission())
            {
                return MqttAdmitResult.Refuse(MqttReasonCode.ServerShuttingDown);
            }

            bool accepted = false;
            try
            {
                MqttWireConnection? existing = Registry.Find(connection.ClientId);
                MqttAdmitResult result = Handler.OnAdmit(connection, existing);
                if (!result.IsAccepted)
                {
                    return result;
                }

                // The clock is read before anything changes, and the reservation is kept only once everything
                // has: whatever throws here (the clock, the table) leaves the connection refused, its send path
                // given up by the finally below.
                long now = Time.GetTimestamp();
                Registry.SetCurrent(connection);
                connection.MarkAdmitted(now);
                accepted = true;
                replaced = existing;
                return result;
            }
            finally
            {
                if (!accepted)
                {
                    connection.LeaveRefusedAdmission();
                }
            }
        }
    }

    /// <summary>
    /// The sweep: lets every connection act on the deadlines that have passed. It walks the slots without the lock
    /// and allocates nothing. It runs on the timer's thread, where an exception would end the process, so nothing a
    /// connection's check throws leaves it: a check does not throw, and should one all the same (a fault inside
    /// MqttWire), that connection is ended as a fault, logged with event 20, and the sweep goes on to the next.
    /// </summary>
    internal void Sweep()
    {
        long now = Time.GetTimestamp();
        MqttWireConnection?[] slots = Registry.Slots;
        for (int i = 0; i < slots.Length; i++)
        {
            MqttWireConnection? connection = Volatile.Read(ref slots[i]);
            if (connection is null)
            {
                continue;
            }

            try
            {
                connection.CheckDeadlines(now);
            }
            catch (Exception exception)
            {
                connection.EndFaulted(exception);
            }
        }
    }

    /// <summary>
    /// Converts a deadline's length to units of a clock's timestamps.
    /// </summary>
    /// <param name="length">
    /// The length.
    /// </param>
    /// <param name="time">
    /// The clock.
    /// </param>
    /// <returns>
    /// The length in timestamp units.
    /// </returns>
    private static long ToTimestampLength(TimeSpan length, TimeProvider time) =>
        (long)(length.Ticks * ((double)time.TimestampFrequency / TimeSpan.TicksPerSecond));
}
