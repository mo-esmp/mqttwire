using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using MqttWire.Link;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <summary>
/// The running server: lets the host look up the connections that are open. It has no public constructor; the
/// host gets it from dependency injection.
/// </summary>
public sealed class MqttWireServer
{
    /// <summary>
    /// The one timer that runs <see cref="Sweep"/>, or <see langword="null"/> before <see cref="Start"/>.
    /// </summary>
    private ITimer? _sweepTimer;

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
    internal MqttWireServer(IMqttWireHandler handler, MqttWireServerOptions options, ILogger<MqttWireServer> logger,
        TimeProvider time, ArrayPool<byte> pool)
    {
        Handler = handler;
        Options = options;
        Logger = logger;
        Time = time;
        Pool = pool;
        ConnectDeadlineLength = ToTimestampLength(ServerLimits.ConnectDeadline, time);
        AuthenticationDeadlineLength = ToTimestampLength(ServerLimits.AuthenticationDeadline, time);
        CloseDeadlineLength = ToTimestampLength(ServerLimits.CloseDeadline, time);
    }

    /// <summary>
    /// Gets the number of open connections.
    /// </summary>
    public int ConnectionCount => Registry.ActiveCount;

    /// <summary>
    /// Gets the host's handler.
    /// </summary>
    internal IMqttWireHandler Handler { get; }

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
    /// Looks up an open connection by client identifier.
    /// </summary>
    /// <param name="clientId">
    /// The client identifier.
    /// </param>
    /// <param name="connection">
    /// The connection when found; otherwise <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when a connection with this client identifier is open; <see langword="false"/>
    /// otherwise.
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
    /// Creates the one timer that runs the sweep every <see cref="ServerLimits.SweepInterval"/>, from
    /// <see cref="Time"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The server was already started; nothing changes.
    /// </exception>
    internal void Start()
    {
        if (_sweepTimer is not null)
        {
            throw new InvalidOperationException("The server was already started.");
        }

        // The state is always this server, passed just below.
        _sweepTimer = Time.CreateTimer(static state => ((MqttWireServer)state!).Sweep(), this,
            ServerLimits.SweepInterval, ServerLimits.SweepInterval);
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
    internal Task RunConnectionAsync(HttpContext httpContext, IMqttLink? link)
    {
        MqttWireConnection connection = new(this, httpContext);
        return connection.RunAsync(link);
    }

    /// <summary>
    /// The sweep: lets every connection act on the deadlines that have passed. It walks the slots without the lock
    /// and allocates nothing.
    /// </summary>
    internal void Sweep()
    {
        long now = Time.GetTimestamp();
        MqttWireConnection?[] slots = Registry.Slots;
        for (int i = 0; i < slots.Length; i++)
        {
            Volatile.Read(ref slots[i])?.CheckDeadlines(now);
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
