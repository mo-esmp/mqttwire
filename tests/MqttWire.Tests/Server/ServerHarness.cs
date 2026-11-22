using System.Buffers;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Time.Testing;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Server;

/// <summary>
/// A started server over in-memory links with a fake clock, a counting pool, a recording handler and a recording
/// logger, for the server tests. Nothing happens in real time: the sweep runs only when <see cref="Advance"/> moves
/// the clock.
/// </summary>
internal sealed class ServerHarness
{
    /// <summary>
    /// The step <see cref="Advance"/> moves the clock by, so that every sweep in between runs.
    /// </summary>
    private static readonly TimeSpan s_sweepStep = ServerLimits.SweepInterval;

    /// <summary>
    /// Creates the harness around a started server.
    /// </summary>
    /// <param name="server">
    /// The server.
    /// </param>
    /// <param name="handler">
    /// The handler the server calls.
    /// </param>
    /// <param name="log">
    /// The logger the server writes to.
    /// </param>
    /// <param name="time">
    /// The fake clock.
    /// </param>
    /// <param name="timers">
    /// The clock the server is given, over <paramref name="time"/>.
    /// </param>
    /// <param name="pool">
    /// The pool the server rents from.
    /// </param>
    private ServerHarness(
        MqttWireServer server,
        RecordingHandler handler,
        RecordingLogger log,
        FakeTimeProvider time,
        CountingTimeProvider timers,
        CountingPool pool)
    {
        Server = server;
        Handler = handler;
        Log = log;
        Time = time;
        Timers = timers;
        Pool = pool;
    }

    /// <summary>
    /// Gets the server.
    /// </summary>
    public MqttWireServer Server { get; }

    /// <summary>
    /// Gets the handler the server calls.
    /// </summary>
    public RecordingHandler Handler { get; }

    /// <summary>
    /// Gets the logger the server writes to.
    /// </summary>
    public RecordingLogger Log { get; }

    /// <summary>
    /// Gets the fake clock; nothing happens in real time.
    /// </summary>
    public FakeTimeProvider Time { get; }

    /// <summary>
    /// Gets the clock the server is given, over <see cref="Time"/>, which keeps every timer the server creates.
    /// </summary>
    public CountingTimeProvider Timers { get; }

    /// <summary>
    /// Gets the pool the server rents from.
    /// </summary>
    public CountingPool Pool { get; }

    /// <summary>
    /// Creates a started server.
    /// </summary>
    /// <param name="configure">
    /// Changes the default options, or <see langword="null"/> to keep them.
    /// </param>
    /// <param name="pool">
    /// The pool the server rents from, or <see langword="null"/> for <see cref="Pool"/>; a test that gives its own
    /// pool asserts on it itself.
    /// </param>
    /// <returns>
    /// The harness.
    /// </returns>
    public static ServerHarness Create(Action<MqttWireServerOptions>? configure = null, ArrayPool<byte>? pool = null)
    {
        MqttWireServerOptions options = new();
        configure?.Invoke(options);
        RecordingHandler handler = new();
        RecordingLogger log = new();
        FakeTimeProvider time = new();
        CountingTimeProvider timers = new(time);
        CountingPool counting = new();
        MqttWireServer server = new(handler, options, log, timers, pool ?? counting);
        server.Start();
        return new ServerHarness(server, handler, log, time, timers, counting);
    }

    /// <summary>
    /// Opens a connection: a new in-memory link and an HTTP context, handed to the server.
    /// </summary>
    /// <returns>
    /// The connection, waiting for its CONNECT.
    /// </returns>
    public TestConnection Accept()
    {
        MemoryLink link = new();
        CancellationTokenSource requestAborted = new();
        DefaultHttpContext httpContext = new() { RequestAborted = requestAborted.Token };
        Task run = Server.RunConnectionAsync(httpContext, link);
        return new TestConnection(this, link, run, requestAborted, httpContext);
    }

    /// <summary>
    /// Moves the fake clock forward in steps of the sweep interval, so that every sweep in between runs.
    /// </summary>
    /// <param name="time">
    /// How far to move the clock.
    /// </param>
    public void Advance(TimeSpan time)
    {
        TimeSpan remaining = time;
        while (remaining > TimeSpan.Zero)
        {
            TimeSpan step = remaining < s_sweepStep ? remaining : s_sweepStep;
            Time.Advance(step);
            remaining -= step;
        }
    }

    /// <summary>
    /// Asserts what must hold at any moment (design 6.13 item 5): no buffer was returned twice or returned that
    /// was not rented, and every connection's counts are within the limits of <see cref="ServerLimits"/> and the
    /// options.
    /// </summary>
    public void AssertBounds()
    {
        Pool.FaultyReturns.ShouldBe(0);
        MqttWireServerOptions options = Server.Options;
        foreach (MqttWireConnection? connection in Server.Registry.Slots)
        {
            if (connection is null)
            {
                continue;
            }

            ConnectionCounts counts = connection.GetCounts();
            counts.IncomingRecords.ShouldBeLessThanOrEqualTo(ServerLimits.ReceiveMaximum);
            counts.WaitingControlPackets.ShouldBeLessThanOrEqualTo(ServerLimits.MaxWaitingControlPackets);
            counts.WaitingDeliveries.ShouldBeLessThanOrEqualTo(options.MaxOutgoingMessages);
            counts.OutgoingBytes.ShouldBeLessThanOrEqualTo(options.MaxOutgoingBytes);
        }
    }
}
