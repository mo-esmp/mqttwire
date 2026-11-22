using System.Buffers;
using System.Net;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Time.Testing;
using MqttWire.Codec;
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
    /// The most buffers one connection has out of the pool between two steps of a test: its read buffer, the buffer
    /// of the write in progress (an encoding buffer), and the control buffer of the packets that wait. The buffer an
    /// answer is encoded into, and that of the reason codes of a SUBSCRIBE of more than 256 filters, are back before
    /// the step that rented them returns.
    /// </summary>
    private const int MaxBuffersPerConnection = 3;

    /// <summary>
    /// Guards <see cref="_windows"/>.
    /// </summary>
    private readonly Lock _windowsGate = new();

    /// <summary>
    /// What the link of every connection the harness opened reports to (design 6.6): the harness asserts on it
    /// without keeping the links alive.
    /// </summary>
    private readonly MemoryLinkChecks _linkChecks = new();

    /// <summary>
    /// The window of each connection whose CONNECT carried a Receive Maximum below 256, by the HTTP context of its
    /// request, as the test computed it from the CONNECT it sent; a connection that is not here has the window 256.
    /// </summary>
    private readonly Dictionary<HttpContext, int> _windows = [];

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
    /// The pool the bounds are asserted on.
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
    /// Gets the counting pool the bounds are asserted on: the pool the server rents from, the inner pool of a
    /// <see cref="ThrowingPool"/> it rents from, or, when a test gave a pool of another kind, one the server never
    /// rents from.
    /// </summary>
    public CountingPool Pool { get; }

    /// <summary>
    /// Creates a started server.
    /// </summary>
    /// <param name="configure">
    /// Changes the default options, or <see langword="null"/> to keep them.
    /// </param>
    /// <param name="pool">
    /// The pool the server rents from, or <see langword="null"/> for a new <see cref="CountingPool"/>. A
    /// <see cref="CountingPool"/> given here is <see cref="Pool"/>, and so is the inner pool of a
    /// <see cref="ThrowingPool"/>; a test that gives a pool of another kind asserts on it itself.
    /// </param>
    /// <param name="pools">
    /// The pools of queues and exchange records, or <see langword="null"/> for the server's own; for the tests of a
    /// pool that fails.
    /// </param>
    /// <returns>
    /// The harness.
    /// </returns>
    public static ServerHarness Create(Action<MqttWireServerOptions>? configure = null, ArrayPool<byte>? pool = null,
        ServerPools? pools = null)
    {
        MqttWireServerOptions options = new();
        configure?.Invoke(options);
        RecordingHandler handler = new();
        RecordingLogger log = new();
        FakeTimeProvider time = new();
        CountingTimeProvider timers = new(time);
        CountingPool counting = pool switch
        {
            CountingPool given => given,
            ThrowingPool throwing => throwing.Inner,
            _ => new CountingPool(),
        };
        MqttWireServer server = new(handler, options, log, timers, pool ?? counting, pools);
        server.Start();
        return new ServerHarness(server, handler, log, time, timers, counting);
    }

    /// <summary>
    /// Opens a connection: a new in-memory link and an HTTP context, handed to the server.
    /// </summary>
    /// <param name="remoteEndPoint">
    /// The remote end point the HTTP context reports, or <see langword="null"/> for none (the context's default).
    /// </param>
    /// <returns>
    /// The connection, waiting for its CONNECT.
    /// </returns>
    public TestConnection Accept(IPEndPoint? remoteEndPoint = null)
    {
        CancellationTokenSource requestAborted = new();
        DefaultHttpContext httpContext = new() { RequestAborted = requestAborted.Token };
        if (remoteEndPoint is not null)
        {
            httpContext.Connection.RemoteIpAddress = remoteEndPoint.Address;
            httpContext.Connection.RemotePort = remoteEndPoint.Port;
        }

        MemoryLink link = new() { Checks = _linkChecks };
        Task run = Server.RunConnectionAsync(httpContext, link);
        link.Run = run;
        return new TestConnection(this, link, run, requestAborted, httpContext);
    }

    /// <summary>
    /// Opens a connection and connects it: delivers a CONNECT, asserts that the server answered with a success
    /// CONNACK, and takes what was written, so the connection's link starts empty.
    /// </summary>
    /// <param name="clientId">
    /// The client id of the CONNECT built when <paramref name="connect"/> is <see langword="null"/>.
    /// </param>
    /// <param name="connect">
    /// The CONNECT to deliver, or <see langword="null"/> for <see cref="ServerPackets.ConnectFor"/> of
    /// <paramref name="clientId"/>, which is answered with <see cref="ServerPackets.ConnAck"/>.
    /// </param>
    /// <returns>
    /// The connection, Active.
    /// </returns>
    public TestConnection Connect(string clientId = "c1", byte[]? connect = null)
    {
        TestConnection connection = Accept();
        connection.Deliver(connect ?? ServerPackets.ConnectFor(clientId));
        byte[] written = connection.TakeWritten();
        if (connect is null)
        {
            written.ShouldBe(ServerPackets.ConnAck);
        }
        else
        {
            // A CONNACK whose reason code, after the flags, is success.
            written.Length.ShouldBeGreaterThan(3);
            written[0].ShouldBe((byte)0x20);
            written[3].ShouldBe((byte)MqttReasonCode.Success);
        }

        return connection;
    }

    /// <summary>
    /// Lists the handler's <c>OnClosed</c> calls so far, oldest first.
    /// </summary>
    /// <returns>
    /// The calls as text, for example <c>"OnClosed c1 TakenOver"</c>; a copy.
    /// </returns>
    public List<string> ClosedCalls() =>
        [.. Handler.CallsNow().Where(static call => call.StartsWith("OnClosed", StringComparison.Ordinal))];

    /// <summary>
    /// Moves the fake clock forward in steps of the sweep interval, so that every sweep in between runs, and asserts
    /// the bounds after each step (design 6.13 item 5).
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
            AssertBounds();
        }
    }

    /// <summary>
    /// Notes the client's window from bytes a test delivers to a connection that start with a whole CONNECT: the
    /// client's Receive Maximum or 256, whichever is smaller (design 6.6, "Deliveries at QoS 1 or 2", item 3). Only a
    /// window below 256 is kept, and only the first one for a connection, so a connection without one costs nothing
    /// here. Bytes that are not a whole CONNECT, or a CONNECT the codec refuses, change nothing.
    /// </summary>
    /// <param name="httpContext">
    /// The HTTP context of the connection's request.
    /// </param>
    /// <param name="bytes">
    /// The bytes delivered.
    /// </param>
    public void NoteConnect(HttpContext httpContext, ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty || bytes[0] != 0x10
            || PacketHeader.TryRead(bytes, out PacketHeader header) != OperationStatus.Done
            || bytes.Length < header.TotalLength
            || ConnectCodec.TryDecode(bytes[..header.TotalLength], out ConnectPacket connect) != MqttReasonCode.Success
            || !connect.Properties.TryGetInteger(PropertyId.ReceiveMaximum, out uint receiveMaximum)
            || receiveMaximum >= ServerLimits.ReceiveMaximum)
        {
            return;
        }

        lock (_windowsGate)
        {
            _windows.TryAdd(httpContext, (int)receiveMaximum);
        }
    }

    /// <summary>
    /// Asserts what must hold between any two steps of a test (design 6.13 item 5): no buffer was returned twice or
    /// returned that was not rented, no rent was larger than the largest buffer the server needs (64 KiB, or a whole
    /// PUBLISH of the first band: the payload limit, its 5 KiB of room and a fixed header of at most 5 bytes), every
    /// connection's counts are within the limits of <see cref="ServerLimits"/> and the options (the exchange records,
    /// the count against the receive maximum, the deliveries in the client's window, at most the window the test
    /// computed from the CONNECT it sent, the waiting deliveries, the waiting control packets and the bytes on the way
    /// out), and the buffers out of the pool are no more than <see cref="MaxBuffersPerConnection"/> for each
    /// connection that has a slot: one that has ended holds none. And no link was aborted after the run over it had
    /// ended (design 6.6, "Closing, bounded"): over a WebSocket that abort throws. Called by
    /// <see cref="TestConnection"/> after every delivery, by <see cref="Advance"/> after every step of the clock, and
    /// by the tests after every send.
    /// </summary>
    public void AssertBounds()
    {
        Pool.FaultyReturns.ShouldBe(0);
        _linkChecks.AbortsAfterEnd.ShouldBe(0);
        MqttWireServerOptions options = Server.Options;
        Pool.LargestRent.ShouldBeLessThanOrEqualTo(Math.Max(ServerLimits.MaxEncodingBufferBytes,
            options.MaxPublishPayloadBytes + ServerLimits.PublishOverheadBytes + 5));
        int connections = 0;
        foreach (MqttWireConnection? connection in Server.Registry.Slots)
        {
            if (connection is null)
            {
                continue;
            }

            connections++;
            ConnectionCounts counts = connection.GetCounts();
            counts.IncomingRecords.ShouldBeLessThanOrEqualTo(ServerLimits.ReceiveMaximum);
            counts.IncomingInFlight.ShouldBeLessThanOrEqualTo(ServerLimits.ReceiveMaximum);
            counts.DeliveryRecords.ShouldBeLessThanOrEqualTo(WindowOf(connection.HttpContext));
            counts.WaitingControlPackets.ShouldBeLessThanOrEqualTo(ServerLimits.MaxWaitingControlPackets);
            counts.WaitingDeliveries.ShouldBeLessThanOrEqualTo(options.MaxOutgoingMessages);
            counts.OutgoingBytes.ShouldBeLessThanOrEqualTo(options.MaxOutgoingBytes);
            counts.OutgoingBytes.ShouldBeGreaterThanOrEqualTo(0);
        }

        Pool.Outstanding.ShouldBeLessThanOrEqualTo(MaxBuffersPerConnection * connections);
    }

    /// <summary>
    /// Gets the client's window the test computed from the CONNECT it sent (<see cref="NoteConnect"/>).
    /// </summary>
    /// <param name="httpContext">
    /// The HTTP context of the connection's request.
    /// </param>
    /// <returns>
    /// The client's Receive Maximum when it was below 256; otherwise 256.
    /// </returns>
    private int WindowOf(HttpContext httpContext)
    {
        lock (_windowsGate)
        {
            return _windows.TryGetValue(httpContext, out int window) ? window : ServerLimits.ReceiveMaximum;
        }
    }
}
