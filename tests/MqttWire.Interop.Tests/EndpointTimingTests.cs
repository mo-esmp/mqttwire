using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace MqttWire.Interop.Tests;

/// <summary>
/// The tests over Kestrel whose outcome depends on real time: they run in a collection of their own that disables
/// parallelization, so that xUnit runs them one at a time, after every other class of the assembly, and no other
/// test competes with them for the machine.
/// </summary>
[Collection(nameof(EndpointTimingTests))]
[CollectionDefinition(nameof(EndpointTimingTests), DisableParallelization = true)]
public sealed partial class EndpointTimingTests
{
    /// <summary>
    /// The event id of the line of the connect deadline (design 6.12).
    /// </summary>
    private const int ConnectTimedOutEvent = 5;

    /// <summary>
    /// How many sockets the connect-deadline test opens.
    /// </summary>
    private const int SilentSockets = 24;

    /// <summary>
    /// A CONNECT with client id "c1" and keep-alive 30.
    /// </summary>
    private static readonly byte[] s_connect = Convert.FromHexString("100F00044D5154540502001E0000026331");

    /// <summary>
    /// How long the logger holds the sweep after it has closed a socket, so that the server's run of that connection
    /// ends before the sweep goes on to abort it.
    /// </summary>
    private static readonly TimeSpan s_gap = TimeSpan.FromMilliseconds(20);

    /// <summary>
    /// The fail-safe wait for something that is expected to happen; a passing test never waits this long.
    /// </summary>
    private static readonly TimeSpan s_failSafe = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Proves, over Kestrel with debug logging on, that sockets that send nothing and are closed by the client just
    /// as the sweep acts on their connect deadline never get their link aborted after their request has ended, where
    /// the WebSocket's first abort throws on the sweep's timer thread and ends the process: half of them are closed
    /// from inside the logger, between the sweep's decision and its abort, and the rest at the same moment from
    /// another thread; every connection ends, nothing is logged as an error, and the server still answers.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectDeadline_SocketsClosedAsTheSweepActsOnIt_EveryConnectionEndsAndNothingThrows()
    {
        // Arrange
        int ended = 0;
        TaskCompletionSource allEnded = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using ServerHost host = await ServerHost.StartAsync(new ServerHost.Setup
        {
            ConfigureApp = app => app.Use(async (HttpContext context, RequestDelegate next) =>
            {
                try
                {
                    await next(context);
                }
                finally
                {
                    if (Interlocked.Increment(ref ended) == SilentSockets)
                    {
                        allEnded.TrySetResult();
                    }
                }
            }),
        });
        Dictionary<string, RawWebSocket> inTheGap = [];
        List<RawWebSocket> fromOutside = [];
        for (int i = 0; i < SilentSockets; i++)
        {
            RawWebSocket socket = await RawWebSocket.ConnectAsync(host.WebSocketUri);
            socket.StatusCode.ShouldBe(StatusCodes.Status101SwitchingProtocols);
            if (i % 2 == 0)
            {
                inTheGap.Add(socket.LocalEndPoint.ToString(), socket);
            }
            else
            {
                fromOutside.Add(socket);
            }
        }

        // The sweep writes the line of an end between its decision to abort and the abort. The logger closes that
        // connection's socket there and holds the sweep until the server has seen the close and ended the run. The
        // dictionary is only read from here on.
        TaskCompletionSource sweepActs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        host.Logs.Recorded = entry =>
        {
            if (entry.EventId.Id != ConnectTimedOutEvent)
            {
                return;
            }

            sweepActs.TrySetResult();
            if (inTheGap.TryGetValue(RemoteEndPoint().Match(entry.Message).Value, out RawWebSocket? socket))
            {
                socket.CloseSocket();
                Thread.Sleep(s_gap);
            }
        };

        // Act: the deadline is 10 s, acted on at the first sweep after it.
        await sweepActs.Task.WaitAsync(s_failSafe);
        foreach (RawWebSocket socket in fromOutside)
        {
            socket.CloseSocket();
        }

        await allEnded.Task.WaitAsync(s_failSafe);
        await using RawWebSocket later = await RawWebSocket.ConnectAsync(host.WebSocketUri);

        // Assert
        IReadOnlyList<LogRecorder.LogEntry> lines = host.Logs.Entries;
        lines.Count(static line => line.EventId.Id == ConnectTimedOutEvent).ShouldBeLessThanOrEqualTo(SilentSockets);
        lines.Where(static line => line.Level >= LogLevel.Warning).ShouldBeEmpty();
        Volatile.Read(ref ended).ShouldBe(SilentSockets);
        later.StatusCode.ShouldBe(StatusCodes.Status101SwitchingProtocols);
    }

    /// <summary>
    /// Proves, over Kestrel, that a WebSocket frame whose header announces a payload the client then withholds does
    /// not pin a read buffer until the keep-alive deadline: the server closes the connection with DISCONNECT
    /// <c>0x82</c> at the partial-packet deadline (10 s, acted on at the first sweep after it), long before the 45 s
    /// of the client's keep-alive.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Frame_PayloadWithheld_ClosesWith0x82AtThePartialPacketDeadline()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        await using RawWebSocket client = await RawWebSocket.ConnectAsync(host.WebSocketUri);
        await client.SendAsync(RawWebSocket.Binary, s_connect);
        RawWebSocket.Frame connAck = await client.ReceiveAsync().WaitAsync(s_failSafe);

        // Act: a final binary frame, masked, of two bytes, of which only the header and the mask are sent.
        await client.SendRawAsync(new byte[] { 0x82, 0x82, 0x01, 0x02, 0x03, 0x04 });
        RawWebSocket.Frame disconnect = await client.ReceiveAsync().WaitAsync(TimeSpan.FromSeconds(20));
        RawWebSocket.Frame close = await client.ReceiveAsync().WaitAsync(s_failSafe);

        // Assert
        connAck.Payload[0].ShouldBe((byte)0x20);
        disconnect.Opcode.ShouldBe(RawWebSocket.Binary);
        disconnect.Payload.ShouldBe(new byte[] { 0xE0, 0x01, 0x82 });
        close.Opcode.ShouldBe(RawWebSocket.Close);
        close.CloseStatus.ShouldBe(1000);
    }

    /// <summary>
    /// Matches the remote end point in a line of a connection that has no client id yet.
    /// </summary>
    /// <returns>
    /// The expression.
    /// </returns>
    [GeneratedRegex(@"127\.0\.0\.1:\d+", RegexOptions.None, matchTimeoutMilliseconds: 1000)]
    private static partial Regex RemoteEndPoint();
}
