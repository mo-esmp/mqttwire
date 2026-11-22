using Microsoft.Extensions.Logging;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of a host whose <c>OnConnectAsync</c> blocks its calling thread and ignores its token (design 6.6,
/// "<c>OnConnectAsync</c> and its deadline" and "When the host stops"): the socket is closed all the same, the stop
/// does not wait for the call, and what only the call's return can release is released when it returns.
/// </summary>
public sealed class BlockedHostTests
{
    /// <summary>
    /// The event id of the line of a host call that did not return (design 6.12).
    /// </summary>
    private const int HostCallDidNotReturnEvent = 22;

    /// <summary>
    /// The event id of the line of the authentication deadline (design 6.12).
    /// </summary>
    private const int AuthenticationTimedOutEvent = 4;

    /// <summary>
    /// Proves that a connection whose host blocks inside <c>OnConnectAsync</c> has its link aborted one close deadline
    /// after the token was cancelled at the authentication deadline, with nothing written; that until the call
    /// returns the connection still holds its slot and the read buffer kept behind the CONNECT; and that the late
    /// acceptance is discarded: no admission, no <c>OnClosed</c>, and the buffer and the slot come back.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Sweep_HostBlocksInOnConnectAsync_AbortsTheLinkACloseDeadlineAfterTheToken()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        using BlockedHost host = new(harness);
        TestConnection connection = harness.Accept();
        host.DeliverOnAnotherThread(connection, [.. ServerPackets.Connect, .. ClientPackets.PingReq]);

        // Act
        harness.Advance(ServerLimits.AuthenticationDeadline);
        bool abortedAtTheDeadline = connection.Link.IsAborted;
        harness.Advance(ServerLimits.CloseDeadline);
        bool abortedACloseDeadlineLater = connection.Link.IsAborted;
        bool endedWhileBlocked = connection.Run.IsCompleted;
        int buffersWhileBlocked = harness.Pool.Outstanding;
        int slotsWhileBlocked = harness.Server.Registry.Slots.Count(static slot => slot is not null);
        host.Release();
        await connection.RunEnded();

        // Assert
        abortedAtTheDeadline.ShouldBeFalse();
        abortedACloseDeadlineLater.ShouldBeTrue();
        endedWhileBlocked.ShouldBeFalse();
        buffersWhileBlocked.ShouldBe(1);
        slotsWhileBlocked.ShouldBe(1);
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.ClosedWith.ShouldBeNull();
        harness.Log.EntriesNow().ShouldContain(static entry =>
            entry.Id.Id == AuthenticationTimedOutEvent && entry.Level == LogLevel.Error);
        harness.Handler.CallsNow().ShouldBe(["OnConnectAsync c1"]);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the stop does not wait for a host that blocks inside <c>OnConnectAsync</c>: one close deadline
    /// after the stop cancelled the token, the link is aborted, the call is logged as not returned (event 22, with
    /// the client id and the method's name) and the stop completes, while the connection keeps its slot until the
    /// call returns.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_HostBlocksInOnConnectAsync_CompletesWithoutItAndLogsTheCall()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        using BlockedHost host = new(harness);
        TestConnection connection = harness.Accept();
        host.DeliverOnAnotherThread(connection, ServerPackets.Connect);

        // Act
        Task stop = harness.Server.StopAsync();
        bool completedAtOnce = stop.IsCompleted;
        harness.Advance(ServerLimits.CloseDeadline);
        await stop.Within("The server's stop");
        bool endedWhenTheStopCompleted = connection.Run.IsCompleted;
        host.Release();
        await connection.RunEnded();

        // Assert
        completedAtOnce.ShouldBeFalse();
        endedWhenTheStopCompleted.ShouldBeFalse();
        connection.Link.IsAborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        (LogLevel Level, EventId Id, string Message, Exception? Exception) line = harness.Log.EntriesNow()
            .Where(static entry => entry.Id.Id == HostCallDidNotReturnEvent).ShouldHaveSingleItem();
        line.Level.ShouldBe(LogLevel.Error);
        line.Message.ShouldContain("c1");
        line.Message.ShouldContain(nameof(IMqttWireHandler.OnConnectAsync));
        harness.Handler.CallsNow().ShouldBe(["OnConnectAsync c1"]);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the stop does not wait either for a connection the sweep had already ended because its host
    /// blocked inside <c>OnConnectAsync</c> past its deadline: the stop completes at its first sweep, with the call
    /// logged as not returned.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_HostStillBlockedSinceItsDeadline_CompletesAtTheFirstSweep()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        using BlockedHost host = new(harness);
        TestConnection connection = harness.Accept();
        host.DeliverOnAnotherThread(connection, ServerPackets.Connect);
        harness.Advance(ServerLimits.AuthenticationDeadline + ServerLimits.CloseDeadline);
        bool abortedBeforeTheStop = connection.Link.IsAborted;

        // Act
        Task stop = harness.Server.StopAsync();
        harness.Advance(ServerLimits.SweepInterval);
        await stop.Within("The server's stop");
        bool endedWhenTheStopCompleted = connection.Run.IsCompleted;
        host.Release();
        await connection.RunEnded();

        // Assert
        abortedBeforeTheStop.ShouldBeTrue();
        endedWhenTheStopCompleted.ShouldBeFalse();
        harness.Log.EntriesNow().Count(static entry => entry.Id.Id == HostCallDidNotReturnEvent).ShouldBe(1);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// A host whose <c>OnConnectAsync</c> blocks the thread that calls it until the test releases it, and then
    /// accepts. The CONNECT is delivered on a thread of its own, because the server calls the host on the thread that
    /// delivers.
    /// </summary>
    private sealed class BlockedHost : IDisposable
    {
        /// <summary>
        /// Set when the host's call has begun.
        /// </summary>
        private readonly ManualResetEventSlim _entered = new();

        /// <summary>
        /// Set by the test to let the host's call return.
        /// </summary>
        private readonly ManualResetEventSlim _released = new();

        /// <summary>
        /// The thread that delivered the CONNECT and is blocked inside the host, or <see langword="null"/>.
        /// </summary>
        private Thread? _delivering;

        /// <summary>
        /// Makes the harness's handler block inside <c>OnConnectAsync</c>.
        /// </summary>
        /// <param name="harness">
        /// The harness whose handler is set.
        /// </param>
        public BlockedHost(ServerHarness harness) =>
            harness.Handler.Connect = (_, _) =>
            {
                _entered.Set();
                _released.Wait(BoundedWaits.Limit);
                return ValueTask.FromResult(MqttConnectResult.Accept());
            };

        /// <summary>
        /// Delivers bytes to a connection on a new thread and returns once the host's call has begun on it.
        /// </summary>
        /// <param name="connection">
        /// The connection.
        /// </param>
        /// <param name="bytes">
        /// The bytes, which start with a CONNECT.
        /// </param>
        public void DeliverOnAnotherThread(TestConnection connection, byte[] bytes)
        {
            _delivering = new Thread(() => connection.Link.Deliver(bytes)) { IsBackground = true };
            _delivering.Start();
            _entered.Wait(BoundedWaits.Limit).ShouldBeTrue("The host's OnConnectAsync was not called.");
        }

        /// <summary>
        /// Lets the host's call return and waits for the thread it blocked to end.
        /// </summary>
        public void Release()
        {
            _released.Set();
            _delivering?.JoinWithin("The thread blocked inside the host");
        }

        /// <summary>
        /// Releases the host's call, should the test have failed before it did, and the two events.
        /// </summary>
        public void Dispose()
        {
            _released.Set();
            _delivering?.Join(BoundedWaits.Limit);
            _entered.Dispose();
            _released.Dispose();
        }
    }
}
