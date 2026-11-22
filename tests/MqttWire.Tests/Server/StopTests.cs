using System.Buffers;
using System.Net.WebSockets;
using Microsoft.Extensions.Time.Testing;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the bounded stop of the whole server (design 6.6, "When the host stops"): every connection is closed with
/// <c>0x8B</c>, links still open at the close deadline are aborted, the stop waits until every buffer is back and
/// every <c>OnClosed</c> has been called, and only then is the sweep's timer disposed.
/// </summary>
public sealed class StopTests
{
    /// <summary>
    /// The DISCONNECT that tells an Active connection the server is shutting down.
    /// </summary>
    private static readonly byte[] s_disconnectShuttingDown = ServerPackets.Disconnect(MqttReasonCode.ServerShuttingDown);

    /// <summary>
    /// The CONNACK that refuses a connection because the server is shutting down.
    /// </summary>
    private static readonly byte[] s_connAckShuttingDown = ServerPackets.ConnAckRefused(MqttReasonCode.ServerShuttingDown);

    /// <summary>
    /// The topic every test sends to.
    /// </summary>
    private static readonly MqttTopic s_topic = MqttTopic.Create("a/b");

    /// <summary>
    /// How far short of the close deadline a test moves the clock before it runs a sweep that must not abort.
    /// </summary>
    private static readonly TimeSpan s_justShort = TimeSpan.FromMilliseconds(1);

    /// <summary>
    /// Proves that the stop writes DISCONNECT 0x8B to every Active connection, closes its WebSocket with status 1000,
    /// and reports each one closed with <see cref="MqttCloseReason.ServerStopping"/> once its peer has ended the link.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_ActiveConnections_AreClosedWith0x8B()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection[] connections = [harness.Connect("c1"), harness.Connect("c2"), harness.Connect("c3")];

        // Act
        Task stop = harness.Server.StopAsync();
        byte[][] written = [.. connections.Select(static connection => connection.TakeWritten())];
        foreach (TestConnection connection in connections)
        {
            connection.Link.EndFromPeer();
        }

        await stop.Within("The server's stop");

        // Assert
        foreach (TestConnection connection in connections)
        {
            connection.Run.IsCompleted.ShouldBeTrue();
            connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
            connection.Link.IsAborted.ShouldBeFalse();
        }

        written.ShouldAllBe(bytes => bytes.SequenceEqual(s_disconnectShuttingDown));
        ClosedCalls(harness).ShouldBe(
            ["OnClosed c1 ServerStopping", "OnClosed c2 ServerStopping", "OnClosed c3 ServerStopping"],
            ignoreOrder: true);
        harness.Server.ConnectionCount.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the stop does not wait on peers that never answer the close: a sweep just short of the close
    /// deadline aborts nothing and the stop is not complete, and the stop completes once the sweep at the deadline
    /// has aborted every link.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_PeersNeverAnswer_CompletesAfterTheCloseDeadline()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection[] connections = [harness.Connect("c1"), harness.Connect("c2")];
        Task stop = harness.Server.StopAsync();
        harness.Advance(ServerLimits.CloseDeadline - s_justShort);
        harness.Server.Sweep();
        bool completedBeforeTheDeadline = stop.IsCompleted;
        bool abortedBeforeTheDeadline = connections.Any(static connection => connection.Link.IsAborted);

        // Act
        harness.Advance(s_justShort);
        await stop.Within("The server's stop");
        foreach (TestConnection connection in connections)
        {
            await connection.RunEnded();
        }

        // Assert
        completedBeforeTheDeadline.ShouldBeFalse();
        abortedBeforeTheDeadline.ShouldBeFalse();
        connections.ShouldAllBe(connection => connection.Link.IsAborted);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ServerStopping", "OnClosed c2 ServerStopping"], ignoreOrder: true);
    }

    /// <summary>
    /// Proves that once the stop has completed every buffer is back in the pool, including those of a connection with
    /// a delivery still waiting for its acknowledgement, and every accepted <c>OnAdmit</c> was matched by exactly one
    /// <c>OnClosed</c>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_Completed_EveryBufferIsBackAndEveryOnClosedWasCalled()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Connect("c1");
        TestConnection busy = harness.Connect("c2");
        harness.Connect("c3");
        MqttWireConnection server = busy.Connection.ShouldNotBeNull();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
        harness.Handler.Admit = static (connection, _) => connection.ClientId == "c4"
            ? MqttAdmitResult.Refuse(MqttReasonCode.NotAuthorized)
            : MqttAdmitResult.Accepted;
        TestConnection refused = harness.Accept();
        refused.Deliver(ServerPackets.ConnectFor("c4"));
        refused.Link.EndFromPeer();
        await refused.RunEnded();

        // Act
        Task stop = harness.Server.StopAsync();
        harness.Advance(ServerLimits.CloseDeadline);
        await stop.Within("The server's stop");

        // Assert
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
        int admitted = harness.Handler.Calls.Count(static call => call.StartsWith("OnAdmit", StringComparison.Ordinal)) - 1;
        ClosedCalls(harness).Count.ShouldBe(admitted);
        ClosedCalls(harness).ShouldBe(
            ["OnClosed c1 ServerStopping", "OnClosed c2 ServerStopping", "OnClosed c3 ServerStopping"],
            ignoreOrder: true);
    }

    /// <summary>
    /// Proves that the sweep's timer is disposed only when the stop completes: not while a connection still holds its
    /// slot, and by the time the stop's task has completed.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_Completed_TheSweepTimerIsDisposed()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        CountingTimeProvider.CountedTimer timer = harness.Timers.PeriodicTimers().ShouldHaveSingleItem();
        Task stop = harness.Server.StopAsync();
        bool disposedWhileStopping = timer.IsDisposed;

        // Act
        connection.Link.EndFromPeer();
        await stop.Within("The server's stop");

        // Assert
        disposedWhileStopping.ShouldBeFalse();
        timer.IsDisposed.ShouldBeTrue();
        harness.Timers.ActiveTimers.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a connection whose write is held when the stop begins has its link aborted at once, without a
    /// DISCONNECT, and that the stop waits for that write to end, past the close deadline, until its buffer is back.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_AConnectionWithAHeldWrite_IsAbortedAndWaitedFor()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);

        // Act
        Task stop = harness.Server.StopAsync();
        bool abortedAtOnce = connection.Link.IsAborted;
        harness.Advance(ServerLimits.CloseDeadline - s_justShort);
        harness.Server.Sweep();
        bool completedBeforeTheDeadline = stop.IsCompleted;
        harness.Advance(s_justShort + ServerLimits.SweepInterval);
        bool completedWhileTheWriteIsHeld = stop.IsCompleted;
        connection.CompleteHeldWrites(new IOException("The socket is gone."));
        await stop.Within("The server's stop");

        // Assert
        abortedAtOnce.ShouldBeTrue();
        completedBeforeTheDeadline.ShouldBeFalse();
        completedWhileTheWriteIsHeld.ShouldBeFalse();
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.ClosedWith.ShouldBeNull();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ServerStopping"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a stop that begins while a second connection for a client id is inside its <c>OnConnectAsync</c>
    /// ends both once: the first, Active, with DISCONNECT 0x8B and one <c>OnClosed</c>; the second, whose token is
    /// cancelled, with CONNACK 0x8B, no <c>OnAdmit</c> and no <c>OnClosed</c>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_WhileATakeoverIsUnderWay_EndsBothConnectionsOnce()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect();
        TaskCompletionSource<MqttConnectResult> never = new();
        CancellationToken given = default;
        harness.Handler.Connect = (_, cancellationToken) =>
        {
            given = cancellationToken;
            return new ValueTask<MqttConnectResult>(never.Task);
        };
        TestConnection second = harness.Accept();
        second.Deliver(ServerPackets.Connect);

        // Act
        Task stop = harness.Server.StopAsync();
        await second.Link.WhenWrittenAsync(1);
        first.Link.EndFromPeer();
        second.Link.EndFromPeer();
        await stop.Within("The server's stop");

        // Assert
        given.IsCancellationRequested.ShouldBeTrue();
        first.TakeWritten().ShouldBe(s_disconnectShuttingDown);
        second.TakeWritten().ShouldBe(s_connAckShuttingDown);
        harness.Handler.Calls.ShouldBe(
            ["OnConnectAsync c1", "OnAdmit c1 replaces=-", "OnConnectAsync c1", "OnClosed c1 ServerStopping"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that an Authenticating connection whose request is aborted during the stop gets nothing written, no
    /// CONNACK, and no <c>OnAdmit</c>, and that the stop completes: the stop cancels the host's token, and the request
    /// is aborted from inside that cancellation, before the connect phase looks at why its wait ended.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_AuthenticatingWhoseRequestIsAbortedDuringIt_WritesNothingAndCompletes()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TaskCompletionSource<MqttConnectResult> never = new();
        CancellationToken given = default;
        harness.Handler.Connect = (_, cancellationToken) =>
        {
            given = cancellationToken;
            return new ValueTask<MqttConnectResult>(never.Task);
        };
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect);

        // Registered after the connect phase began to wait on the token, so it runs before the connect phase's own
        // callback when the token is cancelled (a token runs its callbacks in the reverse order of their registration).
        // The test relies on that order: in another, the connect phase would see the stop first and write CONNACK 0x8B,
        // and the test would fail, not pass.
        using CancellationTokenRegistration abortTheRequest = given.Register(() => connection.RequestAborted.Cancel());

        // Act
        Task stop = harness.Server.StopAsync();
        await connection.RunEnded();
        await stop.Within("The server's stop");

        // Assert
        given.IsCancellationRequested.ShouldBeTrue();
        connection.RequestAborted.IsCancellationRequested.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1"]);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a connection whose host has been deciding for 9 seconds when the stop begins, a second before its
    /// authentication deadline, is refused with the stop's CONNACK 0x8B, not 0x88, and that no authentication timeout
    /// (event 4) is logged.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_HostDecidingForNineSeconds_AnswersConnAck0x8BNot0x88()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TaskCompletionSource<MqttConnectResult> never = new();
        harness.Handler.Connect = (_, _) => new ValueTask<MqttConnectResult>(never.Task);
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect);
        harness.Advance(TimeSpan.FromSeconds(9));

        // Act
        Task stop = harness.Server.StopAsync();
        await connection.RunEnded();
        await stop.Within("The server's stop");

        // Assert
        connection.TakeWritten().ShouldBe(s_connAckShuttingDown);
        harness.Log.Entries.ShouldNotContain(static entry => entry.Id.Id == 4);
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a connection that has sent no CONNECT when the stop begins is aborted at once, without a write or a
    /// handler call.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_AnAcceptedConnectionWithoutConnect_IsAborted()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act
        Task stop = harness.Server.StopAsync();
        bool abortedAtOnce = connection.Link.IsAborted;
        await stop.Within("The server's stop");
        await connection.RunEnded();

        // Assert
        abortedAtOnce.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        harness.Handler.Calls.ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a connection that takes its slot after the stop began, which the stop's walk may have missed, is
    /// aborted at once by its own run, without a write or a handler call, and that the stop completes with no time
    /// advanced.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_ConnectionAcceptedAfterTheStopBegan_IsAbortedAtOnce()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect();
        Task stop = harness.Server.StopAsync();

        // Act
        TestConnection late = harness.Accept();
        bool abortedAtOnce = late.Link.IsAborted;
        first.Link.EndFromPeer();
        await stop.Within("The server's stop");

        // Assert
        abortedAtOnce.ShouldBeTrue();
        late.Run.IsCompleted.ShouldBeTrue();
        late.TakeWritten().ShouldBeEmpty();
        late.Link.ClosedWith.ShouldBeNull();
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1", "OnAdmit c1 replaces=-", "OnClosed c1 ServerStopping"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a connection Accepted before the stop began, with part of its CONNECT already arrived, is aborted
    /// by the stop's walk: nothing is written, the host is not asked, and the rest of the CONNECT changes nothing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_AcceptedWithItsConnectInFlight_IsAbortedByTheWalk()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();
        int half = ServerPackets.Connect.Length / 2;
        connection.Deliver(ServerPackets.Connect.AsSpan(0, half));

        // Act
        Task stop = harness.Server.StopAsync();
        bool abortedAtOnce = connection.Link.IsAborted;
        connection.Link.Deliver(ServerPackets.Connect.AsSpan(half));
        await stop.Within("The server's stop");
        await connection.RunEnded();

        // Assert
        abortedAtOnce.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        harness.Handler.Calls.ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a connection that takes its slot after the stop has completed, when no sweep runs any more, is
    /// aborted at once and holds no slot afterwards.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_ConnectionRegistersAfterTheStopCompleted_IsAbortedAndHoldsNoSlot()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        await harness.Server.StopAsync().Within("The server's stop");
        CountingTimeProvider.CountedTimer timer = harness.Timers.PeriodicTimers().ShouldHaveSingleItem();

        // Act
        TestConnection late = harness.Accept();

        // Assert
        timer.IsDisposed.ShouldBeTrue();
        late.Link.IsAborted.ShouldBeTrue();
        late.Run.IsCompleted.ShouldBeTrue();
        late.TakeWritten().ShouldBeEmpty();
        harness.Server.Registry.Slots.ShouldAllBe(slot => slot == null);
        harness.Handler.Calls.ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a second call of the stop returns the same task as the first and closes nothing a second time.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_CalledTwice_ReturnsTheSameCompletion()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        Task first = harness.Server.StopAsync();

        // Act
        Task second = harness.Server.StopAsync();
        connection.Link.EndFromPeer();
        await second.Within("The server's stop");

        // Assert
        second.ShouldBeSameAs(first);
        connection.TakeWritten().ShouldBe(s_disconnectShuttingDown);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ServerStopping"]);
    }

    /// <summary>
    /// Proves that a stop called before the server was started completes at once, and that the server then lets no
    /// connection in.
    /// </summary>
    [Fact]
    public void StopAsync_BeforeStart_CompletesAtOnce()
    {
        // Arrange
        FakeTimeProvider time = new();
        MqttWireServer server = new(new RecordingHandler(), new MqttWireServerOptions(), new RecordingLogger(), time,
            ArrayPool<byte>.Shared);

        // Act
        Task stop = server.StopAsync();

        // Assert
        stop.IsCompletedSuccessfully.ShouldBeTrue();
        server.IsStopping.ShouldBeTrue();
    }

    /// <summary>
    /// Lists the <c>OnClosed</c> calls the handler recorded, oldest first.
    /// </summary>
    /// <param name="harness">
    /// The harness whose handler recorded them.
    /// </param>
    /// <returns>
    /// The calls as text.
    /// </returns>
    private static List<string> ClosedCalls(ServerHarness harness) =>
        [.. harness.Handler.Calls.Where(static call => call.StartsWith("OnClosed", StringComparison.Ordinal))];
}
