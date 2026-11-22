using System.Net.WebSockets;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of a connection from its accept until a complete CONNECT, and of the one sweep that enforces the deadlines.
/// </summary>
public sealed class AcceptTests
{
    /// <summary>
    /// Proves that a connection that sends nothing is aborted at the first sweep 10 seconds after its accept, not
    /// earlier, without a write or a handler call.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task RunAsync_NoConnectForTenSeconds_AbortsAtThatSweep()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();
        harness.Advance(TimeSpan.FromSeconds(8));
        bool abortedEarly = connection.Link.IsAborted;

        // Act
        harness.Advance(TimeSpan.FromSeconds(2));

        // Assert
        abortedEarly.ShouldBeFalse();
        connection.Link.IsAborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        await connection.RunEnded();
        harness.Handler.Calls.ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a first packet that is not a CONNECT gets no MQTT answer and the WebSocket is closed with 1002.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task RunAsync_FirstPacketIsNotConnect_ClosesWith1002()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(Hex.Parse("C0 00"));

        // Assert
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.ProtocolError);
        connection.TakeWritten().ShouldBeEmpty();
        connection.Run.IsCompleted.ShouldBeTrue();
        await connection.RunEnded();
        harness.Handler.Calls.ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a link the peer ends before any CONNECT ends the run with every rented buffer back in the pool.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task RunAsync_PeerEndsBeforeConnect_ReturnsEveryBuffer()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act
        connection.Link.EndFromPeer();

        // Assert
        connection.Run.IsCompleted.ShouldBeTrue();
        await connection.RunEnded();
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
        harness.Handler.Calls.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves that a CONNECT delivered in two pieces, cut at any position, is read whole and reaches
    /// <c>OnConnectAsync</c> exactly once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task RunAsync_ConnectCutAtEveryPosition_ReachesOnConnectAsyncOnce()
    {
        byte[] connect = ServerPackets.Connect;
        for (int cut = 1; cut < connect.Length; cut++)
        {
            // Arrange
            ServerHarness harness = ServerHarness.Create();
            TestConnection connection = harness.Accept();
            connection.Deliver(connect.AsSpan(0, cut));

            // Act
            connection.Deliver(connect.AsSpan(cut));

            // Assert
            harness.Handler.Calls.ShouldBe(["OnConnectAsync c1", "OnAdmit c1 replaces=-"], $"cut at {cut}");
            connection.Link.EndFromPeer();
            await connection.RunEnded();
            harness.Pool.Outstanding.ShouldBe(0);
        }
    }

    /// <summary>
    /// Proves that a connection that sends half a CONNECT and then nothing is aborted at the connect deadline, and
    /// the buffer that held the half is back in the pool afterwards.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task RunAsync_HalfAConnectThenSilence_AbortsAtTheConnectDeadline()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect.AsSpan(0, 8));
        int heldWhileWaiting = harness.Pool.Outstanding;

        // Act
        harness.Advance(ServerLimits.ConnectDeadline);

        // Assert
        heldWhileWaiting.ShouldBe(1);
        connection.Link.IsAborted.ShouldBeTrue();
        await connection.RunEnded();
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Handler.Calls.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves that the server enforces the deadlines of all its connections with one periodic timer of 2 seconds,
    /// whatever the number of connections.
    /// </summary>
    [Fact]
    public void Start_HundredConnections_CreatesOneTimer()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();

        // Act
        for (int i = 0; i < 100; i++)
        {
            harness.Accept();
        }

        // Assert
        List<CountingTimeProvider.CountedTimer> timers = harness.Timers.PeriodicTimers();
        timers.Count.ShouldBe(1);
        timers[0].Period.ShouldBe(TimeSpan.FromSeconds(2));
        harness.Server.Registry.Slots.Count(static slot => slot is not null).ShouldBe(100);
    }

    /// <summary>
    /// Proves that a connection whose run has ended holds no slot, so the sweep no longer visits it.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Sweep_ConnectionEnded_NoLongerHoldsItsSlot()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Accept();
        TestConnection second = harness.Accept();
        first.Link.EndFromPeer();
        second.Deliver(ServerPackets.Connect);
        second.Link.EndFromPeer();
        await first.RunEnded();
        await second.RunEnded();

        // Act
        harness.Advance(ServerLimits.SweepInterval);

        // Assert
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        harness.Server.ConnectionCount.ShouldBe(0);
    }
}
