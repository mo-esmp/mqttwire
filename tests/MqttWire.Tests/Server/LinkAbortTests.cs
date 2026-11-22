using Microsoft.Extensions.Logging;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the one rule for aborting a link (design 6.6, "Closing, bounded"): no abort is issued once the run has
/// ended, the run does not end while one is in flight, and a link that throws when it is aborted, as the WebSocket of
/// a request that has ended does, never breaks the sweep, the stop, a takeover or a host's call.
/// </summary>
public sealed class LinkAbortTests
{
    /// <summary>
    /// The event id of the line of a fault inside MqttWire (design 6.12).
    /// </summary>
    private const int FaultEvent = 20;

    /// <summary>
    /// The event id of the line of the connect deadline (design 6.12).
    /// </summary>
    private const int ConnectTimedOutEvent = 5;

    /// <summary>
    /// The topic every test sends to.
    /// </summary>
    private static readonly MqttTopic s_topic = MqttTopic.Create("a/b");

    /// <summary>
    /// Proves that a link whose abort throws at the connect deadline does not stop the sweep: the next connection is
    /// aborted in the same sweep, the throw is logged as a fault, and the connection ends at the close deadline.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Sweep_AbortThrowsAtTheConnectDeadline_GoesOnAndEndsTheConnectionAtTheCloseDeadline()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Accept();
        TestConnection second = harness.Accept();
        ObjectDisposedException thrown = new("HttpContext");
        first.Link.FailNextAbort(thrown);

        // Act
        harness.Advance(ServerLimits.ConnectDeadline);
        await second.RunEnded();
        bool firstAbortedAtTheConnectDeadline = first.Link.IsAborted;
        harness.Advance(ServerLimits.CloseDeadline);
        await first.RunEnded();

        // Assert
        second.Link.IsAborted.ShouldBeTrue();
        firstAbortedAtTheConnectDeadline.ShouldBeFalse();
        first.Link.IsAborted.ShouldBeTrue();
        first.Link.AbortCalls.ShouldBe(2);
        FaultsLogged(harness).ShouldBe([thrown]);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a link whose abort throws at the close deadline does not stop the sweep: the next connection is
    /// aborted in the same sweep, and the first at the next one, each with one <c>OnClosed</c>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Sweep_AbortThrowsAtTheCloseDeadline_GoesOnAndAbortsAgainAtTheNextSweep()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect("c1");
        TestConnection second = harness.Connect("c2");
        first.Connection.ShouldNotBeNull().Close(MqttReasonCode.AdministrativeAction);
        second.Connection.ShouldNotBeNull().Close(MqttReasonCode.AdministrativeAction);
        ObjectDisposedException thrown = new("HttpContext");
        first.Link.FailNextAbort(thrown);

        // Act
        harness.Advance(ServerLimits.CloseDeadline);
        await second.RunEnded();
        bool firstEndedAtTheCloseDeadline = first.Run.IsCompleted;
        harness.Advance(ServerLimits.SweepInterval);
        await first.RunEnded();

        // Assert
        firstEndedAtTheCloseDeadline.ShouldBeFalse();
        first.Link.AbortCalls.ShouldBe(2);
        FaultsLogged(harness).ShouldBe([thrown]);
        harness.ClosedCalls().ShouldBe(["OnClosed c1 ClosedByHost", "OnClosed c2 ClosedByHost"], ignoreOrder: true);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a link whose abort throws at the write deadline does not stop the sweep, and that the connection
    /// still ends, with one <c>OnClosed</c>, once the link has been aborted at the close deadline and its write has
    /// ended.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Sweep_AbortThrowsAtTheWriteDeadline_GoesOnAndEndsTheConnection()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        ObjectDisposedException thrown = new("HttpContext");
        connection.Link.FailNextAbort(thrown);

        // Act
        harness.Advance(ServerLimits.WriteDeadline + ServerLimits.SweepInterval);
        ConnectionState stateAtTheWriteDeadline = server.StateNow;
        bool abortedAtTheWriteDeadline = connection.Link.IsAborted;
        harness.Advance(ServerLimits.CloseDeadline);
        bool abortedAtTheCloseDeadline = connection.Link.IsAborted;
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.RunEnded();

        // Assert
        stateAtTheWriteDeadline.ShouldBe(ConnectionState.Closing);
        abortedAtTheWriteDeadline.ShouldBeFalse();
        abortedAtTheCloseDeadline.ShouldBeTrue();
        FaultsLogged(harness).ShouldBe([thrown]);
        harness.ClosedCalls().ShouldBe(["OnClosed c1 WriteTimedOut"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host's <c>Close</c> while a write is in progress does not throw when the link's abort does, and
    /// that the connection still ends with one <c>OnClosed</c>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_AbortThrowsWhileAWriteIsInProgress_DoesNotThrowAndTheConnectionEnds()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        ObjectDisposedException thrown = new("HttpContext");
        connection.Link.FailNextAbort(thrown);

        // Act
        Should.NotThrow(() => server.Close(MqttReasonCode.AdministrativeAction));
        harness.Advance(ServerLimits.CloseDeadline);
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.RunEnded();

        // Assert
        connection.Link.AbortCalls.ShouldBeGreaterThanOrEqualTo(2);
        FaultsLogged(harness).ShouldBe([thrown]);
        harness.ClosedCalls().ShouldBe(["OnClosed c1 ClosedByHost"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host's <c>TrySend</c> that evicts the connection while a write is in progress says "evicted"
    /// and does not throw when the link's abort does.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_AbortThrowsAtTheEviction_SaysEvictedAndDoesNotThrow()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxOutgoingMessages = 1);
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        ObjectDisposedException thrown = new("HttpContext");
        connection.Link.FailNextAbort(thrown);

        // Act
        MqttSendResult result = MqttSendResult.Taken;
        Should.NotThrow(() => result = server.TrySend(s_topic, "m3"u8.ToArray(), MqttQoS.AtMostOnce));
        harness.Advance(ServerLimits.CloseDeadline);
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.RunEnded();

        // Assert
        result.ShouldBe(MqttSendResult.Evicted);
        FaultsLogged(harness).ShouldBe([thrown]);
        harness.ClosedCalls().ShouldBe(["OnClosed c1 OutgoingLimitPassed"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a takeover whose close of the replaced connection hits a link that throws when aborted still
    /// admits the new connection with its CONNACK, and still ends the replaced one with exactly one <c>OnClosed</c>
    /// saying "taken over".
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Admit_AbortOfTheReplacedConnectionThrows_AdmitsTheNewOneAndEndsTheOldOneOnce()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection old = harness.Connect();
        old.Link.HoldWrites();
        old.Connection.ShouldNotBeNull().TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce)
            .ShouldBe(MqttSendResult.Taken);
        ObjectDisposedException thrown = new("HttpContext");
        old.Link.FailNextAbort(thrown);
        TestConnection replacement = harness.Accept();

        // Act
        replacement.Deliver(ServerPackets.ConnectFor("c1"));
        byte[] connAck = replacement.TakeWritten();
        harness.Advance(ServerLimits.CloseDeadline);
        old.CompleteHeldWrites(new IOException("The socket was aborted."));
        await old.RunEnded();
        List<string> closedAfterTheOldOne = harness.ClosedCalls();
        replacement.Link.EndFromPeer();
        await replacement.RunEnded();

        // Assert
        connAck.ShouldBe(ServerPackets.ConnAck);
        FaultsLogged(harness).ShouldBe([thrown]);
        closedAfterTheOldOne.ShouldBe(["OnClosed c1 TakenOver"]);
        harness.ClosedCalls().ShouldBe(["OnClosed c1 TakenOver", "OnClosed c1 ConnectionLost"]);
        harness.Server.ConnectionCount.ShouldBe(0);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the stop walks every connection and completes when links throw as they are aborted: one that has
    /// sent no CONNECT and one with a write in progress, each ended at the close deadline.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_AbortsThrow_WalksEveryConnectionAndCompletes()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection silent = harness.Accept();
        TestConnection alsoSilent = harness.Accept();
        TestConnection writing = harness.Connect();
        writing.Link.HoldWrites();
        writing.Connection.ShouldNotBeNull().TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce)
            .ShouldBe(MqttSendResult.Taken);
        silent.Link.FailNextAbort(new ObjectDisposedException("HttpContext"));
        writing.Link.FailNextAbort(new ObjectDisposedException("HttpContext"));

        // Act
        Task stop = Task.CompletedTask;
        Should.NotThrow(() =>
        {
            stop = harness.Server.StopAsync();
        });
        bool walkedPastTheFirst = alsoSilent.Link.IsAborted;
        harness.Advance(ServerLimits.CloseDeadline);
        writing.CompleteHeldWrites(new IOException("The socket was aborted."));
        await stop.Within("The server's stop");
        await silent.RunEnded();
        await alsoSilent.RunEnded();
        await writing.RunEnded();

        // Assert
        walkedPastTheFirst.ShouldBeTrue();
        FaultsLogged(harness).Count.ShouldBe(2);
        harness.ClosedCalls().ShouldBe(["OnClosed c1 ServerStopping"]);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves the order that keeps an abort off a link whose request has ended: when the peer closes between the
    /// sweep's decision to abort and the abort itself, the run does not end until the abort has been issued.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Sweep_PeerClosesBetweenTheDecisionAndTheAbort_TheRunEndsOnlyAfterTheAbort()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();
        bool? endedInTheGap = null;
        bool? endedWhenAborted = null;
        connection.Link.Aborting = () => endedWhenAborted = connection.Run.IsCompleted;

        // The end's log line is written between the hold that decides on the abort and the abort: the peer closes
        // there, and the run carries on, on this thread, as far as it can.
        harness.Log.Logged = eventId =>
        {
            if (eventId == ConnectTimedOutEvent)
            {
                connection.Link.EndFromPeer();
                endedInTheGap = connection.Run.IsCompleted;
            }
        };

        // Act
        harness.Advance(ServerLimits.ConnectDeadline);
        await connection.RunEnded();

        // Assert
        endedInTheGap.ShouldBe(false);
        endedWhenAborted.ShouldBe(false);
        connection.Link.AbortCalls.ShouldBe(1);
        FaultsLogged(harness).ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a check of the sweep that throws (here the clock, as the connection is stamped Closing) does not
    /// leave the sweep: that connection is ended as a fault, logged with event 20, and the next one is still checked.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Sweep_AConnectionsCheckThrows_EndsItAsAFaultAndChecksTheNext()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect("c1");
        TestConnection second = harness.Connect("c2");

        // The keep-alive of 30 s gives a deadline of 45 s; the sweep at 46 s acts on it.
        harness.Advance(TimeSpan.FromSeconds(44));

        // The sweep reads the clock once; the second read is the stamp of the first connection's move to Closing.
        harness.Timers.ThrowOnTimestamp(2);

        // Act
        harness.Advance(ServerLimits.SweepInterval);
        await first.RunEnded();
        byte[] secondWritten = second.TakeWritten();
        second.Link.EndFromPeer();
        await second.RunEnded();

        // Assert
        first.Link.IsAborted.ShouldBeTrue();
        first.TakeWritten().ShouldBeEmpty();
        secondWritten.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.KeepAliveTimeout));
        FaultsLogged(harness).ShouldHaveSingleItem().ShouldBeOfType<InvalidOperationException>();
        harness.ClosedCalls().ShouldBe(["OnClosed c1 KeepAliveTimedOut", "OnClosed c2 KeepAliveTimedOut"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the reservation of the send path does not outlive an admission that fails after the host accepted
    /// (here the clock throws as the admission is stamped): the connection is refused with CONNACK <c>0x83</c>, is in
    /// no table, gets no <c>OnClosed</c>, and its run ends.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Admit_TheClockThrowsAfterTheHostAccepted_RefusesAndLeavesNothingReserved()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Admit = (_, _) =>
        {
            harness.Timers.ThrowOnTimestamp(1);
            return MqttAdmitResult.Accepted;
        };
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.Connect);
        byte[] written = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        written.ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.ImplementationSpecificError));
        connection.Link.IsAborted.ShouldBeFalse();
        harness.Server.ConnectionCount.ShouldBe(0);
        harness.ClosedCalls().ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the reservation of the send path does not outlive a takeover whose close of the replaced
    /// connection throws (here the clock, as the replaced connection is stamped Closing): the new connection ends as a
    /// fault with exactly one <c>OnClosed</c>, and its send path is given up.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Admit_TheCloseOfTheReplacedConnectionThrows_EndsTheNewOneAndLeavesNothingReserved()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection old = harness.Connect();
        TestConnection replacement = harness.Accept();

        // After the host's acceptance the clock is read for the admission, then for the replaced connection's move to
        // Closing: that second read throws.
        harness.Handler.Admit = (_, replaces) =>
        {
            if (replaces is not null)
            {
                harness.Timers.ThrowOnTimestamp(2);
            }

            return MqttAdmitResult.Accepted;
        };

        // Act
        replacement.Deliver(ServerPackets.ConnectFor("c1"));
        MqttWireConnection admitted = replacement.Connection.ShouldNotBeNull();
        bool reserved = admitted.GetCounts().IsWriting;
        await replacement.RunEnded();
        old.Link.EndFromPeer();
        await old.RunEnded();

        // Assert
        reserved.ShouldBeFalse();
        replacement.TakeWritten().ShouldBeEmpty();
        replacement.Link.IsAborted.ShouldBeTrue();
        FaultsLogged(harness).ShouldHaveSingleItem().ShouldBeOfType<InvalidOperationException>();
        harness.ClosedCalls().ShouldBe(["OnClosed c1 ConnectionLost", "OnClosed c1 TakenOver"]);
        harness.Server.ConnectionCount.ShouldBe(0);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Lists the exceptions of the fault lines (event 20) logged so far, oldest first.
    /// </summary>
    /// <param name="harness">
    /// The harness whose log is read.
    /// </param>
    /// <returns>
    /// The exception of each line, each logged at the error level.
    /// </returns>
    private static List<Exception?> FaultsLogged(ServerHarness harness)
    {
        List<(LogLevel Level, EventId Id, string Message, Exception? Exception)> faults =
            [.. harness.Log.EntriesNow().Where(static entry => entry.Id.Id == FaultEvent)];
        faults.ShouldAllBe(static entry => entry.Level == LogLevel.Error);
        return [.. faults.Select(static entry => entry.Exception)];
    }
}
