using System.Net.WebSockets;
using Microsoft.Extensions.Logging;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the admission step, the takeover of a client id by a new connection, and the one
/// <c>OnClosed</c> that matches every accepted <c>OnAdmit</c>.
/// </summary>
public sealed class AdmissionTests
{
    /// <summary>
    /// The event id of the log entry for a handler that threw.
    /// </summary>
    private const int HandlerThrewEvent = 15;

    /// <summary>
    /// Proves that a connection whose client id no other connection has is admitted with <c>replaces</c> null,
    /// counted, and found by its client id.
    /// </summary>
    [Fact]
    public void Admit_NoOtherConnection_PassesNullAsReplaces()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();

        // Act
        TestConnection connection = harness.Connect();

        // Assert
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1", "OnAdmit c1 replaces=-"]);
        harness.Server.ConnectionCount.ShouldBe(1);
        harness.Server.TryGetConnection("c1", out MqttWireConnection? found).ShouldBeTrue();
        found.ShouldBeSameAs(connection.Connection);
    }

    /// <summary>
    /// Proves that a second connection the host admits for a client id takes it over: <c>OnAdmit</c> sees the first
    /// as <c>replaces</c>, the first is sent DISCONNECT 0x8E and ends with <see cref="MqttCloseReason.TakenOver"/>,
    /// and the second is the one found by the client id.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Admit_SecondConnectionAccepted_TakesOverTheFirst()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect();
        MqttWireConnection? replaces = null;
        harness.Handler.Admit = (_, existing) =>
        {
            replaces = existing;
            return MqttAdmitResult.Accepted;
        };

        // Act
        TestConnection second = harness.Connect();
        byte[] writtenToFirst = first.TakeWritten();
        first.Link.EndFromPeer();
        await first.RunEnded();

        // Assert
        writtenToFirst.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.SessionTakenOver));
        first.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        replaces.ShouldNotBeNull();
        replaces.ShouldBeSameAs(first.Connection);
        harness.Handler.Calls.Where(static call => call.StartsWith("OnClosed", StringComparison.Ordinal))
            .ShouldBe(["OnClosed c1 TakenOver"]);
        harness.Server.TryGetConnection("c1", out MqttWireConnection? found).ShouldBeTrue();
        found.ShouldBeSameAs(second.Connection);
        harness.Server.ConnectionCount.ShouldBe(1);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a takeover the host refuses answers the second connection with the host's code and leaves the
    /// first Active, found by the client id and sent nothing; the refused connection gets no <c>OnClosed</c>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Admit_TakeoverRefused_LeavesTheFirstUntouched()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect();
        harness.Handler.Admit = static (_, existing) =>
            existing is null ? MqttAdmitResult.Accepted : MqttAdmitResult.Refuse(MqttReasonCode.NotAuthorized);
        TestConnection second = harness.Accept();

        // Act
        second.Deliver(ServerPackets.Connect);
        await second.RunEnded();

        // Assert
        second.TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.NotAuthorized));
        second.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        first.TakeWritten().ShouldBeEmpty();
        first.Connection.ShouldNotBeNull().StateNow.ShouldBe(ConnectionState.Active);
        harness.Server.TryGetConnection("c1", out MqttWireConnection? found).ShouldBeTrue();
        found.ShouldBeSameAs(first.Connection);
        harness.Handler.Calls.ShouldBe(
            ["OnConnectAsync c1", "OnAdmit c1 replaces=-", "OnConnectAsync c1", "OnAdmit c1 replaces=c1"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that two CONNECTs for one client id whose <c>OnConnectAsync</c> complete together, on two thread-pool
    /// threads, go through the admission step one after the other: exactly one of the two <c>OnAdmit</c> calls sees
    /// the other connection as <c>replaces</c>, and each accepted <c>OnAdmit</c> gets exactly one <c>OnClosed</c>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Admit_TwoConnectsFinishTogether_GoThroughOneAfterTheOther()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();

        // Its continuations, one per connection, are queued to the thread pool, so the two connect phases go on
        // from there on two threads, not one after the other on the thread that sets the result.
        TaskCompletionSource<MqttConnectResult> release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.Handler.Connect = (_, _) => new ValueTask<MqttConnectResult>(release.Task);
        TaskCompletionSource bothAdmitted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<(MqttWireConnection Admitted, MqttWireConnection? Replaces)> admissions = [];
        harness.Handler.Admit = (admitted, replaces) =>
        {
            // Called under the registry's lock, so one at a time.
            admissions.Add((admitted, replaces));
            if (admissions.Count == 2)
            {
                bothAdmitted.SetResult();
            }

            return MqttAdmitResult.Accepted;
        };
        List<MqttWireConnection> closed = [];
        harness.Handler.Closed = (connection, _) =>
        {
            lock (closed)
            {
                closed.Add(connection);
            }
        };
        TestConnection first = harness.Accept();
        TestConnection second = harness.Accept();
        first.Deliver(ServerPackets.Connect);
        second.Deliver(ServerPackets.Connect);

        // Act
        release.SetResult(MqttConnectResult.Accept());
        await bothAdmitted.Task.Within("Both admissions");
        first.Link.EndFromPeer();
        second.Link.EndFromPeer();
        await Task.WhenAll(first.Run, second.Run).Within("The connections' runs");

        // Assert
        admissions.Count.ShouldBe(2);
        admissions[0].Replaces.ShouldBeNull();
        admissions[1].Replaces.ShouldBeSameAs(admissions[0].Admitted);
        admissions[1].Admitted.ShouldNotBeSameAs(admissions[0].Admitted);
        harness.Handler.Calls.Where(static call => call.StartsWith("OnAdmit", StringComparison.Ordinal))
            .ShouldBe(["OnAdmit c1 replaces=-", "OnAdmit c1 replaces=c1"]);
        closed.Count.ShouldBe(2);
        closed.ShouldBe([admissions[0].Admitted, admissions[1].Admitted], ignoreOrder: true);
        harness.Server.ConnectionCount.ShouldBe(0);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a new connection that fails right after the admission, its CONNACK not written, ends as an
    /// ordinary connection with <see cref="MqttCloseReason.ConnectionLost"/>, and the connection it replaced stays
    /// taken over: it is sent DISCONNECT 0x8E, is not found again and ends with <see cref="MqttCloseReason.TakenOver"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Admit_NewConnectionFailsRightAfter_GetsOnClosed()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect();
        TestConnection second = harness.Accept();
        second.Link.FailWrites(new WebSocketException("The socket is gone."));

        // Act
        second.Deliver(ServerPackets.Connect);
        await second.RunEnded();
        bool foundAfterTheFailure = harness.Server.TryGetConnection("c1", out _);
        first.Link.EndFromPeer();
        await first.RunEnded();

        // Assert
        foundAfterTheFailure.ShouldBeFalse();
        first.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.SessionTakenOver));
        harness.Handler.Calls.ShouldBe(
        [
            "OnConnectAsync c1", "OnAdmit c1 replaces=-", "OnConnectAsync c1", "OnAdmit c1 replaces=c1",
            "OnClosed c1 ConnectionLost", "OnClosed c1 TakenOver",
        ]);
        harness.Server.ConnectionCount.ShouldBe(0);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that an <c>OnAdmit</c> that throws is logged as an error, answered with CONNACK 0x83, gets no
    /// <c>OnClosed</c>, and leaves the client-id table as it was.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Admit_HostThrows_AnswersConnAck0x83AndCallsNoOnClosed()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect();
        InvalidOperationException thrown = new("The host failed.");
        harness.Handler.Admit = (_, _) => throw thrown;
        TestConnection second = harness.Accept();

        // Act
        second.Deliver(ServerPackets.Connect);
        await second.RunEnded();

        // Assert
        second.TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.ImplementationSpecificError));
        second.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Log.Entries.ShouldContain(entry =>
            entry.Id.Id == HandlerThrewEvent && entry.Level == LogLevel.Error && entry.Exception == thrown
            && entry.Message.Contains("OnAdmit", StringComparison.Ordinal));
        harness.Handler.Calls.ShouldNotContain(static call => call.StartsWith("OnClosed", StringComparison.Ordinal));
        harness.Server.TryGetConnection("c1", out MqttWireConnection? found).ShouldBeTrue();
        found.ShouldBeSameAs(first.Connection);
        first.TakeWritten().ShouldBeEmpty();
        harness.Server.ConnectionCount.ShouldBe(1);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a CONNECT for a client id whose connection is already closing sees that connection as
    /// <c>replaces</c>, sends it no DISCONNECT 0x8E, and that each connection gets exactly one <c>OnClosed</c>, the old
    /// one with the reason it already had.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Admit_ReplacedConnectionAlreadyClosing_KeepsItsOwnReason()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect();
        MqttWireConnection closing = first.Connection.ShouldNotBeNull();

        // Held in Closing: the close is not answered, so the run waits until the peer ends the link.
        closing.BeginClose(CloseCause.ClosedByHost, MqttReasonCode.AdministrativeAction, reasonString: null);
        first.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.AdministrativeAction));
        MqttWireConnection? replaces = null;
        harness.Handler.Admit = (_, existing) =>
        {
            replaces = existing;
            return MqttAdmitResult.Accepted;
        };

        // Act
        TestConnection second = harness.Connect();
        first.Link.EndFromPeer();
        await first.RunEnded();
        second.Link.EndFromPeer();
        await second.RunEnded();

        // Assert
        replaces.ShouldBeSameAs(closing);
        first.TakeWritten().ShouldBeEmpty();
        harness.Handler.Calls.Where(static call => call.StartsWith("OnClosed", StringComparison.Ordinal))
            .ShouldBe(["OnClosed c1 ClosedByHost", "OnClosed c1 ConnectionLost"]);
        harness.Server.ConnectionCount.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a connection taken over after its admission but before its CONNACK is sent no MQTT packet at all:
    /// the takeover finds the send path reserved for the CONNACK, so the link is aborted, and the connection ends
    /// once with <see cref="MqttCloseReason.TakenOver"/>, while the connection that took over is connected normally.
    /// The window is reached deterministically: the second
    /// connection replaces a first one, whose DISCONNECT 0x8E is written between the second's admission and its
    /// CONNACK; when that DISCONNECT's buffer goes back to the pool, the pool's return hook connects the third
    /// connection, inline, which takes the client id over from the second.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Admit_TakenOverBeforeItsConnAck_GetsNoMqttPacket()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect();
        TestConnection? third = null;
        bool armed = false;
        harness.Handler.Admit = (_, existing) =>
        {
            if (!armed && existing is not null)
            {
                // The second's admission: the next buffer returned is the first's DISCONNECT, after this step.
                armed = true;
                harness.Pool.Returning = _ =>
                {
                    harness.Pool.Returning = null;
                    third = harness.Connect();
                };
            }

            return MqttAdmitResult.Accepted;
        };
        TestConnection second = harness.Accept();

        // Act
        second.Deliver(ServerPackets.Connect);
        await second.RunEnded();

        // Assert
        second.TakeWritten().ShouldBeEmpty();
        second.Link.IsAborted.ShouldBeTrue();
        second.Link.ClosedWith.ShouldBeNull();
        third.ShouldNotBeNull();
        harness.Server.TryGetConnection("c1", out MqttWireConnection? found).ShouldBeTrue();
        found.ShouldBeSameAs(third.Connection);
        first.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.SessionTakenOver));
        harness.Handler.Calls.ShouldBe(
        [
            "OnConnectAsync c1", "OnAdmit c1 replaces=-", "OnConnectAsync c1", "OnAdmit c1 replaces=c1",
            "OnConnectAsync c1", "OnAdmit c1 replaces=c1", "OnClosed c1 TakenOver",
        ]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host that closes the new connection from inside <c>OnAdmit</c> and then accepts gets the close
    /// after the step: the send path is reserved for the CONNACK from the start of the step, so no CONNACK and no
    /// DISCONNECT are written and the link is aborted, and <c>OnClosed</c> is called once with
    /// <see cref="MqttCloseReason.ClosedByHost"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_FromInsideOnAdmit_TakesEffectAfterTheStep()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Admit = static (connection, _) =>
        {
            connection.BeginClose(CloseCause.ClosedByHost, MqttReasonCode.AdministrativeAction, reasonString: null);
            return MqttAdmitResult.Accepted;
        };
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.Connect);
        await connection.RunEnded();

        // Assert
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.IsAborted.ShouldBeTrue();
        connection.Link.ClosedWith.ShouldBeNull();
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1", "OnAdmit c1 replaces=-", "OnClosed c1 ClosedByHost"]);
        harness.Server.ConnectionCount.ShouldBe(0);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host that closes the new connection from inside <c>OnAdmit</c> and then refuses it leaves
    /// nothing behind: nothing is written, the link is aborted, no <c>OnClosed</c> is called, and the connection ends
    /// with its slot free.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_FromInsideOnAdmitThenRefused_EndsWithoutOnClosed()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Admit = static (connection, _) =>
        {
            connection.Close(MqttReasonCode.AdministrativeAction);
            return MqttAdmitResult.Refuse(MqttReasonCode.NotAuthorized);
        };
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.Connect);
        await connection.RunEnded();

        // Assert
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.IsAborted.ShouldBeTrue();
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1", "OnAdmit c1 replaces=-"]);
        harness.Server.ConnectionCount.ShouldBe(0);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the keep-alive runs from the moment the connection becomes Active, not from the start of the
    /// admission step: with keep-alive 30 and an <c>OnAdmit</c> that takes 44 seconds, the connection is open 44
    /// seconds after its CONNACK and is closed with 0x8D once 45 have passed.
    /// </summary>
    [Fact]
    public void Admit_SlowOnAdmit_DoesNotCountAgainstTheKeepAlive()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Admit = (_, _) =>
        {
            harness.Advance(TimeSpan.FromSeconds(44));
            return MqttAdmitResult.Accepted;
        };
        TestConnection connection = harness.Connect();

        // Act
        harness.Advance(TimeSpan.FromSeconds(44));
        byte[] writtenAt44 = connection.TakeWritten();
        harness.Advance(TimeSpan.FromSeconds(2));
        byte[] writtenAt46 = connection.TakeWritten();

        // Assert
        writtenAt44.ShouldBeEmpty();
        writtenAt46.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.KeepAliveTimeout));
    }

    /// <summary>
    /// Proves that the end of a connection that was taken over does not remove the connection that replaced it from
    /// the client-id table.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task OnClosed_OldConnectionEndsAfterTakeover_DoesNotRemoveItsReplacement()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect();
        TestConnection second = harness.Connect();

        // Act
        first.Link.EndFromPeer();
        await first.RunEnded();

        // Assert
        harness.Server.TryGetConnection("c1", out MqttWireConnection? found).ShouldBeTrue();
        found.ShouldBeSameAs(second.Connection);
        found.StateNow.ShouldBe(ConnectionState.Active);
        harness.Server.ConnectionCount.ShouldBe(1);
        second.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that over 50 cycles of connects, takeovers, refusals and disconnects across 5 client ids, every
    /// connection whose <c>OnAdmit</c> was accepted gets exactly one <c>OnClosed</c>, and no other connection gets one.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task OnClosed_EveryAcceptedAdmit_IsMatchedByExactlyOne()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        List<MqttWireConnection> accepted = [];
        List<MqttWireConnection> closed = [];
        int admits = 0;
        harness.Handler.Admit = (connection, _) =>
        {
            if (++admits % 4 == 0)
            {
                return MqttAdmitResult.Refuse(MqttReasonCode.NotAuthorized);
            }

            accepted.Add(connection);
            return MqttAdmitResult.Accepted;
        };
        harness.Handler.Closed = (connection, _) =>
        {
            lock (closed)
            {
                closed.Add(connection);
            }
        };
        List<TestConnection> connections = [];

        // Act
        for (int i = 0; i < 50; i++)
        {
            TestConnection connection = harness.Accept();
            connection.Deliver(ServerPackets.ConnectFor($"c{i % 5}"));
            if (i % 3 == 0)
            {
                connection.Deliver(ClientPackets.Disconnect);
            }

            connections.Add(connection);
        }

        foreach (TestConnection connection in connections)
        {
            connection.Link.EndFromPeer();
            await connection.RunEnded();
        }

        // Assert
        accepted.Count.ShouldBeGreaterThan(0);
        admits.ShouldBe(50);
        closed.Count.ShouldBe(accepted.Count);
        closed.Distinct().Count().ShouldBe(closed.Count);
        closed.ShouldBe(accepted, ignoreOrder: true);
        harness.Handler.Calls.Count(static call => call.StartsWith("OnClosed", StringComparison.Ordinal))
            .ShouldBe(accepted.Count);
        harness.Handler.Calls.ShouldContain(static call => call.EndsWith(" TakenOver", StringComparison.Ordinal));
        harness.Server.ConnectionCount.ShouldBe(0);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host that calls <c>TrySend</c> and <c>Close</c> from <c>OnClosed</c> is told "not open" and that
    /// nothing is written and no second <c>OnClosed</c> follows (Review Focus 1).
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task OnClosed_HostCallsTrySendAndClose_GetsNotOpenAndNothingHappens()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        MqttSendResult? sent = null;
        harness.Handler.Closed = (connection, _) =>
        {
            sent = connection.TrySend(MqttTopic.Create("a/b"), "hi"u8.ToArray(), MqttQoS.AtLeastOnce);
            connection.Close(MqttReasonCode.AdministrativeAction);
        };
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ClientPackets.Disconnect);
        await connection.RunEnded();

        // Assert
        sent.ShouldBe(MqttSendResult.NotOpen);
        connection.TakeWritten().ShouldBeEmpty();
        harness.Handler.Calls.Where(static call => call.StartsWith("OnClosed", StringComparison.Ordinal))
            .ShouldBe(["OnClosed c1 ClientDisconnected"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that an <c>OnClosed</c> that throws is logged as an error and the connection still ends: it is Closed,
    /// its slot is free and the registry is empty.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task OnClosed_HostThrows_IsLoggedAndTheConnectionStillEnds()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        InvalidOperationException thrown = new("The host failed.");
        harness.Handler.Closed = (_, _) => throw thrown;
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        connection.Deliver(ClientPackets.Disconnect);
        await connection.RunEnded();

        // Assert
        harness.Log.Entries.ShouldContain(entry =>
            entry.Id.Id == HandlerThrewEvent && entry.Level == LogLevel.Error && entry.Exception == thrown
            && entry.Message.Contains("OnClosed", StringComparison.Ordinal));
        server.StateNow.ShouldBe(ConnectionState.Closed);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        harness.Server.Registry.WhenEmptyAsync().IsCompleted.ShouldBeTrue();
        harness.Server.ConnectionCount.ShouldBe(0);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a DISCONNECT from the client ends the connection with
    /// <see cref="MqttCloseReason.ClientDisconnected"/>, closes the WebSocket and sends nothing back.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Disconnect_FromClient_EndsWithClientDisconnected()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ClientPackets.Disconnect);
        await connection.RunEnded();

        // Assert
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1", "OnAdmit c1 replaces=-", "OnClosed c1 ClientDisconnected"]);
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Server.ConnectionCount.ShouldBe(0);
        harness.Pool.Outstanding.ShouldBe(0);
    }
}
