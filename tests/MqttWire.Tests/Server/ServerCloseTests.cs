using System.Net.WebSockets;
using System.Text;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the bounded close (design 6.6, "Closing, bounded") and of the write deadline: a close with no write in
/// progress hands DISCONNECT and then the WebSocket close to the link through the one send path; a close with a
/// write in progress aborts the link and writes nothing; an abort releases nothing by itself; and the connection is
/// Closed only when the write in progress has ended and returned its buffer.
/// </summary>
public sealed class ServerCloseTests
{
    /// <summary>
    /// The topic every test sends to.
    /// </summary>
    private static readonly MqttTopic s_topic = MqttTopic.Create("a/b");

    /// <summary>
    /// Proves that the host's <c>Close</c> on a connection with no write in progress writes DISCONNECT with its code
    /// and then closes the WebSocket with status 1000, without waiting for the peer, and that <c>OnClosed</c> reports
    /// <see cref="MqttCloseReason.ClosedByHost"/> once the peer has ended the link.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_NoWriteInProgress_WritesDisconnectThenClosesTheWebSocket()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        server.Close(MqttReasonCode.AdministrativeAction);
        byte[] written = connection.TakeWritten();
        WebSocketCloseStatus? closedWith = connection.Link.ClosedWith;
        bool endedBeforeThePeer = connection.Run.IsCompleted;
        int closedBeforeThePeer = ClosedCalls(harness).Count;
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        written.ShouldBe(Hex.Parse("E0 01 98"));
        closedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        connection.Link.IsAborted.ShouldBeFalse();
        endedBeforeThePeer.ShouldBeFalse();
        closedBeforeThePeer.ShouldBe(0);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
        connection.TakeWritten().ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the reason string the host hands to <c>Close</c> is carried in the DISCONNECT, cut to 256 bytes at
    /// a character boundary: 300 two-byte characters to 128.
    /// </summary>
    [Fact]
    public void Close_WithAReasonString_CarriesItCut()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        server.Close(MqttReasonCode.AdministrativeAction, new string('é', 300));

        // Assert
        // Remaining Length 262 and property length 259: the code, and the reason string's 3 + 256 bytes.
        connection.TakeWritten().ShouldBe(
            [.. Hex.Parse("E0 86 02 98 83 02 1F 01 00"), .. Encoding.UTF8.GetBytes(new string('é', 128))]);
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a reason string no packet can carry throws in the host's code and changes nothing: the connection
    /// stays open.
    /// </summary>
    [Fact]
    public void Close_ReasonStringNotEncodable_ThrowsAndClosesNothing()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        ArgumentException thrown =
            Should.Throw<ArgumentException>(() => server.Close(MqttReasonCode.AdministrativeAction, "a\0b"));

        // Assert
        thrown.ParamName.ShouldBe("reasonString");
        server.StateNow.ShouldBe(ConnectionState.Active);
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.ClosedWith.ShouldBeNull();
    }

    /// <summary>
    /// Proves that a close while a write is in progress aborts the link at once and writes no DISCONNECT and no
    /// WebSocket close, even when the write in progress then completes.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_WriteInProgress_AbortsAtOnceAndWritesNoDisconnect()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);

        // Act
        server.Close(MqttReasonCode.AdministrativeAction);
        bool abortedAtOnce = connection.Link.IsAborted;

        // The write in progress completes at the same moment as the abort: it ends, and nothing follows it.
        connection.CompleteHeldWrites();
        await connection.RunEnded();

        // Assert
        abortedAtOnce.ShouldBeTrue();
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery("m1"u8));
        connection.Link.ClosedWith.ShouldBeNull();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
        server.GetCounts().WaitingDeliveries.ShouldBe(0);
        server.GetCounts().OutgoingBytes.ShouldBe(0);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a close that begins while a caller is inside a write that completes at once aborts the link and
    /// writes no DISCONNECT, and that the caller returns its own buffer.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_WhileACallerIsInsideAWrite_AbortsAndWritesNoDisconnect()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.Writing = _ =>
        {
            // The caller has taken the send path and is handing its packet to the link.
            connection.Link.Writing = null;
            server.Close(MqttReasonCode.AdministrativeAction);
        };

        // Act
        MqttSendResult result = server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
        await connection.RunEnded();

        // Assert
        result.ShouldBe(MqttSendResult.Taken);
        connection.Link.IsAborted.ShouldBeTrue();
        connection.Link.ClosedWith.ShouldBeNull();
        connection.TakeWritten().ShouldBeEmpty();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the WebSocket close follows the DISCONNECT through the one send path: while the link holds the
    /// DISCONNECT's write, the close has not been handed over, and it is once the write has completed.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_DisconnectWriteHeld_ClosesTheWebSocketOnlyAfterIt()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();

        // Act
        server.Close(MqttReasonCode.AdministrativeAction);
        WebSocketCloseStatus? closedWhileHeld = connection.Link.ClosedWith;
        bool writingWhileHeld = server.GetCounts().IsWriting;
        connection.CompleteHeldWrites();
        WebSocketCloseStatus? closedAfterwards = connection.Link.ClosedWith;
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        closedWhileHeld.ShouldBeNull();
        writingWhileHeld.ShouldBeTrue();
        closedAfterwards.ShouldBe(WebSocketCloseStatus.NormalClosure);
        connection.Link.IsAborted.ShouldBeFalse();
        connection.TakeWritten().ShouldBe(Hex.Parse("E0 01 98"));
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that <c>Close</c> called from two threads at once closes the connection once: one DISCONNECT, with the
    /// code of whichever call came first, one WebSocket close and one <c>OnClosed</c>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_CalledTwiceFromTwoThreads_ClosesOnce()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        using Barrier together = new(participantCount: 2);

        // Act
        Task first = Task.Run(() =>
        {
            together.SignalAndWait();
            server.Close(MqttReasonCode.AdministrativeAction);
        });
        Task second = Task.Run(() =>
        {
            together.SignalAndWait();
            server.Close(MqttReasonCode.ServerShuttingDown);
        });
        await Task.WhenAll(first, second).Within("The two closes");
        byte[] written = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        written.Length.ShouldBe(3);
        written.AsSpan(0, 2).ToArray().ShouldBe(Hex.Parse("E0 01"));
        written[2].ShouldBeOneOf((byte)MqttReasonCode.AdministrativeAction, (byte)MqttReasonCode.ServerShuttingDown);
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        connection.Link.IsAborted.ShouldBeFalse();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a close the peer never answers is bounded: the link is aborted at the close deadline, two
    /// seconds after the close began, and <c>OnClosed</c> is called once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_PeerNeverAnswers_AbortsAtTheCloseDeadline()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        server.Close(MqttReasonCode.AdministrativeAction);
        bool abortedAtOnce = connection.Link.IsAborted;
        bool endedAtOnce = connection.Run.IsCompleted;
        harness.Advance(TimeSpan.FromSeconds(2));
        await connection.RunEnded();

        // Assert
        abortedAtOnce.ShouldBeFalse();
        endedAtOnce.ShouldBeFalse();
        connection.Link.IsAborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBe(Hex.Parse("E0 01 98"));
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
        server.StateNow.ShouldBe(ConnectionState.Closed);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that aborting releases nothing by itself (design 6.8 rules 3 and 4): the buffer of the write in
    /// progress stays out, and the connection is not Closed, until that write has ended; only then does the buffer
    /// go back, once, and the connection end.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_Abort_DoesNotReleaseTheHeldWritesBuffer()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        int outstandingWhileWriting = harness.Pool.Outstanding;

        // Act
        server.Close(MqttReasonCode.AdministrativeAction);
        bool aborted = connection.Link.IsAborted;
        int outstandingAfterTheAbort = harness.Pool.Outstanding;
        ConnectionState stateAfterTheAbort = server.StateNow;
        bool endedAfterTheAbort = connection.Run.IsCompleted;
        bool emptyAfterTheAbort = harness.Server.Registry.WhenEmptyAsync().IsCompleted;
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        int outstandingAfterTheWrite = harness.Pool.Outstanding;
        await connection.RunEnded();

        // Assert
        outstandingWhileWriting.ShouldBe(1);
        aborted.ShouldBeTrue();
        outstandingAfterTheAbort.ShouldBe(1);
        stateAfterTheAbort.ShouldBe(ConnectionState.Closing);
        endedAfterTheAbort.ShouldBeFalse();
        emptyAfterTheAbort.ShouldBeFalse();
        outstandingAfterTheWrite.ShouldBe(0);
        server.StateNow.ShouldBe(ConnectionState.Closed);
        harness.Server.Registry.WhenEmptyAsync().IsCompleted.ShouldBeTrue();
        harness.Pool.FaultyReturns.ShouldBe(0);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
    }

    /// <summary>
    /// Proves that a connection whose read loop has ended is not Closed while a write is still in progress: its
    /// slot stays taken and <c>OnClosed</c> is not called until the write has ended and returned its buffer.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Run_PeerEndsWhileAWriteIsInProgress_IsClosedOnlyWhenTheWriteHasEnded()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);

        // Act
        connection.Link.EndFromPeer();
        ConnectionState stateWhileWriting = server.StateNow;
        bool endedWhileWriting = connection.Run.IsCompleted;
        int closedWhileWriting = ClosedCalls(harness).Count;
        bool slotHeldWhileWriting = harness.Server.Registry.Slots.Contains(server);
        connection.CompleteHeldWrites(new IOException("The socket is gone."));
        await connection.RunEnded();

        // Assert
        stateWhileWriting.ShouldBe(ConnectionState.Closing);
        endedWhileWriting.ShouldBeFalse();
        closedWhileWriting.ShouldBe(0);
        slotHeldWhileWriting.ShouldBeTrue();
        connection.Link.IsAborted.ShouldBeTrue();
        server.StateNow.ShouldBe(ConnectionState.Closed);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a close between a connection's admission and the write of its CONNACK finds the send path taken
    /// for that CONNACK and aborts the link: no MQTT packet precedes a CONNACK, so none is written at all. The window
    /// is reached as in <c>TrySend_BetweenTheAdmissionAndTheConnAck_WaitsBehindTheConnAck</c>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_BetweenTheAdmissionAndTheConnAck_AbortsAndWritesNothing()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect("c1");
        harness.Handler.Admit = (admitted, existing) =>
        {
            if (existing is not null)
            {
                harness.Pool.Returning = _ =>
                {
                    harness.Pool.Returning = null;
                    admitted.Close(MqttReasonCode.AdministrativeAction);
                };
            }

            return MqttAdmitResult.Accepted;
        };
        TestConnection second = harness.Accept();

        // Act
        second.Deliver(ServerPackets.Connect);
        await second.RunEnded();

        // Assert
        second.Link.IsAborted.ShouldBeTrue();
        second.Link.ClosedWith.ShouldBeNull();
        second.TakeWritten().ShouldBeEmpty();
        first.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.SessionTakenOver));
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a close while the link holds the CONNACK's write aborts the link and writes no DISCONNECT, and
    /// that the CONNACK's buffer goes back when its write has ended.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_ConnAckWriteHeld_AbortsAndWritesNoDisconnect()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();
        connection.Link.HoldWrites();
        connection.Deliver(ServerPackets.Connect);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        server.Close(MqttReasonCode.AdministrativeAction);
        bool aborted = connection.Link.IsAborted;
        int outstandingAfterTheAbort = harness.Pool.Outstanding;
        connection.CompleteHeldWrites();
        await connection.RunEnded();

        // Assert
        aborted.ShouldBeTrue();
        outstandingAfterTheAbort.ShouldBe(1);
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAck);
        connection.Link.ClosedWith.ShouldBeNull();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a write the link does not take is bounded by the write deadline: after 14 seconds the connection
    /// is open, and at the first sweep past 15 the link is aborted, no DISCONNECT is attempted, <c>OnClosed</c>
    /// reports <see cref="MqttCloseReason.WriteTimedOut"/>, and what waited behind the write is released.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Write_StalledForFifteenSeconds_AbortsWithoutADisconnect()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        connection.Deliver(ClientPackets.PingReq);

        // Act
        harness.Advance(TimeSpan.FromSeconds(14));
        bool abortedAt14 = connection.Link.IsAborted;
        ConnectionState stateAt14 = server.StateNow;
        harness.Advance(TimeSpan.FromSeconds(2));
        bool abortedAt16 = connection.Link.IsAborted;
        ConnectionCounts heldAt16 = server.GetCounts();
        harness.AssertBounds();
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.RunEnded();

        // Assert
        abortedAt14.ShouldBeFalse();
        stateAt14.ShouldBe(ConnectionState.Active);
        abortedAt16.ShouldBeTrue();

        // Aborting released nothing: the delivery and the PINGRESP still waited behind the write in progress.
        heldAt16.WaitingDeliveries.ShouldBe(1);
        heldAt16.WaitingControlPackets.ShouldBe(1);
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.ClosedWith.ShouldBeNull();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 WriteTimedOut"]);
        ConnectionCounts afterwards = server.GetCounts();
        afterwards.WaitingDeliveries.ShouldBe(0);
        afterwards.WaitingControlPackets.ShouldBe(0);
        afterwards.OutgoingBytes.ShouldBe(0);
        afterwards.IsWriting.ShouldBeFalse();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the write deadline runs for each write handed to the link, from that write's own start: a write
    /// the link holds for 10 seconds, right after an earlier one it also held for 10, is not aborted.
    /// </summary>
    [Fact]
    public void Write_HeldAfterAnEarlierWriteWasHeld_IsMeasuredFromItsOwnStart()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        harness.Advance(TimeSpan.FromSeconds(10));
        connection.CompleteHeldWrites();

        // Act
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        harness.Advance(TimeSpan.FromSeconds(10));
        bool aborted = connection.Link.IsAborted;
        ConnectionState state = server.StateNow;
        connection.CompleteHeldWrites();

        // Assert
        aborted.ShouldBeFalse();
        state.ShouldBe(ConnectionState.Active);
        connection.TakeWritten().ShouldBe([.. ServerPackets.Delivery("m1"u8), .. ServerPackets.Delivery("m2"u8)]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a write that fails ends the connection with <see cref="MqttCloseReason.ConnectionLost"/>, writes
    /// nothing more, and returns its buffer.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Write_Fails_EndsWithConnectionLostAndReturnsTheBuffer()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.FailWrites(new WebSocketException("The socket is gone."));

        // Act
        MqttSendResult result = server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
        await connection.RunEnded();

        // Assert
        result.ShouldBe(MqttSendResult.Taken);
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.IsAborted.ShouldBeTrue();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        server.GetCounts().IsWriting.ShouldBeFalse();
        server.GetCounts().OutgoingBytes.ShouldBe(0);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a link that throws from its write, instead of returning a failed task, is treated as a write that
    /// failed: the connection ends with <see cref="MqttCloseReason.ConnectionLost"/>, the buffer comes back, and
    /// nothing reaches the host's <c>TrySend</c>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Write_LinkThrows_EndsWithConnectionLostAndReturnsTheBuffer()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.Writing = static _ => throw new ObjectDisposedException("socket");

        // Act
        MqttSendResult result = server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
        await connection.RunEnded();

        // Assert
        result.ShouldBe(MqttSendResult.Taken);
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.IsAborted.ShouldBeTrue();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        server.GetCounts().IsWriting.ShouldBeFalse();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a write that fails later, after the link held it, ends the connection the same way, and that what
    /// waited behind it is released.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Write_FailsAfterBeingHeld_EndsWithConnectionLostAndReleasesWhatWaited()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        connection.Deliver(ClientPackets.PingReq);

        // Act
        connection.CompleteHeldWrites(new WebSocketException("The socket is gone."));
        await connection.RunEnded();

        // Assert
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.IsAborted.ShouldBeTrue();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        ConnectionCounts afterwards = server.GetCounts();
        afterwards.WaitingDeliveries.ShouldBe(0);
        afterwards.WaitingControlPackets.ShouldBe(0);
        afterwards.OutgoingBytes.ShouldBe(0);
        afterwards.IsWriting.ShouldBeFalse();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the sweep's close of a connection with no write in progress goes through the send path like any
    /// other: at the keep-alive deadline DISCONNECT 0x8D is written, then the WebSocket is closed.
    /// </summary>
    [Fact]
    public void Sweep_KeepAliveDeadlineWithNoWriteInProgress_WritesDisconnectThenClosesTheWebSocket()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        harness.Advance(TimeSpan.FromSeconds(46));

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.KeepAliveTimeout));
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        connection.Link.IsAborted.ShouldBeFalse();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Gets the calls of <c>OnClosed</c> the handler recorded, oldest first.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <returns>
    /// The calls as text.
    /// </returns>
    private static List<string> ClosedCalls(ServerHarness harness) =>
        [.. harness.Handler.Calls.Where(static call => call.StartsWith("OnClosed", StringComparison.Ordinal))];
}
