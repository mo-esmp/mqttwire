using System.Net.WebSockets;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the read path after the CONNACK: no buffer while idle, the framing of what arrives, the partial-packet
/// and keep-alive deadlines, the limit on packets other than PUBLISH, and the ends the client causes.
/// </summary>
public sealed class ReadTests
{
    /// <summary>
    /// Proves that a connected connection with nothing to read holds no buffer and waits in the link's zero-byte
    /// wait, not in a read.
    /// </summary>
    [Fact]
    public void Read_Idle_HoldsNoBufferAndWaitsInTheZeroByteWait()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();

        // Act
        TestConnection connection = harness.Connect();

        // Assert
        MqttWireConnection? server = connection.Connection;
        server.ShouldNotBeNull();
        harness.Pool.Outstanding.ShouldBe(0);
        server.GetCounts().HoldsReadBuffer.ShouldBeFalse();
        connection.Link.IsReadPending.ShouldBeTrue();
        connection.Link.PendingReadBuffer.IsEmpty.ShouldBeTrue();
    }

    /// <summary>
    /// Proves that several packets that arrive in one message are each handled, in order.
    /// </summary>
    [Fact]
    public void Read_SeveralPacketsInOneMessage_HandlesThemInOrder()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver([.. ClientPackets.PingReq, .. ClientPackets.PublishQoS0, .. ClientPackets.PingReq]);

        // Assert
        connection.TakeWritten().ShouldBe([.. ClientPackets.PingResp, .. ClientPackets.PingResp]);
        harness.Handler.Calls.Where(static call => call.StartsWith("OnPublish", StringComparison.Ordinal))
            .ShouldBe(["OnPublish c1 a/b hi q0 retain=0"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a packet cut in two at any position is handled exactly once, and that no buffer is held after it.
    /// </summary>
    [Fact]
    public void Read_PacketCutAtEveryPosition_IsHandledOnce()
    {
        for (int cut = 1; cut < ClientPackets.PublishQoS1.Length; cut++)
        {
            // Arrange
            ServerHarness harness = ServerHarness.Create();
            TestConnection connection = harness.Connect();

            // Act
            connection.Deliver(ClientPackets.PublishQoS1.AsSpan(0, cut));
            connection.Deliver(ClientPackets.PublishQoS1.AsSpan(cut));

            // Assert
            connection.TakeWritten().ShouldBe(ClientPackets.PubAck1, $"cut at {cut}");
            harness.Handler.Calls.Count(static call => call.StartsWith("OnPublish", StringComparison.Ordinal))
                .ShouldBe(1, $"cut at {cut}");
            harness.Pool.Outstanding.ShouldBe(0, $"cut at {cut}");
        }
    }

    /// <summary>
    /// Proves that the read buffer goes back to the pool after every packet that is handled.
    /// </summary>
    [Fact]
    public void Read_AfterEveryPacket_ReturnsTheBuffer()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection? server = connection.Connection;
        server.ShouldNotBeNull();
        byte[][] packets = [ClientPackets.PingReq, ClientPackets.PublishQoS0, ClientPackets.PublishQoS1, ClientPackets.PingReq];

        foreach (byte[] packet in packets)
        {
            // Act
            connection.Deliver(packet);

            // Assert
            harness.Pool.Outstanding.ShouldBe(0);
            server.GetCounts().HoldsReadBuffer.ShouldBeFalse();
        }
    }

    /// <summary>
    /// Proves that a packet that stays incomplete is still open after eight seconds from its first byte, and at ten
    /// closes the connection with DISCONNECT 0x82 and <see cref="MqttCloseReason.PartialPacketTimedOut"/>, and that
    /// its buffer comes back.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_PartialPacketForTenSeconds_ClosesWith0x82()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        connection.Deliver(Hex.Parse("32 0A 00"));

        // Act
        harness.Advance(TimeSpan.FromSeconds(8));
        byte[] writtenAt8 = connection.TakeWritten();
        WebSocketCloseStatus? closedAt8 = connection.Link.ClosedWith;
        harness.Advance(TimeSpan.FromSeconds(2));
        byte[] written = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        writtenAt8.ShouldBeEmpty();
        closedAt8.ShouldBeNull();
        written.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Handler.Calls.ShouldContain("OnClosed c1 PartialPacketTimedOut");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a read buffer rented for a wait that ended with nothing to read (a WebSocket frame whose payload
    /// the peer withholds) is not held until the keep-alive deadline: the connection is closed with DISCONNECT 0x82
    /// and <see cref="MqttCloseReason.PartialPacketTimedOut"/> ten seconds after the buffer was rented, and the
    /// buffer comes back when the link ends at the close deadline.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_WaitEndsWithNothingToRead_ClosesWith0x82AtThePartialPacketDeadline()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // The wait after this PINGREQ completes at once, and the read that follows it waits, holding its buffer.
        connection.Link.SkipWaits();
        connection.Deliver(ClientPackets.PingReq);
        byte[] pingResp = connection.TakeWritten();
        bool held = server.GetCounts().HoldsReadBuffer;

        // Act
        harness.Advance(TimeSpan.FromSeconds(8));
        byte[] writtenAt8 = connection.TakeWritten();
        harness.Advance(TimeSpan.FromSeconds(2));
        byte[] writtenAt10 = connection.TakeWritten();
        int outstandingAt10 = harness.Pool.Outstanding;
        harness.Advance(ServerLimits.CloseDeadline);
        await connection.RunEnded();

        // Assert
        pingResp.ShouldBe(ClientPackets.PingResp);
        held.ShouldBeTrue();
        writtenAt8.ShouldBeEmpty();
        writtenAt10.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        outstandingAt10.ShouldBe(1);
        connection.Link.IsAborted.ShouldBeTrue();
        harness.ClosedCalls().ShouldBe(["OnClosed c1 PartialPacketTimedOut"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a client with keep-alive 30 that sends nothing is still open after 44 seconds and is closed with
    /// DISCONNECT 0x8D and <see cref="MqttCloseReason.KeepAliveTimedOut"/> once 45 have passed.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_NoPacketFor45SecondsAtKeepAlive30_ClosesWith0x8D()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        harness.Advance(TimeSpan.FromSeconds(44));
        byte[] writtenAt44 = connection.TakeWritten();
        harness.Advance(TimeSpan.FromSeconds(2));
        byte[] writtenAt46 = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        writtenAt44.ShouldBeEmpty();
        writtenAt46.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.KeepAliveTimeout));
        harness.Handler.Calls.ShouldContain("OnClosed c1 KeepAliveTimedOut");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the keep-alive is measured from the moment the connection becomes Active, not from the CONNECT: with
    /// keep-alive 2 and a host that answers after 8 seconds, the connection is open at the first sweep after the
    /// CONNACK and closed with 0x8D at the first sweep 3 seconds or more after it. The CONNACK's send path is waited
    /// out first: a close that found it still taken would abort instead of writing its DISCONNECT.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_SlowOnConnectWithAShortKeepAlive_MeasuresTheKeepAliveFromTheConnAck()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TaskCompletionSource<MqttConnectResult> answer = new();
        harness.Handler.Connect = (_, _) => new ValueTask<MqttConnectResult>(answer.Task);
        TestConnection connection = harness.Accept();
        connection.Deliver(Hex.Parse("10 0F 00 04 4D 51 54 54 05 02 00 02 00 00 02 63 31"));
        harness.Advance(TimeSpan.FromSeconds(8));
        Task connAckWritten = connection.Link.WhenWrittenAsync(1);
        answer.SetResult(MqttConnectResult.Accept());

        // The link has the CONNACK before the connect phase, on another thread, gives the send path up.
        await connAckWritten.Within("The CONNACK's write");
        await connection.WhenSendPathIdleAsync();
        byte[] connAck = connection.TakeWritten();

        // Act
        harness.Advance(TimeSpan.FromSeconds(2));
        byte[] writtenAt2 = connection.TakeWritten();
        harness.Advance(TimeSpan.FromSeconds(2));
        byte[] writtenAt4 = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        connAck.ShouldBe(ServerPackets.ConnAck);
        writtenAt2.ShouldBeEmpty();
        writtenAt4.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.KeepAliveTimeout));
        harness.Handler.Calls.ShouldContain("OnClosed c1 KeepAliveTimedOut");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a client with keep-alive 30 that sends PINGREQ every 30 seconds stays open, each answered.
    /// </summary>
    [Fact]
    public void Read_PingReqEvery30Seconds_KeepsTheConnectionOpen()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        for (int round = 0; round < 5; round++)
        {
            // Act
            harness.Advance(TimeSpan.FromSeconds(30));
            connection.Deliver(ClientPackets.PingReq);

            // Assert
            connection.TakeWritten().ShouldBe(ClientPackets.PingResp, $"round {round}");
        }

        connection.Run.IsCompleted.ShouldBeFalse();
        connection.Link.ClosedWith.ShouldBeNull();
    }

    /// <summary>
    /// Proves that a client that asked for no keep-alive is held to 60 seconds: it is closed with 0x8D at the sweep at
    /// 90 seconds, and not before.
    /// </summary>
    [Fact]
    public void Read_KeepAliveZero_UsesSixtySeconds()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectKeepAlive0);

        // Act
        harness.Advance(TimeSpan.FromSeconds(88));
        byte[] writtenAt88 = connection.TakeWritten();
        harness.Advance(TimeSpan.FromSeconds(2));
        byte[] writtenAt90 = connection.TakeWritten();

        // Assert
        writtenAt88.ShouldBeEmpty();
        writtenAt90.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.KeepAliveTimeout));
    }

    /// <summary>
    /// Proves that a packet other than PUBLISH longer than 16 KiB closes the connection with DISCONNECT 0x95 from its
    /// fixed header alone, with no buffer larger than the first read's.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_OtherPacketOver16KiB_ClosesWith0x95()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(Hex.Parse("82 81 80 01 00 01 00"));
        await connection.RunEnded();

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.PacketTooLarge));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        harness.Pool.LargestRent.ShouldBeLessThanOrEqualTo(4_096);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that bytes that are not a valid fixed header after the CONNACK close the connection with DISCONNECT
    /// 0x81, WebSocket status 1000 and <see cref="MqttCloseReason.MalformedPacket"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_GarbageAfterConnect_ClosesWith0x81()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(Hex.Parse("FF FF FF FF FF"));
        await connection.RunEnded();

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.MalformedPacket));
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Handler.Calls.ShouldContain("OnClosed c1 MalformedPacket");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a peer that ends the link ends the connection with <see cref="MqttCloseReason.ConnectionLost"/>
    /// and no packet.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_PeerEnds_EndsWithConnectionLost()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        connection.TakeWritten().ShouldBeEmpty();
        harness.Handler.Calls.ShouldContain("OnClosed c1 ConnectionLost");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a read that fails ends the connection with <see cref="MqttCloseReason.ConnectionLost"/> and no
    /// packet.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_ReadFails_EndsWithConnectionLost()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Link.FailReads(new WebSocketException());
        await connection.RunEnded();

        // Assert
        connection.TakeWritten().ShouldBeEmpty();
        harness.Handler.Calls.ShouldContain("OnClosed c1 ConnectionLost");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that what arrives while the connection is closing is read and thrown away: a PUBLISH reaches no
    /// handler, and the connection ends when the link does.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_WhileClosing_DiscardsWhatArrives()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection? server = connection.Connection;
        server.ShouldNotBeNull();
        server.BeginClose(CloseCause.ClosedByHost, MqttReasonCode.AdministrativeAction, reasonString: null);
        byte[] written = connection.TakeWritten();

        // Act
        connection.Deliver(ClientPackets.PublishQoS0);
        bool openAfterDelivery = !connection.Run.IsCompleted;
        int outstandingAfterDelivery = harness.Pool.Outstanding;
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        written.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.AdministrativeAction));
        openAfterDelivery.ShouldBeTrue();
        outstandingAfterDelivery.ShouldBe(0);
        connection.TakeWritten().ShouldBeEmpty();
        harness.Handler.Calls.ShouldNotContain(static call => call.StartsWith("OnPublish", StringComparison.Ordinal));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ClosedByHost");
        harness.Pool.Outstanding.ShouldBe(0);
    }
}
