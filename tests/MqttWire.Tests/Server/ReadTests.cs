using System.Net.WebSockets;
using MqttWire.Server;
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
    /// Proves that a packet that stays incomplete for ten seconds from its first byte closes the connection with
    /// DISCONNECT 0x82 and <see cref="MqttCloseReason.PartialPacketTimedOut"/>, and that its buffer comes back.
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
        harness.Advance(TimeSpan.FromSeconds(10));
        byte[] written = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.Run;

        // Assert
        written.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Handler.Calls.ShouldContain("OnClosed c1 PartialPacketTimedOut");
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
        await connection.Run;

        // Assert
        writtenAt44.ShouldBeEmpty();
        writtenAt46.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.KeepAliveTimeout));
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
        await connection.Run;

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
        await connection.Run;

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
        await connection.Run;

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
        await connection.Run;

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
        server.BeginClose(MqttCloseReason.ClosedByHost, MqttReasonCode.AdministrativeAction, reasonString: null);
        byte[] written = connection.TakeWritten();

        // Act
        connection.Deliver(ClientPackets.PublishQoS0);
        bool openAfterDelivery = !connection.Run.IsCompleted;
        int outstandingAfterDelivery = harness.Pool.Outstanding;
        connection.Link.EndFromPeer();
        await connection.Run;

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
