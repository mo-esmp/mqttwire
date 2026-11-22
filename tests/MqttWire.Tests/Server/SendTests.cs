using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of sending (design 6.6, "Sending: taken at once, written when the socket allows"): <c>TrySend</c> returns
/// at once, one write is in progress at a time, control packets go before waiting deliveries, a payload is copied
/// only when it is small, and the two outgoing limits evict a connection that falls behind.
/// </summary>
public sealed class SendTests
{
    /// <summary>
    /// The topic every test sends to.
    /// </summary>
    private static readonly MqttTopic s_topic = MqttTopic.Create("a/b");

    /// <summary>
    /// What <c>GetCounts</c> reports for a connection that holds nothing and writes nothing.
    /// </summary>
    private static readonly ConnectionCounts s_idle = new(IncomingRecords: 0, DeliveryRecords: 0, IncomingInFlight: 0,
        HoldsRecords: false, WaitingDeliveries: 0,
        WaitingControlPackets: 0, OutgoingBytes: 0, IsWriting: false, HoldsReadBuffer: false);

    /// <summary>
    /// Proves that a message sent while nothing is in progress is written at once, as one PUBLISH at QoS 0, and that
    /// its buffer is back in the pool when <c>TrySend</c> returns.
    /// </summary>
    [Fact]
    public void TrySend_NothingInProgress_WritesAtOnceAndReturnsTheBuffer()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        MqttSendResult result = server.TrySend(s_topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce);

        // Assert
        result.ShouldBe(MqttSendResult.Taken);
        connection.TakeWritten().ShouldBe(Hex.Parse("30 08 00 03 61 2F 62 00 68 69"));
        harness.Pool.Outstanding.ShouldBe(0);
        server.GetCounts().ShouldBe(s_idle);
        harness.AssertBounds();
    }

    /// <summary>
    /// Proves that <c>TrySend</c> returns at once while a write is held by the link, that what it took waits, and
    /// that it is written in the order of the calls once the link takes the write.
    /// </summary>
    [Fact]
    public void TrySend_WriteHeld_ReturnsAtOnceAndWritesInOrderWhenReleased()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();

        // Act
        MqttSendResult first = server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
        MqttSendResult second = server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce);
        MqttSendResult third = server.TrySend(s_topic, "m3"u8.ToArray(), MqttQoS.AtMostOnce);
        int writesWhileHeld = connection.Link.WriteCount;
        ConnectionCounts whileHeld = server.GetCounts();
        harness.AssertBounds();
        connection.CompleteHeldWrites();

        // Assert
        first.ShouldBe(MqttSendResult.Taken);
        second.ShouldBe(MqttSendResult.Taken);
        third.ShouldBe(MqttSendResult.Taken);

        // Only the CONNACK has been written: the first message is with the link, the other two wait.
        writesWhileHeld.ShouldBe(1);
        whileHeld.WaitingDeliveries.ShouldBe(2);
        whileHeld.OutgoingBytes.ShouldBe(6);
        whileHeld.IsWriting.ShouldBeTrue();
        connection.TakeWritten().ShouldBe(
            [.. ServerPackets.Delivery("m1"u8), .. ServerPackets.Delivery("m2"u8), .. ServerPackets.Delivery("m3"u8)]);
        server.GetCounts().ShouldBe(s_idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the queue of waiting deliveries goes back to the server's pool when it is empty again, and that
    /// the same instance then serves another connection.
    /// </summary>
    [Fact]
    public void TrySend_QueueEmptiedAgain_GoesBackToThePool()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect("c1");
        TestConnection second = harness.Connect("c2");
        MqttWireConnection firstServer = first.Connection.ShouldNotBeNull();
        MqttWireConnection secondServer = second.Connection.ShouldNotBeNull();

        // Act
        // The first connection takes the only queue there is, and gives it back when the last delivery leaves it.
        first.Link.HoldWrites();
        firstServer.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
        firstServer.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce);
        first.CompleteHeldWrites();
        WaitingDeliveries pooled = harness.Server.Pools.Waiting.Get();
        int countWhenPooled = pooled.Count;
        long bytesWhenPooled = pooled.PayloadBytes;
        harness.Server.Pools.Waiting.Return(pooled);

        // While the second connection has it, the pool has none to give and creates another.
        second.Link.HoldWrites();
        secondServer.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
        secondServer.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce);
        WaitingDeliveries whileInUse = harness.Server.Pools.Waiting.Get();
        second.CompleteHeldWrites();
        WaitingDeliveries afterwards = harness.Server.Pools.Waiting.Get();

        // Assert
        countWhenPooled.ShouldBe(0);
        bytesWhenPooled.ShouldBe(0);
        whileInUse.ShouldNotBeSameAs(pooled);
        afterwards.ShouldBeSameAs(pooled);
        afterwards.Count.ShouldBe(0);
        second.TakeWritten().ShouldBe([.. ServerPackets.Delivery("m1"u8), .. ServerPackets.Delivery("m2"u8)]);
    }

    /// <summary>
    /// Proves that a control packet produced while a write is in progress is written before the deliveries that
    /// already wait.
    /// </summary>
    [Fact]
    public void TrySend_ControlPacketWaitsBehindAWrite_IsWrittenBeforeWaitingDeliveries()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
        server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce);

        // Act
        connection.Deliver(ClientPackets.PingReq);
        ConnectionCounts whileHeld = server.GetCounts();
        connection.CompleteHeldWrites();

        // Assert
        whileHeld.WaitingControlPackets.ShouldBe(1);
        whileHeld.WaitingDeliveries.ShouldBe(1);
        connection.TakeWritten().ShouldBe(
            [.. ServerPackets.Delivery("m1"u8), .. ClientPackets.PingResp, .. ServerPackets.Delivery("m2"u8)]);
        server.GetCounts().ShouldBe(s_idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that at most one PINGRESP waits: a second PINGREQ behind a write in progress adds nothing.
    /// </summary>
    [Fact]
    public void TrySend_TwoPingReqsWhileAWriteIsHeld_OnePingRespWaits()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);

        // Act
        connection.Deliver(ClientPackets.PingReq);
        connection.Deliver(ClientPackets.PingReq);
        ConnectionCounts whileHeld = server.GetCounts();
        connection.CompleteHeldWrites();

        // Assert
        whileHeld.WaitingControlPackets.ShouldBe(1);
        whileHeld.OutgoingBytes.ShouldBe(4);
        connection.TakeWritten().ShouldBe([.. ServerPackets.Delivery("m1"u8), .. ClientPackets.PingResp]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the control packets that wait behind a write are written together, as one WebSocket message, in
    /// the order they were produced.
    /// </summary>
    [Fact]
    public void TrySend_WaitingControlPackets_AreWrittenTogetherInProducedOrder()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);

        // Act
        connection.Deliver(ServerPackets.BuildUnsubscribe(packetIdentifier: 2, ["a/b"]));
        connection.Deliver(ServerPackets.BuildUnsubscribe(packetIdentifier: 3, ["a/b"]));
        connection.Deliver(ServerPackets.BuildUnsubscribe(packetIdentifier: 4, ["a/b"]));
        ConnectionCounts whileHeld = server.GetCounts();
        connection.CompleteHeldWrites();

        // Assert
        whileHeld.WaitingControlPackets.ShouldBe(3);
        whileHeld.OutgoingBytes.ShouldBe(2 + 18);

        // The CONNACK, the message, and one write that holds the three UNSUBACKs.
        connection.Link.WriteCount.ShouldBe(3);
        connection.TakeWritten().ShouldBe(
        [
            .. ServerPackets.Delivery("m1"u8), .. Hex.Parse("B0 04 00 02 00 00"), .. Hex.Parse("B0 04 00 03 00 00"),
            .. Hex.Parse("B0 04 00 04 00 00"),
        ]);
        server.GetCounts().ShouldBe(s_idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a message whose whole packet is 16,384 bytes is copied behind its header into one rented buffer
    /// and written as one WebSocket message.
    /// </summary>
    [Fact]
    public void TrySend_PacketOf16384Bytes_IsCopiedIntoOneBuffer()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // A fixed header of 3 bytes, the topic's 5, the property length's 1 and 16,375 bytes of payload.
        byte[] payload = ServerPackets.Letters('x', 16_375);
        connection.Link.HoldWrites();

        // Act
        MqttSendResult result = server.TrySend(s_topic, payload, MqttQoS.AtMostOnce);
        ReadOnlyMemory<byte> held = connection.Link.HeldWriteBuffer;
        bool heldIsRented = harness.Pool.IsViewIntoRentedBuffer(held.Span);
        MemoryMarshal.TryGetArray(held, out ArraySegment<byte> heldArray).ShouldBeTrue();
        connection.CompleteHeldWrites();

        // Assert
        result.ShouldBe(MqttSendResult.Taken);
        held.Length.ShouldBe(16_384);
        heldIsRented.ShouldBeTrue();
        heldArray.Array.ShouldNotBeSameAs(payload);
        connection.Link.WriteCount.ShouldBe(2);
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery(payload));
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a large message is written as its header from a rented buffer and then its payload straight from
    /// the host's memory, in pieces of at most 65,536 bytes, so that no rented buffer is larger than that.
    /// </summary>
    [Fact]
    public void TrySend_PayloadOf200000Bytes_IsWrittenFromTheHostsMemoryInPieces()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        byte[] payload = new byte[200_000];
        new Random(8).NextBytes(payload);
        List<ReadOnlyMemory<byte>> writes = [];
        connection.Link.Writing = writes.Add;

        // Act
        MqttSendResult result = server.TrySend(s_topic, payload, MqttQoS.AtMostOnce);

        // Assert
        result.ShouldBe(MqttSendResult.Taken);
        writes.Count.ShouldBe(5);

        // Remaining Length 200,006 in three bytes, the topic and the property length.
        writes[0].ToArray().ShouldBe(Hex.Parse("30 C6 9A 0C 00 03 61 2F 62 00"));
        MemoryMarshal.TryGetArray(writes[0], out ArraySegment<byte> header).ShouldBeTrue();
        header.Array.ShouldNotBeSameAs(payload);
        (int Offset, int Count)[] pieces = [(0, 65_536), (65_536, 65_536), (131_072, 65_536), (196_608, 3_392)];
        for (int i = 0; i < pieces.Length; i++)
        {
            MemoryMarshal.TryGetArray(writes[i + 1], out ArraySegment<byte> piece).ShouldBeTrue();
            piece.Array.ShouldBeSameAs(payload, $"piece {i}");
            piece.Offset.ShouldBe(pieces[i].Offset, $"piece {i}");
            piece.Count.ShouldBe(pieces[i].Count, $"piece {i}");
        }

        connection.TakeWritten().ShouldBe(ServerPackets.Delivery(payload));
        harness.Pool.LargestRent.ShouldBeLessThanOrEqualTo(65_536);
        harness.Pool.Outstanding.ShouldBe(0);
        server.GetCounts().ShouldBe(s_idle);
    }

    /// <summary>
    /// Proves that one payload handed to ten connections is never copied: every payload piece every link is given
    /// points into the host's one array.
    /// </summary>
    [Fact]
    public void TrySend_OnePayloadToTenConnections_IsNeverCopied()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        byte[] payload = ServerPackets.Letters('x', 100_000);
        List<TestConnection> connections = [];
        List<List<ReadOnlyMemory<byte>>> writes = [];
        for (int i = 0; i < 10; i++)
        {
            TestConnection connection = harness.Connect($"c{i}");
            List<ReadOnlyMemory<byte>> written = [];
            connection.Link.Writing = written.Add;
            connections.Add(connection);
            writes.Add(written);
        }

        // Act
        foreach (TestConnection connection in connections)
        {
            connection.Connection.ShouldNotBeNull().TrySend(s_topic, payload, MqttQoS.AtMostOnce)
                .ShouldBe(MqttSendResult.Taken);
        }

        // Assert
        for (int i = 0; i < 10; i++)
        {
            // The header, then 65,536 and 34,464 bytes of the payload.
            writes[i].Count.ShouldBe(3, $"connection {i}");
            MemoryMarshal.TryGetArray(writes[i][1], out ArraySegment<byte> first).ShouldBeTrue();
            MemoryMarshal.TryGetArray(writes[i][2], out ArraySegment<byte> second).ShouldBeTrue();
            first.Array.ShouldBeSameAs(payload, $"connection {i}");
            second.Array.ShouldBeSameAs(payload, $"connection {i}");
            (first.Offset, first.Count).ShouldBe((0, 65_536), $"connection {i}");
            (second.Offset, second.Count).ShouldBe((65_536, 34_464), $"connection {i}");
            connections[i].TakeWritten().ShouldBe(ServerPackets.Delivery(payload), $"connection {i}");
        }

        harness.Pool.LargestRent.ShouldBeLessThanOrEqualTo(65_536);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the header of a PUBLISH with the longest topic a host can create, 65,526 bytes, fits one buffer
    /// of 65,536 bytes, and that with an empty payload nothing follows the header.
    /// </summary>
    [Fact]
    public void TrySend_LongestTopicWithAnEmptyPayload_IsWrittenAsItsHeaderAlone()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        byte[] topic = ServerPackets.Letters('a', 65_526);

        // Act
        MqttSendResult result = server.TrySend(MqttTopic.Create(topic), ReadOnlyMemory<byte>.Empty, MqttQoS.AtMostOnce);

        // Assert
        result.ShouldBe(MqttSendResult.Taken);

        // The CONNACK and the header.
        connection.Link.WriteCount.ShouldBe(2);
        connection.TakeWritten().ShouldBe(
            ServerPackets.BuildPublish(MqttQoS.AtMostOnce, topic, packetIdentifier: 0, properties: [], payload: []));
        harness.Pool.LargestRent.ShouldBeLessThanOrEqualTo(65_536);
        server.GetCounts().ShouldBe(s_idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a connection keeps no reference to a host's payload once the message has been written: a large
    /// payload, written from the host's memory in pieces, can be collected while the connection stays open.
    /// </summary>
    [Fact]
    public void TrySend_LargePayloadWritten_IsNoLongerReferenced()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        WeakReference payload = SendLargePayload(server);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Assert
        payload.IsAlive.ShouldBeFalse();
        server.StateNow.ShouldBe(ConnectionState.Active);
        GC.KeepAlive(connection);
    }

    /// <summary>
    /// Proves that a message with an empty payload is a valid message and is delivered (Review Focus 5).
    /// </summary>
    [Fact]
    public void TrySend_EmptyPayload_IsDelivered()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        MqttSendResult result = server.TrySend(s_topic, ReadOnlyMemory<byte>.Empty, MqttQoS.AtMostOnce);

        // Assert
        result.ShouldBe(MqttSendResult.Taken);
        connection.TakeWritten().ShouldBe(Hex.Parse("30 06 00 03 61 2F 62 00"));
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the default topic, which holds none, and a QoS that is not 0, 1 or 2 throw
    /// <see cref="ArgumentException"/> in the host's code, and that nothing is written or changed (Review Focus 5).
    /// </summary>
    [Fact]
    public void TrySend_DefaultTopic_ThrowsAndWritesNothing()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        byte[] payload = "hi"u8.ToArray();

        // Act
        ArgumentException noTopic =
            Should.Throw<ArgumentException>(() => { _ = server.TrySend(default, payload, MqttQoS.AtMostOnce); });
        ArgumentException noSuchQoS =
            Should.Throw<ArgumentException>(() => { _ = server.TrySend(s_topic, payload, (MqttQoS)3); });

        // Assert
        noTopic.ParamName.ShouldBe("topic");
        noSuchQoS.ParamName.ShouldBe("qos");
        connection.TakeWritten().ShouldBeEmpty();
        server.StateNow.ShouldBe(ConnectionState.Active);
        server.GetCounts().ShouldBe(s_idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a message sent at QoS 2 to a server whose <see cref="MqttWireServerOptions.MaxQoS"/> is 1 goes
    /// out at QoS 1 (Review Focus 5).
    /// </summary>
    [Fact]
    public void TrySend_QoS2WithMaxQoS1_GoesOutAtQoS1()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxQoS = MqttQoS.AtLeastOnce);
        TestConnection connection = harness.Connect(connect: ServerPackets.Connect);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        MqttSendResult result = server.TrySend(s_topic, "hi"u8.ToArray(), MqttQoS.ExactlyOnce);

        // Assert
        result.ShouldBe(MqttSendResult.Taken);
        connection.TakeWritten()[0].ShouldBe((byte)0x32);
    }

    /// <summary>
    /// Proves that one waiting delivery more than <see cref="MqttWireServerOptions.MaxOutgoingMessages"/> evicts the
    /// connection: the message is not taken, the link is aborted because a write is in progress, no DISCONNECT is
    /// written, and <c>OnClosed</c> reports <see cref="MqttCloseReason.OutgoingLimitPassed"/> once the write in
    /// progress has ended and returned its buffer.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_OneMoreThanMaxOutgoingMessages_EvictsWith0x97()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxOutgoingMessages = 3);
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        byte[] payload = "hi"u8.ToArray();
        server.TrySend(s_topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        for (int i = 2; i <= 4; i++)
        {
            server.TrySend(s_topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken, $"message {i}");
            harness.AssertBounds();
        }

        // Act
        MqttSendResult fifth = server.TrySend(s_topic, payload, MqttQoS.AtMostOnce);
        harness.AssertBounds();
        bool abortedAtOnce = connection.Link.IsAborted;
        bool endedWhileWriting = connection.Run.IsCompleted;
        int closedWhileWriting = ClosedCalls(harness).Count;
        MqttSendResult afterwards = server.TrySend(s_topic, payload, MqttQoS.AtMostOnce);
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.Run;

        // Assert
        fifth.ShouldBe(MqttSendResult.Evicted);
        abortedAtOnce.ShouldBeTrue();
        endedWhileWriting.ShouldBeFalse();
        closedWhileWriting.ShouldBe(0);
        afterwards.ShouldBe(MqttSendResult.NotOpen);
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.ClosedWith.ShouldBeNull();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 OutgoingLimitPassed"]);
        server.GetCounts().ShouldBe(s_idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that <see cref="MqttWireServerOptions.MaxOutgoingBytes"/> is a hard bound on the payload bytes on the
    /// way out, the delivery being written included: a message that would pass it evicts the connection.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_WaitingBytesPassMaxOutgoingBytes_EvictsWith0x97()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxOutgoingBytes = 65_536);
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();

        // Act
        MqttSendResult held = server.TrySend(s_topic, new byte[40_000], MqttQoS.AtMostOnce);
        MqttSendResult waiting = server.TrySend(s_topic, new byte[20_000], MqttQoS.AtMostOnce);
        long bytesBefore = server.GetCounts().OutgoingBytes;
        harness.AssertBounds();
        MqttSendResult over = server.TrySend(s_topic, new byte[10_000], MqttQoS.AtMostOnce);
        harness.AssertBounds();
        bool aborted = connection.Link.IsAborted;
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.Run;

        // Assert
        held.ShouldBe(MqttSendResult.Taken);
        waiting.ShouldBe(MqttSendResult.Taken);
        bytesBefore.ShouldBe(60_000);
        over.ShouldBe(MqttSendResult.Evicted);
        aborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 OutgoingLimitPassed"]);
        server.GetCounts().ShouldBe(s_idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a client survives a burst that fits within the two limits: 1,000 messages behind one write in
    /// progress are all taken, and all written, once each and in order, when the link takes the write.
    /// </summary>
    [Fact]
    public void TrySend_BurstWithinBothLimits_IsDeliveredInFull()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        List<byte> expected = [];

        // Act
        for (int i = 0; i <= 1_000; i++)
        {
            byte[] payload = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(payload, i);
            expected.AddRange(ServerPackets.Delivery(payload));
            server.TrySend(s_topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken, $"message {i}");
        }

        ConnectionCounts whileHeld = server.GetCounts();
        harness.AssertBounds();
        connection.CompleteHeldWrites();

        // Assert
        whileHeld.WaitingDeliveries.ShouldBe(1_000);
        whileHeld.OutgoingBytes.ShouldBe(4_004);
        connection.Link.IsAborted.ShouldBeFalse();
        connection.TakeWritten().ShouldBe(expected.ToArray());
        server.StateNow.ShouldBe(ConnectionState.Active);
        server.GetCounts().ShouldBe(s_idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a single message larger than <see cref="MqttWireServerOptions.MaxOutgoingBytes"/> is never sent:
    /// <c>TrySend</c> says "too large" and the connection is closed with DISCONNECT 0x95 and
    /// <see cref="MqttCloseReason.MessageTooLargeToSend"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_SingleMessageOverMaxOutgoingBytes_ReturnsTooLargeAndClosesWith0x95()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        MqttSendResult result = server.TrySend(s_topic, new byte[1_048_577], MqttQoS.AtMostOnce);
        byte[] written = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.Run;

        // Assert
        result.ShouldBe(MqttSendResult.TooLarge);
        written.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.PacketTooLarge));
        ClosedCalls(harness).ShouldBe(["OnClosed c1 MessageTooLargeToSend"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a message whose packet is larger than the Maximum Packet Size the client announced cannot be sent
    /// to it: a packet of exactly that size is delivered, and one over it gets "too large" and closes the connection
    /// with DISCONNECT 0x95.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_PacketOverTheClientsMaximumPacketSize_ReturnsTooLargeAndClosesWith0x95()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectMaxPacketSize4096);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // A fixed header of 3 bytes, the topic's 5, the property length's 1 and 4,087 bytes of payload: 4,096.
        byte[] fits = ServerPackets.Letters('x', 4_087);

        // Act
        MqttSendResult atTheLimit = server.TrySend(s_topic, fits, MqttQoS.AtMostOnce);
        byte[] writtenAtTheLimit = connection.TakeWritten();
        MqttSendResult over = server.TrySend(s_topic, new byte[5_000], MqttQoS.AtMostOnce);
        byte[] written = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.Run;

        // Assert
        atTheLimit.ShouldBe(MqttSendResult.Taken);
        writtenAtTheLimit.ShouldBe(ServerPackets.Delivery(fits));
        over.ShouldBe(MqttSendResult.TooLarge);
        written.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.PacketTooLarge));
        ClosedCalls(harness).ShouldBe(["OnClosed c1 MessageTooLargeToSend"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that <c>TrySend</c> says "not open" for a connection that is closing and for one that is closed, and
    /// that nothing is written.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_ConnectionClosed_ReturnsNotOpen()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.Close(MqttReasonCode.AdministrativeAction);
        connection.TakeWritten();

        // Act
        MqttSendResult whileClosing = server.TrySend(s_topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce);
        connection.Link.EndFromPeer();
        await connection.Run;
        MqttSendResult whenClosed = server.TrySend(s_topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce);

        // Assert
        whileClosing.ShouldBe(MqttSendResult.NotOpen);
        whenClosed.ShouldBe(MqttSendResult.NotOpen);
        server.StateNow.ShouldBe(ConnectionState.Closed);
        connection.TakeWritten().ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host that sends to the publishing connection from inside <c>OnPublish</c> does not deadlock on
    /// the connection's lock, and that both the echo and the PUBACK of the publish go out (Review Focus 1).
    /// </summary>
    [Fact]
    public void TrySend_FromInsideOnPublishToTheSameConnection_SendsTheAckAndTheEcho()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        MqttSendResult? echoed = null;
        harness.Handler.Publish = (publisher, in publish) =>
        {
            echoed = publisher.TrySend(MqttTopic.Create(publish.Topic), publish.Payload.ToArray(), MqttQoS.AtMostOnce);
            return MqttPublishResult.Accepted;
        };
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ClientPackets.PublishQoS1);

        // Assert
        echoed.ShouldBe(MqttSendResult.Taken);

        // The echo was sent during OnPublish, the PUBACK when OnPublish had answered.
        connection.TakeWritten().ShouldBe([.. ClientPackets.PublishQoS0, .. ClientPackets.PubAck1]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host that sends from inside <c>OnPublish</c> while a write is in progress gets both taken, and
    /// that the PUBACK, a control packet, is written before the echo, a delivery.
    /// </summary>
    [Fact]
    public void TrySend_FromInsideOnPublishWhileAWriteIsHeld_WritesTheAckBeforeTheEcho()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        MqttSendResult? echoed = null;
        harness.Handler.Publish = (publisher, in publish) =>
        {
            echoed = publisher.TrySend(MqttTopic.Create(publish.Topic), publish.Payload.ToArray(), MqttQoS.AtMostOnce);
            return MqttPublishResult.Accepted;
        };
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);

        // Act
        connection.Deliver(ClientPackets.PublishQoS1);
        connection.CompleteHeldWrites();

        // Assert
        echoed.ShouldBe(MqttSendResult.Taken);
        connection.TakeWritten().ShouldBe(
            [.. ServerPackets.Delivery("m1"u8), .. ClientPackets.PubAck1, .. ClientPackets.PublishQoS0]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that more than 1,024 waiting control packets evict the connection: 1,024 UNSUBACKs wait behind a write
    /// in progress, and the next one aborts the link and ends the connection with
    /// <see cref="MqttCloseReason.OutgoingLimitPassed"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_1025ControlPacketsWaiting_Evicts()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
        for (int i = 1; i <= 1_024; i++)
        {
            connection.Deliver(ServerPackets.BuildUnsubscribe((ushort)i, ["a/b"]));
        }

        int waiting = server.GetCounts().WaitingControlPackets;
        bool abortedBefore = connection.Link.IsAborted;

        // Act
        connection.Deliver(ServerPackets.BuildUnsubscribe(packetIdentifier: 1_025, ["a/b"]));
        bool aborted = connection.Link.IsAborted;
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.Run;

        // Assert
        waiting.ShouldBe(1_024);
        abortedBefore.ShouldBeFalse();
        aborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 OutgoingLimitPassed"]);
        server.GetCounts().ShouldBe(s_idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that many small valid packets from a client that reads nothing never make the connection hold more
    /// than its bounds: behind a write in progress, 2,000 PINGREQs and QoS 0 publishes leave one PINGRESP waiting, and
    /// the bounds hold after every one of them.
    /// </summary>
    [Fact]
    public void TrySend_ManyValidPacketsFromAClientThatReadsNothing_StaysWithinTheBounds()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);

        // Act
        for (int i = 0; i < 1_000; i++)
        {
            // Each delivery asserts the bounds.
            connection.Deliver(ClientPackets.PingReq);
            connection.Deliver(ClientPackets.PublishQoS0);
        }

        ConnectionCounts whileHeld = server.GetCounts();
        connection.CompleteHeldWrites();

        // Assert
        whileHeld.WaitingControlPackets.ShouldBe(1);
        whileHeld.WaitingDeliveries.ShouldBe(0);
        whileHeld.OutgoingBytes.ShouldBe(4);
        connection.Link.IsAborted.ShouldBeFalse();
        connection.TakeWritten().ShouldBe([.. ServerPackets.Delivery("m1"u8), .. ClientPackets.PingResp]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a message a host sends between a connection's admission and the write of its CONNACK is taken and
    /// waits behind the CONNACK. The window is reached deterministically: the second connection replaces a first one,
    /// whose DISCONNECT is written between the second's admission and its CONNACK; when that DISCONNECT's buffer goes
    /// back to the pool, the pool's return hook sends to the second connection.
    /// </summary>
    [Fact]
    public void TrySend_BetweenTheAdmissionAndTheConnAck_WaitsBehindTheConnAck()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection first = harness.Connect();
        MqttSendResult? sent = null;
        ConnectionCounts whenSent = default;
        harness.Handler.Admit = (admitted, existing) =>
        {
            if (existing is not null)
            {
                harness.Pool.Returning = _ =>
                {
                    harness.Pool.Returning = null;
                    sent = admitted.TrySend(s_topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce);
                    whenSent = admitted.GetCounts();
                };
            }

            return MqttAdmitResult.Accepted;
        };
        TestConnection second = harness.Accept();

        // Act
        second.Deliver(ServerPackets.Connect);

        // Assert
        sent.ShouldBe(MqttSendResult.Taken);
        whenSent.IsWriting.ShouldBeTrue();
        whenSent.WaitingDeliveries.ShouldBe(1);
        second.TakeWritten().ShouldBe([.. ServerPackets.ConnAck, .. ClientPackets.PublishQoS0]);
        first.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.SessionTakenOver));
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Sends a payload of 200,000 bytes that only this method holds, in a frame of its own so that no local of the
    /// test keeps the payload alive.
    /// </summary>
    /// <param name="server">
    /// The connection to send to.
    /// </param>
    /// <returns>
    /// A weak reference to the payload.
    /// </returns>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference SendLargePayload(MqttWireConnection server)
    {
        byte[] payload = new byte[200_000];
        server.TrySend(s_topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        return new WeakReference(payload);
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
