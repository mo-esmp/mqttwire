using System.Buffers;
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
        MqttSendResult result = server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce);

        // Assert
        result.ShouldBe(MqttSendResult.Taken);
        connection.TakeWritten().ShouldBe(Hex.Parse("30 08 00 03 61 2F 62 00 68 69"));
        harness.Pool.Outstanding.ShouldBe(0);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
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
        MqttSendResult first = server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
        MqttSendResult second = server.TrySend(ServerHarness.Topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce);
        MqttSendResult third = server.TrySend(ServerHarness.Topic, "m3"u8.ToArray(), MqttQoS.AtMostOnce);
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

        // Each message is counted with its topic, "a/b", and its payload of two bytes.
        whileHeld.OutgoingBytes.ShouldBe(3 * (3 + 2));
        whileHeld.IsWriting.ShouldBeTrue();
        connection.TakeWritten().ShouldBe(
            [.. ServerPackets.Delivery("m1"u8), .. ServerPackets.Delivery("m2"u8), .. ServerPackets.Delivery("m3"u8)]);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
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
        firstServer.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
        firstServer.TrySend(ServerHarness.Topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce);
        first.CompleteHeldWrites();
        WaitingDeliveries pooled = harness.Server.Pools.Waiting.Get();
        int countWhenPooled = pooled.Count;
        harness.Server.Pools.Waiting.Return(pooled);

        // While the second connection has it, the pool has none to give and creates another.
        second.Link.HoldWrites();
        secondServer.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
        secondServer.TrySend(ServerHarness.Topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce);
        WaitingDeliveries whileInUse = harness.Server.Pools.Waiting.Get();
        second.CompleteHeldWrites();
        WaitingDeliveries afterwards = harness.Server.Pools.Waiting.Get();

        // Assert
        countWhenPooled.ShouldBe(0);
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
        server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
        server.TrySend(ServerHarness.Topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce);

        // Act
        connection.Deliver(ClientPackets.PingReq);
        ConnectionCounts whileHeld = server.GetCounts();
        connection.CompleteHeldWrites();

        // Assert
        whileHeld.WaitingControlPackets.ShouldBe(1);
        whileHeld.WaitingDeliveries.ShouldBe(1);
        connection.TakeWritten().ShouldBe(
            [.. ServerPackets.Delivery("m1"u8), .. ClientPackets.PingResp, .. ServerPackets.Delivery("m2"u8)]);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
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
        server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);

        // Act
        connection.Deliver(ClientPackets.PingReq);
        connection.Deliver(ClientPackets.PingReq);
        ConnectionCounts whileHeld = server.GetCounts();
        connection.CompleteHeldWrites();

        // Assert
        whileHeld.WaitingControlPackets.ShouldBe(1);

        // The message being written, its topic and its payload, and the two bytes of the PINGRESP.
        whileHeld.OutgoingBytes.ShouldBe((3 + 2) + 2);
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
        server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);

        // Act
        connection.Deliver(ServerPackets.BuildUnsubscribe(packetIdentifier: 2, ["a/b"]));
        connection.Deliver(ServerPackets.BuildUnsubscribe(packetIdentifier: 3, ["a/b"]));
        connection.Deliver(ServerPackets.BuildUnsubscribe(packetIdentifier: 4, ["a/b"]));
        ConnectionCounts whileHeld = server.GetCounts();
        connection.CompleteHeldWrites();

        // Assert
        whileHeld.WaitingControlPackets.ShouldBe(3);

        // The message being written, its topic and its payload, and three UNSUBACKs of six bytes.
        whileHeld.OutgoingBytes.ShouldBe((3 + 2) + 18);

        // The CONNACK, the message, and one write that holds the three UNSUBACKs.
        connection.Link.WriteCount.ShouldBe(3);
        connection.TakeWritten().ShouldBe(
        [
            .. ServerPackets.Delivery("m1"u8), .. ServerPackets.UnsubAck, .. ServerPackets.UnsubAckFor(3),
            .. ServerPackets.UnsubAckFor(4),
        ]);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
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
        MqttSendResult result = server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtMostOnce);
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
        MqttSendResult result = server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtMostOnce);

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
        server.GetCounts().ShouldBe(ServerHarness.Idle);
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
            connection.Connection.ShouldNotBeNull().TrySend(ServerHarness.Topic, payload, MqttQoS.AtMostOnce)
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
        server.GetCounts().ShouldBe(ServerHarness.Idle);
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
        MqttSendResult result = server.TrySend(ServerHarness.Topic, ReadOnlyMemory<byte>.Empty, MqttQoS.AtMostOnce);

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
            Should.Throw<ArgumentException>(() => { _ = server.TrySend(ServerHarness.Topic, payload, (MqttQoS)3); });

        // Assert
        noTopic.ParamName.ShouldBe("topic");
        noSuchQoS.ParamName.ShouldBe("qos");
        connection.TakeWritten().ShouldBeEmpty();
        server.StateNow.ShouldBe(ConnectionState.Active);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
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
        MqttSendResult result = server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.ExactlyOnce);

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
        server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        for (int i = 2; i <= 4; i++)
        {
            server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken, $"message {i}");
            harness.AssertBounds();
        }

        // Act
        MqttSendResult fifth = server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtMostOnce);
        harness.AssertBounds();
        bool abortedAtOnce = connection.Link.IsAborted;
        bool endedWhileWriting = connection.Run.IsCompleted;
        int closedWhileWriting = harness.ClosedCalls().Count;
        MqttSendResult afterwards = server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtMostOnce);
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.RunEnded();

        // Assert
        fifth.ShouldBe(MqttSendResult.Evicted);
        abortedAtOnce.ShouldBeTrue();
        endedWhileWriting.ShouldBeFalse();
        closedWhileWriting.ShouldBe(0);
        afterwards.ShouldBe(MqttSendResult.NotOpen);
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.ClosedWith.ShouldBeNull();
        harness.ClosedCalls().ShouldBe(["OnClosed c1 OutgoingLimitPassed"]);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that <see cref="MqttWireServerOptions.MaxOutgoingBytes"/> is a hard bound on the bytes on the way out,
    /// topics and payloads, the delivery being written included: a message that would pass it evicts the connection.
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
        MqttSendResult held = server.TrySend(ServerHarness.Topic, new byte[40_000], MqttQoS.AtMostOnce);
        MqttSendResult waiting = server.TrySend(ServerHarness.Topic, new byte[20_000], MqttQoS.AtMostOnce);
        long bytesBefore = server.GetCounts().OutgoingBytes;
        harness.AssertBounds();
        MqttSendResult over = server.TrySend(ServerHarness.Topic, new byte[10_000], MqttQoS.AtMostOnce);
        harness.AssertBounds();
        bool aborted = connection.Link.IsAborted;
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.RunEnded();

        // Assert
        held.ShouldBe(MqttSendResult.Taken);
        waiting.ShouldBe(MqttSendResult.Taken);
        bytesBefore.ShouldBe(40_003 + 20_003);
        over.ShouldBe(MqttSendResult.Evicted);
        aborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        harness.ClosedCalls().ShouldBe(["OnClosed c1 OutgoingLimitPassed"]);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that <see cref="MqttWireServerOptions.MaxOutgoingBytes"/> counts the topic of a delivery as well as its
    /// payload, because a waiting delivery keeps both alive: 32 messages with empty payloads, each to a topic of its
    /// own of 65,522 bytes, behind a held write at the default limit of 1,048,576. Sixteen of them are 1,048,352
    /// bytes, so exactly 16 are taken (the one being written and 15 that wait); the 17th would make 1,113,874 and
    /// evicts the connection, and nothing is taken after it. They used to be taken all 32, 2,096,704 bytes held
    /// against a count of 0.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_DistinctLongTopicsWithEmptyPayloadsBehindAHeldWrite_EvictsAtTheSeventeenth()
    {
        // Arrange
        const int TopicBytes = 65_522;
        const int Sends = 32;
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        MqttTopic[] topics = new MqttTopic[Sends];
        for (int i = 0; i < Sends; i++)
        {
            byte[] utf8 = ServerPackets.Letters('t', TopicBytes);
            utf8[0] = (byte)('A' + i);
            topics[i] = MqttTopic.Create(utf8);
        }

        connection.Link.HoldWrites();
        MqttSendResult[] results = new MqttSendResult[Sends];
        ConnectionCounts atSixteen = default;

        // Act
        for (int i = 0; i < Sends; i++)
        {
            results[i] = server.TrySend(topics[i], ReadOnlyMemory<byte>.Empty, MqttQoS.AtMostOnce);
            harness.AssertBounds();
            if (i == 15)
            {
                atSixteen = server.GetCounts();
            }
        }

        bool aborted = connection.Link.IsAborted;
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.RunEnded();

        // Assert
        results[..16].ShouldAllBe(static result => result == MqttSendResult.Taken);
        atSixteen.OutgoingBytes.ShouldBe(16 * TopicBytes);
        atSixteen.OutgoingBytes.ShouldBe(1_048_352);
        atSixteen.WaitingDeliveries.ShouldBe(15);
        results[16].ShouldBe(MqttSendResult.Evicted);
        results[17..].ShouldAllBe(static result => result == MqttSendResult.NotOpen);
        aborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        harness.ClosedCalls().ShouldBe(["OnClosed c1 OutgoingLimitPassed"]);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a topic shared by the deliveries that wait is counted once for each of them, with its payload,
    /// from the moment a delivery is taken until its write has ended, and that the count is 0 again after the drain.
    /// </summary>
    [Fact]
    public void TrySend_OneTopicForEveryWaitingDelivery_IsCountedForEachAndReleasedByTheDrain()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        MqttTopic topic = MqttTopic.Create("sensors/kitchen/temperature");
        connection.Link.HoldWrites();

        // Act
        server.TrySend(topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        long written = server.GetCounts().OutgoingBytes;
        server.TrySend(topic, "m2"u8.ToArray(), MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(topic, ReadOnlyMemory<byte>.Empty, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        ConnectionCounts whileHeld = server.GetCounts();
        harness.AssertBounds();
        connection.CompleteHeldWrites();

        // Assert: the topic is 27 bytes.
        written.ShouldBe(27 + 2);
        whileHeld.WaitingDeliveries.ShouldBe(2);
        whileHeld.OutgoingBytes.ShouldBe((27 + 2) + (27 + 2) + 27);
        server.GetCounts().OutgoingBytes.ShouldBe(0);
        server.GetCounts().WaitingDeliveries.ShouldBe(0);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that "too large" for <see cref="MqttWireServerOptions.MaxOutgoingBytes"/> is the topic and the payload
    /// together: behind a topic of 65,526 bytes and a limit of 65,536, a payload of 10 bytes is sent, the count going
    /// back to 0, and one of 11 gets "too large" and closes the connection with DISCONNECT 0x95.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_LongTopicAndPayloadTogetherOverMaxOutgoingBytes_ReturnsTooLargeAndClosesWith0x95()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxOutgoingBytes = 65_536);
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        MqttTopic topic = MqttTopic.Create(ServerPackets.Letters('t', ServerLimits.MaxTopicBytes));

        // Act
        MqttSendResult atTheLimit = server.TrySend(topic, new byte[10], MqttQoS.AtMostOnce);
        int writtenAtTheLimit = connection.TakeWritten().Length;
        ConnectionCounts afterTheFirst = server.GetCounts();
        MqttSendResult over = server.TrySend(topic, new byte[11], MqttQoS.AtMostOnce);
        byte[] written = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert: a fixed header of 4 bytes, the topic with its length, the property length and the payload.
        atTheLimit.ShouldBe(MqttSendResult.Taken);
        writtenAtTheLimit.ShouldBe(4 + 2 + ServerLimits.MaxTopicBytes + 1 + 10);
        afterTheFirst.ShouldBe(ServerHarness.Idle);
        over.ShouldBe(MqttSendResult.TooLarge);
        written.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.PacketTooLarge));
        harness.ClosedCalls().ShouldBe(["OnClosed c1 MessageTooLargeToSend"]);
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
            server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken, $"message {i}");
        }

        ConnectionCounts whileHeld = server.GetCounts();
        harness.AssertBounds();
        connection.CompleteHeldWrites();

        // Assert
        whileHeld.WaitingDeliveries.ShouldBe(1_000);
        whileHeld.OutgoingBytes.ShouldBe(1_001 * (3 + 4));
        connection.Link.IsAborted.ShouldBeFalse();
        connection.TakeWritten().ShouldBe(expected.ToArray());
        server.StateNow.ShouldBe(ConnectionState.Active);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
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
        MqttSendResult result = server.TrySend(ServerHarness.Topic, new byte[1_048_577], MqttQoS.AtMostOnce);
        byte[] written = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        result.ShouldBe(MqttSendResult.TooLarge);
        written.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.PacketTooLarge));
        harness.ClosedCalls().ShouldBe(["OnClosed c1 MessageTooLargeToSend"]);
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
        MqttSendResult atTheLimit = server.TrySend(ServerHarness.Topic, fits, MqttQoS.AtMostOnce);
        byte[] writtenAtTheLimit = connection.TakeWritten();
        MqttSendResult over = server.TrySend(ServerHarness.Topic, new byte[5_000], MqttQoS.AtMostOnce);
        byte[] written = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        atTheLimit.ShouldBe(MqttSendResult.Taken);
        writtenAtTheLimit.ShouldBe(ServerPackets.Delivery(fits));
        over.ShouldBe(MqttSendResult.TooLarge);
        written.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.PacketTooLarge));
        harness.ClosedCalls().ShouldBe(["OnClosed c1 MessageTooLargeToSend"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that "too large" is decided from the packet the message would really be: a PUBLISH whose Remaining
    /// Length, computed from the real topic length, the packet identifier and the property length, is exactly the
    /// 268,435,455 MQTT can carry is sendable, and one byte more is not. Decided on the sizing function itself, so
    /// that no payload of 256 MiB is allocated.
    /// </summary>
    /// <param name="topicLength">
    /// The topic's length in UTF-8 bytes.
    /// </param>
    /// <param name="qos">
    /// The QoS, which decides whether the packet carries an identifier.
    /// </param>
    /// <param name="payloadLength">
    /// The payload's length.
    /// </param>
    /// <param name="tooLarge">
    /// Whether the message is too large to send.
    /// </param>
    [Theory]
    [InlineData(1, MqttQoS.AtMostOnce, 268_435_451, false)]
    [InlineData(1, MqttQoS.AtMostOnce, 268_435_452, true)]
    [InlineData(1, MqttQoS.AtLeastOnce, 268_435_449, false)]
    [InlineData(1, MqttQoS.AtLeastOnce, 268_435_450, true)]
    [InlineData(65_526, MqttQoS.ExactlyOnce, 268_369_924, false)]
    [InlineData(65_526, MqttQoS.ExactlyOnce, 268_369_925, true)]
    [InlineData(65_526, MqttQoS.AtMostOnce, 268_369_926, false)]
    [InlineData(65_526, MqttQoS.AtMostOnce, 268_369_927, true)]
    [InlineData(1, MqttQoS.AtMostOnce, int.MaxValue, true)]
    public void IsTooLargeToSend_RemainingLengthAtTheLargestMqttCarries_IsSendableAndOneMoreIsNot(int topicLength,
        MqttQoS qos, int payloadLength, bool tooLarge)
    {
        // Arrange
        const int NoLimit = int.MaxValue;

        // Act
        bool result = MqttWireConnection.IsTooLargeToSend(topicLength, payloadLength, qos,
            maxOutgoingBytes: NoLimit, clientMaxPacketSize: NoLimit);

        // Assert
        result.ShouldBe(tooLarge);
    }

    /// <summary>
    /// Proves the case of the review: with <see cref="MqttWireServerOptions.MaxOutgoingBytes"/> at 256 MiB, a
    /// payload of 268,369,920 bytes behind the topic "t" at QoS 0, whose Remaining Length of 268,369,924 is within
    /// what MQTT carries, is not too large; it used to be refused by a ceiling that assumed the longest topic.
    /// </summary>
    [Fact]
    public void IsTooLargeToSend_PayloadOf268369920BytesBehindAOneByteTopic_IsSendable()
    {
        // Arrange
        const int MaxOutgoingBytes = 256 * 1024 * 1024;
        const int PayloadLength = 268_369_920;

        // Act
        bool result = MqttWireConnection.IsTooLargeToSend(topicLength: 1, PayloadLength, MqttQoS.AtMostOnce,
            MaxOutgoingBytes, clientMaxPacketSize: int.MaxValue);

        // Assert
        result.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that the sizing function holds a message to the client's Maximum Packet Size by the whole packet, its
    /// fixed header included, and to <see cref="MqttWireServerOptions.MaxOutgoingBytes"/> by its topic and its
    /// payload together, which is what a waiting delivery keeps alive.
    /// </summary>
    /// <param name="topicLength">
    /// The topic's length in UTF-8 bytes, at QoS 0.
    /// </param>
    /// <param name="payloadLength">
    /// The payload's length.
    /// </param>
    /// <param name="maxOutgoingBytes">
    /// The limit of the options.
    /// </param>
    /// <param name="clientMaxPacketSize">
    /// The Maximum Packet Size the client announced.
    /// </param>
    /// <param name="tooLarge">
    /// Whether the message is too large to send.
    /// </param>
    [Theory]
    [InlineData(3, 4_087, 1_048_576, 4_096, false)]
    [InlineData(3, 4_088, 1_048_576, 4_096, true)]
    [InlineData(3, 121, 1_048_576, 129, false)]
    [InlineData(3, 122, 1_048_576, 129, true)]
    [InlineData(3, 65_533, 65_536, int.MaxValue, false)]
    [InlineData(3, 65_534, 65_536, int.MaxValue, true)]
    [InlineData(65_526, 10, 65_536, int.MaxValue, false)]
    [InlineData(65_526, 11, 65_536, int.MaxValue, true)]
    [InlineData(65_526, 982_024, 1_048_576, int.MaxValue, false)]
    [InlineData(65_526, 983_050, 1_048_576, int.MaxValue, false)]
    [InlineData(65_526, 983_051, 1_048_576, int.MaxValue, true)]
    [InlineData(65_526, int.MaxValue, int.MaxValue, int.MaxValue, true)]
    public void IsTooLargeToSend_ClientsMaximumPacketSizeAndMaxOutgoingBytes_AreBothHeld(int topicLength,
        int payloadLength, int maxOutgoingBytes, int clientMaxPacketSize, bool tooLarge)
    {
        // Act
        bool result = MqttWireConnection.IsTooLargeToSend(topicLength, payloadLength, MqttQoS.AtMostOnce,
            maxOutgoingBytes, clientMaxPacketSize);

        // Assert
        result.ShouldBe(tooLarge);
    }

    /// <summary>
    /// Proves the case of the review end to end, without the 256 MiB: a payload of 268,369,920 bytes, of which only
    /// the length exists, behind the topic "t" at QoS 0 with <see cref="MqttWireServerOptions.MaxOutgoingBytes"/> at
    /// 256 MiB is taken, and the header handed to the link carries the Remaining Length 268,369,924.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_PayloadOf268369920BytesWithMaxOutgoingBytesOf256MiB_IsTaken()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxOutgoingBytes = 256 * 1024 * 1024);
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        using LengthOnlyMemory payload = new();
        connection.Link.HoldWrites();

        // Act
        MqttSendResult result = server.TrySend(MqttTopic.Create("t"), payload.Claim(268_369_920), MqttQoS.AtMostOnce);
        byte[] header = connection.Link.HeldWriteBuffer.ToArray();
        harness.AssertBounds();
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.RunEnded();

        // Assert: the Remaining Length 268,369,924 is 84 80 FC 7F as a variable byte integer.
        result.ShouldBe(MqttSendResult.Taken);
        header.ShouldBe(Hex.Parse("30 84 80 FC 7F 00 01 74 00"));
        payload.Reads.ShouldBe(0);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
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
        MqttSendResult whileClosing = server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce);
        connection.Link.EndFromPeer();
        await connection.RunEnded();
        MqttSendResult whenClosed = server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce);

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
        server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);

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
        server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
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
        await connection.RunEnded();

        // Assert
        waiting.ShouldBe(1_024);
        abortedBefore.ShouldBeFalse();
        aborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        harness.ClosedCalls().ShouldBe(["OnClosed c1 OutgoingLimitPassed"]);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
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
        server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);

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
        whileHeld.OutgoingBytes.ShouldBe((3 + 2) + 2);
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
                    sent = admitted.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce);
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
    /// Proves that answers whose encoded bytes would take the one control buffer past 65,536 evict the connection,
    /// long before 1,024 packets wait: behind a held write, sixteen SUBACKs of 4,006 bytes wait, and the seventeenth
    /// aborts the link and ends the connection with <see cref="MqttCloseReason.OutgoingLimitPassed"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_WaitingControlBytesPass64KiB_Evicts()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);

        // Each SUBSCRIBE of 4,000 filters is answered with a SUBACK of 4,006 bytes: sixteen take 64,096.
        for (int i = 1; i <= 16; i++)
        {
            connection.Deliver(SubscribeOf((ushort)i, filters: 4_000));
        }

        ConnectionCounts before = server.GetCounts();
        bool abortedBefore = connection.Link.IsAborted;

        // Act
        connection.Deliver(SubscribeOf(packetIdentifier: 17, filters: 4_000));
        bool aborted = connection.Link.IsAborted;
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.RunEnded();

        // Assert
        before.WaitingControlPackets.ShouldBe(16);
        before.OutgoingBytes.ShouldBe((3 + 2) + 64_096);
        abortedBefore.ShouldBeFalse();
        aborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        harness.ClosedCalls().ShouldBe(["OnClosed c1 OutgoingLimitPassed"]);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the bytes of waiting answers count against <see cref="MqttWireServerOptions.MaxOutgoingBytes"/>
    /// with the delivery being written: with 65,536 allowed and a delivery of 60,003 bytes held (its topic and a
    /// payload of 60,000), a SUBACK of 3,006 bytes waits, and a second, which would make 66,015, evicts the
    /// connection.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_WaitingControlBytesPassMaxOutgoingBytes_Evicts()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxOutgoingBytes = 65_536);
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(ServerHarness.Topic, new byte[60_000], MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        connection.Deliver(SubscribeOf(packetIdentifier: 1, filters: 3_000));
        ConnectionCounts before = server.GetCounts();
        bool abortedBefore = connection.Link.IsAborted;

        // Act
        connection.Deliver(SubscribeOf(packetIdentifier: 2, filters: 3_000));
        bool aborted = connection.Link.IsAborted;
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.RunEnded();

        // Assert
        before.WaitingControlPackets.ShouldBe(1);
        before.OutgoingBytes.ShouldBe(60_003 + 3_006);
        abortedBefore.ShouldBeFalse();
        aborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        harness.ClosedCalls().ShouldBe(["OnClosed c1 OutgoingLimitPassed"]);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the pieces of one PUBLISH count as one write towards the writes a caller makes on its own thread:
    /// a payload of 1 MiB, a header and sixteen pieces, is written whole before <c>TrySend</c> returns, with no hand-over
    /// to the thread pool, and the send path is idle on return.
    /// </summary>
    [Fact]
    public void TrySend_PayloadOfOneMebibyte_IsWrittenOnTheCallersThreadWithoutAHandOver()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxOutgoingBytes = 2_097_152);
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        byte[] payload = new byte[1_048_576];
        new Random(10).NextBytes(payload);

        // Act
        MqttSendResult result = server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtMostOnce);
        int writes = connection.Link.WriteCount;
        ConnectionCounts onReturn = server.GetCounts();

        // Assert
        result.ShouldBe(MqttSendResult.Taken);

        // The CONNACK, the header and sixteen pieces of 65,536 bytes.
        writes.ShouldBe(18);
        onReturn.ShouldBe(ServerHarness.Idle);
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery(payload));
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the hand-over to the thread pool still happens when it is needed: a first message whose write adds
    /// seventeen more behind itself is written with fifteen of them on the caller's thread, sixteen writes in all, and
    /// the last two are handed to the thread pool once; everything is written once and in order.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_SeventeenWaitingMessages_HandsOverOnceAndWritesAllInOrder()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Set in the test's own call and nowhere else: the thread pool runs what is handed to it without the caller's
        // execution context, whichever thread it uses, so there the value is false.
        AsyncLocal<bool> insideTheCall = new() { Value = true };
        int writes = 0;
        int writesInsideTheCall = 0;
        connection.Link.Writing = _ =>
        {
            // Runs under the send path, one write at a time. The first write adds the seventeen behind itself.
            if (++writes == 1)
            {
                for (int i = 1; i <= 17; i++)
                {
                    server.TrySend(ServerHarness.Topic, new[] { (byte)i }, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
                }
            }

            if (insideTheCall.Value)
            {
                writesInsideTheCall++;
            }
        };
        List<byte> expected = [];
        for (int i = 0; i <= 17; i++)
        {
            expected.AddRange(ServerPackets.Delivery([(byte)i]));
        }

        // Act
        MqttSendResult first = server.TrySend(ServerHarness.Topic, new byte[1], MqttQoS.AtMostOnce);

        // The CONNACK, the first message and the seventeen.
        await connection.Link.WhenWrittenAsync(count: 19);
        await harness.Pool.WhenNoneOutstandingAsync();
        await connection.WhenSendPathIdleAsync();

        // Assert
        first.ShouldBe(MqttSendResult.Taken);
        writes.ShouldBe(18);
        writesInsideTheCall.ShouldBe(16);
        connection.TakeWritten().ShouldBe(expected.ToArray());
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.AssertBounds();
    }

    /// <summary>
    /// Builds a SUBSCRIBE of many one-letter topic filters at QoS 0, whose SUBACK is as many reason codes long.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier.
    /// </param>
    /// <param name="filters">
    /// How many topic filters; at most 4,000, so the SUBSCRIBE stays a control packet the server reads.
    /// </param>
    /// <returns>
    /// The packet's bytes.
    /// </returns>
    private static byte[] SubscribeOf(ushort packetIdentifier, int filters) =>
        ServerPackets.BuildSubscribe(packetIdentifier, properties: [], Enumerable.Repeat(("a", (byte)0), filters));

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
        server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        return new WeakReference(payload);
    }

    /// <summary>
    /// Memory of which only the length exists, for a test that sends a payload too large to allocate: the server may
    /// measure and slice it, and hand it to a link that holds the write, but nothing may read it.
    /// </summary>
    private sealed class LengthOnlyMemory : MemoryManager<byte>
    {
        /// <summary>
        /// Gets the number of times the bytes were asked for; every one of them threw.
        /// </summary>
        public int Reads { get; private set; }

        /// <summary>
        /// Gets memory of a length, with no bytes behind it.
        /// </summary>
        /// <param name="length">
        /// The length the memory reports.
        /// </param>
        /// <returns>
        /// The memory.
        /// </returns>
        public Memory<byte> Claim(int length) => CreateMemory(length);

        /// <summary>
        /// Counts the read and refuses it: there are no bytes.
        /// </summary>
        /// <returns>
        /// Nothing.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Always.
        /// </exception>
        public override Span<byte> GetSpan()
        {
            Reads++;
            throw new InvalidOperationException("The memory has a length and no bytes.");
        }

        /// <summary>
        /// Counts the read and refuses it, as <see cref="GetSpan"/> does.
        /// </summary>
        /// <param name="elementIndex">
        /// Not used.
        /// </param>
        /// <returns>
        /// Nothing.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Always.
        /// </exception>
        public override MemoryHandle Pin(int elementIndex = 0)
        {
            Reads++;
            throw new InvalidOperationException("The memory has a length and no bytes.");
        }

        /// <summary>
        /// Does nothing: nothing was pinned.
        /// </summary>
        public override void Unpin()
        {
        }

        /// <summary>
        /// Does nothing: there is nothing to release.
        /// </summary>
        /// <param name="disposing">
        /// Not used.
        /// </param>
        protected override void Dispose(bool disposing)
        {
        }
    }
}
