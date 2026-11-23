using System.Runtime.CompilerServices;
using MqttWire.Server;
using MqttWire.Server.Internal;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the numbers in the design's table "Worst case held for one connection" (6.6): for each pooled buffer,
/// the size the server asks the pool for, which is its logical limit, and the length of the array the shared pool
/// hands out for it, which is the next power of two and is what is really held; and, for a buffer that grows, the
/// moment at which its old array and its new one are both held. The harness's pool rents from the shared pool and
/// records both.
/// </summary>
public sealed class BufferCapacityTests
{
    /// <summary>
    /// The length of the array the first read of a connection is given.
    /// </summary>
    private const int FirstArray = 4_096;

    /// <summary>
    /// Proves the first row of the table: a CONNECT longer than the first read buffer is read into a buffer the
    /// server asks for at exactly the CONNECT's length, at most 16,384; the array it is handed is the next power of
    /// two; and while the buffer grows, the first array of 4,096 bytes and the new one are both held, which is the
    /// most the connect phase ever holds.
    /// </summary>
    /// <param name="connectLength">
    /// The total length of the CONNECT.
    /// </param>
    /// <param name="arrayLength">
    /// The length of the array the grown buffer is held in.
    /// </param>
    [Theory]
    [InlineData(4_097, 8_192)]
    [InlineData(8_192, 8_192)]
    [InlineData(8_193, 16_384)]
    [InlineData(16_384, 16_384)]
    public void Connect_LongerThanTheFirstReadBuffer_IsAskedForAtItsLengthAndHeldInTheNextPowerOfTwo(
        int connectLength, int arrayLength)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        byte[] connect = ConnectOfLength(connectLength);
        TestConnection connection = harness.Accept();

        // Act
        connection.Link.DeliverInChunks(connect, chunkSize: 1_000);

        // Assert
        connect.Length.ShouldBe(connectLength);
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAck);
        harness.Pool.LargestRent.ShouldBe(connectLength);
        harness.Pool.LargestArray.ShouldBe(arrayLength);
        harness.Pool.PeakOutstanding.ShouldBe(2);
        harness.Pool.PeakBytesOutstanding.ShouldBe(FirstArray + arrayLength);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.AssertBounds();
    }

    /// <summary>
    /// Proves the row of the read buffer with the default options: the largest PUBLISH that is read whole, a
    /// Remaining Length of the payload limit and its 5 KiB of room behind a fixed header of 4 bytes, is asked for at
    /// 70,660 bytes and held in an array of 131,072; while the buffer grows from its first array, 4,096 and 131,072
    /// bytes are held.
    /// </summary>
    [Fact]
    public void Read_LargestPublishThatIsReadWhole_IsAskedForAt70660BytesAndHeldIn131072()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.RecordsCalls = false;
        int publishes = 0;
        harness.Handler.Publish = (_, in publish) =>
        {
            publishes += publish.Payload.Length == 65_536 ? 1 : 0;
            return MqttPublishResult.Accepted;
        };
        TestConnection connection = harness.Connect();
        byte[] publish = ServerPackets.BuildPublishOfLength(MqttQoS.AtMostOnce,
            remainingLength: 65_536 + ServerLimits.PublishOverheadBytes, topicLength: ServerLimits.PublishOverheadBytes - 3);

        // Act
        connection.Link.DeliverInChunks(publish, chunkSize: 4_000);

        // Assert
        publish.Length.ShouldBe(70_660);
        publishes.ShouldBe(1);
        harness.Pool.LargestRent.ShouldBe(70_660);
        harness.Pool.LargestArray.ShouldBe(131_072);
        harness.Pool.PeakOutstanding.ShouldBe(2);
        harness.Pool.PeakBytesOutstanding.ShouldBe(FirstArray + 131_072);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.AssertBounds();
    }

    /// <summary>
    /// Proves the most the read buffer holds while it grows, with the default options: a PUBLISH of 40,000 bytes has
    /// grown the buffer to an array of 65,536 when the start of the largest PUBLISH arrives behind it in the same
    /// read, so the buffer grows again while it holds that array, and 65,536 and 131,072 bytes are held for the copy:
    /// 196,608, and never more. Both messages reach the host.
    /// </summary>
    [Fact]
    public void Read_LargestPublishBehindAPublishOf40000BytesInOneRead_Holds196608BytesWhileItGrows()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.RecordsCalls = false;
        List<int> payloads = [];
        harness.Handler.Publish = (_, in publish) =>
        {
            payloads.Add(publish.Payload.Length);
            return MqttPublishResult.Accepted;
        };
        TestConnection connection = harness.Connect();
        byte[] first = ServerPackets.BuildPublishOfLength(MqttQoS.AtMostOnce, remainingLength: 39_996);
        byte[] largest = ServerPackets.BuildPublishOfLength(MqttQoS.AtMostOnce,
            remainingLength: 65_536 + ServerLimits.PublishOverheadBytes, topicLength: ServerLimits.PublishOverheadBytes - 3);

        // Act
        connection.Deliver(first.AsSpan(start: 0, length: 1_000));
        long heldForTheFirst = harness.Pool.PeakBytesOutstanding;
        connection.Deliver([.. first.AsSpan(start: 1_000), .. largest.AsSpan(start: 0, length: 100)]);
        long heldWhileItGrewAgain = harness.Pool.PeakBytesOutstanding;
        connection.Deliver(largest.AsSpan(start: 100));

        // Assert
        first.Length.ShouldBe(40_000);
        largest.Length.ShouldBe(70_660);
        heldForTheFirst.ShouldBe(FirstArray + 65_536);
        heldWhileItGrewAgain.ShouldBe(65_536 + 131_072);
        harness.Pool.PeakBytesOutstanding.ShouldBe(196_608);
        harness.Pool.LargestArray.ShouldBe(131_072);
        payloads.ShouldBe([39_990, 65_536]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves the row of the encoding buffer: the largest buffer a delivery is encoded into is the header of a
    /// PUBLISH with the longest topic a host can create, at QoS 1 with a payload whose Remaining Length takes four
    /// bytes, which is asked for at exactly 65,536 bytes and held in an array of that length, a power of two; the
    /// payload is written from the host's memory and no other buffer is held with it.
    /// </summary>
    [Fact]
    public void TrySend_LongestTopicWithAPayloadOfTwoMebibytes_IsEncodedIntoOneBufferOf65536Bytes()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxOutgoingBytes = 4 * 1_048_576);
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        MqttTopic topic = MqttTopic.Create(ServerPackets.Letters('t', ServerLimits.MaxTopicBytes));
        byte[] payload = new byte[2_100_000];
        connection.Link.DiscardsWritten = true;

        // Act
        MqttSendResult result = server.TrySend(topic, payload, MqttQoS.AtLeastOnce);

        // Assert: the header, then the payload in pieces of at most 65,536 bytes.
        result.ShouldBe(MqttSendResult.Taken);
        connection.Link.WriteCount.ShouldBe(1 + 1 + 33);
        harness.Pool.LargestRent.ShouldBe(ServerLimits.MaxEncodingBufferBytes);
        harness.Pool.LargestArray.ShouldBe(65_536);
        harness.Pool.PeakBytesOutstanding.ShouldBe(65_536);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.AssertBounds();
    }

    /// <summary>
    /// Proves the row of the waiting control packets: sixteen SUBACKs of 4,006 bytes behind a held write, 64,096
    /// bytes, are held in one control buffer whose array has grown, by doubling, to 65,536 bytes and no further; no
    /// array the server was handed is longer.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Answers_SixteenSubAcksBehindAHeldWrite_AreHeldInOneControlBufferOf65536Bytes()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);

        // Act
        for (int i = 1; i <= 16; i++)
        {
            connection.Deliver(ServerPackets.BuildSubscribe((ushort)i, properties: [],
                Enumerable.Repeat(("a", (byte)0), 4_000)));
        }

        ConnectionCounts waiting = server.GetCounts();
        int buffersHeld = harness.Pool.Outstanding;
        connection.CompleteHeldWrites();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert: the buffer of the held write and the control buffer.
        waiting.WaitingControlPackets.ShouldBe(16);
        waiting.OutgoingBytes.ShouldBe((3 + 2) + (16 * 4_006));
        buffersHeld.ShouldBe(2);
        harness.Pool.LargestArray.ShouldBe(65_536);
        harness.Pool.LargestRent.ShouldBeLessThanOrEqualTo(ServerLimits.MaxEncodingBufferBytes);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves the size of the largest answer the read path encodes into a buffer of its own before it adds it to
    /// what waits: an UNSUBACK for as many filters as the largest UNSUBSCRIBE the server reads can hold, 5,459 of one
    /// letter in 16,383 bytes, is 5,465 bytes, which the pool holds in an array of 8,192.
    /// </summary>
    [Fact]
    public void Unsubscribe_AsManyFiltersAsSixteenKibibytesHold_IsAnsweredWithAnUnsubAckOf5465Bytes()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        byte[] unsubscribe = ServerPackets.BuildUnsubscribe(packetIdentifier: 7, Enumerable.Repeat("a", 5_459));
        byte[] oneMore = ServerPackets.BuildUnsubscribe(packetIdentifier: 7, Enumerable.Repeat("a", 5_460));

        // Act
        connection.Deliver(unsubscribe);
        byte[] unsubAck = connection.TakeWritten();

        // Assert
        unsubscribe.Length.ShouldBe(16_383);
        oneMore.Length.ShouldBeGreaterThan(ServerLimits.MaxControlPacketBytes);
        unsubAck.Length.ShouldBe(5_465);
        unsubAck[0].ShouldBe((byte)0xB0);
        harness.Pool.LargestRent.ShouldBe(16_383);
        harness.Pool.LargestArray.ShouldBe(16_384);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves what the table says of the queue of waiting deliveries, which is not a pooled buffer and is not counted
    /// against <see cref="MqttWireServerOptions.MaxOutgoingBytes"/>: a slot is 32 bytes, and the ring that holds the
    /// default 1,000 waiting deliveries has grown, by doubling, to 1,024 slots.
    /// </summary>
    [Fact]
    public void WaitingDeliveries_ThousandWaiting_AreHeldIn1024SlotsOf32Bytes()
    {
        // Arrange
        WaitingDeliveries waiting = new();
        PendingDelivery delivery = new(ServerHarness.Topic, ReadOnlyMemory<byte>.Empty, MqttQoS.AtMostOnce);

        // Act
        for (int i = 0; i < new MqttWireServerOptions().MaxOutgoingMessages; i++)
        {
            waiting.Enqueue(delivery);
        }

        // Assert
        Unsafe.SizeOf<PendingDelivery>().ShouldBe(32);
        waiting.Count.ShouldBe(1_000);
        waiting.Capacity.ShouldBe(1_024);
    }

    /// <summary>
    /// Builds a CONNECT with client id "c1" and a password that makes its total length the one asked for.
    /// </summary>
    /// <param name="length">
    /// The total length of the packet, from 4,097 to 16,384, so that its Remaining Length takes two bytes.
    /// </param>
    /// <returns>
    /// The packet's bytes, fixed header included.
    /// </returns>
    private static byte[] ConnectOfLength(int length)
    {
        // The fixed header's 3 bytes, the variable header's 10, the property length's 1, the client id's 4 and the
        // password's length prefix of 2.
        byte[] password = new byte[length - 3 - 10 - 1 - 4 - 2];
        return ServerPackets.BuildConnect(flags: 0x42, properties: [],
            payload: [.. ServerPackets.Field("c1"u8), .. ServerPackets.Field(password)]);
    }
}
