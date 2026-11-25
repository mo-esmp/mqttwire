using System.Buffers;
using MqttWire.Tests.Codec;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests how the read loop writes its answers (PUBACK, PUBREC, PUBREL and PUBCOMP): it keeps them while input is
/// there to be handled, writes them as one WebSocket message just before it would wait for the link, and at the latest
/// before the sixteenth packet after the oldest one; and on every way out it drops them and returns their buffer.
/// </summary>
public sealed class AnswerBatchingTests
{
    /// <summary>
    /// The payload of the publishes these tests send, "hi".
    /// </summary>
    private static readonly byte[] s_hi = "hi"u8.ToArray();

    /// <summary>
    /// The QoS 0 PUBLISH of "hi" to "x" the client writes for <c>PublishAsync("x", s_hi, MqttQoS.AtMostOnce)</c>.
    /// </summary>
    private static readonly byte[] s_publishXQoS0 = [0x30, 0x06, 0x00, 0x01, 0x78, 0x00, 0x68, 0x69];

    /// <summary>
    /// A way the connection ends while an answer waits, on a packet that came in the same run of input.
    /// </summary>
    public enum WayOut
    {
        /// <summary>
        /// A PUBLISH whose topic is not valid UTF-8: the client sends DISCONNECT 0x81.
        /// </summary>
        MalformedPacket,

        /// <summary>
        /// A DISCONNECT from the server: the client sends nothing.
        /// </summary>
        ServerDisconnect,

        /// <summary>
        /// A second QoS 1 PUBLISH whose <c>OnMessage</c> throws: the client sends DISCONNECT 0x83.
        /// </summary>
        OnMessageThrows,
    }

    /// <summary>
    /// Proves a single QoS 1 PUBLISH on a quiet connection has its PUBACK written before the read loop waits again.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_PublishQoS1OnQuietConnection_WritesPubAckBeforeWaiting()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        List<byte[]> writes = CaptureWrites(harness.Link);

        // Act
        harness.Link.Deliver(PublishQoS1(1));

        // Assert
        writes.ShouldBe([PubAck(1)]);
        harness.Link.IsReadPending.ShouldBeTrue();
    }

    /// <summary>
    /// Proves the PUBACK for the client's own publish completes it while the client's PUBACK for a message that came
    /// in the same read has not been written yet.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_PublishQoS1AndOwnPubAckTogether_CompletesPublishWhilePubAckWriteIsHeld()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        harness.NextPacket();
        harness.Link.HoldWrites();

        // Act
        harness.Link.Deliver([.. PublishQoS1(1), .. PubAck(1)]);
        MqttRequestResult result = await publish.AsTask().Within("the publish");

        // Assert
        result.Status.ShouldBe(MqttRequestStatus.Completed);
        harness.Link.HeldWriteBuffer.ToArray().ShouldBe(PubAck(1));
    }

    /// <summary>
    /// Proves that when the link's wait completes at once and its read stays pending, the PUBACK is written while
    /// that read is pending, and the packet the read then brings is handled exactly once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_WaitCompletesAndReadPends_WritesPubAckWhileReadIsPending()
    {
        // Arrange
        int received = 0;
        ClientHarness harness = await ConnectAsync((in _) => received++);
        List<byte[]> writes = CaptureWrites(harness.Link);
        harness.Link.SkipWaits();

        // Act
        harness.Link.Deliver(PublishQoS1(1));
        byte[][] writtenWhileReadPends = [.. writes];
        bool readPending = !harness.Link.PendingReadBuffer.IsEmpty;
        harness.Link.Deliver(KnownPackets.PublishQoS0);

        // Assert
        writtenWhileReadPends.ShouldBe([PubAck(1)]);
        readPending.ShouldBeTrue();
        received.ShouldBe(2);
        writes.Count.ShouldBe(1);
    }

    /// <summary>
    /// Proves seventeen QoS 1 messages arriving together are answered with one write of sixteen PUBACKs, then one of
    /// the seventeenth, in order.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_SeventeenQoS1Together_WritesSixteenPubAcksThenOne()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        List<byte[]> writes = CaptureWrites(harness.Link);

        // Act
        harness.Link.Deliver(PublishesQoS1(17));

        // Assert
        writes.ShouldBe([PubAcks(1, 16), PubAck(17)]);
    }

    /// <summary>
    /// Proves a PUBACK waiting behind QoS 0 messages is written before the sixteenth of them is handed to
    /// <c>OnMessage</c>, although none of them needs an answer.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_QoS1ThenSixteenQoS0Together_WritesBeforeSixteenthQoS0()
    {
        // Arrange
        List<string> log = [];
        ClientHarness harness = await ConnectAsync((in _) => log.Add("m"));
        harness.Link.Writing = _ => log.Add("w");
        List<byte> together = [.. PublishQoS1(1)];
        for (int i = 0; i < 16; i++)
        {
            together.AddRange(KnownPackets.PublishQoS0);
        }

        // Act
        harness.Link.Deliver([.. together]);

        // Assert
        log.IndexOf("w").ShouldBe(16);
        log.Count(e => e == "m").ShouldBe(17);
    }

    /// <summary>
    /// Proves three QoS 1 messages arriving together are answered with one write of their three PUBACKs, in order.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_ThreeQoS1Together_WritesThreePubAcksInOneWrite()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        List<byte[]> writes = CaptureWrites(harness.Link);

        // Act
        harness.Link.Deliver(PublishesQoS1(3));

        // Assert
        writes.ShouldBe([PubAcks(1, 3)]);
    }

    /// <summary>
    /// Proves a new QoS 2 PUBLISH and the PUBREL of an earlier exchange, arriving together, are answered with one
    /// write of the PUBREC and then the PUBCOMP, in the order the packets came.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_NewQoS2AndPubRelOfOldTogether_WritesPubRecThenPubCompInOneWrite()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        List<byte[]> writes = CaptureWrites(harness.Link);
        harness.Link.Deliver(KnownPackets.PublishQoS2);
        byte[][] first = [.. writes];
        writes.Clear();

        // Act
        harness.Link.Deliver([.. ClientPackets.PublishQoS2Id7, .. ClientPackets.PubRel1]);

        // Assert
        first.ShouldBe([ClientPackets.PubRec1]);
        writes.ShouldBe([[.. ClientPackets.PubRec7, .. ClientPackets.PubComp1]]);
    }

    /// <summary>
    /// Proves the PUBRECs of two QoS 2 publishes of the client's own, arriving together, are answered with one write
    /// of both PUBRELs in the order of the PUBRECs, and each publish completes only on its own PUBCOMP.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_TwoPubRecsTogether_WritesBothPubRelsInOneWriteAndEachCompletesOnItsPubComp()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        Task<MqttRequestResult> first = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.ExactlyOnce).AsTask();
        Task<MqttRequestResult> second = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.ExactlyOnce).AsTask();
        harness.NextPacket();
        harness.NextPacket();
        List<byte[]> writes = CaptureWrites(harness.Link);

        // Act
        harness.Link.Deliver([0x50, 0x02, 0x00, 0x02, 0x50, 0x02, 0x00, 0x01]);
        bool completedOnPubRecs = first.IsCompleted || second.IsCompleted;
        harness.Link.Deliver([0x70, 0x02, 0x00, 0x02]);
        MqttRequestResult secondResult = await second.Within("the second publish");
        bool firstCompletedOnSecondPubComp = first.IsCompleted;
        harness.Link.Deliver(ClientPackets.PubComp1);
        MqttRequestResult firstResult = await first.Within("the first publish");

        // Assert
        writes.ShouldBe([[0x62, 0x02, 0x00, 0x02, 0x62, 0x02, 0x00, 0x01]]);
        completedOnPubRecs.ShouldBeFalse();
        firstCompletedOnSecondPubComp.ShouldBeFalse();
        secondResult.Status.ShouldBe(MqttRequestStatus.Completed);
        firstResult.Status.ShouldBe(MqttRequestStatus.Completed);
    }

    /// <summary>
    /// Proves that when the connection ends on a packet that came with a QoS 1 PUBLISH, its PUBACK is dropped, only
    /// the DISCONNECT the way out calls for is written, and every buffer goes back to the pool exactly once.
    /// </summary>
    /// <param name="wayOut">
    /// The packet that ends the connection.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(WayOut.MalformedPacket)]
    [InlineData(WayOut.ServerDisconnect)]
    [InlineData(WayOut.OnMessageThrows)]
    public async Task Read_EndsWithAnswerWaiting_WritesOnlyTheDisconnectAndReturnsEveryBuffer(WayOut wayOut)
    {
        // Arrange
        CountingPool pool = new();
        int messages = 0;
        ClientHarness harness = await ConnectAsync(
            (in _) =>
            {
                messages++;
                if (wayOut == WayOut.OnMessageThrows && messages == 2)
                {
                    throw new InvalidOperationException("handler failed");
                }
            },
            pool);
        List<byte[]> writes = CaptureWrites(harness.Link);
        byte[] ending = wayOut switch
        {
            WayOut.MalformedPacket => ClientPackets.PublishTopicNotUtf8,
            WayOut.ServerDisconnect => ClientPackets.Disconnect,
            _ => PublishQoS1(2),
        };
        byte[][] expected = wayOut switch
        {
            WayOut.MalformedPacket => [ClientPackets.DisconnectMalformedPacket],
            WayOut.ServerDisconnect => [],
            _ => [ClientPackets.DisconnectImplementationSpecific],
        };

        // Act
        harness.Link.Deliver([.. PublishQoS1(1), .. ending]);
        byte[][] written = [.. writes];
        harness.Link.EndFromPeer();

        // Assert
        written.ShouldBe(expected);
        writes.Count.ShouldBe(expected.Length);
        harness.Closed.Count.ShouldBe(1);
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a write of answers that fails while the read the loop started is pending ends the connection as a
    /// failed write, and the read loop finishes only after that read has ended, with every buffer back.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_AnswerWriteFailsWhileReadPends_EndsWithWriteFailedAfterTheReadEnds()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool: pool);
        harness.Link.SkipWaits();
        harness.Link.HoldReadsOnAbort();
        harness.Link.FailWrites(new IOException());

        // Act
        harness.Link.Deliver(PublishQoS1(1));
        int closedWhileReadPends = harness.Closed.Count;
        bool readPending = !harness.Link.PendingReadBuffer.IsEmpty;
        harness.Link.EndFromPeer();

        // Assert
        closedWhileReadPends.ShouldBe(0);
        readPending.ShouldBeTrue();
        harness.Closed.Single().Reason.ShouldBe(MqttClientCloseReason.WriteFailed);
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a write of answers that fails while the link's wait is pending ends the connection as a failed write,
    /// and the read loop finishes only after that wait has ended, with every buffer back exactly once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_AnswerWriteFailsWhileWaitPends_EndsWithWriteFailedAfterTheWaitEnds()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool: pool);
        harness.Link.HoldReadsOnAbort();
        harness.Link.FailWrites(new IOException());

        // Act
        harness.Link.Deliver(PublishQoS1(1));
        int closedWhileWaitPends = harness.Closed.Count;
        harness.Link.EndFromPeer();

        // Assert
        closedWhileWaitPends.ShouldBe(0);
        harness.Closed.Single().Reason.ShouldBe(MqttClientCloseReason.WriteFailed);
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves an answer over the server's Maximum Packet Size ends the connection without writing it or the answers
    /// that waited before it, and returns their buffer.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_AnswerOverServerMaximumPacketSize_EndsAndReturnsTheWaitingAnswers()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool: pool, connAck: [0x20, 0x08, 0x00, 0x00, 0x05, 0x27, 0x00, 0x00, 0x00, 0x04]);
        List<byte[]> writes = CaptureWrites(harness.Link);

        // Act
        harness.Link.Deliver([.. PublishQoS1(1), .. ClientPackets.PubRel7]);

        // Assert
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.PacketTooLarge, null, null)]);
        writes.ShouldBeEmpty();
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a publish an <c>OnMessage</c> makes while answers wait is written at once, and the waiting PUBACKs
    /// follow in one write, in order.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_OnMessagePublishesWhileAnswersWait_PublishesFirstThenWritesPubAcksInOrder()
    {
        // Arrange
        MqttWireClient? client = null;
        List<Task<MqttRequestResult>> publishes = [];
        ClientHarness harness = await ConnectAsync((in message) =>
        {
            if (client is not null)
            {
                publishes.Add(client.PublishAsync("x", s_hi, MqttQoS.AtMostOnce).AsTask());
            }
        });
        client = harness.Client;
        List<byte[]> writes = CaptureWrites(harness.Link);

        // Act
        harness.Link.Deliver(PublishesQoS1(2));
        MqttRequestResult[] results = await Task.WhenAll(publishes).Within("the publishes");

        // Assert
        writes.ShouldBe([s_publishXQoS0, s_publishXQoS0, PubAcks(1, 2)]);
        results.Length.ShouldBe(2);
    }

    /// <summary>
    /// Proves a dispose begun by the <c>OnMessage</c> of a second QoS 1 PUBLISH, while the PUBACK of the first
    /// waits, writes only the DISCONNECT, and returns every buffer exactly once when the dispose completes.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_DisposeWhileAnswersWait_WritesOnlyTheDisconnect()
    {
        // Arrange
        CountingPool pool = new();
        MqttWireClient? client = null;
        Task? dispose = null;
        int messages = 0;
        ClientHarness harness = await ConnectAsync(
            (in _) =>
            {
                messages++;
                if (messages == 2)
                {
                    dispose = client?.DisposeAsync().AsTask();
                }
            },
            pool);
        client = harness.Client;
        List<byte[]> writes = CaptureWrites(harness.Link);

        // Act
        harness.Link.Deliver(PublishesQoS1(2));
        byte[][] written = [.. writes];
        harness.Link.EndFromPeer();
        await dispose.ShouldNotBeNull().Within("the dispose");

        // Assert
        written.ShouldBe([ClientPackets.Disconnect]);
        writes.Count.ShouldBe(1);
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves three QoS 1 messages that the link hands over one byte per read, all there to be read, are answered
    /// with one write of their three PUBACKs.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_ThreeQoS1SplitAtEveryByte_WritesThreePubAcksInOneWrite()
    {
        // Arrange
        int qoS1Messages = 0;
        MemoryLink? link = null;
        byte[] bytes = PublishesQoS1(3);
        ClientHarness harness = await ConnectAsync(
            (in message) =>
            {
                if (message.QoS == MqttQoS.AtMostOnce)
                {
                    // The read loop is busy here, so every one-byte chunk is queued before it reads the first.
                    link?.DeliverInChunks(bytes, 1);
                }
                else
                {
                    qoS1Messages++;
                }
            });
        link = harness.Link;
        List<byte[]> writes = CaptureWrites(link);

        // Act
        link.Deliver(KnownPackets.PublishQoS0);

        // Assert
        qoS1Messages.ShouldBe(3);
        writes.ShouldBe([PubAcks(1, 3)]);
    }

    /// <summary>
    /// Proves a QoS 2 PUBLISH and its PUBREL arriving together are answered with one write of the PUBREC and then the
    /// PUBCOMP, and the identifier is forgotten, so a later PUBLISH with it is handed on again.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_PublishQoS2AndItsPubRelTogether_WritesPubRecThenPubCompAndForgets()
    {
        // Arrange
        int received = 0;
        ClientHarness harness = await ConnectAsync((in _) => received++);
        List<byte[]> writes = CaptureWrites(harness.Link);

        // Act
        harness.Link.Deliver([.. KnownPackets.PublishQoS2, .. ClientPackets.PubRel1]);
        byte[][] first = [.. writes];
        harness.Link.Deliver(KnownPackets.PublishQoS2);

        // Assert
        first.ShouldBe([[.. ClientPackets.PubRec1, .. ClientPackets.PubComp1]]);
        received.ShouldBe(2);
        writes[^1].ShouldBe(ClientPackets.PubRec1);
    }

    /// <summary>
    /// Builds a QoS 1 PUBLISH of "hi" to "a/b" with the given packet identifier.
    /// </summary>
    /// <param name="id">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// <c>32 0A 00 03 61 2F 62 00 &lt;id&gt; 00 68 69</c>.
    /// </returns>
    private static byte[] PublishQoS1(ushort id)
        => [0x32, 0x0A, 0x00, 0x03, 0x61, 0x2F, 0x62, (byte)(id >> 8), (byte)id, 0x00, 0x68, 0x69];

    /// <summary>
    /// Builds a PUBACK with success for the given packet identifier.
    /// </summary>
    /// <param name="id">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// <c>40 02 &lt;id&gt;</c>.
    /// </returns>
    private static byte[] PubAck(ushort id) => [0x40, 0x02, (byte)(id >> 8), (byte)id];

    /// <summary>
    /// Builds QoS 1 PUBLISHes with identifiers 1 to <paramref name="count"/>, one after the other.
    /// </summary>
    /// <param name="count">
    /// How many.
    /// </param>
    /// <returns>
    /// The packets, concatenated.
    /// </returns>
    private static byte[] PublishesQoS1(int count)
    {
        List<byte> bytes = [];
        for (int id = 1; id <= count; id++)
        {
            bytes.AddRange(PublishQoS1((ushort)id));
        }

        return [.. bytes];
    }

    /// <summary>
    /// Builds the PUBACKs for identifiers <paramref name="first"/> to <paramref name="last"/>, one after the other.
    /// </summary>
    /// <param name="first">
    /// The first identifier.
    /// </param>
    /// <param name="last">
    /// The last identifier.
    /// </param>
    /// <returns>
    /// The packets, concatenated.
    /// </returns>
    private static byte[] PubAcks(int first, int last)
    {
        List<byte> bytes = [];
        for (int id = first; id <= last; id++)
        {
            bytes.AddRange(PubAck((ushort)id));
        }

        return [.. bytes];
    }

    /// <summary>
    /// Records every write the link is handed from now on, one entry per WebSocket message.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <returns>
    /// The list the writes are added to, oldest first.
    /// </returns>
    private static List<byte[]> CaptureWrites(MemoryLink link)
    {
        List<byte[]> writes = [];
        link.Writing = packet => writes.Add(packet.ToArray());
        return writes;
    }

    /// <summary>
    /// Creates a client and connects it, taking the CONNECT it wrote.
    /// </summary>
    /// <param name="onMessage">
    /// The message handler, or <see langword="null"/> for none.
    /// </param>
    /// <param name="pool">
    /// The pool the client rents from, or <see langword="null"/> for the shared pool.
    /// </param>
    /// <param name="connAck">
    /// The CONNACK the server sends, or <see langword="null"/> for <see cref="ClientPackets.ConnAckAccepted"/>.
    /// </param>
    /// <returns>
    /// The harness around the connected client.
    /// </returns>
    private static async Task<ClientHarness> ConnectAsync(
        MqttMessageHandler? onMessage = null,
        ArrayPool<byte>? pool = null,
        byte[]? connAck = null)
    {
        ClientHarness harness = ClientHarness.Create(pool: pool, onMessage: onMessage);
        await harness.ConnectAsync(connAck ?? ClientPackets.ConnAckAccepted);
        harness.NextPacket();
        return harness;
    }
}
