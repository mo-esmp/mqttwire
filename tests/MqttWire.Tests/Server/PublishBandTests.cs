using System.Net.WebSockets;
using MqttWire.Server;
using MqttWire.Tests.Client;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the three bands of an incoming PUBLISH (design 6.6, plan decision 6), with a payload limit of 1,024
/// bytes: read whole up to a Remaining Length of 6,144, skipped without being buffered up to 16,384, and the
/// connection closed with 0x95 above that.
/// </summary>
public sealed class PublishBandTests
{
    /// <summary>
    /// The payload limit of these tests.
    /// </summary>
    private const int Limit = 1_024;

    /// <summary>
    /// The largest Remaining Length of the skip band: 16 times the limit.
    /// </summary>
    private const int SkipBandEnd = 16 * Limit;

    /// <summary>
    /// Proves that a payload of exactly the limit reaches the host and is answered in the form of its QoS.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the publish.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce)]
    [InlineData(MqttQoS.AtLeastOnce)]
    [InlineData(MqttQoS.ExactlyOnce)]
    public void Publish_PayloadAtTheLimit_ReachesTheHost(MqttQoS qos)
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        byte[] packet = ServerPackets.BuildPublish(qos, "a/b"u8, packetIdentifier: 1, properties: [],
            payload: ServerPackets.Letters('x', Limit));

        // Act
        connection.Deliver(packet);

        // Assert
        connection.TakeWritten().ShouldBe(Acceptance(qos));
        PublishCalls(harness).ShouldBe([$"OnPublish c1 a/b {new string('x', Limit)} q{(int)qos} retain=0"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a payload one byte over the limit in the first band is refused with 0x97 in the form of its QoS,
    /// without <c>OnPublish</c>, and that the connection stays open.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the publish.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce)]
    [InlineData(MqttQoS.AtLeastOnce)]
    [InlineData(MqttQoS.ExactlyOnce)]
    public void Publish_PayloadOverTheLimitInBandOne_IsRefusedWithoutOnPublish(MqttQoS qos)
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        byte[] packet = ServerPackets.BuildPublish(qos, "a/b"u8, packetIdentifier: 1, properties: [],
            payload: ServerPackets.Letters('x', Limit + 1));

        // Act
        connection.Deliver(packet);
        byte[] refusal = connection.TakeWritten();
        connection.Deliver(ClientPackets.PingReq);

        // Assert
        refusal.ShouldBe(Refusal(qos, packetIdentifier: 1));
        connection.TakeWritten().ShouldBe(ClientPackets.PingResp);
        PublishCalls(harness).ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the failing PUBREC of a publish refused as too large leaves nothing remembered: a later PUBREL for
    /// its identifier is answered PUBCOMP 0x92 (packet identifier not found).
    /// </summary>
    [Fact]
    public void Publish_PayloadOverTheLimitAtQoS2_RemembersNothing()
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        connection.Deliver(ServerPackets.BuildPublish(MqttQoS.ExactlyOnce, "a/b"u8, packetIdentifier: 1,
            properties: [], payload: ServerPackets.Letters('x', Limit + 1)));
        connection.TakeWritten().ShouldBe(ServerPackets.TooLargePubRec1);

        // Act
        connection.Deliver(ClientPackets.PubRel1);

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("70 03 00 01 92"));
    }

    /// <summary>
    /// Proves that a QoS 2 publish over the limit whose identifier is remembered is answered like any other repeat of
    /// a remembered identifier (design 6.6, "Publishes from the client at QoS 2", item 2): PUBREC success, without
    /// the host and without the refusal, and the identifier stays remembered.
    /// </summary>
    /// <param name="remainingLength">
    /// The Remaining Length of the repeat: in the band read whole, or in the band skipped.
    /// </param>
    [Theory]
    [InlineData(Limit + 100)]
    [InlineData(SkipBandEnd)]
    public void Publish_PayloadOverTheLimitAtQoS2WithARememberedIdentifier_AnswersPubRecSuccess(int remainingLength)
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        connection.Deliver(ClientPackets.PublishQoS2);
        connection.TakeWritten().ShouldBe(ClientPackets.PubRec1);

        // Act
        connection.Deliver(ServerPackets.BuildPublishOfLength(MqttQoS.ExactlyOnce, remainingLength));
        byte[] answer = connection.TakeWritten();
        connection.Deliver(ClientPackets.PubRel1);

        // Assert
        answer.ShouldBe(ClientPackets.PubRec1);
        PublishCalls(harness).ShouldBe(["OnPublish c1 a/b hi q2 retain=0"]);
        connection.TakeWritten().ShouldBe(ClientPackets.PubComp1);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a topic of 5,000 bytes with a small payload is read whole in the first band and reaches the host.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the publish.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce)]
    [InlineData(MqttQoS.AtLeastOnce)]
    [InlineData(MqttQoS.ExactlyOnce)]
    public void Publish_LargeTopicSmallPayloadInBandOne_ReachesTheHost(MqttQoS qos)
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        byte[] topic = ServerPackets.Letters('a', 5_000);
        byte[] packet = ServerPackets.BuildPublish(qos, topic, packetIdentifier: 1, properties: [], payload: "hi"u8);

        // Act
        connection.Deliver(packet);

        // Assert
        connection.TakeWritten().ShouldBe(Acceptance(qos));
        PublishCalls(harness).ShouldBe([$"OnPublish c1 {new string('a', 5_000)} hi q{(int)qos} retain=0"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a user property of 5,000 bytes with a small payload is read whole in the first band and reaches
    /// the host.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the publish.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce)]
    [InlineData(MqttQoS.AtLeastOnce)]
    [InlineData(MqttQoS.ExactlyOnce)]
    public void Publish_LargePropertiesSmallPayloadInBandOne_ReachesTheHost(MqttQoS qos)
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();

        // Identifier 0x26, the name "k" and a value of 4,994 bytes, each string with its length: 5,000 bytes.
        byte[] userProperty = [0x26, .. ServerPackets.Field("k"u8), .. ServerPackets.Field(ServerPackets.Letters('v', 4_994))];
        byte[] packet = ServerPackets.BuildPublish(qos, "a/b"u8, packetIdentifier: 1, userProperty, payload: "hi"u8);

        // Act
        connection.Deliver(packet);

        // Assert
        userProperty.Length.ShouldBe(5_000);
        connection.TakeWritten().ShouldBe(Acceptance(qos));
        PublishCalls(harness).ShouldBe([$"OnPublish c1 a/b hi q{(int)qos} retain=0"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a PUBLISH at the top of the skip band is refused in the form of its QoS without being buffered:
    /// no rent is larger than the first read's, and the connection stays open.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the publish.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce)]
    [InlineData(MqttQoS.AtLeastOnce)]
    [InlineData(MqttQoS.ExactlyOnce)]
    public void Publish_InTheSkipBand_IsRefusedAndNeverBuffered(MqttQoS qos)
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        byte[] packet = ServerPackets.BuildPublishOfLength(qos, SkipBandEnd);

        // Act
        connection.Deliver(packet);
        byte[] refusal = connection.TakeWritten();
        connection.Deliver(ClientPackets.PingReq);

        // Assert
        refusal.ShouldBe(Refusal(qos, packetIdentifier: 1));
        connection.TakeWritten().ShouldBe(ClientPackets.PingResp);
        PublishCalls(harness).ShouldBeEmpty();
        harness.Pool.LargestRent.ShouldBeLessThanOrEqualTo(4_096);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a PUBLISH whose Remaining Length is exactly the end of the first band, 6,144, is read whole: a
    /// buffer larger than the first read's is rented, and its payload over the limit is refused with 0x97.
    /// </summary>
    [Fact]
    public void Publish_RemainingLengthAtTheEndOfBandOne_IsReadWhole()
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        byte[] packet = ServerPackets.BuildPublishOfLength(MqttQoS.AtLeastOnce, Limit + 5_120);

        // Act
        connection.Deliver(packet);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.TooLargePubAck1);
        PublishCalls(harness).ShouldBeEmpty();
        harness.Pool.LargestRent.ShouldBeGreaterThan(4_096);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a PUBLISH whose Remaining Length is one byte past the end of the first band, 6,145, is skipped: no
    /// buffer larger than the first read's is rented, and it is refused with 0x97.
    /// </summary>
    [Fact]
    public void Publish_RemainingLengthOnePastBandOne_IsSkipped()
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        byte[] packet = ServerPackets.BuildPublishOfLength(MqttQoS.AtLeastOnce, Limit + 5_120 + 1);

        // Act
        connection.Deliver(packet);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.TooLargePubAck1);
        PublishCalls(harness).ShouldBeEmpty();
        harness.Pool.LargestRent.ShouldBeLessThanOrEqualTo(4_096);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a skipped PUBLISH that ends right where its property length should start is malformed (0x81),
    /// exactly as the codec judges the same shape in the first band: at QoS 0 right after its topic, at QoS 1 and 2
    /// right after its packet identifier.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the publish.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce)]
    [InlineData(MqttQoS.AtLeastOnce)]
    [InlineData(MqttQoS.ExactlyOnce)]
    public async Task Publish_SkipBandEndsWithoutAPropertyLength_ClosesWith0x81(MqttQoS qos)
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        int identifierLength = qos == MqttQoS.AtMostOnce ? 0 : 2;
        byte[] topic = ServerPackets.Field(ServerPackets.Letters('a', SkipBandEnd - 2 - identifierLength));
        byte[] identifier = qos == MqttQoS.AtMostOnce ? [] : [0x00, 0x01];
        byte[] packet = [(byte)(0x30 | ((int)qos << 1)), 0x80, 0x80, 0x01, .. topic, .. identifier];

        // Act
        connection.Deliver(packet);
        await connection.Run;

        // Assert
        packet.Length.ShouldBe(SkipBandEnd + 4);
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.MalformedPacket));
        harness.Handler.Calls.ShouldContain("OnClosed c1 MalformedPacket");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a skipped QoS 1 PUBLISH with packet identifier 0 that ends right after it is a protocol error
    /// (0x82), as the codec judges it in the first band, where the identifier is checked before the properties.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Publish_SkipBandIdentifierZeroAtTheEnd_ClosesWith0x82()
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        byte[] packet = [0x32, 0x80, 0x80, 0x01, .. ServerPackets.Field(ServerPackets.Letters('a', SkipBandEnd - 4)), 0x00, 0x00];

        // Act
        connection.Deliver(packet);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the skip passes over a topic of 8,000 bytes to find the packet identifier, which the refusal
    /// carries.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the publish.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce)]
    [InlineData(MqttQoS.AtLeastOnce)]
    [InlineData(MqttQoS.ExactlyOnce)]
    public void Publish_LargeTopicInTheSkipBand_FindsTheIdentifier(MqttQoS qos)
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        byte[] packet = ServerPackets.BuildPublishOfLength(qos, SkipBandEnd, topicLength: 8_000, packetIdentifier: 7);

        // Act
        connection.Deliver(packet);

        // Assert
        connection.TakeWritten().ShouldBe(Refusal(qos, packetIdentifier: 7));
        PublishCalls(harness).ShouldBeEmpty();
        connection.Run.IsCompleted.ShouldBeFalse();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a PUBLISH one byte above the skip band closes the connection with DISCONNECT 0x95 and
    /// <see cref="MqttCloseReason.ProtocolError"/>, from its fixed header alone.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the publish.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce)]
    [InlineData(MqttQoS.AtLeastOnce)]
    [InlineData(MqttQoS.ExactlyOnce)]
    public async Task Publish_AboveTheSkipBand_ClosesWith0x95(MqttQoS qos)
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        byte[] packet = ServerPackets.BuildPublishOfLength(qos, SkipBandEnd + 1);

        // Act
        connection.Deliver(packet);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.PacketTooLarge));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        PublishCalls(harness).ShouldBeEmpty();
        harness.Pool.LargestRent.ShouldBeLessThanOrEqualTo(4_096);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a PUBLISH of the first band or of the skip band gives the same result wherever it is cut in two,
    /// and that the packet behind it, a PINGREQ, is answered. An accepted QoS 2 publish is released by a PUBREL behind
    /// the PINGREQ, so that its identifier is new again for the next cut.
    /// </summary>
    /// <param name="skipBand">
    /// <see langword="true"/> for a packet at the top of the skip band; <see langword="false"/> for one of the first
    /// band with a payload at the limit.
    /// </param>
    /// <param name="qos">
    /// The QoS of the publish.
    /// </param>
    [Theory]
    [InlineData(false, MqttQoS.AtMostOnce)]
    [InlineData(false, MqttQoS.AtLeastOnce)]
    [InlineData(false, MqttQoS.ExactlyOnce)]
    [InlineData(true, MqttQoS.AtMostOnce)]
    [InlineData(true, MqttQoS.AtLeastOnce)]
    [InlineData(true, MqttQoS.ExactlyOnce)]
    public void Publish_CutAtEveryPosition_GivesTheSameResult(bool skipBand, MqttQoS qos)
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        byte[] packet = skipBand
            ? ServerPackets.BuildPublishOfLength(qos, SkipBandEnd)
            : ServerPackets.BuildPublish(qos, "a/b"u8, packetIdentifier: 1, properties: [],
                payload: ServerPackets.Letters('x', Limit));
        bool release = !skipBand && qos == MqttQoS.ExactlyOnce;
        byte[] behind = release ? [.. ClientPackets.PingReq, .. ClientPackets.PubRel1] : ClientPackets.PingReq;
        byte[] expected =
        [
            .. (skipBand ? Refusal(qos, packetIdentifier: 1) : Acceptance(qos)), .. ClientPackets.PingResp,
            .. (release ? ClientPackets.PubComp1 : []),
        ];
        int publishes = 0;

        // One connection for every cut: the packets of the two bands leave nothing remembered behind them once a
        // QoS 2 publish that was accepted has been released.
        for (int cut = 1; cut < packet.Length; cut++)
        {
            // Act
            connection.Deliver(packet.AsSpan(0, cut));
            connection.Deliver([.. packet.AsSpan(cut), .. behind]);

            // Assert
            publishes += skipBand ? 0 : 1;
            connection.TakeWritten().ShouldBe(expected, $"cut at {cut}");
            PublishCalls(harness).Count.ShouldBe(publishes, $"cut at {cut}");
            harness.Pool.Outstanding.ShouldBe(0, $"cut at {cut}");
        }

        harness.Pool.LargestRent.ShouldBeLessThanOrEqualTo(skipBand ? 4_096 : 6_149);
    }

    /// <summary>
    /// Proves that a skipped PUBLISH whose topic length runs past the packet closes the connection with DISCONNECT
    /// 0x81 and <see cref="MqttCloseReason.MalformedPacket"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Publish_SkipBandTopicLengthRunsPastThePacket_ClosesWith0x81()
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();

        // QoS 1, Remaining Length 16,384, a topic length of 65,535.
        byte[] start = Hex.Parse("32 80 80 01 FF FF 61 61 61");

        // Act
        connection.Deliver(start);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.MalformedPacket));
        harness.Handler.Calls.ShouldContain("OnClosed c1 MalformedPacket");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a skipped QoS 1 PUBLISH whose topic fills the packet, leaving no room for its identifier, closes
    /// the connection with DISCONNECT 0x81 and <see cref="MqttCloseReason.MalformedPacket"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Publish_SkipBandEndsBeforeItsIdentifier_ClosesWith0x81()
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();

        // QoS 1, Remaining Length 16,384: the topic's length and 16,382 bytes of topic, nothing after.
        byte[] packet = [.. Hex.Parse("32 80 80 01"), .. ServerPackets.Field(ServerPackets.Letters('a', SkipBandEnd - 2))];

        // Act
        connection.Deliver(packet);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.MalformedPacket));
        harness.Handler.Calls.ShouldContain("OnClosed c1 MalformedPacket");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the partial-packet deadline covers a packet that is being skipped: one that stalls is still open
    /// after eight seconds and is closed at ten with DISCONNECT 0x82 and
    /// <see cref="MqttCloseReason.PartialPacketTimedOut"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Publish_SkippedPacketStallsForTenSeconds_ClosesWith0x82()
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect();
        byte[] packet = ServerPackets.BuildPublishOfLength(MqttQoS.AtLeastOnce, SkipBandEnd);
        connection.Deliver(packet.AsSpan(0, packet.Length / 2));

        // Act
        harness.Advance(TimeSpan.FromSeconds(8));
        byte[] writtenAt8 = connection.TakeWritten();
        WebSocketCloseStatus? closedAt8 = connection.Link.ClosedWith;
        harness.Advance(TimeSpan.FromSeconds(2));
        byte[] written = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.Run;

        // Assert
        writtenAt8.ShouldBeEmpty();
        closedAt8.ShouldBeNull();
        written.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        harness.Handler.Calls.ShouldContain("OnClosed c1 PartialPacketTimedOut");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that with a limit so small that 16 times it is below the first band's end the skip band is empty: a
    /// PUBLISH just above the first band closes the connection with DISCONNECT 0x95.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Publish_SkipBandEmptyWithASmallLimit_GoesStraightTo0x95()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxPublishPayloadBytes = 16);
        TestConnection connection = harness.Connect();
        byte[] packet = ServerPackets.BuildPublishOfLength(MqttQoS.AtLeastOnce, 16 + 5_120 + 1);

        // Act
        connection.Deliver(packet);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.PacketTooLarge));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a client that sent Request Problem Information 0 gets the refusal of a publish too large without
    /// the reason string.
    /// </summary>
    [Fact]
    public void Publish_NoPayloadRequestProblemInformationZero_RefusalHasNoReasonString()
    {
        // Arrange
        ServerHarness harness = CreateHarness();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectNoProblemInfo);
        byte[] packet = ServerPackets.BuildPublish(MqttQoS.AtLeastOnce, "a/b"u8, packetIdentifier: 1, properties: [],
            payload: ServerPackets.Letters('x', Limit + 1));

        // Act
        connection.Deliver(packet);

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("40 03 00 01 97"));
        PublishCalls(harness).ShouldBeEmpty();
    }

    /// <summary>
    /// Creates a started server whose payload limit is <see cref="Limit"/>.
    /// </summary>
    /// <returns>
    /// The harness.
    /// </returns>
    private static ServerHarness CreateHarness() =>
        ServerHarness.Create(static options => options.MaxPublishPayloadBytes = Limit);

    /// <summary>
    /// Gets the calls of <c>OnPublish</c> the handler recorded, oldest first.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <returns>
    /// The calls as text.
    /// </returns>
    private static List<string> PublishCalls(ServerHarness harness) =>
        [.. harness.Handler.Calls.Where(static call => call.StartsWith("OnPublish", StringComparison.Ordinal))];

    /// <summary>
    /// Gets what the server writes for an accepted publish with packet identifier 1.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the publish.
    /// </param>
    /// <returns>
    /// Nothing at QoS 0, PUBACK at QoS 1, PUBREC at QoS 2.
    /// </returns>
    private static byte[] Acceptance(MqttQoS qos) => qos switch
    {
        MqttQoS.AtLeastOnce => ClientPackets.PubAck1,
        MqttQoS.ExactlyOnce => ClientPackets.PubRec1,
        _ => [],
    };

    /// <summary>
    /// Gets what the server writes for a publish refused as too large.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the publish.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier of the publish.
    /// </param>
    /// <returns>
    /// Nothing at QoS 0, PUBACK 0x97 at QoS 1, PUBREC 0x97 at QoS 2, with the reason string.
    /// </returns>
    private static byte[] Refusal(MqttQoS qos, ushort packetIdentifier) => qos switch
    {
        MqttQoS.AtLeastOnce => ServerPackets.TooLargePubAck(packetIdentifier),
        MqttQoS.ExactlyOnce => ServerPackets.TooLargePubRec(packetIdentifier),
        _ => [],
    };
}
