using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests publishing from the client at QoS 0, 1 and 2: what is written, when the request completes, the server's
/// limits, the order of the writes, and the acknowledgements the read loop handles.
/// </summary>
public sealed class PublishTests
{
    /// <summary>
    /// The payload of the known publish packets, "hi".
    /// </summary>
    private static readonly byte[] s_hi = "hi"u8.ToArray();

    /// <summary>
    /// Topics a caller cannot publish to: empty, a single-level wildcard, a multi-level wildcard, U+0000, an
    /// unpaired surrogate, and 65,536 UTF-8 bytes.
    /// </summary>
    private static readonly string[] s_topicsThatCannotBeSent =
    [
        string.Empty,
        "a/+",
        "a/#",
        "a\0b",
        "a\uD800",
        new('a', 65_536),
    ];

    /// <summary>
    /// Proves a QoS 0 publish writes its packet and completes at once with success.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_QoS0_WritesThePacketAndCompletesAtOnce()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool: pool);

        // Act
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        bool completedAtOnce = publish.IsCompleted;
        MqttRequestResult result = await publish;

        // Assert
        completedAtOnce.ShouldBeTrue();
        result.ShouldBe(new MqttRequestResult(MqttRequestStatus.Completed, MqttReasonCode.Success));
        harness.NextPacket().ShouldBe(ClientPackets.PublishQoS0);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a QoS 1 publish stays pending until its PUBACK, then completes with the PUBACK's reason code.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_QoS1_CompletesOnPubAck()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        byte[] written = harness.NextPacket();
        bool completedBeforePubAck = publish.IsCompleted;

        // Act
        harness.Link.Deliver(ClientPackets.PubAck1);
        MqttRequestResult result = await publish;

        // Assert
        written.ShouldBe(ClientPackets.PublishQoS1);
        completedBeforePubAck.ShouldBeFalse();
        result.ShouldBe(new MqttRequestResult(MqttRequestStatus.Completed, MqttReasonCode.Success));
        result.IsSuccess.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a QoS 1 publish the server refuses in its PUBACK completes with the server's code and is not a
    /// success.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_QoS1Refused_CompletesWithTheServersCode()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(Hex.Parse("40 03 00 01 97"));
        MqttRequestResult result = await publish;

        // Assert
        result.ShouldBe(new MqttRequestResult(MqttRequestStatus.Completed, MqttReasonCode.QuotaExceeded));
        result.IsSuccess.ShouldBeFalse();
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a QoS 2 publish answers the PUBREC with PUBREL, stays pending, and completes on the PUBCOMP.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_QoS2_SendsPubRelOnPubRecAndCompletesOnPubComp()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool: pool);
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.ExactlyOnce);
        byte[] written = harness.NextPacket();

        // Act
        harness.Link.Deliver(ClientPackets.PubRec1);
        byte[] pubRel = harness.NextPacket();
        bool completedBeforePubComp = publish.IsCompleted;
        harness.Link.Deliver(ClientPackets.PubComp1);
        MqttRequestResult result = await publish;

        // Assert
        written.ShouldBe(ClientPackets.PublishQoS2);
        pubRel.ShouldBe(ClientPackets.PubRel1);
        completedBeforePubComp.ShouldBeFalse();
        result.ShouldBe(new MqttRequestResult(MqttRequestStatus.Completed, MqttReasonCode.Success));
        harness.Link.TakeWritten().ShouldBeEmpty();
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a QoS 2 publish refused at PUBREC completes with the PUBREC's code and sends no PUBREL.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_QoS2RefusedAtPubRec_CompletesWithoutPubRel()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.ExactlyOnce);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(Hex.Parse("50 03 00 01 97"));
        MqttRequestResult result = await publish;

        // Assert
        result.ShouldBe(new MqttRequestResult(MqttRequestStatus.Completed, MqttReasonCode.QuotaExceeded));
        harness.Link.TakeWritten().ShouldBeEmpty();
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves that with the server's Receive Maximum of 2 reached, a third QoS 1 publish fails at once and sends
    /// nothing, while a QoS 0 publish still goes out.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_ReceiveMaximumOfTwoReached_FailsAtOnceAndSendsNothing()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(ClientPackets.ConnAckReceiveMaximum2);
        ValueTask<MqttRequestResult> first = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        ValueTask<MqttRequestResult> second = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        byte[] firstWritten = harness.NextPacket();
        byte[] secondWritten = harness.NextPacket();

        // Act
        ValueTask<MqttRequestResult> third = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        bool thirdCompletedAtOnce = third.IsCompleted;
        MqttRequestResult thirdResult = await third;
        byte[] writtenAfterTheThird = harness.Link.TakeWritten();
        MqttRequestResult qos0Result = await harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);

        // Assert
        firstWritten.ShouldBe(ClientPackets.PublishQoS1);
        secondWritten.ShouldBe(Hex.Parse("32 0A 00 03 61 2F 62 00 02 00 68 69"));
        first.IsCompleted.ShouldBeFalse();
        second.IsCompleted.ShouldBeFalse();
        thirdCompletedAtOnce.ShouldBeTrue();
        thirdResult.Status.ShouldBe(MqttRequestStatus.ReceiveMaximumReached);
        writtenAfterTheThird.ShouldBeEmpty();
        qos0Result.ShouldBe(new MqttRequestResult(MqttRequestStatus.Completed, MqttReasonCode.Success));
        harness.NextPacket().ShouldBe(ClientPackets.PublishQoS0);
    }

    /// <summary>
    /// Proves a PUBACK frees its publish slot, so a publish that the Receive Maximum held back can be sent.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_AfterAPubAckFreesASlot_SendsThePublish()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(ClientPackets.ConnAckReceiveMaximum2);
        ValueTask<MqttRequestResult> first = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        harness.NextPacket();
        harness.NextPacket();
        harness.Link.Deliver(ClientPackets.PubAck1);
        await first;

        // Act
        ValueTask<MqttRequestResult> third = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);

        // Assert
        third.IsCompleted.ShouldBeFalse();
        harness.NextPacket().ShouldBe(Hex.Parse("32 0A 00 03 61 2F 62 00 03 00 68 69"));
    }

    /// <summary>
    /// Proves a server Receive Maximum above 1,024 is capped at 1,024: the 1,025th unacknowledged QoS 1 publish
    /// fails at once and is not written.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_ReceiveMaximumOver1024_Uses1024()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(ClientPackets.ConnAckReceiveMaximum65535);
        int pendingCount = 0;
        for (int i = 0; i < 1024; i++)
        {
            if (!harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce).IsCompleted)
            {
                pendingCount++;
            }
        }

        int writesBefore = harness.Link.WriteCount;

        // Act
        MqttRequestResult result = await harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);

        // Assert
        pendingCount.ShouldBe(1024);
        result.Status.ShouldBe(MqttRequestStatus.ReceiveMaximumReached);
        harness.Link.WriteCount.ShouldBe(writesBefore);
        writesBefore.ShouldBe(1025);
    }

    /// <summary>
    /// Proves a publish above the server's Maximum QoS fails at once with <see cref="MqttRequestStatus.QoSNotSupported"/>
    /// and sends nothing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_AboveMaximumQoS_FailsWithQoSNotSupported()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(ClientPackets.ConnAckMaximumQoS1);

        // Act
        MqttRequestResult result = await harness.Client.PublishAsync("a/b", s_hi, MqttQoS.ExactlyOnce);

        // Assert
        result.Status.ShouldBe(MqttRequestStatus.QoSNotSupported);
        harness.Link.TakeWritten().ShouldBeEmpty();
    }

    /// <summary>
    /// Proves a publish one byte over the server's Maximum Packet Size fails at once with
    /// <see cref="MqttRequestStatus.PacketTooLarge"/> and sends nothing, while one of exactly that size goes out.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_OverMaximumPacketSize_FailsWithPacketTooLarge()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(ClientPackets.ConnAckMaximumPacketSize32);

        // Act
        MqttRequestResult tooLarge = await harness.Client.PublishAsync("a/b", new byte[23], MqttQoS.AtLeastOnce);
        byte[] writtenAfterTooLarge = harness.Link.TakeWritten();
        ValueTask<MqttRequestResult> fits = harness.Client.PublishAsync("a/b", new byte[22], MqttQoS.AtLeastOnce);

        // Assert
        tooLarge.Status.ShouldBe(MqttRequestStatus.PacketTooLarge);
        writtenAfterTooLarge.ShouldBeEmpty();
        fits.IsCompleted.ShouldBeFalse();
        harness.NextPacket().Length.ShouldBe(32);
    }

    /// <summary>
    /// Proves a publish before the client is connected fails at once with
    /// <see cref="MqttRequestStatus.NotConnected"/> and opens nothing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_NotConnected_FailsWithNotConnected()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);

        // Act
        MqttRequestResult result = await harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);

        // Assert
        result.Status.ShouldBe(MqttRequestStatus.NotConnected);
        harness.Connector.Links.ShouldBeEmpty();
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a publish after the connection has ended fails at once with
    /// <see cref="MqttRequestStatus.NotConnected"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_AfterTheConnectionEnded_FailsWithNotConnected()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        harness.Link.EndFromPeer();

        // Act
        MqttRequestResult result = await harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);

        // Assert
        result.Status.ShouldBe(MqttRequestStatus.NotConnected);
        harness.Link.TakeWritten().ShouldBeEmpty();
    }

    /// <summary>
    /// Proves a topic MQTT cannot carry as a topic name throws <see cref="ArgumentException"/> before anything is
    /// written or any packet identifier is used: the next valid QoS 1 publish still gets identifier 1.
    /// </summary>
    /// <param name="index">
    /// The index of the topic in <see cref="s_topicsThatCannotBeSent"/>.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public async Task PublishAsync_TopicCannotBeSent_ThrowsAndWritesNothing(int index)
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool: pool);
        string topic = s_topicsThatCannotBeSent[index];

        // Act
        Exception? exception = Record.Exception(() => { _ = harness.Client.PublishAsync(topic, s_hi, MqttQoS.AtLeastOnce); });
        byte[] writtenAfterTheThrow = harness.Link.TakeWritten();
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);

        // Assert
        ArgumentException argumentException = exception.ShouldBeOfType<ArgumentException>();
        argumentException.ParamName.ShouldBe("topic");
        writtenAfterTheThrow.ShouldBeEmpty();
        harness.NextPacket().ShouldBe(ClientPackets.PublishQoS1);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a topic too long to encode on the stack is encoded through a rented buffer, written whole, and its
    /// buffer returned.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_LongTopic_WritesItAndReturnsEveryBuffer()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool: pool);
        string topic = new('t', 300);

        // Act
        await harness.Client.PublishAsync(topic, s_hi, MqttQoS.AtMostOnce);
        byte[] written = harness.NextPacket();

        // Assert
        written.Length.ShouldBe(308);
        written.AsSpan(5, 300).ToArray().ShouldAllBe(b => b == (byte)'t');
        written.AsSpan(305).ToArray().ShouldBe([(byte)0, (byte)'h', (byte)'i']);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a null topic throws <see cref="ArgumentNullException"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_NullTopic_ThrowsArgumentNull()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();

        // Act
        Exception? exception = Record.Exception(() => { _ = harness.Client.PublishAsync(null!, s_hi, MqttQoS.AtMostOnce); }); // The null is the point of the test.

        // Assert
        exception.ShouldBeOfType<ArgumentNullException>().ParamName.ShouldBe("topic");
        harness.Link.TakeWritten().ShouldBeEmpty();
    }

    /// <summary>
    /// Proves a QoS above 2 throws <see cref="ArgumentOutOfRangeException"/> and sends nothing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_QoSAbove2_ThrowsArgumentOutOfRange()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();

        // Act
        Exception? exception = Record.Exception(() => { _ = harness.Client.PublishAsync("a/b", s_hi, (MqttQoS)3); });

        // Assert
        exception.ShouldBeOfType<ArgumentOutOfRangeException>().ParamName.ShouldBe("qos");
        harness.Link.TakeWritten().ShouldBeEmpty();
    }

    /// <summary>
    /// Proves the payload is copied before <c>PublishAsync</c> returns: changing it while the write is still held
    /// does not change what is sent.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_PayloadChangedAfterTheCall_SendsTheOriginalBytes()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        byte[] payload = "hi"u8.ToArray();
        harness.Link.HoldWrites();
        _ = harness.Client.PublishAsync("a/b", payload, MqttQoS.AtMostOnce);

        // Act
        payload[0] = (byte)'X';
        payload[1] = (byte)'Y';
        harness.Link.CompleteHeldWrites();

        // Assert
        harness.NextPacket().ShouldBe(ClientPackets.PublishQoS0);
    }

    /// <summary>
    /// Proves publishes made while a write is held go out afterwards in the order of the calls.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_ThreeCallsWhileAWriteIsHeld_WritesInCallOrder()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        harness.Link.HoldWrites();
        _ = harness.Client.PublishAsync("a/b", "1"u8.ToArray(), MqttQoS.AtMostOnce);
        _ = harness.Client.PublishAsync("a/b", "2"u8.ToArray(), MqttQoS.AtMostOnce);
        _ = harness.Client.PublishAsync("a/b", "3"u8.ToArray(), MqttQoS.AtMostOnce);
        int writesWhileHeld = harness.Link.WriteCount;

        // Act
        harness.Link.CompleteHeldWrites();
        await harness.Link.WhenWrittenAsync(writesWhileHeld + 3);

        // Assert
        harness.NextPacket().ShouldBe(Hex.Parse("30 07 00 03 61 2F 62 00 31"));
        harness.NextPacket().ShouldBe(Hex.Parse("30 07 00 03 61 2F 62 00 32"));
        harness.NextPacket().ShouldBe(Hex.Parse("30 07 00 03 61 2F 62 00 33"));
    }

    /// <summary>
    /// Proves publishes still waiting for their acknowledgement complete with
    /// <see cref="MqttRequestStatus.ConnectionLost"/> when the connection ends, also one already released with
    /// PUBREL, and every buffer comes back.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_ConnectionLostInFlight_CompletesWithConnectionLost()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool: pool);
        ValueTask<MqttRequestResult> qos1 = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        ValueTask<MqttRequestResult> qos2 = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.ExactlyOnce);
        ValueTask<MqttRequestResult> qos2AfterPubRec = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.ExactlyOnce);
        harness.Link.Deliver(Hex.Parse("50 02 00 03"));
        harness.NextPacket();
        harness.NextPacket();
        harness.NextPacket();
        byte[] pubRel = harness.NextPacket();

        // Act
        harness.Link.EndFromPeer();
        MqttRequestResult qos1Result = await qos1;
        MqttRequestResult qos2Result = await qos2;
        MqttRequestResult qos2AfterPubRecResult = await qos2AfterPubRec;

        // Assert
        pubRel.ShouldBe(Hex.Parse("62 02 00 03"));
        qos1Result.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
        qos2Result.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
        qos2AfterPubRecResult.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ConnectionLost, MqttReasonCode.Success, null, null)]);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a publish whose write fails ends the connection with <see cref="MqttClientCloseReason.WriteFailed"/>,
    /// aborts the link, and completes with <see cref="MqttRequestStatus.ConnectionLost"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_WriteFails_EndsWithWriteFailed()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool: pool);
        IOException error = new("write failed");
        harness.Link.FailWrites(error);

        // Act
        MqttRequestResult result = await harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);

        // Assert
        result.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
        harness.Link.IsAborted.ShouldBeTrue();
        harness.Client.IsConnected.ShouldBeFalse();
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.WriteFailed, MqttReasonCode.Success, null, error)]);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a PUBACK for an identifier not in use is ignored: nothing is written and the connection goes on.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_PubAckForUnknownIdentifier_IsIgnored()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();

        // Act
        harness.Link.Deliver(Hex.Parse("40 02 00 07"));

        // Assert
        harness.Client.IsConnected.ShouldBeTrue();
        harness.Link.TakeWritten().ShouldBeEmpty();
        harness.Closed.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves a PUBCOMP for an identifier not in use is ignored: nothing is written and the connection goes on.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_PubCompForUnknownIdentifier_IsIgnored()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();

        // Act
        harness.Link.Deliver(Hex.Parse("70 02 00 07"));

        // Assert
        harness.Client.IsConnected.ShouldBeTrue();
        harness.Link.TakeWritten().ShouldBeEmpty();
        harness.Closed.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves a PUBREC for an identifier not in use is answered with PUBREL 0x92, packet identifier not found,
    /// and the connection goes on.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_PubRecForUnknownIdentifier_AnswersPubRelNotFound()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool: pool);

        // Act
        harness.Link.Deliver(Hex.Parse("50 02 00 07"));

        // Assert
        harness.NextPacket().ShouldBe(ClientPackets.PubRelNotFound7);
        harness.Client.IsConnected.ShouldBeTrue();
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves two PUBRECs that arrive in one read are answered with their PUBRELs in the order they arrived.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_TwoPubRecsInOneRead_SendsPubRelsInOrder()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.ExactlyOnce);
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.ExactlyOnce);
        harness.NextPacket();
        harness.NextPacket();

        // Act
        harness.Link.Deliver(Hex.Parse("50 02 00 02 50 02 00 01"));

        // Assert
        harness.NextPacket().ShouldBe(Hex.Parse("62 02 00 02"));
        harness.NextPacket().ShouldBe(ClientPackets.PubRel1);
    }

    /// <summary>
    /// Proves a PUBACK for the identifier of a QoS 2 publish is an acknowledgement of the wrong kind: the client
    /// sends DISCONNECT 0x82 and the publish completes with <see cref="MqttRequestStatus.ConnectionLost"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_PubAckForQoS2Publish_SendsDisconnect82()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.ExactlyOnce);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(ClientPackets.PubAck1);
        byte[] disconnect = harness.NextPacket();
        harness.Link.EndFromPeer();
        MqttRequestResult result = await publish;

        // Assert
        disconnect.ShouldBe(ClientPackets.DisconnectProtocolError);
        result.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.ProtocolError, null, null)]);
    }

    /// <summary>
    /// Proves a PUBCOMP for a QoS 2 publish that has had no PUBREC is an acknowledgement of the wrong kind: the
    /// client sends DISCONNECT 0x82 and the publish completes with <see cref="MqttRequestStatus.ConnectionLost"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_PubCompBeforePubRec_SendsDisconnect82()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.ExactlyOnce);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(ClientPackets.PubComp1);
        byte[] disconnect = harness.NextPacket();
        harness.Link.EndFromPeer();
        MqttRequestResult result = await publish;

        // Assert
        disconnect.ShouldBe(ClientPackets.DisconnectProtocolError);
        result.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.ProtocolError, null, null)]);
    }

    /// <summary>
    /// Creates a client and connects it with the given CONNACK, taking the CONNECT it wrote.
    /// </summary>
    /// <param name="connAck">
    /// The server's CONNACK, or <see langword="null"/> for a plain accepting one.
    /// </param>
    /// <param name="pool">
    /// The pool the client rents from, or <see langword="null"/> for the shared pool.
    /// </param>
    /// <returns>
    /// The harness around the connected client.
    /// </returns>
    private static async Task<ClientHarness> ConnectAsync(byte[]? connAck = null, CountingPool? pool = null)
    {
        ClientHarness harness = ClientHarness.Create(pool: pool);
        await harness.ConnectAsync(connAck ?? ClientPackets.ConnAckAccepted);
        harness.NextPacket();
        return harness;
    }
}
