using System.Buffers;
using System.Text;
using MQTTnet.Packets;
using MQTTnet.Protocol;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests decoding and encoding the PUBLISH packet, including a cross-check against MQTTnet.
/// </summary>
public sealed class PublishCodecTests
{
    /// <summary>
    /// The known PUBLISH packets and the values they carry.
    /// </summary>
    public static TheoryData<byte[], MqttQoS, bool, bool, string, ushort, string> KnownPublishes => new()
    {
        { KnownPackets.PublishQoS0, MqttQoS.AtMostOnce, false, false, "t", 0, string.Empty },
        { KnownPackets.PublishQoS1, MqttQoS.AtLeastOnce, false, false, "a/b", 10, "hi" },
        { KnownPackets.PublishQoS2, MqttQoS.ExactlyOnce, true, true, "t", 1, "x" },
    };

    /// <summary>
    /// Proves each known PUBLISH decodes to its values and is the bytes the encoder writes for them.
    /// </summary>
    /// <param name="packet">
    /// The known packet.
    /// </param>
    /// <param name="qos">
    /// The expected QoS.
    /// </param>
    /// <param name="dup">
    /// The expected DUP flag.
    /// </param>
    /// <param name="retain">
    /// The expected retain flag.
    /// </param>
    /// <param name="topic">
    /// The expected topic.
    /// </param>
    /// <param name="id">
    /// The expected packet identifier.
    /// </param>
    /// <param name="payload">
    /// The expected payload.
    /// </param>
    [Theory]
    [MemberData(nameof(KnownPublishes))]
    public void TryDecode_KnownPublish_DecodesAndIsWhatTheEncoderWrites(
        byte[] packet, MqttQoS qos, bool dup, bool retain, string topic, ushort id, string payload)
    {
        // Arrange
        byte[] topicBytes = Encoding.UTF8.GetBytes(topic);
        byte[] payloadBytes = Encoding.UTF8.GetBytes(payload);
        byte[] destination = new byte[packet.Length];

        // Act
        MqttReasonCode result = PublishCodec.TryDecode(packet, PacketSource.Client, out PublishPacket publish);
        PublishFields fields = new() { Topic = topicBytes, QoS = qos, Dup = dup, Retain = retain, PacketIdentifier = id };
        int size = PublishCodec.GetSize(fields, payloadBytes.Length);
        int written = PublishCodec.Write(destination, fields, payloadBytes);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        publish.QoS.ShouldBe(qos);
        publish.Dup.ShouldBe(dup);
        publish.Retain.ShouldBe(retain);
        Encoding.UTF8.GetString(publish.Topic).ShouldBe(topic);
        publish.PacketIdentifier.ShouldBe(id);
        Encoding.UTF8.GetString(publish.Payload).ShouldBe(payload);
        publish.HasTopicAlias.ShouldBeFalse();
        size.ShouldBe(packet.Length);
        written.ShouldBe(packet.Length);
        destination.ShouldBe(packet);
    }

    /// <summary>
    /// Proves a header written apart followed by the payload equals the packet written whole.
    /// </summary>
    /// <param name="payloadLength">
    /// The payload size.
    /// </param>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(70_000)]
    public void WriteHeader_HeaderThenPayload_EqualsWrite(int payloadLength)
    {
        // Arrange
        byte[] payload = new byte[payloadLength];
        new Random(7).NextBytes(payload);
        byte[] topic = "a/b"u8.ToArray();
        PublishFields fields = new() { Topic = topic, QoS = MqttQoS.AtLeastOnce, PacketIdentifier = 9 };
        byte[] whole = new byte[PublishCodec.GetSize(fields, payloadLength)];
        byte[] header = new byte[PublishCodec.GetHeaderSize(fields, payloadLength)];

        // Act
        int wholeWritten = PublishCodec.Write(whole, fields, payload);
        int headerWritten = PublishCodec.WriteHeader(header, fields, payloadLength);

        // Assert
        wholeWritten.ShouldBe(whole.Length);
        headerWritten.ShouldBe(header.Length);
        header.Concat(payload).ToArray().ShouldBe(whole);
    }

    /// <summary>
    /// Proves the fixed header grows from two to three bytes as the remaining length passes 127, and decoding
    /// still returns the payload.
    /// </summary>
    /// <param name="payloadLength">
    /// The payload size; with "a/b" at QoS 1 the remaining length is the payload plus 8.
    /// </param>
    [Theory]
    [InlineData(119)]
    [InlineData(120)]
    [InlineData(121)]
    public void GetHeaderSize_RemainingLengthAcrossTheBoundary_MatchesTheFixedHeaderLength(int payloadLength)
    {
        // Arrange
        byte[] payload = new byte[payloadLength];
        payload.AsSpan().Fill(0x5A);
        byte[] topic = "a/b"u8.ToArray();
        PublishFields fields = new() { Topic = topic, QoS = MqttQoS.AtLeastOnce, PacketIdentifier = 1 };
        int remainingLength = payloadLength + 8;
        byte[] packet = new byte[PublishCodec.GetSize(fields, payloadLength)];

        // Act
        int headerSize = PublishCodec.GetHeaderSize(fields, payloadLength);
        PublishCodec.Write(packet, fields, payload);
        MqttReasonCode result = PublishCodec.TryDecode(packet, PacketSource.Client, out PublishPacket publish);

        // Assert
        headerSize.ShouldBe(PacketHeader.GetLength(remainingLength) + 8);
        packet.Length.ShouldBe(PacketHeader.GetLength(remainingLength) + remainingLength);
        result.ShouldBe(MqttReasonCode.Success);
        publish.Payload.ToArray().ShouldBe(payload);
    }

    /// <summary>
    /// Proves the smallest and largest packet identifiers survive an encode and decode.
    /// </summary>
    /// <param name="id">
    /// The packet identifier.
    /// </param>
    [Theory]
    [InlineData((ushort)1)]
    [InlineData((ushort)65535)]
    public void Write_IdentifierAtTheEdge_RoundTrips(ushort id)
    {
        // Arrange
        byte[] topic = "t"u8.ToArray();
        PublishFields fields = new() { Topic = topic, QoS = MqttQoS.ExactlyOnce, PacketIdentifier = id };
        byte[] destination = new byte[PublishCodec.GetSize(fields, 0)];

        // Act
        PublishCodec.Write(destination, fields, []);
        MqttReasonCode result = PublishCodec.TryDecode(destination, PacketSource.Server, out PublishPacket publish);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        publish.PacketIdentifier.ShouldBe(id);
    }

    /// <summary>
    /// Proves a packet identifier of zero at QoS 1 is a protocol error and leaves the output at its default.
    /// </summary>
    [Fact]
    public void TryDecode_IdentifierZeroAtQoS1_IsProtocolError()
    {
        // Arrange
        byte[] packet = Hex.Parse("32 06 00 01 74 00 00 00");

        // Act
        MqttReasonCode result = PublishCodec.TryDecode(packet, PacketSource.Client, out PublishPacket publish);

        // Assert
        result.ShouldBe(MqttReasonCode.ProtocolError);
        publish.Topic.IsEmpty.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a Subscription Identifier is refused from a client and read from a server.
    /// </summary>
    [Fact]
    public void TryDecode_SubscriptionIdentifier_RefusedFromClientReadFromServer()
    {
        // Arrange
        byte[] packet = Hex.Parse("30 07 00 01 74 02 0B 05 78");

        // Act
        MqttReasonCode fromClient = PublishCodec.TryDecode(packet, PacketSource.Client, out _);
        MqttReasonCode fromServer = PublishCodec.TryDecode(packet, PacketSource.Server, out PublishPacket publish);

        // Assert
        fromClient.ShouldBe(MqttReasonCode.ProtocolError);
        fromServer.ShouldBe(MqttReasonCode.Success);
        publish.Properties.TryGetInteger(PropertyId.SubscriptionIdentifier, out uint identifier).ShouldBeTrue();
        identifier.ShouldBe(5u);
        publish.Payload.ToArray().ShouldBe("x"u8.ToArray());
    }

    /// <summary>
    /// Proves a Topic Alias is reported, not refused, and that an empty topic is then allowed.
    /// </summary>
    [Fact]
    public void TryDecode_TopicAlias_IsReportedNotRefused()
    {
        // Arrange
        byte[] withTopic = Hex.Parse("30 07 00 01 74 03 23 00 02");
        byte[] withoutTopic = Hex.Parse("30 06 00 00 03 23 00 02");

        // Act
        MqttReasonCode first = PublishCodec.TryDecode(withTopic, PacketSource.Client, out PublishPacket named);
        MqttReasonCode second = PublishCodec.TryDecode(withoutTopic, PacketSource.Client, out PublishPacket aliased);

        // Assert
        first.ShouldBe(MqttReasonCode.Success);
        named.HasTopicAlias.ShouldBeTrue();
        second.ShouldBe(MqttReasonCode.Success);
        aliased.HasTopicAlias.ShouldBeTrue();
        aliased.Topic.IsEmpty.ShouldBeTrue();
    }

    /// <summary>
    /// Proves an empty topic without a Topic Alias is a protocol error.
    /// </summary>
    [Fact]
    public void TryDecode_EmptyTopicWithoutAlias_IsProtocolError()
    {
        // Arrange
        byte[] packet = Hex.Parse("30 03 00 00 00");

        // Act
        MqttReasonCode result = PublishCodec.TryDecode(packet, PacketSource.Client, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.ProtocolError);
    }

    /// <summary>
    /// Proves a topic name with a wildcard is invalid.
    /// </summary>
    /// <param name="topic">
    /// The topic name.
    /// </param>
    [Theory]
    [InlineData("a/#")]
    [InlineData("+")]
    [InlineData("a+b")]
    public void TryDecode_TopicWithWildcard_IsTopicNameInvalid(string topic)
    {
        // Arrange
        byte[] name = Encoding.UTF8.GetBytes(topic);
        byte[] packet = [0x30, (byte)(name.Length + 3), 0, (byte)name.Length, .. name, 0];

        // Act
        MqttReasonCode result = PublishCodec.TryDecode(packet, PacketSource.Client, out PublishPacket publish);

        // Assert
        result.ShouldBe(MqttReasonCode.TopicNameInvalid);
        publish.Topic.IsEmpty.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a topic running past the end of the packet is malformed.
    /// </summary>
    [Fact]
    public void TryDecode_TopicPastThePacket_IsMalformed()
    {
        // Arrange
        byte[] packet = Hex.Parse("30 03 00 05 74");

        // Act
        MqttReasonCode result = PublishCodec.TryDecode(packet, PacketSource.Client, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
    }

    /// <summary>
    /// Proves a body cut short is malformed.
    /// </summary>
    /// <param name="hex">
    /// The broken packet.
    /// </param>
    [Theory]
    [InlineData("30 03 00 01 74")] // no property length
    [InlineData("32 03 00 01 74")] // no identifier
    [InlineData("30 05 00 01 74 02 0B")] // property block runs past the end
    public void TryDecode_BodyCutShort_IsMalformed(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = PublishCodec.TryDecode(packet, PacketSource.Server, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
    }

    /// <summary>
    /// Proves a packet of another type is a caller bug.
    /// </summary>
    [Fact]
    public void TryDecode_NotAPublish_Throws()
    {
        // Arrange
        byte[] packet = Hex.Parse("20 03 00 00 00");

        // Act & Assert
        Should.Throw<ArgumentException>(() => PublishCodec.TryDecode(packet, PacketSource.Client, out _));
    }

    /// <summary>
    /// Proves a multi-byte topic round-trips byte for byte.
    /// </summary>
    /// <param name="topic">
    /// The topic.
    /// </param>
    [Theory]
    [InlineData("ü/x")]
    [InlineData("😀")]
    public void Write_MultiByteTopic_RoundTrips(string topic)
    {
        // Arrange
        byte[] name = Encoding.UTF8.GetBytes(topic);
        PublishFields fields = new() { Topic = name };
        byte[] destination = new byte[PublishCodec.GetSize(fields, 1)];

        // Act
        PublishCodec.Write(destination, fields, [0x41]);
        MqttReasonCode result = PublishCodec.TryDecode(destination, PacketSource.Client, out PublishPacket publish);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        publish.Topic.ToArray().ShouldBe(name);
        Encoding.UTF8.GetString(publish.Topic).ShouldBe(topic);
    }

    /// <summary>
    /// Proves a wrong packet identifier for the QoS throws from GetSize, GetHeaderSize, WriteHeader and Write, and
    /// nothing is written.
    /// </summary>
    /// <param name="qos">
    /// The QoS.
    /// </param>
    /// <param name="id">
    /// The wrong identifier for it.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce, 7)]
    [InlineData(MqttQoS.AtLeastOnce, 0)]
    [InlineData(MqttQoS.ExactlyOnce, 0)]
    public void Write_WrongIdentifierForTheQoS_Throws(MqttQoS qos, ushort id)
    {
        // Arrange
        byte[] topic = "t"u8.ToArray();
        byte[] destination = new byte[32];

        // Act & Assert
        Should.Throw<ArgumentException>(() => PublishCodec.GetSize(new PublishFields { Topic = topic, QoS = qos, PacketIdentifier = id }, 0));
        Should.Throw<ArgumentException>(() => PublishCodec.GetHeaderSize(new PublishFields { Topic = topic, QoS = qos, PacketIdentifier = id }, 0));
        Should.Throw<ArgumentException>(() => PublishCodec.WriteHeader(destination, new PublishFields { Topic = topic, QoS = qos, PacketIdentifier = id }, 0));
        Should.Throw<ArgumentException>(() => PublishCodec.Write(destination, new PublishFields { Topic = topic, QoS = qos, PacketIdentifier = id }, []));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a topic that is empty, holds a wildcard or is over 65,535 bytes throws and nothing is written.
    /// </summary>
    /// <param name="topic">
    /// The unusable topic; "long" stands for 65,536 bytes.
    /// </param>
    [Theory]
    [InlineData("")]
    [InlineData("a/+")]
    [InlineData("#")]
    [InlineData("long")]
    public void Write_InvalidTopic_Throws(string topic)
    {
        // Arrange
        byte[] name = topic == "long" ? new byte[65_536].Select(_ => (byte)'a').ToArray() : Encoding.UTF8.GetBytes(topic);
        byte[] destination = new byte[70_000];

        // Act & Assert
        Should.Throw<ArgumentException>(() => PublishCodec.GetSize(new PublishFields { Topic = name }, 0));
        Should.Throw<ArgumentException>(() => PublishCodec.Write(destination, new PublishFields { Topic = name }, []));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a topic holding U+0000, which the decoder answers with a malformed packet, throws from every sizing
    /// and writing method and writes nothing.
    /// </summary>
    [Fact]
    public void Write_TopicWithNullCharacter_ThrowsWithoutWriting()
    {
        // Arrange
        byte[] topic = Hex.Parse("61 00");
        byte[] destination = new byte[16];

        // Act & Assert
        Should.Throw<ArgumentException>(() => PublishCodec.GetSize(new PublishFields { Topic = topic }, 0));
        Should.Throw<ArgumentException>(() => PublishCodec.GetHeaderSize(new PublishFields { Topic = topic }, 0));
        Should.Throw<ArgumentException>(() => PublishCodec.WriteHeader(destination, new PublishFields { Topic = topic }, 0));
        Should.Throw<ArgumentException>(() => PublishCodec.Write(destination, new PublishFields { Topic = topic }, []));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a packet one byte over the largest remaining length, and a negative payload length, throw from
    /// every sizing and writing method and write nothing.
    /// </summary>
    /// <param name="payloadLength">
    /// The unusable payload length.
    /// </param>
    [Theory]
    [InlineData(268_435_452)]
    [InlineData(-1)]
    public void Write_PayloadLengthOutOfRange_ThrowsWithoutWriting(int payloadLength)
    {
        // Arrange
        byte[] topic = "t"u8.ToArray();
        byte[] destination = new byte[16];

        // Act & Assert
        Should.Throw<ArgumentException>(() => PublishCodec.GetSize(new PublishFields { Topic = topic }, payloadLength));
        Should.Throw<ArgumentException>(() => PublishCodec.GetHeaderSize(new PublishFields { Topic = topic }, payloadLength));
        Should.Throw<ArgumentException>(() => PublishCodec.WriteHeader(destination, new PublishFields { Topic = topic }, payloadLength));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a QoS above 2 throws from every sizing and writing method and writes nothing.
    /// </summary>
    [Fact]
    public void Write_QoSThree_ThrowsWithoutWriting()
    {
        // Arrange
        byte[] topic = "t"u8.ToArray();
        byte[] destination = new byte[16];

        // Act & Assert
        Should.Throw<ArgumentException>(() => PublishCodec.GetSize(new PublishFields { Topic = topic, QoS = (MqttQoS)3, PacketIdentifier = 1 }, 0));
        Should.Throw<ArgumentException>(() => PublishCodec.GetHeaderSize(new PublishFields { Topic = topic, QoS = (MqttQoS)3, PacketIdentifier = 1 }, 0));
        Should.Throw<ArgumentException>(() => PublishCodec.WriteHeader(destination, new PublishFields { Topic = topic, QoS = (MqttQoS)3, PacketIdentifier = 1 }, 0));
        Should.Throw<ArgumentException>(() => PublishCodec.Write(destination, new PublishFields { Topic = topic, QoS = (MqttQoS)3, PacketIdentifier = 1 }, []));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves the DUP flag at QoS 0 is decoded as it is set (plan decision 6).
    /// </summary>
    [Fact]
    public void TryDecode_DupAtQoS0_SucceedsAndReportsDup()
    {
        // Arrange
        byte[] packet = Hex.Parse("38 04 00 01 74 00");

        // Act
        MqttReasonCode result = PublishCodec.TryDecode(packet, PacketSource.Client, out PublishPacket publish);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        publish.Dup.ShouldBeTrue();
        publish.QoS.ShouldBe(MqttQoS.AtMostOnce);
    }

    /// <summary>
    /// Proves a packet over the largest remaining length, and a negative payload length, cannot be sized.
    /// </summary>
    [Fact]
    public void GetSize_PayloadTooLarge_Throws()
    {
        // Arrange
        byte[] topic = "t"u8.ToArray();

        // Act & Assert
        Should.Throw<ArgumentException>(() => PublishCodec.GetSize(new PublishFields { Topic = topic }, 268_435_455));
        Should.Throw<ArgumentException>(() => PublishCodec.GetHeaderSize(new PublishFields { Topic = topic }, int.MaxValue));
        Should.Throw<ArgumentException>(() => PublishCodec.GetSize(new PublishFields { Topic = topic }, -1));
    }

    /// <summary>
    /// Proves the largest payload that fits the largest packet is accepted.
    /// </summary>
    [Fact]
    public void GetHeaderSize_LargestPayload_FillsTheLargestPacket()
    {
        // Arrange
        byte[] topic = "t"u8.ToArray();
        int payloadLength = 268_435_455 - 4;

        // Act
        int headerSize = PublishCodec.GetHeaderSize(new PublishFields { Topic = topic }, payloadLength);

        // Assert
        headerSize.ShouldBe(1 + 4 + 3 + 1);
    }

    /// <summary>
    /// Proves a destination that is too small throws and nothing is written.
    /// </summary>
    [Fact]
    public void Write_DestinationTooSmall_ThrowsWithoutWriting()
    {
        // Arrange
        byte[] topic = "t"u8.ToArray();
        byte[] whole = new byte[5];
        byte[] header = new byte[4];

        // Act & Assert
        Should.Throw<ArgumentException>(() => PublishCodec.Write(whole, new PublishFields { Topic = topic }, "x"u8));
        Should.Throw<ArgumentException>(() => PublishCodec.WriteHeader(header, new PublishFields { Topic = topic }, 1));
        whole.ShouldAllBe(b => b == 0);
        header.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a PUBLISH that MQTTnet encodes, with properties, decodes here to the same values.
    /// </summary>
    [Fact]
    public void TryDecode_PublishMqttNetEncodes_DecodesToTheSameValues()
    {
        // Arrange
        MqttPublishPacket source = new()
        {
            Topic = "a/b",
            QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
            PacketIdentifier = 10,
            Retain = true,
            Payload = new ReadOnlySequence<byte>("hi"u8.ToArray()),
            ContentType = "text/plain",
            ResponseTopic = "r",
            CorrelationData = [1, 2],
            MessageExpiryInterval = 30,
            PayloadFormatIndicator = MqttPayloadFormatIndicator.CharacterData,
            UserProperties = [new MqttUserProperty("k", "v"u8.ToArray())],
        };
        byte[] packet = MqttNetFormatter.Encode(source);

        // Act
        MqttReasonCode result = PublishCodec.TryDecode(packet, PacketSource.Client, out PublishPacket publish);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        publish.QoS.ShouldBe(MqttQoS.AtLeastOnce);
        publish.Retain.ShouldBeTrue();
        publish.Dup.ShouldBeFalse();
        Encoding.UTF8.GetString(publish.Topic).ShouldBe("a/b");
        publish.PacketIdentifier.ShouldBe((ushort)10);
        Encoding.UTF8.GetString(publish.Payload).ShouldBe("hi");
        publish.Properties.TryGetBytes(PropertyId.ContentType, out ReadOnlySpan<byte> contentType).ShouldBeTrue();
        Encoding.UTF8.GetString(contentType).ShouldBe("text/plain");
        publish.Properties.TryGetInteger(PropertyId.MessageExpiryInterval, out uint expiry).ShouldBeTrue();
        expiry.ShouldBe(30u);
        publish.Properties.Contains(PropertyId.ResponseTopic).ShouldBeTrue();
        publish.Properties.Contains(PropertyId.CorrelationData).ShouldBeTrue();
        publish.Properties.Contains(PropertyId.PayloadFormatIndicator).ShouldBeTrue();
        publish.Properties.Contains(PropertyId.UserProperty).ShouldBeTrue();
    }

    /// <summary>
    /// Proves a PUBLISH this codec encodes decodes in MQTTnet to the same values.
    /// </summary>
    [Fact]
    public void Write_Qos2WithDupAndRetain_MqttNetDecodesTheSameValues()
    {
        // Arrange
        byte[] topic = "t"u8.ToArray();
        PublishFields fields = new() { Topic = topic, QoS = MqttQoS.ExactlyOnce, Dup = true, Retain = true, PacketIdentifier = 1 };
        byte[] destination = new byte[PublishCodec.GetSize(fields, 1)];
        PublishCodec.Write(destination, fields, "x"u8);

        // Act
        MqttPublishPacket decoded = (MqttPublishPacket)MqttNetFormatter.Decode(destination);

        // Assert
        decoded.Topic.ShouldBe("t");
        decoded.QualityOfServiceLevel.ShouldBe(MqttQualityOfServiceLevel.ExactlyOnce);
        decoded.Dup.ShouldBeTrue();
        decoded.Retain.ShouldBeTrue();
        decoded.PacketIdentifier.ShouldBe((ushort)1);
        decoded.Payload.ToArray().ShouldBe("x"u8.ToArray());
    }
}
