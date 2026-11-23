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
    /// Proves that a topic the caller vouches for (<see cref="PublishFields.IsTopicValidated"/>), which the encoder
    /// then does not scan, is sized and written exactly as the same topic checked on every call.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the packet.
    /// </param>
    /// <param name="topicLength">
    /// The topic's length in bytes.
    /// </param>
    /// <param name="payloadLength">
    /// The payload size.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce, 1, 0)]
    [InlineData(MqttQoS.AtMostOnce, 20, 64)]
    [InlineData(MqttQoS.AtLeastOnce, 20, 64)]
    [InlineData(MqttQoS.ExactlyOnce, 3, 121)]
    [InlineData(MqttQoS.AtLeastOnce, 60_000, 64)]
    [InlineData(MqttQoS.ExactlyOnce, 65_535, 70_000)]
    public void Write_TopicTheCallerVouchesFor_GivesTheSameSizesAndBytesAsTheCheckedTopic(MqttQoS qos,
        int topicLength, int payloadLength)
    {
        // Arrange
        byte[] topic = TopicOf(topicLength);
        byte[] payload = new byte[payloadLength];
        new Random(11).NextBytes(payload);
        ushort identifier = qos == MqttQoS.AtMostOnce ? (ushort)0 : (ushort)9;
        PublishFields scanned = new() { Topic = topic, QoS = qos, PacketIdentifier = identifier };
        PublishFields vouched = new()
        {
            Topic = topic, QoS = qos, PacketIdentifier = identifier, IsTopicValidated = true,
        };
        byte[] wholeScanned = new byte[PublishCodec.GetSize(scanned, payloadLength)];
        byte[] headerScanned = new byte[PublishCodec.GetHeaderSize(scanned, payloadLength)];
        PublishCodec.Write(wholeScanned, scanned, payload);
        PublishCodec.WriteHeader(headerScanned, scanned, payloadLength);

        // Act
        int size = PublishCodec.GetSize(vouched, payloadLength);
        int headerSize = PublishCodec.GetHeaderSize(vouched, payloadLength);
        byte[] whole = new byte[size];
        byte[] header = new byte[headerSize];
        int wholeWritten = PublishCodec.Write(whole, vouched, payload);
        int headerWritten = PublishCodec.WriteHeader(header, vouched, payloadLength);

        // Assert
        size.ShouldBe(wholeScanned.Length);
        headerSize.ShouldBe(headerScanned.Length);
        wholeWritten.ShouldBe(size);
        headerWritten.ShouldBe(headerSize);
        whole.AsSpan().SequenceEqual(wholeScanned).ShouldBeTrue();
        header.AsSpan().SequenceEqual(headerScanned).ShouldBeTrue();
    }

    /// <summary>
    /// Proves that vouching for the topic leaves out the scan of the topic and nothing else: every fault that is
    /// found without reading the topic's bytes still throws from every sizing and writing method, and nothing is
    /// written.
    /// </summary>
    /// <param name="fault">
    /// The fault: <c>"qos"</c>, <c>"identifier at QoS 0"</c>, <c>"no identifier"</c>, <c>"dup"</c>,
    /// <c>"empty topic"</c>, <c>"long topic"</c>, <c>"negative payload"</c> or <c>"remaining length"</c>.
    /// </param>
    [Theory]
    [InlineData("qos")]
    [InlineData("identifier at QoS 0")]
    [InlineData("no identifier")]
    [InlineData("dup")]
    [InlineData("empty topic")]
    [InlineData("long topic")]
    [InlineData("negative payload")]
    [InlineData("remaining length")]
    public void Write_TopicTheCallerVouchesForWithAnotherFault_StillThrowsWithoutWriting(string fault)
    {
        // Arrange
        byte[] topic = fault switch
        {
            "empty topic" => [],
            "long topic" => TopicOf(65_536),
            _ => "t"u8.ToArray(),
        };
        MqttQoS qos = fault switch
        {
            "qos" => (MqttQoS)3,
            "no identifier" => MqttQoS.AtLeastOnce,
            _ => MqttQoS.AtMostOnce,
        };
        ushort identifier = fault is "identifier at QoS 0" or "qos" ? (ushort)7 : (ushort)0;
        bool dup = fault == "dup";
        int payloadLength = fault switch
        {
            "negative payload" => -1,
            "remaining length" => 268_435_452,
            _ => 0,
        };
        byte[] destination = new byte[70_000];

        // Act & Assert
        Should.Throw<ArgumentException>(() => PublishCodec.GetSize(Vouched(), payloadLength));
        Should.Throw<ArgumentException>(() => PublishCodec.GetHeaderSize(Vouched(), payloadLength));
        Should.Throw<ArgumentException>(() => PublishCodec.WriteHeader(destination, Vouched(), payloadLength));
        if (payloadLength == 0)
        {
            Should.Throw<ArgumentException>(() => PublishCodec.Write(destination, Vouched(), []));
        }

        destination.AsSpan().IndexOfAnyExcept((byte)0).ShouldBe(-1);

        PublishFields Vouched() => new()
        {
            Topic = topic, QoS = qos, PacketIdentifier = identifier, Dup = dup, IsTopicValidated = true,
        };
    }

    /// <summary>
    /// Proves that a destination too small for a vouched-for topic still throws and nothing is written.
    /// </summary>
    [Fact]
    public void Write_TopicTheCallerVouchesForIntoADestinationTooSmall_ThrowsWithoutWriting()
    {
        // Arrange
        byte[] topic = "t"u8.ToArray();
        byte[] whole = new byte[5];
        byte[] header = new byte[4];

        // Act & Assert
        Should.Throw<ArgumentException>(() =>
            PublishCodec.Write(whole, new PublishFields { Topic = topic, IsTopicValidated = true }, "x"u8));
        Should.Throw<ArgumentException>(() =>
            PublishCodec.WriteHeader(header, new PublishFields { Topic = topic, IsTopicValidated = true }, 1));
        whole.ShouldAllBe(b => b == 0);
        header.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves that the Remaining Length measured from the lengths alone is the one the sizes are built on, for a
    /// packet that can exist, and that it is computed in 64 bits, so that lengths no packet can have do not wrap.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the packet.
    /// </param>
    /// <param name="topicLength">
    /// The topic's length in bytes.
    /// </param>
    /// <param name="payloadLength">
    /// The payload size.
    /// </param>
    /// <param name="expected">
    /// The Remaining Length: two bytes of length and the topic, two of identifier at QoS 1 and 2, one of property
    /// length, and the payload.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce, 1, 0, 4L)]
    [InlineData(MqttQoS.AtLeastOnce, 3, 121, 129L)]
    [InlineData(MqttQoS.ExactlyOnce, 65_526, 10, 65_541L)]
    [InlineData(MqttQoS.AtMostOnce, 1, 268_435_451, 268_435_455L)]
    [InlineData(MqttQoS.AtMostOnce, 1, 268_435_452, 268_435_456L)]
    [InlineData(MqttQoS.ExactlyOnce, 65_535, int.MaxValue, 2_147_549_187L)]
    public void MeasureRemainingLength_FromTheLengthsAlone_IsWhatTheSizesAreBuiltOn(MqttQoS qos, int topicLength,
        int payloadLength, long expected)
    {
        // Arrange
        byte[] topic = TopicOf(topicLength);
        ushort identifier = qos == MqttQoS.AtMostOnce ? (ushort)0 : (ushort)9;

        // Act
        long measured = PublishCodec.MeasureRemainingLength(topicLength, qos, payloadLength);

        // Assert
        measured.ShouldBe(expected);
        if (measured <= VariableByteInteger.MaxValue)
        {
            PublishFields fields = new() { Topic = topic, QoS = qos, PacketIdentifier = identifier };
            PublishCodec.GetSize(fields, payloadLength)
                .ShouldBe(PacketHeader.GetLength((int)measured) + (int)measured);
        }
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
    /// Proves a PUBLISH whose Response Topic is not a topic name, empty or holding a wildcard, is a protocol error
    /// and leaves the output at its default.
    /// </summary>
    /// <param name="hex">
    /// The PUBLISH with the topic "t" and the invalid Response Topic.
    /// </param>
    [Theory]
    [InlineData("30 0E 00 01 74 0A 08 00 07 72 65 70 6C 79 2F 2B")] // "reply/+"
    [InlineData("30 0E 00 01 74 0A 08 00 07 72 65 70 6C 79 2F 23")] // "reply/#"
    [InlineData("30 07 00 01 74 03 08 00 00")] // empty
    public void TryDecode_ResponseTopicNotATopicName_IsProtocolError(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = PublishCodec.TryDecode(packet, PacketSource.Client, out PublishPacket publish);

        // Assert
        result.ShouldBe(MqttReasonCode.ProtocolError);
        publish.Topic.IsEmpty.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a PUBLISH whose Response Topic is a topic name decodes and reports the property.
    /// </summary>
    [Fact]
    public void TryDecode_ResponseTopicThatIsATopicName_Succeeds()
    {
        // Arrange: topic "t", Response Topic "reply/x".
        byte[] packet = Hex.Parse("30 0E 00 01 74 0A 08 00 07 72 65 70 6C 79 2F 78");

        // Act
        MqttReasonCode result = PublishCodec.TryDecode(packet, PacketSource.Client, out PublishPacket publish);
        bool found = publish.Properties.TryGetBytes(PropertyId.ResponseTopic, out ReadOnlySpan<byte> responseTopic);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        found.ShouldBeTrue();
        Encoding.UTF8.GetString(responseTopic).ShouldBe("reply/x");
    }

    /// <summary>
    /// Proves DUP at QoS 0, which a sender must clear (MQTT-3.3.1-2), throws from every sizing and writing method,
    /// with and without retain, and writes nothing.
    /// </summary>
    /// <param name="retain">
    /// Whether the retain flag is set as well.
    /// </param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Write_DupAtQoS0_ThrowsWithoutWriting(bool retain)
    {
        // Arrange
        byte[] topic = "t"u8.ToArray();
        byte[] destination = new byte[16];

        // Act & Assert
        Should.Throw<ArgumentException>(() => PublishCodec.GetSize(new PublishFields { Topic = topic, Dup = true, Retain = retain }, 0))
            .ParamName.ShouldBe("Dup");
        Should.Throw<ArgumentException>(() => PublishCodec.GetHeaderSize(new PublishFields { Topic = topic, Dup = true, Retain = retain }, 0));
        Should.Throw<ArgumentException>(() => PublishCodec.WriteHeader(destination, new PublishFields { Topic = topic, Dup = true, Retain = retain }, 0));
        Should.Throw<ArgumentException>(() => PublishCodec.Write(destination, new PublishFields { Topic = topic, Dup = true, Retain = retain }, []));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves DUP is still written at QoS 1 and 2, where a sender may set it.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the packet.
    /// </param>
    /// <param name="firstByte">
    /// The expected first byte: PUBLISH with DUP and the QoS.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtLeastOnce, 0x3A)]
    [InlineData(MqttQoS.ExactlyOnce, 0x3C)]
    public void Write_DupAtQoS1And2_WritesTheFlag(MqttQoS qos, byte firstByte)
    {
        // Arrange
        byte[] topic = "t"u8.ToArray();
        PublishFields fields = new() { Topic = topic, QoS = qos, Dup = true, PacketIdentifier = 1 };
        byte[] destination = new byte[PublishCodec.GetSize(fields, 0)];

        // Act
        int written = PublishCodec.Write(destination, fields, []);

        // Assert
        written.ShouldBe(destination.Length);
        destination[0].ShouldBe(firstByte);
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

    /// <summary>
    /// Builds a valid topic name of a length: levels of seven letters.
    /// </summary>
    /// <param name="length">
    /// The topic's length in bytes, at least 1.
    /// </param>
    /// <returns>
    /// The topic as UTF-8.
    /// </returns>
    private static byte[] TopicOf(int length)
    {
        byte[] topic = new byte[length];
        for (int i = 0; i < topic.Length; i++)
        {
            topic[i] = i % 8 == 7 && i != topic.Length - 1 ? (byte)'/' : (byte)('a' + (i % 8));
        }

        return topic;
    }
}
