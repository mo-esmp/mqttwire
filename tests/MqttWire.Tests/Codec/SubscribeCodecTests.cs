using System.Text;
using MQTTnet.Packets;
using MQTTnet.Protocol;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests decoding and encoding SUBSCRIBE and SUBACK, including a cross-check against MQTTnet.
/// </summary>
public class SubscribeCodecTests
{
    /// <summary>
    /// Every packet type of the family at the smallest and largest packet identifier.
    /// </summary>
    public static TheoryData<byte, ushort> EveryPacketAtBothEdges => new()
    {
        { (byte)PacketType.Subscribe, 1 },
        { (byte)PacketType.Subscribe, 65535 },
        { (byte)PacketType.SubAck, 1 },
        { (byte)PacketType.SubAck, 65535 },
        { (byte)PacketType.Unsubscribe, 1 },
        { (byte)PacketType.Unsubscribe, 65535 },
        { (byte)PacketType.UnsubAck, 1 },
        { (byte)PacketType.UnsubAck, 65535 },
    };

    /// <summary>
    /// Proves the known SUBSCRIBE decodes to its values and is the bytes the encoder writes for them.
    /// </summary>
    [Fact]
    public void TryDecode_KnownSubscribe_DecodesAndIsWhatTheEncoderWrites()
    {
        // Arrange
        byte[] packet = KnownPackets.Subscribe;
        byte[] destination = new byte[packet.Length];

        // Act
        MqttReasonCode result = SubscribeCodec.TryDecode(packet, out SubscribePacket subscribe);
        int size = SubscribeCodec.GetSize("a/+"u8);
        int written = SubscribeCodec.Write(destination, 1, "a/+"u8, MqttQoS.AtLeastOnce);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        subscribe.PacketIdentifier.ShouldBe((ushort)1);
        subscribe.Count.ShouldBe(1);
        size.ShouldBe(packet.Length);
        written.ShouldBe(packet.Length);
        destination.ShouldBe(packet);
    }

    /// <summary>
    /// Proves the known SUBACK decodes to its values and is the bytes the encoder writes for them.
    /// </summary>
    [Fact]
    public void TryDecode_KnownSubAck_DecodesAndIsWhatTheEncoderWrites()
    {
        // Arrange
        byte[] packet = KnownPackets.SubAck;
        byte[] destination = new byte[packet.Length];
        MqttReasonCode[] codes = [MqttReasonCode.GrantedQoS1];

        // Act
        MqttReasonCode result = SubAckCodec.TryDecode(packet, out SubAckPacket subAck);
        int size = SubAckCodec.GetSize(1, null);
        int written = SubAckCodec.Write(destination, 1, codes, null);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        subAck.PacketIdentifier.ShouldBe((ushort)1);
        subAck.ReasonCodes.ToArray().ShouldBe([(byte)MqttReasonCode.GrantedQoS1]);
        subAck.Properties.Contains(PropertyId.ReasonString).ShouldBeFalse();
        size.ShouldBe(packet.Length);
        written.ShouldBe(packet.Length);
        destination.ShouldBe(packet);
    }

    /// <summary>
    /// Proves a SUBACK with a reason string writes it as property 0x1F and decodes it back.
    /// </summary>
    [Fact]
    public void Write_SubAckWithReasonString_WritesThePropertyAndDecodesIt()
    {
        // Arrange
        MqttReasonCode[] codes = [MqttReasonCode.Success, MqttReasonCode.NotAuthorized];
        byte[] destination = new byte[SubAckCodec.GetSize(2, "x")];

        // Act
        int written = SubAckCodec.Write(destination, 5, codes, "x");
        MqttReasonCode result = SubAckCodec.TryDecode(destination, out SubAckPacket subAck);

        // Assert
        written.ShouldBe(destination.Length);
        destination.ShouldBe(Hex.Parse("90 09 00 05 04 1F 00 01 78 00 87"));
        result.ShouldBe(MqttReasonCode.Success);
        subAck.ReasonCodes.ToArray().ShouldBe([(byte)MqttReasonCode.Success, (byte)MqttReasonCode.NotAuthorized]);
        subAck.Properties.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> text).ShouldBeTrue();
        Encoding.UTF8.GetString(text).ShouldBe("x");
    }

    /// <summary>
    /// Proves every subscription of a SUBSCRIBE and each of its options is read, in order.
    /// </summary>
    [Fact]
    public void GetSubscriptions_TwoSubscriptions_ReadsEveryFilterAndOption()
    {
        // Arrange
        byte[] packet = KnownPackets.SubscribeTwo;
        List<string> filters = [];
        List<(MqttQoS Qos, bool NoLocal, bool Retain, byte Handling)> options = [];

        // Act
        MqttReasonCode result = SubscribeCodec.TryDecode(packet, out SubscribePacket subscribe);
        foreach (Subscription subscription in subscribe.GetSubscriptions())
        {
            filters.Add(Encoding.UTF8.GetString(subscription.TopicFilter));
            options.Add((subscription.MaximumQoS, subscription.NoLocal, subscription.RetainAsPublished, subscription.RetainHandling));
        }

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        subscribe.Count.ShouldBe(2);
        filters.ShouldBe(["a/+", "x"]);
        options.Count.ShouldBe(2);
        options[0].ShouldBe((MqttQoS.AtLeastOnce, false, false, (byte)0));
        options[1].ShouldBe((MqttQoS.AtLeastOnce, true, true, (byte)2));
    }

    /// <summary>
    /// Proves each faulty SUBSCRIBE is refused with the reason code the standard calls for and leaves the output at
    /// its default.
    /// </summary>
    /// <param name="hex">
    /// The packet.
    /// </param>
    /// <param name="expected">
    /// The expected result.
    /// </param>
    [Theory]
    [InlineData("82 07 00 01 00 00 01 78 40", MqttReasonCode.MalformedPacket)]
    [InlineData("82 07 00 01 00 00 01 78 03", MqttReasonCode.ProtocolError)]
    [InlineData("82 07 00 01 00 00 01 78 30", MqttReasonCode.ProtocolError)]
    [InlineData("82 03 00 01 00", MqttReasonCode.ProtocolError)]
    [InlineData("82 10 00 01 00 00 0A 24 73 68 61 72 65 2F 67 2F 61 04", MqttReasonCode.ProtocolError)]
    [InlineData("82 07 00 00 00 00 01 78 01", MqttReasonCode.ProtocolError)]
    [InlineData("82 06 00 01 00 00 01 78", MqttReasonCode.MalformedPacket)]
    [InlineData("82 08 00 01 00 00 02 FF FE 01", MqttReasonCode.MalformedPacket)]
    [InlineData("82 02 00 01", MqttReasonCode.MalformedPacket)]
    [InlineData("82 01 00", MqttReasonCode.MalformedPacket)]
    public void TryDecode_WrongSubscribe_IsRefused(string hex, MqttReasonCode expected)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = SubscribeCodec.TryDecode(packet, out SubscribePacket subscribe);

        // Assert
        result.ShouldBe(expected);
        subscribe.PacketIdentifier.ShouldBe((ushort)0);
        subscribe.Count.ShouldBe(0);
    }

    /// <summary>
    /// Proves a filter that is not a valid topic filter is still decoded, since the server answers it per filter.
    /// </summary>
    [Fact]
    public void TryDecode_InvalidFilter_IsStillDecoded()
    {
        // Arrange
        byte[] packet = Hex.Parse("82 08 00 01 00 00 02 61 23 01");

        // Act
        MqttReasonCode result = SubscribeCodec.TryDecode(packet, out SubscribePacket subscribe);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        subscribe.Count.ShouldBe(1);
        foreach (Subscription subscription in subscribe.GetSubscriptions())
        {
            Encoding.UTF8.GetString(subscription.TopicFilter).ShouldBe("a#");
        }
    }

    /// <summary>
    /// Proves No Local is accepted on a filter that only looks like a shared subscription, and an empty filter
    /// decodes.
    /// </summary>
    [Fact]
    public void TryDecode_NoLocalOnPlainFilterStartingLikeShare_IsAccepted()
    {
        // Arrange
        byte[] packet = Hex.Parse("82 0F 00 01 00 00 06 24 73 68 61 72 65 04 00 00 01");

        // Act
        MqttReasonCode result = SubscribeCodec.TryDecode(packet, out SubscribePacket subscribe);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        subscribe.Count.ShouldBe(2);
    }

    /// <summary>
    /// Proves a Subscription Identifier property is reported by the property block, for the server to refuse.
    /// </summary>
    [Fact]
    public void TryDecode_SubscriptionIdentifier_IsReported()
    {
        // Arrange
        byte[] packet = Hex.Parse("82 09 00 01 02 0B 05 00 01 78 01");

        // Act
        MqttReasonCode result = SubscribeCodec.TryDecode(packet, out SubscribePacket subscribe);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        subscribe.Properties.Contains(PropertyId.SubscriptionIdentifier).ShouldBeTrue();
        subscribe.Properties.TryGetInteger(PropertyId.SubscriptionIdentifier, out uint value).ShouldBeTrue();
        value.ShouldBe(5u);
    }

    /// <summary>
    /// Proves a SUBACK without reason codes is a protocol error.
    /// </summary>
    [Fact]
    public void TryDecode_SubAckWithoutReasonCodes_IsProtocolError()
    {
        // Arrange
        byte[] packet = Hex.Parse("90 03 00 01 00");

        // Act
        MqttReasonCode result = SubAckCodec.TryDecode(packet, out SubAckPacket subAck);

        // Assert
        result.ShouldBe(MqttReasonCode.ProtocolError);
        subAck.ReasonCodes.IsEmpty.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a SUBACK with an identifier of 0, a cut-off body or a property that does not belong to it is refused.
    /// </summary>
    /// <param name="hex">
    /// The packet.
    /// </param>
    /// <param name="expected">
    /// The expected result.
    /// </param>
    [Theory]
    [InlineData("90 04 00 00 00 01", MqttReasonCode.ProtocolError)]
    [InlineData("90 01 00", MqttReasonCode.MalformedPacket)]
    [InlineData("90 02 00 01", MqttReasonCode.MalformedPacket)]
    [InlineData("90 05 00 01 01 00 01", MqttReasonCode.MalformedPacket)]
    [InlineData("90 06 00 01 02 0B 05 01", MqttReasonCode.MalformedPacket)]
    public void TryDecode_WrongSubAck_IsRefused(string hex, MqttReasonCode expected)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = SubAckCodec.TryDecode(packet, out SubAckPacket subAck);

        // Assert
        result.ShouldBe(expected);
        subAck.PacketIdentifier.ShouldBe((ushort)0);
    }

    /// <summary>
    /// Proves the smallest and largest identifiers of the four packet types survive an encode and decode.
    /// </summary>
    /// <param name="type">
    /// The packet type.
    /// </param>
    /// <param name="id">
    /// The packet identifier.
    /// </param>
    [Theory]
    [MemberData(nameof(EveryPacketAtBothEdges))]
    public void Write_IdentifierAtTheEdge_RoundTrips(byte type, ushort id)
    {
        // Arrange
        MqttReasonCode[] codes = [MqttReasonCode.Success];
        byte[] destination = new byte[16];
        ushort decoded;
        MqttReasonCode result;

        // Act
        switch ((PacketType)type)
        {
            case PacketType.Subscribe:
                SubscribeCodec.Write(destination, id, "a"u8, MqttQoS.AtMostOnce);
                result = SubscribeCodec.TryDecode(destination.AsSpan(0, SubscribeCodec.GetSize("a"u8)), out SubscribePacket subscribe);
                decoded = subscribe.PacketIdentifier;
                break;
            case PacketType.SubAck:
                SubAckCodec.Write(destination, id, codes, null);
                result = SubAckCodec.TryDecode(destination.AsSpan(0, SubAckCodec.GetSize(1, null)), out SubAckPacket subAck);
                decoded = subAck.PacketIdentifier;
                break;
            case PacketType.Unsubscribe:
                UnsubscribeCodec.Write(destination, id, "a"u8);
                result = UnsubscribeCodec.TryDecode(destination.AsSpan(0, UnsubscribeCodec.GetSize("a"u8)), out UnsubscribePacket unsubscribe);
                decoded = unsubscribe.PacketIdentifier;
                break;
            default:
                UnsubAckCodec.Write(destination, id, codes, null);
                result = UnsubAckCodec.TryDecode(destination.AsSpan(0, UnsubAckCodec.GetSize(1, null)), out UnsubAckPacket unsubAck);
                decoded = unsubAck.PacketIdentifier;
                break;
        }

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        decoded.ShouldBe(id);
    }

    /// <summary>
    /// Proves a buffer that is not exactly one framed SUBSCRIBE is a caller bug.
    /// </summary>
    /// <param name="hex">
    /// The unusable buffer.
    /// </param>
    [Theory]
    [InlineData("90 04 00 01 00 01")]
    [InlineData("A2 08 00 02 00 00 03 61 2F 2B")]
    [InlineData("82 09 00 01 00 00 03 61 2F 2B")]
    [InlineData("")]
    public void TryDecode_NotExactlyOneSubscribe_Throws(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act & Assert
        Should.Throw<ArgumentException>(() => SubscribeCodec.TryDecode(packet, out _));
    }

    /// <summary>
    /// Proves a buffer that is not exactly one framed SUBACK is a caller bug.
    /// </summary>
    /// <param name="hex">
    /// The unusable buffer.
    /// </param>
    [Theory]
    [InlineData("82 09 00 01 00 00 03 61 2F 2B 01")]
    [InlineData("B0 04 00 02 00 00")]
    [InlineData("90 04 00 01 00")]
    [InlineData("")]
    public void TryDecode_NotExactlyOneSubAck_Throws(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act & Assert
        Should.Throw<ArgumentException>(() => SubAckCodec.TryDecode(packet, out _));
    }

    /// <summary>
    /// Proves the SUBSCRIBE encoder refuses what the decoder would refuse, and a destination that is too small,
    /// before writing anything.
    /// </summary>
    [Fact]
    public void Write_Subscribe_RefusesWhatItCannotCarryWithoutWriting()
    {
        // Arrange
        byte[] destination = new byte[16];
        byte[] tooLong = new byte[65536];
        byte[] small = new byte[4];

        // Act & Assert
        Should.Throw<ArgumentException>(() => SubscribeCodec.Write(destination, 0, "a"u8, MqttQoS.AtMostOnce));
        Should.Throw<ArgumentException>(() => SubscribeCodec.Write(destination, 1, "a"u8, (MqttQoS)3));
        Should.Throw<ArgumentException>(() => SubscribeCodec.Write(destination, 1, tooLong, MqttQoS.AtMostOnce));
        Should.Throw<ArgumentException>(() => SubscribeCodec.GetSize(tooLong));
        Should.Throw<ArgumentException>(() => SubscribeCodec.Write(small, 1, "a/+"u8, MqttQoS.AtMostOnce));
        destination.ShouldAllBe(b => b == 0);
        small.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves the SUBSCRIBE encoder carries an empty filter, as its decoder accepts one.
    /// </summary>
    [Fact]
    public void Write_EmptyFilter_IsWritten()
    {
        // Arrange
        byte[] destination = new byte[SubscribeCodec.GetSize(default)];

        // Act
        int written = SubscribeCodec.Write(destination, 1, default, MqttQoS.ExactlyOnce);
        MqttReasonCode result = SubscribeCodec.TryDecode(destination, out SubscribePacket subscribe);

        // Assert
        written.ShouldBe(destination.Length);
        result.ShouldBe(MqttReasonCode.Success);
        subscribe.Count.ShouldBe(1);
    }

    /// <summary>
    /// Proves the SUBACK encoder refuses an identifier of 0, an empty list, a reason string it cannot carry and a
    /// destination that is too small, before writing anything.
    /// </summary>
    [Fact]
    public void Write_SubAck_RefusesWhatItCannotCarryWithoutWriting()
    {
        // Arrange
        byte[] destination = new byte[16];
        byte[] small = new byte[5];
        MqttReasonCode[] one = [MqttReasonCode.Success];

        // Act & Assert
        Should.Throw<ArgumentException>(() => SubAckCodec.Write(destination, 0, one, null));
        Should.Throw<ArgumentException>(() => SubAckCodec.Write(destination, 1, [], null));
        Should.Throw<ArgumentException>(() => SubAckCodec.GetSize(0, null));
        Should.Throw<ArgumentException>(() => SubAckCodec.GetSize(-1, null));
        Should.Throw<ArgumentException>(() => SubAckCodec.GetSize(1, "a\0b"));
        Should.Throw<ArgumentException>(() => SubAckCodec.Write(destination, 1, one, "a\0b"));
        Should.Throw<ArgumentException>(() => SubAckCodec.Write(small, 1, one, null));
        destination.ShouldAllBe(b => b == 0);
        small.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a SUBSCRIBE MQTTnet encodes decodes here to the same filters and options.
    /// </summary>
    [Fact]
    public void TryDecode_SubscribeMqttNetEncodes_DecodesToTheSameValues()
    {
        // Arrange
        byte[] packet = MqttNetFormatter.Encode(new MqttSubscribePacket
        {
            PacketIdentifier = 12,
            TopicFilters =
            [
                new MqttTopicFilter { Topic = "a/+", QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce },
                new MqttTopicFilter
                {
                    Topic = "x/#",
                    QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce,
                    NoLocal = true,
                    RetainAsPublished = true,
                    RetainHandling = MqttRetainHandling.DoNotSendOnSubscribe,
                },
            ],
        });
        List<string> filters = [];
        List<(MqttQoS Qos, bool NoLocal, bool Retain, byte Handling)> options = [];

        // Act
        MqttReasonCode result = SubscribeCodec.TryDecode(packet, out SubscribePacket subscribe);
        foreach (Subscription subscription in subscribe.GetSubscriptions())
        {
            filters.Add(Encoding.UTF8.GetString(subscription.TopicFilter));
            options.Add((subscription.MaximumQoS, subscription.NoLocal, subscription.RetainAsPublished, subscription.RetainHandling));
        }

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        subscribe.PacketIdentifier.ShouldBe((ushort)12);
        filters.ShouldBe(["a/+", "x/#"]);
        options[0].ShouldBe((MqttQoS.AtLeastOnce, false, false, (byte)0));
        options[1].ShouldBe((MqttQoS.ExactlyOnce, true, true, (byte)2));
    }

    /// <summary>
    /// Proves a SUBACK MQTTnet encodes decodes here to the same reason codes and reason string.
    /// </summary>
    [Fact]
    public void TryDecode_SubAckMqttNetEncodes_DecodesToTheSameValues()
    {
        // Arrange
        byte[] packet = MqttNetFormatter.Encode(new MqttSubAckPacket
        {
            PacketIdentifier = 13,
            ReasonCodes = [MqttSubscribeReasonCode.GrantedQoS2, MqttSubscribeReasonCode.NotAuthorized],
            ReasonString = "no",
        });

        // Act
        MqttReasonCode result = SubAckCodec.TryDecode(packet, out SubAckPacket subAck);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        subAck.PacketIdentifier.ShouldBe((ushort)13);
        subAck.ReasonCodes.ToArray().ShouldBe([(byte)MqttReasonCode.GrantedQoS2, (byte)MqttReasonCode.NotAuthorized]);
        subAck.Properties.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> text).ShouldBeTrue();
        Encoding.UTF8.GetString(text).ShouldBe("no");
    }

    /// <summary>
    /// Proves a SUBSCRIBE and a SUBACK this codec encodes decode in MQTTnet to the same values.
    /// </summary>
    [Fact]
    public void Write_SubscribeAndSubAck_MqttNetDecodesTheSameValues()
    {
        // Arrange
        MqttReasonCode[] codes = [MqttReasonCode.GrantedQoS1, MqttReasonCode.TopicFilterInvalid];
        byte[] subscribePacket = new byte[SubscribeCodec.GetSize("a/+"u8)];
        byte[] subAckPacket = new byte[SubAckCodec.GetSize(2, "no")];
        SubscribeCodec.Write(subscribePacket, 12, "a/+"u8, MqttQoS.ExactlyOnce);
        SubAckCodec.Write(subAckPacket, 13, codes, "no");

        // Act
        MqttSubscribePacket subscribe = (MqttSubscribePacket)MqttNetFormatter.Decode(subscribePacket);
        MqttSubAckPacket subAck = (MqttSubAckPacket)MqttNetFormatter.Decode(subAckPacket);

        // Assert
        subscribe.PacketIdentifier.ShouldBe((ushort)12);
        subscribe.TopicFilters.Count.ShouldBe(1);
        subscribe.TopicFilters[0].Topic.ShouldBe("a/+");
        subscribe.TopicFilters[0].QualityOfServiceLevel.ShouldBe(MqttQualityOfServiceLevel.ExactlyOnce);
        subscribe.TopicFilters[0].NoLocal.ShouldBeFalse();
        subAck.PacketIdentifier.ShouldBe((ushort)13);
        subAck.ReasonCodes.ShouldBe([MqttSubscribeReasonCode.GrantedQoS1, MqttSubscribeReasonCode.TopicFilterInvalid]);
        subAck.ReasonString.ShouldBe("no");
    }
}
