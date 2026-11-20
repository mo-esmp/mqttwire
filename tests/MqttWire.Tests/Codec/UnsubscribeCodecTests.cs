using System.Text;
using MQTTnet.Packets;
using MQTTnet.Protocol;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests decoding and encoding UNSUBSCRIBE and UNSUBACK, including a cross-check against MQTTnet.
/// </summary>
public sealed class UnsubscribeCodecTests
{
    /// <summary>
    /// Proves the known UNSUBSCRIBE decodes to its values and is the bytes the encoder writes for them.
    /// </summary>
    [Fact]
    public void TryDecode_KnownUnsubscribe_DecodesAndIsWhatTheEncoderWrites()
    {
        // Arrange
        byte[] packet = KnownPackets.Unsubscribe;
        byte[] destination = new byte[packet.Length];
        List<string> filters = [];

        // Act
        MqttReasonCode result = UnsubscribeCodec.TryDecode(packet, out UnsubscribePacket unsubscribe);
        foreach (ReadOnlySpan<byte> filter in unsubscribe.GetTopicFilters())
        {
            filters.Add(Encoding.UTF8.GetString(filter));
        }

        int size = UnsubscribeCodec.GetSize("a/+"u8);
        int written = UnsubscribeCodec.Write(destination, 2, "a/+"u8);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        unsubscribe.PacketIdentifier.ShouldBe((ushort)2);
        unsubscribe.Count.ShouldBe(1);
        filters.ShouldBe(["a/+"]);
        size.ShouldBe(packet.Length);
        written.ShouldBe(packet.Length);
        destination.ShouldBe(packet);
    }

    /// <summary>
    /// Proves the known UNSUBACK decodes to its values and is the bytes the encoder writes for them.
    /// </summary>
    [Fact]
    public void TryDecode_KnownUnsubAck_DecodesAndIsWhatTheEncoderWrites()
    {
        // Arrange
        byte[] packet = KnownPackets.UnsubAck;
        byte[] destination = new byte[packet.Length];
        MqttReasonCode[] codes = [MqttReasonCode.Success];

        // Act
        MqttReasonCode result = UnsubAckCodec.TryDecode(packet, out UnsubAckPacket unsubAck);
        int size = UnsubAckCodec.GetSize(1, null);
        int written = UnsubAckCodec.Write(destination, 2, codes, null);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        unsubAck.PacketIdentifier.ShouldBe((ushort)2);
        unsubAck.ReasonCodes.ToArray().ShouldBe([(byte)MqttReasonCode.Success]);
        unsubAck.Properties.Contains(PropertyId.ReasonString).ShouldBeFalse();
        size.ShouldBe(packet.Length);
        written.ShouldBe(packet.Length);
        destination.ShouldBe(packet);
    }

    /// <summary>
    /// Proves an UNSUBACK with a reason string writes it as property 0x1F and decodes it back.
    /// </summary>
    [Fact]
    public void Write_UnsubAckWithReasonString_WritesThePropertyAndDecodesIt()
    {
        // Arrange
        MqttReasonCode[] codes = [MqttReasonCode.NoSubscriptionExisted];
        byte[] destination = new byte[UnsubAckCodec.GetSize(1, "x")];

        // Act
        int written = UnsubAckCodec.Write(destination, 5, codes, "x");
        MqttReasonCode result = UnsubAckCodec.TryDecode(destination, out UnsubAckPacket unsubAck);

        // Assert
        written.ShouldBe(destination.Length);
        destination.ShouldBe(Hex.Parse("B0 08 00 05 04 1F 00 01 78 11"));
        result.ShouldBe(MqttReasonCode.Success);
        unsubAck.ReasonCodes.ToArray().ShouldBe([(byte)MqttReasonCode.NoSubscriptionExisted]);
        unsubAck.Properties.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> text).ShouldBeTrue();
        Encoding.UTF8.GetString(text).ShouldBe("x");
    }

    /// <summary>
    /// Proves every filter of an UNSUBSCRIBE is read, in order, and that an invalid one is still decoded.
    /// </summary>
    [Fact]
    public void GetTopicFilters_TwoFilters_ReadsEveryFilter()
    {
        // Arrange
        byte[] packet = Hex.Parse("A2 0C 00 02 00 00 03 61 2F 2B 00 02 61 23");
        List<string> filters = [];

        // Act
        MqttReasonCode result = UnsubscribeCodec.TryDecode(packet, out UnsubscribePacket unsubscribe);
        foreach (ReadOnlySpan<byte> filter in unsubscribe.GetTopicFilters())
        {
            filters.Add(Encoding.UTF8.GetString(filter));
        }

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        unsubscribe.Count.ShouldBe(2);
        filters.ShouldBe(["a/+", "a#"]);
    }

    /// <summary>
    /// Proves each faulty UNSUBSCRIBE is refused with the reason code the standard calls for and leaves the output
    /// at its default.
    /// </summary>
    /// <param name="hex">
    /// The packet.
    /// </param>
    /// <param name="expected">
    /// The expected result.
    /// </param>
    [Theory]
    [InlineData("A2 03 00 02 00", MqttReasonCode.ProtocolError)]
    [InlineData("A2 06 00 00 00 00 01 78", MqttReasonCode.ProtocolError)]
    [InlineData("A2 06 00 02 00 00 02 78", MqttReasonCode.MalformedPacket)]
    [InlineData("A2 07 00 02 00 00 02 FF FE", MqttReasonCode.MalformedPacket)]
    [InlineData("A2 02 00 02", MqttReasonCode.MalformedPacket)]
    [InlineData("A2 01 00", MqttReasonCode.MalformedPacket)]
    [InlineData("A2 08 00 02 02 0B 05 00 01 78", MqttReasonCode.MalformedPacket)]
    public void TryDecode_WrongUnsubscribe_IsRefused(string hex, MqttReasonCode expected)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = UnsubscribeCodec.TryDecode(packet, out UnsubscribePacket unsubscribe);

        // Assert
        result.ShouldBe(expected);
        unsubscribe.PacketIdentifier.ShouldBe((ushort)0);
        unsubscribe.Count.ShouldBe(0);
    }

    /// <summary>
    /// Proves an UNSUBACK without reason codes is a protocol error.
    /// </summary>
    [Fact]
    public void TryDecode_UnsubAckWithoutReasonCodes_IsProtocolError()
    {
        // Arrange
        byte[] packet = Hex.Parse("B0 03 00 02 00");

        // Act
        MqttReasonCode result = UnsubAckCodec.TryDecode(packet, out UnsubAckPacket unsubAck);

        // Assert
        result.ShouldBe(MqttReasonCode.ProtocolError);
        unsubAck.ReasonCodes.IsEmpty.ShouldBeTrue();
    }

    /// <summary>
    /// Proves an UNSUBACK reason code is handed on as sent, even 0xFF, which MQTT does not define (plan decision 8).
    /// </summary>
    [Fact]
    public void TryDecode_UnsubAckWithUndefinedReasonCode_HandsItOnUnchanged()
    {
        // Arrange
        byte[] packet = Hex.Parse("B0 04 00 02 00 FF");

        // Act
        MqttReasonCode result = UnsubAckCodec.TryDecode(packet, out UnsubAckPacket unsubAck);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        unsubAck.ReasonCodes.ToArray().ShouldBe([(byte)0xFF]);
    }

    /// <summary>
    /// Proves an UNSUBACK with an identifier of 0 or a cut-off body is refused.
    /// </summary>
    /// <param name="hex">
    /// The packet.
    /// </param>
    /// <param name="expected">
    /// The expected result.
    /// </param>
    [Theory]
    [InlineData("B0 04 00 00 00 00", MqttReasonCode.ProtocolError)]
    [InlineData("B0 01 00", MqttReasonCode.MalformedPacket)]
    [InlineData("B0 02 00 02", MqttReasonCode.MalformedPacket)]
    [InlineData("B0 05 00 02 01 00 00", MqttReasonCode.MalformedPacket)]
    public void TryDecode_WrongUnsubAck_IsRefused(string hex, MqttReasonCode expected)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = UnsubAckCodec.TryDecode(packet, out UnsubAckPacket unsubAck);

        // Assert
        result.ShouldBe(expected);
        unsubAck.PacketIdentifier.ShouldBe((ushort)0);
    }

    /// <summary>
    /// Proves a buffer that is not exactly one framed UNSUBSCRIBE is a caller bug.
    /// </summary>
    /// <param name="hex">
    /// The unusable buffer.
    /// </param>
    [Theory]
    [InlineData("82 09 00 01 00 00 03 61 2F 2B 01")]
    [InlineData("A2 08 00 02 00 00 03 61 2F")]
    [InlineData("A2 08 00 02 00 00 03 61 2F 2B 00")]
    [InlineData("")]
    public void TryDecode_NotExactlyOneUnsubscribe_Throws(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act & Assert
        Should.Throw<ArgumentException>(() => UnsubscribeCodec.TryDecode(packet, out _));
    }

    /// <summary>
    /// Proves a buffer that is not exactly one framed UNSUBACK is a caller bug.
    /// </summary>
    /// <param name="hex">
    /// The unusable buffer.
    /// </param>
    [Theory]
    [InlineData("90 04 00 01 00 01")]
    [InlineData("B0 04 00 02 00")]
    [InlineData("")]
    public void TryDecode_NotExactlyOneUnsubAck_Throws(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act & Assert
        Should.Throw<ArgumentException>(() => UnsubAckCodec.TryDecode(packet, out _));
    }

    /// <summary>
    /// Proves the UNSUBSCRIBE encoder refuses what the decoder would refuse, and a destination that is too small,
    /// before writing anything.
    /// </summary>
    [Fact]
    public void Write_Unsubscribe_RefusesWhatItCannotCarryWithoutWriting()
    {
        // Arrange
        byte[] destination = new byte[16];
        byte[] tooLong = new byte[65536];
        byte[] small = new byte[4];

        // Act & Assert
        Should.Throw<ArgumentException>(() => UnsubscribeCodec.Write(destination, 0, "a"u8));
        Should.Throw<ArgumentException>(() => UnsubscribeCodec.Write(destination, 1, tooLong));
        Should.Throw<ArgumentException>(() => UnsubscribeCodec.GetSize(tooLong));
        Should.Throw<ArgumentException>(() => UnsubscribeCodec.Write(small, 1, "a/+"u8));
        destination.ShouldAllBe(b => b == 0);
        small.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a filter the decoder answers with a malformed packet, ill-formed UTF-8 or one holding U+0000, throws
    /// from the size and the writer and writes nothing.
    /// </summary>
    /// <param name="hex">
    /// The filter's bytes.
    /// </param>
    [Theory]
    [InlineData("C3")]
    [InlineData("61 00")]
    public void Write_FilterNotMqttUtf8_ThrowsWithoutWriting(string hex)
    {
        // Arrange
        byte[] filter = Hex.Parse(hex);
        byte[] destination = new byte[16];

        // Act & Assert
        Should.Throw<ArgumentException>(() => UnsubscribeCodec.GetSize(filter));
        Should.Throw<ArgumentException>(() => UnsubscribeCodec.Write(destination, 1, filter));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves the filter enumerator stops, rather than spinning, on a filter whose length runs past its bytes.
    /// </summary>
    [Fact]
    public void MoveNext_FilterRunsPastTheEnd_ReturnsFalse()
    {
        // Arrange
        TopicFilterEnumerator filters = new(Hex.Parse("00 05 61"));

        // Act
        bool moved = filters.MoveNext();

        // Assert
        moved.ShouldBeFalse();
    }

    /// <summary>
    /// Proves the UNSUBACK encoder refuses an identifier of 0, an empty list, a reason string it cannot carry and a
    /// destination that is too small, before writing anything.
    /// </summary>
    [Fact]
    public void Write_UnsubAck_RefusesWhatItCannotCarryWithoutWriting()
    {
        // Arrange
        byte[] destination = new byte[16];
        byte[] small = new byte[5];
        MqttReasonCode[] one = [MqttReasonCode.Success];

        // Act & Assert
        Should.Throw<ArgumentException>(() => UnsubAckCodec.Write(destination, 0, one, null));
        Should.Throw<ArgumentException>(() => UnsubAckCodec.Write(destination, 1, [], null));
        Should.Throw<ArgumentException>(() => UnsubAckCodec.GetSize(0, null));
        Should.Throw<ArgumentException>(() => UnsubAckCodec.GetSize(1, "a\0b"));
        Should.Throw<ArgumentException>(() => UnsubAckCodec.Write(destination, 1, one, "a\0b"));
        Should.Throw<ArgumentException>(() => UnsubAckCodec.Write(small, 1, one, null));
        destination.ShouldAllBe(b => b == 0);
        small.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves an UNSUBSCRIBE and an UNSUBACK MQTTnet encodes decode here to the same values.
    /// </summary>
    [Fact]
    public void TryDecode_UnsubscribeAndUnsubAckMqttNetEncodes_DecodeToTheSameValues()
    {
        // Arrange
        byte[] unsubscribePacket = MqttNetFormatter.Encode(new MqttUnsubscribePacket
        {
            PacketIdentifier = 14,
            TopicFilters = ["a/+", "b"],
        });
        byte[] unsubAckPacket = MqttNetFormatter.Encode(new MqttUnsubAckPacket
        {
            PacketIdentifier = 15,
            ReasonCodes = [MqttUnsubscribeReasonCode.Success, MqttUnsubscribeReasonCode.NoSubscriptionExisted],
            ReasonString = "no",
        });
        List<string> filters = [];

        // Act
        MqttReasonCode first = UnsubscribeCodec.TryDecode(unsubscribePacket, out UnsubscribePacket unsubscribe);
        foreach (ReadOnlySpan<byte> filter in unsubscribe.GetTopicFilters())
        {
            filters.Add(Encoding.UTF8.GetString(filter));
        }

        MqttReasonCode second = UnsubAckCodec.TryDecode(unsubAckPacket, out UnsubAckPacket unsubAck);

        // Assert
        first.ShouldBe(MqttReasonCode.Success);
        unsubscribe.PacketIdentifier.ShouldBe((ushort)14);
        filters.ShouldBe(["a/+", "b"]);
        second.ShouldBe(MqttReasonCode.Success);
        unsubAck.PacketIdentifier.ShouldBe((ushort)15);
        unsubAck.ReasonCodes.ToArray().ShouldBe([(byte)MqttReasonCode.Success, (byte)MqttReasonCode.NoSubscriptionExisted]);
        unsubAck.Properties.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> text).ShouldBeTrue();
        Encoding.UTF8.GetString(text).ShouldBe("no");
    }

    /// <summary>
    /// Proves an UNSUBSCRIBE and an UNSUBACK this codec encodes decode in MQTTnet to the same values.
    /// </summary>
    [Fact]
    public void Write_UnsubscribeAndUnsubAck_MqttNetDecodesTheSameValues()
    {
        // Arrange
        MqttReasonCode[] codes = [MqttReasonCode.NoSubscriptionExisted];
        byte[] unsubscribePacket = new byte[UnsubscribeCodec.GetSize("a/+"u8)];
        byte[] unsubAckPacket = new byte[UnsubAckCodec.GetSize(1, "no")];
        UnsubscribeCodec.Write(unsubscribePacket, 14, "a/+"u8);
        UnsubAckCodec.Write(unsubAckPacket, 15, codes, "no");

        // Act
        MqttUnsubscribePacket unsubscribe = (MqttUnsubscribePacket)MqttNetFormatter.Decode(unsubscribePacket);
        MqttUnsubAckPacket unsubAck = (MqttUnsubAckPacket)MqttNetFormatter.Decode(unsubAckPacket);

        // Assert
        unsubscribe.PacketIdentifier.ShouldBe((ushort)14);
        unsubscribe.TopicFilters.ShouldBe(["a/+"]);
        unsubAck.PacketIdentifier.ShouldBe((ushort)15);
        unsubAck.ReasonCodes.ShouldBe([MqttUnsubscribeReasonCode.NoSubscriptionExisted]);
        unsubAck.ReasonString.ShouldBe("no");
    }
}
