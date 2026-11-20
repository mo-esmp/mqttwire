using System.Text;
using MQTTnet.Packets;
using MQTTnet.Protocol;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests decoding and encoding PUBACK, PUBREC, PUBREL and PUBCOMP, including a cross-check against MQTTnet.
/// </summary>
public sealed class AckCodecTests
{
    /// <summary>
    /// The known acknowledgements and the values they carry.
    /// </summary>
    public static TheoryData<byte[], byte, ushort, MqttReasonCode, string?> KnownAcks => new()
    {
        { KnownPackets.PubAck, (byte)PacketType.PubAck, 10, MqttReasonCode.Success, null },
        { KnownPackets.PubRecRefused, (byte)PacketType.PubRec, 10, MqttReasonCode.QuotaExceeded, null },
        { KnownPackets.PubRel, (byte)PacketType.PubRel, 10, MqttReasonCode.Success, null },
        { KnownPackets.PubCompNotFound, (byte)PacketType.PubComp, 10, MqttReasonCode.PacketIdentifierNotFound, null },
        { KnownPackets.PubAckWithReason, (byte)PacketType.PubAck, 10, MqttReasonCode.QuotaExceeded, "x" },
    };

    /// <summary>
    /// Every acknowledgement type at the smallest and largest packet identifier.
    /// </summary>
    public static TheoryData<byte, ushort> EveryAckTypeAtBothEdges => new()
    {
        { (byte)PacketType.PubAck, 1 },
        { (byte)PacketType.PubAck, 65535 },
        { (byte)PacketType.PubRec, 1 },
        { (byte)PacketType.PubRec, 65535 },
        { (byte)PacketType.PubRel, 1 },
        { (byte)PacketType.PubRel, 65535 },
        { (byte)PacketType.PubComp, 1 },
        { (byte)PacketType.PubComp, 65535 },
    };

    /// <summary>
    /// Proves each known acknowledgement decodes to its values and is the bytes the encoder writes for them.
    /// </summary>
    /// <param name="packet">
    /// The known packet.
    /// </param>
    /// <param name="type">
    /// The expected packet type.
    /// </param>
    /// <param name="id">
    /// The expected packet identifier.
    /// </param>
    /// <param name="code">
    /// The expected reason code.
    /// </param>
    /// <param name="reasonString">
    /// The expected reason string, or <see langword="null"/> for none.
    /// </param>
    [Theory]
    [MemberData(nameof(KnownAcks))]
    public void TryDecode_KnownAck_DecodesAndIsWhatTheEncoderWrites(byte[] packet, byte type, ushort id, MqttReasonCode code, string? reasonString)
    {
        // Arrange
        byte[] destination = new byte[packet.Length];

        // Act
        MqttReasonCode result = AckCodec.TryDecode(packet, out AckPacket ack);
        int size = AckCodec.GetSize(code, reasonString);
        int written = AckCodec.Write(destination, (PacketType)type, id, code, reasonString);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        ((byte)ack.Type).ShouldBe(type);
        ack.PacketIdentifier.ShouldBe(id);
        ack.ReasonCode.ShouldBe(code);
        ack.Properties.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> text).ShouldBe(reasonString is not null);
        if (reasonString is not null)
        {
            Encoding.UTF8.GetString(text).ShouldBe(reasonString);
        }

        size.ShouldBe(packet.Length);
        written.ShouldBe(packet.Length);
        destination.ShouldBe(packet);
    }

    /// <summary>
    /// Proves the three forms of a success acknowledgement all decode to success.
    /// </summary>
    /// <param name="hex">
    /// The packet.
    /// </param>
    [Theory]
    [InlineData("40 02 00 0A")]
    [InlineData("40 03 00 0A 00")]
    [InlineData("40 04 00 0A 00 00")]
    public void TryDecode_EveryShortForm_IsSuccess(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = AckCodec.TryDecode(packet, out AckPacket ack);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        ack.ReasonCode.ShouldBe(MqttReasonCode.Success);
        ack.PacketIdentifier.ShouldBe((ushort)10);
        ack.Properties.Contains(PropertyId.ReasonString).ShouldBeFalse();
    }

    /// <summary>
    /// Proves the smallest and largest identifiers of every acknowledgement type survive an encode and decode.
    /// </summary>
    /// <param name="type">
    /// The packet type.
    /// </param>
    /// <param name="id">
    /// The packet identifier.
    /// </param>
    [Theory]
    [MemberData(nameof(EveryAckTypeAtBothEdges))]
    public void Write_IdentifierAtTheEdge_RoundTrips(byte type, ushort id)
    {
        // Arrange
        byte[] destination = new byte[AckCodec.GetSize(MqttReasonCode.Success, null)];

        // Act
        AckCodec.Write(destination, (PacketType)type, id, MqttReasonCode.Success, null);
        MqttReasonCode result = AckCodec.TryDecode(destination, out AckPacket ack);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        ((byte)ack.Type).ShouldBe(type);
        ack.PacketIdentifier.ShouldBe(id);
    }

    /// <summary>
    /// Proves a packet identifier of zero is a protocol error and leaves the output at its default.
    /// </summary>
    [Fact]
    public void TryDecode_IdentifierZero_IsProtocolError()
    {
        // Arrange
        byte[] packet = Hex.Parse("40 02 00 00");

        // Act
        MqttReasonCode result = AckCodec.TryDecode(packet, out AckPacket ack);

        // Assert
        result.ShouldBe(MqttReasonCode.ProtocolError);
        ack.PacketIdentifier.ShouldBe((ushort)0);
    }

    /// <summary>
    /// Proves a remaining length below 2 is malformed.
    /// </summary>
    /// <param name="hex">
    /// The packet.
    /// </param>
    [Theory]
    [InlineData("40 00")]
    [InlineData("40 01 00")]
    public void TryDecode_RemainingLengthBelowTwo_IsMalformed(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = AckCodec.TryDecode(packet, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
    }

    /// <summary>
    /// Proves a property past its block, a block past the packet and a left-over byte are malformed.
    /// </summary>
    /// <param name="hex">
    /// The broken packet.
    /// </param>
    [Theory]
    [InlineData("40 05 00 0A 00 05 1F")]
    [InlineData("40 04 00 0A 00 01")]
    [InlineData("40 05 00 0A 00 00 00")]
    public void TryDecode_BrokenAck_IsMalformed(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = AckCodec.TryDecode(packet, out AckPacket ack);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
        ack.PacketIdentifier.ShouldBe((ushort)0);
    }

    /// <summary>
    /// Proves a buffer that is not exactly one framed acknowledgement is a caller bug.
    /// </summary>
    /// <param name="hex">
    /// The unusable buffer.
    /// </param>
    [Theory]
    [InlineData("20 03 00 00 00")]
    [InlineData("C0 00")]
    [InlineData("40 02 00")]
    [InlineData("40 02 00 0A 00")]
    [InlineData("")]
    public void TryDecode_NotExactlyOneAck_Throws(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act & Assert
        Should.Throw<ArgumentException>(() => AckCodec.TryDecode(packet, out _));
    }

    /// <summary>
    /// Proves a type that is not one of the four acknowledgements, or an identifier of 0, throws from Write and
    /// leaves the destination untouched.
    /// </summary>
    [Fact]
    public void Write_TypeNotAnAckOrIdentifierZero_ThrowsWithoutWriting()
    {
        // Arrange
        byte[] destination = new byte[16];

        // Act & Assert
        Should.Throw<ArgumentException>(() => AckCodec.Write(destination, PacketType.Publish, 1, MqttReasonCode.Success, null));
        Should.Throw<ArgumentException>(() => AckCodec.Write(destination, PacketType.SubAck, 1, MqttReasonCode.Success, null));
        Should.Throw<ArgumentException>(() => AckCodec.Write(destination, PacketType.PubAck, 0, MqttReasonCode.Success, null));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a reason string that cannot be carried throws from GetSize and Write and nothing is written.
    /// </summary>
    [Fact]
    public void Write_ReasonStringWithNul_ThrowsWithoutWriting()
    {
        // Arrange
        byte[] destination = new byte[16];

        // Act & Assert
        Should.Throw<ArgumentException>(() => AckCodec.GetSize(MqttReasonCode.QuotaExceeded, "a\0b"));
        Should.Throw<ArgumentException>(() => AckCodec.Write(destination, PacketType.PubAck, 1, MqttReasonCode.QuotaExceeded, "a\0b"));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a destination smaller than the packet throws and nothing is written.
    /// </summary>
    [Fact]
    public void Write_DestinationTooSmall_ThrowsWithoutWriting()
    {
        // Arrange
        byte[] destination = new byte[3];

        // Act & Assert
        Should.Throw<ArgumentException>(() => AckCodec.Write(destination, PacketType.PubAck, 1, MqttReasonCode.Success, null));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves the encoded sizes: success alone is four bytes, a failure code alone adds one, and a reason string
    /// adds the property block.
    /// </summary>
    [Fact]
    public void GetSize_EachForm_MatchesTheEncodedLength()
    {
        // Act & Assert
        AckCodec.GetSize(MqttReasonCode.Success, null).ShouldBe(4);
        AckCodec.GetSize(MqttReasonCode.QuotaExceeded, null).ShouldBe(5);
        AckCodec.GetSize(MqttReasonCode.Success, "x").ShouldBe(10);
    }

    /// <summary>
    /// Proves a success acknowledgement with a reason string is written as the reason code and the property block.
    /// </summary>
    [Fact]
    public void Write_SuccessWithReasonString_WritesTheReasonCodeAndTheBlock()
    {
        // Arrange
        byte[] destination = new byte[10];

        // Act
        int written = AckCodec.Write(destination, PacketType.PubComp, 10, MqttReasonCode.Success, "x");

        // Assert
        written.ShouldBe(10);
        destination.ShouldBe(Hex.Parse("70 08 00 0A 00 04 1F 00 01 78"));
    }

    /// <summary>
    /// Proves acknowledgements MQTTnet encodes decode here to the same values.
    /// </summary>
    [Fact]
    public void TryDecode_AcksMqttNetEncodes_DecodeToTheSameValues()
    {
        // Arrange
        byte[] pubAck = MqttNetFormatter.Encode(new MqttPubAckPacket { PacketIdentifier = 7 });
        byte[] pubRec = MqttNetFormatter.Encode(new MqttPubRecPacket
        {
            PacketIdentifier = 8,
            ReasonCode = MqttPubRecReasonCode.QuotaExceeded,
            ReasonString = "full",
        });
        byte[] pubRel = MqttNetFormatter.Encode(new MqttPubRelPacket { PacketIdentifier = 9 });
        byte[] pubComp = MqttNetFormatter.Encode(new MqttPubCompPacket
        {
            PacketIdentifier = 10,
            ReasonCode = MqttPubCompReasonCode.PacketIdentifierNotFound,
        });

        // Act
        MqttReasonCode first = AckCodec.TryDecode(pubAck, out AckPacket ack);
        MqttReasonCode second = AckCodec.TryDecode(pubRec, out AckPacket rec);
        MqttReasonCode third = AckCodec.TryDecode(pubRel, out AckPacket rel);
        MqttReasonCode fourth = AckCodec.TryDecode(pubComp, out AckPacket comp);

        // Assert
        first.ShouldBe(MqttReasonCode.Success);
        ack.Type.ShouldBe(PacketType.PubAck);
        ack.PacketIdentifier.ShouldBe((ushort)7);
        ack.ReasonCode.ShouldBe(MqttReasonCode.Success);
        second.ShouldBe(MqttReasonCode.Success);
        rec.Type.ShouldBe(PacketType.PubRec);
        rec.PacketIdentifier.ShouldBe((ushort)8);
        rec.ReasonCode.ShouldBe(MqttReasonCode.QuotaExceeded);
        rec.Properties.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> text).ShouldBeTrue();
        Encoding.UTF8.GetString(text).ShouldBe("full");
        third.ShouldBe(MqttReasonCode.Success);
        rel.Type.ShouldBe(PacketType.PubRel);
        rel.PacketIdentifier.ShouldBe((ushort)9);
        fourth.ShouldBe(MqttReasonCode.Success);
        comp.Type.ShouldBe(PacketType.PubComp);
        comp.PacketIdentifier.ShouldBe((ushort)10);
        comp.ReasonCode.ShouldBe(MqttReasonCode.PacketIdentifierNotFound);
    }

    /// <summary>
    /// Proves acknowledgements this codec encodes decode in MQTTnet to the same values.
    /// </summary>
    [Fact]
    public void Write_Acks_MqttNetDecodesTheSameValues()
    {
        // Arrange
        byte[] pubAck = new byte[AckCodec.GetSize(MqttReasonCode.Success, null)];
        byte[] pubRec = new byte[AckCodec.GetSize(MqttReasonCode.QuotaExceeded, "full")];
        byte[] pubRel = new byte[AckCodec.GetSize(MqttReasonCode.Success, null)];
        byte[] pubComp = new byte[AckCodec.GetSize(MqttReasonCode.PacketIdentifierNotFound, null)];
        AckCodec.Write(pubAck, PacketType.PubAck, 7, MqttReasonCode.Success, null);
        AckCodec.Write(pubRec, PacketType.PubRec, 8, MqttReasonCode.QuotaExceeded, "full");
        AckCodec.Write(pubRel, PacketType.PubRel, 9, MqttReasonCode.Success, null);
        AckCodec.Write(pubComp, PacketType.PubComp, 10, MqttReasonCode.PacketIdentifierNotFound, null);

        // Act
        MqttPubAckPacket ack = (MqttPubAckPacket)MqttNetFormatter.Decode(pubAck);
        MqttPubRecPacket rec = (MqttPubRecPacket)MqttNetFormatter.Decode(pubRec);
        MqttPubRelPacket rel = (MqttPubRelPacket)MqttNetFormatter.Decode(pubRel);
        MqttPubCompPacket comp = (MqttPubCompPacket)MqttNetFormatter.Decode(pubComp);

        // Assert
        ack.PacketIdentifier.ShouldBe((ushort)7);
        ack.ReasonCode.ShouldBe(MqttPubAckReasonCode.Success);
        rec.PacketIdentifier.ShouldBe((ushort)8);
        rec.ReasonCode.ShouldBe(MqttPubRecReasonCode.QuotaExceeded);
        rec.ReasonString.ShouldBe("full");
        rel.PacketIdentifier.ShouldBe((ushort)9);
        rel.ReasonCode.ShouldBe(MqttPubRelReasonCode.Success);
        comp.PacketIdentifier.ShouldBe((ushort)10);
        comp.ReasonCode.ShouldBe(MqttPubCompReasonCode.PacketIdentifierNotFound);
    }
}
