using System.Text;
using MQTTnet.Packets;
using MQTTnet.Protocol;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests decoding and encoding the DISCONNECT packet, including a cross-check against MQTTnet.
/// </summary>
public class DisconnectCodecTests
{
    /// <summary>
    /// The known DISCONNECT packets and the values they carry.
    /// </summary>
    public static TheoryData<byte[], MqttReasonCode, string?> KnownDisconnects => new()
    {
        { KnownPackets.Disconnect, MqttReasonCode.Success, null },
        { KnownPackets.DisconnectTakenOver, MqttReasonCode.SessionTakenOver, null },
        { KnownPackets.DisconnectWithReason, MqttReasonCode.SessionTakenOver, "x" },
    };

    /// <summary>
    /// Proves each known DISCONNECT decodes to its values and is the bytes the encoder writes for them.
    /// </summary>
    /// <param name="packet">
    /// The known packet.
    /// </param>
    /// <param name="code">
    /// The expected reason code.
    /// </param>
    /// <param name="reasonString">
    /// The expected reason string, or <see langword="null"/> for none.
    /// </param>
    [Theory]
    [MemberData(nameof(KnownDisconnects))]
    public void TryDecode_KnownDisconnect_DecodesAndIsWhatTheEncoderWrites(byte[] packet, MqttReasonCode code, string? reasonString)
    {
        // Arrange
        byte[] destination = new byte[packet.Length];

        // Act
        MqttReasonCode result = DisconnectCodec.TryDecode(packet, out DisconnectPacket disconnect);
        int size = DisconnectCodec.GetSize(code, reasonString);
        int written = DisconnectCodec.Write(destination, code, reasonString);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        disconnect.ReasonCode.ShouldBe(code);
        disconnect.Properties.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> text).ShouldBe(reasonString is not null);
        if (reasonString is not null)
        {
            Encoding.UTF8.GetString(text).ShouldBe(reasonString);
        }

        size.ShouldBe(packet.Length);
        written.ShouldBe(packet.Length);
        destination.ShouldBe(packet);
    }

    /// <summary>
    /// Proves the Session Expiry Interval and Server Reference properties, which DISCONNECT allows, are read.
    /// </summary>
    [Fact]
    public void TryDecode_SessionExpiryAndServerReference_AreRead()
    {
        // Arrange
        byte[] packet = Hex.Parse("E0 0B 9C 09 11 00 00 00 1E 1C 00 01 73");

        // Act
        MqttReasonCode result = DisconnectCodec.TryDecode(packet, out DisconnectPacket disconnect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        disconnect.ReasonCode.ShouldBe(MqttReasonCode.UseAnotherServer);
        disconnect.Properties.TryGetInteger(PropertyId.SessionExpiryInterval, out uint expiry).ShouldBeTrue();
        expiry.ShouldBe(30u);
        disconnect.Properties.TryGetBytes(PropertyId.ServerReference, out ReadOnlySpan<byte> reference).ShouldBeTrue();
        Encoding.UTF8.GetString(reference).ShouldBe("s");
    }

    /// <summary>
    /// Proves a reason code followed by an empty property block decodes.
    /// </summary>
    [Fact]
    public void TryDecode_ReasonCodeAndEmptyBlock_IsSuccess()
    {
        // Arrange
        byte[] packet = Hex.Parse("E0 02 8E 00");

        // Act
        MqttReasonCode result = DisconnectCodec.TryDecode(packet, out DisconnectPacket disconnect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        disconnect.ReasonCode.ShouldBe(MqttReasonCode.SessionTakenOver);
    }

    /// <summary>
    /// Proves a property past its block, a block past the packet, a left-over byte and a property DISCONNECT does
    /// not allow are malformed, and the output stays at its default.
    /// </summary>
    /// <param name="hex">
    /// The broken packet.
    /// </param>
    [Theory]
    [InlineData("E0 03 8E 05 1F")]
    [InlineData("E0 02 8E 01")]
    [InlineData("E0 03 8E 00 00")]
    [InlineData("E0 06 8E 04 21 00 01 00")]
    public void TryDecode_BrokenDisconnect_IsMalformed(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = DisconnectCodec.TryDecode(packet, out DisconnectPacket disconnect);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
        disconnect.ReasonCode.ShouldBe(MqttReasonCode.Success);
    }

    /// <summary>
    /// Proves a buffer that is not exactly one framed DISCONNECT is a caller bug.
    /// </summary>
    /// <param name="hex">
    /// The unusable buffer.
    /// </param>
    [Theory]
    [InlineData("C0 00")]
    [InlineData("E0")]
    [InlineData("E0 00 00")]
    public void TryDecode_NotExactlyOneDisconnect_Throws(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act & Assert
        Should.Throw<ArgumentException>(() => DisconnectCodec.TryDecode(packet, out _));
    }

    /// <summary>
    /// Proves a reason string that cannot be carried, or a destination that is too small, throws and nothing is
    /// written.
    /// </summary>
    [Fact]
    public void Write_UnusableReasonStringOrSmallDestination_ThrowsWithoutWriting()
    {
        // Arrange
        byte[] destination = new byte[16];
        byte[] small = new byte[1];

        // Act & Assert
        Should.Throw<ArgumentException>(() => DisconnectCodec.GetSize(MqttReasonCode.SessionTakenOver, "a\0b"));
        Should.Throw<ArgumentException>(() => DisconnectCodec.Write(destination, MqttReasonCode.SessionTakenOver, "a\0b"));
        Should.Throw<ArgumentException>(() => DisconnectCodec.Write(small, MqttReasonCode.Success, null));
        destination.ShouldAllBe(b => b == 0);
        small.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a success DISCONNECT with a reason string is written as the reason code and the property block.
    /// </summary>
    [Fact]
    public void Write_SuccessWithReasonString_WritesTheReasonCodeAndTheBlock()
    {
        // Arrange
        byte[] destination = new byte[DisconnectCodec.GetSize(MqttReasonCode.Success, "x")];

        // Act
        DisconnectCodec.Write(destination, MqttReasonCode.Success, "x");

        // Assert
        destination.ShouldBe(Hex.Parse("E0 06 00 04 1F 00 01 78"));
    }

    /// <summary>
    /// Proves a DISCONNECT that MQTTnet encodes decodes here to the same values.
    /// </summary>
    [Fact]
    public void TryDecode_DisconnectMqttNetEncodes_DecodesToTheSameValues()
    {
        // Arrange
        byte[] packet = MqttNetFormatter.Encode(new MqttDisconnectPacket
        {
            ReasonCode = MqttDisconnectReasonCode.SessionTakenOver,
            ReasonString = "bye",
        });

        // Act
        MqttReasonCode result = DisconnectCodec.TryDecode(packet, out DisconnectPacket disconnect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        disconnect.ReasonCode.ShouldBe(MqttReasonCode.SessionTakenOver);
        disconnect.Properties.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> text).ShouldBeTrue();
        Encoding.UTF8.GetString(text).ShouldBe("bye");
    }

    /// <summary>
    /// Proves a DISCONNECT this codec encodes decodes in MQTTnet to the same values.
    /// </summary>
    [Fact]
    public void Write_Disconnect_MqttNetDecodesTheSameValues()
    {
        // Arrange
        byte[] destination = new byte[DisconnectCodec.GetSize(MqttReasonCode.SessionTakenOver, "bye")];
        DisconnectCodec.Write(destination, MqttReasonCode.SessionTakenOver, "bye");

        // Act
        MqttDisconnectPacket decoded = (MqttDisconnectPacket)MqttNetFormatter.Decode(destination);

        // Assert
        decoded.ReasonCode.ShouldBe(MqttDisconnectReasonCode.SessionTakenOver);
        decoded.ReasonString.ShouldBe("bye");
    }
}
