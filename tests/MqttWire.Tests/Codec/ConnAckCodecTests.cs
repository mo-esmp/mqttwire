using System.Text;
using MQTTnet.Packets;
using MQTTnet.Protocol;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests decoding and encoding the CONNACK packet, including a cross-check against MQTTnet.
/// </summary>
public class ConnAckCodecTests
{
    /// <summary>
    /// Proves the encoder writes the known CONNACK bytes and sizes them correctly.
    /// </summary>
    [Fact]
    public void Write_KnownFields_WritesTheKnownConnAck()
    {
        // Arrange
        ConnAckFields fields = new() { ReasonCode = MqttReasonCode.Success, ReceiveMaximum = 256, SubscriptionIdentifiersAvailable = false };
        byte[] destination = new byte[16];

        // Act
        int size = ConnAckCodec.GetSize(fields);
        int written = ConnAckCodec.Write(destination, fields);

        // Assert
        size.ShouldBe(10);
        written.ShouldBe(10);
        destination.AsSpan(0, written).ToArray().ShouldBe(KnownPackets.ConnAck);
    }

    /// <summary>
    /// Proves the known CONNACK decodes to its reason code and properties.
    /// </summary>
    [Fact]
    public void TryDecode_KnownConnAck_ReturnsItsFields()
    {
        // Arrange
        byte[] packet = KnownPackets.ConnAck;

        // Act
        MqttReasonCode result = ConnAckCodec.TryDecode(packet, out ConnAckPacket connAck);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        connAck.SessionPresent.ShouldBeFalse();
        connAck.ReasonCode.ShouldBe(MqttReasonCode.Success);
        connAck.Properties.TryGetInteger(PropertyId.ReceiveMaximum, out uint receiveMaximum).ShouldBeTrue();
        receiveMaximum.ShouldBe(256u);
        connAck.Properties.TryGetInteger(PropertyId.SubscriptionIdentifierAvailable, out uint available).ShouldBeTrue();
        available.ShouldBe(0u);
    }

    /// <summary>
    /// Proves a refusal with a reason string and a user property survives an encode and decode.
    /// </summary>
    [Fact]
    public void Write_RefusalWithReasonStringAndUserProperty_RoundTrips()
    {
        // Arrange
        MqttWireUserProperty[] userProperties = [new MqttWireUserProperty("k", "v")];
        ConnAckFields fields = new() { ReasonCode = MqttReasonCode.NotAuthorized, ReasonString = "no", UserProperties = userProperties };
        byte[] destination = new byte[ConnAckCodec.GetSize(fields)];

        // Act
        ConnAckCodec.Write(destination, fields);
        MqttReasonCode result = ConnAckCodec.TryDecode(destination, out ConnAckPacket connAck);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        connAck.ReasonCode.ShouldBe(MqttReasonCode.NotAuthorized);
        connAck.Properties.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> reason).ShouldBeTrue();
        Encoding.UTF8.GetString(reason).ShouldBe("no");
        int count = 0;
        foreach (Utf8Pair pair in connAck.Properties.UserProperties)
        {
            Encoding.UTF8.GetString(pair.Name).ShouldBe("k");
            Encoding.UTF8.GetString(pair.Value).ShouldBe("v");
            count++;
        }

        count.ShouldBe(1);
    }

    /// <summary>
    /// Proves every property is written, in the fixed order 11 21 24 29 13 1F 26.
    /// </summary>
    [Fact]
    public void Write_AllFieldsSet_WritesPropertiesInTheFixedOrder()
    {
        // Arrange
        MqttWireUserProperty[] userProperties = [new MqttWireUserProperty("k", "v")];
        ConnAckFields fields = new()
        {
            ReasonCode = MqttReasonCode.Success,
            SessionExpiryInterval = 1,
            ReceiveMaximum = 2,
            MaximumQoS = 1,
            SubscriptionIdentifiersAvailable = true,
            ServerKeepAlive = 3,
            ReasonString = "r",
            UserProperties = userProperties,
        };
        byte[] destination = new byte[64];

        // Act
        int written = ConnAckCodec.Write(destination, fields);

        // Assert
        written.ShouldBe(ConnAckCodec.GetSize(fields));
        destination.AsSpan(0, written).ToArray().ShouldBe(
            Hex.Parse("20 1D 00 00 1A 11 00 00 00 01 21 00 02 24 01 29 01 13 00 03 1F 00 01 72 26 00 01 6B 00 01 76"));
    }

    /// <summary>
    /// Proves the session present flag is reported.
    /// </summary>
    [Fact]
    public void TryDecode_SessionPresentFlag_IsReported()
    {
        // Arrange
        byte[] packet = Hex.Parse("20 03 01 00 00");

        // Act
        MqttReasonCode result = ConnAckCodec.TryDecode(packet, out ConnAckPacket connAck);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        connAck.SessionPresent.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a broken packet body is malformed and leaves the output at its default.
    /// </summary>
    /// <param name="hex">
    /// The broken packet.
    /// </param>
    [Theory]
    [InlineData("20 03 02 00 00")] // reserved flag
    [InlineData("20 02 00 00")] // no property length
    [InlineData("20 04 00 00 00 00")] // a left-over byte
    public void TryDecode_BrokenConnAck_IsMalformed(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = ConnAckCodec.TryDecode(packet, out ConnAckPacket connAck);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
        connAck.SessionPresent.ShouldBeFalse();
        connAck.ReasonCode.ShouldBe(MqttReasonCode.Success);
    }

    /// <summary>
    /// Proves a forbidden property value is a protocol error.
    /// </summary>
    [Fact]
    public void TryDecode_MaximumQoSTwo_IsProtocolError()
    {
        // Arrange
        byte[] packet = Hex.Parse("20 05 00 00 02 24 02");

        // Act
        MqttReasonCode result = ConnAckCodec.TryDecode(packet, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.ProtocolError);
    }

    /// <summary>
    /// Proves a packet of another type, an incomplete one and one with trailing bytes are caller bugs.
    /// </summary>
    /// <param name="hex">
    /// The bytes that are not exactly one CONNACK.
    /// </param>
    [Theory]
    [InlineData("10 03 00 00 00")] // CONNECT
    [InlineData("20 03 00 00")] // cut short
    [InlineData("20 03 00 00 00 00")] // trailing byte
    public void TryDecode_NotExactlyOneConnAck_Throws(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act & Assert
        Should.Throw<ArgumentException>(() => ConnAckCodec.TryDecode(packet, out _));
    }

    /// <summary>
    /// Proves a CONNACK that MQTTnet encodes decodes here to the same values.
    /// </summary>
    [Fact]
    public void TryDecode_ConnAckMqttNetEncodes_DecodesToTheSameValues()
    {
        // Arrange
        MqttConnAckPacket source = new()
        {
            ReasonCode = MqttConnectReasonCode.Success,
            ReceiveMaximum = 256,
            MaximumQoS = MqttQualityOfServiceLevel.AtLeastOnce,
            ServerKeepAlive = 60,
            ReasonString = "r",
            SubscriptionIdentifiersAvailable = false,
            UserProperties = [new MqttUserProperty("k", "v"u8.ToArray())],
        };
        byte[] packet = MqttNetFormatter.Encode(source);

        // Act
        MqttReasonCode result = ConnAckCodec.TryDecode(packet, out ConnAckPacket connAck);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        connAck.ReasonCode.ShouldBe(MqttReasonCode.Success);
        connAck.Properties.TryGetInteger(PropertyId.ReceiveMaximum, out uint receiveMaximum).ShouldBeTrue();
        receiveMaximum.ShouldBe(256u);
        connAck.Properties.TryGetInteger(PropertyId.MaximumQoS, out uint maximumQoS).ShouldBeTrue();
        maximumQoS.ShouldBe(1u);
        connAck.Properties.TryGetInteger(PropertyId.ServerKeepAlive, out uint keepAlive).ShouldBeTrue();
        keepAlive.ShouldBe(60u);
        connAck.Properties.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> reason).ShouldBeTrue();
        Encoding.UTF8.GetString(reason).ShouldBe("r");
        connAck.Properties.TryGetInteger(PropertyId.SubscriptionIdentifierAvailable, out uint available).ShouldBeTrue();
        available.ShouldBe(0u);
        int count = 0;
        foreach (Utf8Pair pair in connAck.Properties.UserProperties)
        {
            Encoding.UTF8.GetString(pair.Name).ShouldBe("k");
            Encoding.UTF8.GetString(pair.Value).ShouldBe("v");
            count++;
        }

        count.ShouldBe(1);
    }

    /// <summary>
    /// Proves a CONNACK this codec encodes decodes in MQTTnet to the same values.
    /// </summary>
    [Fact]
    public void Write_AllFieldsSet_MqttNetDecodesTheSameValues()
    {
        // Arrange
        MqttWireUserProperty[] userProperties = [new MqttWireUserProperty("k", "v")];
        ConnAckFields fields = new()
        {
            ReasonCode = MqttReasonCode.Success,
            ReceiveMaximum = 256,
            MaximumQoS = 1,
            ServerKeepAlive = 60,
            ReasonString = "r",
            SubscriptionIdentifiersAvailable = false,
            UserProperties = userProperties,
        };
        byte[] destination = new byte[ConnAckCodec.GetSize(fields)];
        ConnAckCodec.Write(destination, fields);

        // Act
        MqttConnAckPacket decoded = (MqttConnAckPacket)MqttNetFormatter.Decode(destination);

        // Assert
        decoded.ReasonCode.ShouldBe(MqttConnectReasonCode.Success);
        decoded.ReceiveMaximum.ShouldBe((ushort)256);
        decoded.MaximumQoS.ShouldBe(MqttQualityOfServiceLevel.AtLeastOnce);
        decoded.ServerKeepAlive.ShouldBe((ushort)60);
        decoded.ReasonString.ShouldBe("r");
        decoded.SubscriptionIdentifiersAvailable.ShouldBeFalse();
        decoded.UserProperties.Count.ShouldBe(1);
        decoded.UserProperties[0].Name.ShouldBe("k");
    }

    /// <summary>
    /// Proves a Receive Maximum of zero throws from GetSize and Write before anything is written.
    /// </summary>
    [Fact]
    public void Write_ReceiveMaximumZero_Throws()
    {
        // Arrange
        byte[] destination = new byte[64];

        // Act & Assert
        Should.Throw<ArgumentOutOfRangeException>(() => ConnAckCodec.GetSize(new ConnAckFields { ReceiveMaximum = 0 }));
        Should.Throw<ArgumentOutOfRangeException>(() => ConnAckCodec.Write(destination, new ConnAckFields { ReceiveMaximum = 0 }));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a Maximum QoS above 1 throws from GetSize and Write before anything is written.
    /// </summary>
    [Fact]
    public void Write_MaximumQoSTwo_Throws()
    {
        // Arrange
        byte[] destination = new byte[64];

        // Act & Assert
        Should.Throw<ArgumentOutOfRangeException>(() => ConnAckCodec.GetSize(new ConnAckFields { MaximumQoS = 2 }));
        Should.Throw<ArgumentOutOfRangeException>(() => ConnAckCodec.Write(destination, new ConnAckFields { MaximumQoS = 2 }));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a destination smaller than the packet throws and nothing is written.
    /// </summary>
    [Fact]
    public void Write_DestinationTooSmall_ThrowsWithoutWriting()
    {
        // Arrange
        byte[] destination = new byte[9];

        // Act & Assert
        Should.Throw<ArgumentException>(() => ConnAckCodec.Write(
            destination,
            new ConnAckFields { ReceiveMaximum = 256, SubscriptionIdentifiersAvailable = false }));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a string that MQTT cannot carry throws before the first byte is written.
    /// </summary>
    [Fact]
    public void Write_ReasonStringWithNul_ThrowsWithoutWriting()
    {
        // Arrange
        byte[] destination = new byte[64];

        // Act & Assert
        Should.Throw<ArgumentException>(() => ConnAckCodec.Write(destination, new ConnAckFields { ReasonString = "r\0" }));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves user properties too large for the largest remaining length throw from the size and the writer and
    /// write nothing, both just past the limit and past the range of an <see cref="int"/>.
    /// </summary>
    /// <param name="count">
    /// The number of user properties, each a 65,535-byte name and value.
    /// </param>
    [Theory]
    [InlineData(2_100)]
    [InlineData(16_400)]
    public void Write_UserPropertiesPastTheLargestRemainingLength_ThrowsWithoutWriting(int count)
    {
        // Arrange
        string text = new('a', 65_535);
        MqttWireUserProperty[] userProperties = new MqttWireUserProperty[count];
        Array.Fill(userProperties, new MqttWireUserProperty(text, text));
        byte[] destination = new byte[64];

        // Act & Assert
        Should.Throw<ArgumentException>(() => ConnAckCodec.GetSize(new ConnAckFields { UserProperties = userProperties }));
        Should.Throw<ArgumentException>(() => ConnAckCodec.Write(destination, new ConnAckFields { UserProperties = userProperties }));
        destination.ShouldAllBe(b => b == 0);
    }
}
