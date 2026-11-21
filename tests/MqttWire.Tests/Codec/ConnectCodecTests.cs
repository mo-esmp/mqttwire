using System.Text;
using MQTTnet.Packets;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests decoding and encoding the CONNECT packet, including a cross-check against MQTTnet.
/// </summary>
public sealed class ConnectCodecTests
{
    /// <summary>
    /// Proves the known CONNECT decodes to its fields and properties.
    /// </summary>
    [Fact]
    public void TryDecode_KnownConnect_ReturnsItsFields()
    {
        // Arrange
        byte[] packet = KnownPackets.Connect;

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        Encoding.UTF8.GetString(connect.ClientId).ShouldBe("c1");
        connect.HasUserName.ShouldBeTrue();
        Encoding.UTF8.GetString(connect.UserName).ShouldBe("u");
        connect.HasPassword.ShouldBeTrue();
        connect.Password.ToArray().ShouldBe(new byte[] { 0x70 });
        connect.KeepAliveSeconds.ShouldBe((ushort)15);
        connect.CleanStart.ShouldBeTrue();
        connect.HasWill.ShouldBeFalse();
        connect.Properties.TryGetInteger(PropertyId.ReceiveMaximum, out uint receiveMaximum).ShouldBeTrue();
        receiveMaximum.ShouldBe(1024u);
    }

    /// <summary>
    /// Proves the encoder writes the known CONNECT bytes and sizes them correctly.
    /// </summary>
    [Fact]
    public void Write_KnownFields_WritesTheKnownConnect()
    {
        // Arrange
        ConnectFields fields = new() { ClientId = "c1", UserName = "u", Password = [0x70], KeepAliveSeconds = 15, ReceiveMaximum = 1024 };
        byte[] destination = new byte[64];

        // Act
        int size = ConnectCodec.GetSize(fields);
        int written = ConnectCodec.Write(destination, fields);

        // Assert
        size.ShouldBe(26);
        written.ShouldBe(26);
        destination.AsSpan(0, written).ToArray().ShouldBe(KnownPackets.Connect);
    }

    /// <summary>
    /// Proves a CONNECT with neither user name nor password reports neither.
    /// </summary>
    [Fact]
    public void TryDecode_ConnectWithoutUserNameOrPassword_ReportsNeither()
    {
        // Arrange
        byte[] packet = KnownPackets.ConnectMinimal;

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        connect.HasUserName.ShouldBeFalse();
        connect.HasPassword.ShouldBeFalse();
        connect.UserName.IsEmpty.ShouldBeTrue();
        connect.Password.IsEmpty.ShouldBeTrue();
    }

    /// <summary>
    /// Proves the encoder leaves out the user name and the password, and their flags, when they are null.
    /// </summary>
    [Fact]
    public void Write_NoUserNameOrPassword_WritesNeitherFlagNorField()
    {
        // Arrange
        ConnectFields fields = new() { ClientId = "c", KeepAliveSeconds = 0, ReceiveMaximum = 1024 };
        byte[] destination = new byte[64];

        // Act
        int written = ConnectCodec.Write(destination, fields);
        MqttReasonCode result = ConnectCodec.TryDecode(destination.AsSpan(0, written), out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        written.ShouldBe(ConnectCodec.GetSize(fields));
        destination[9].ShouldBe((byte)0x02);
        connect.HasUserName.ShouldBeFalse();
        connect.HasPassword.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a password without a user name is accepted, as MQTT 5 allows it.
    /// </summary>
    [Fact]
    public void TryDecode_PasswordWithoutUserName_Succeeds()
    {
        // Arrange
        byte[] packet = Hex.Parse("10 11 00 04 4D 51 54 54 05 42 00 00 00 00 01 63 00 01 70");

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        connect.HasUserName.ShouldBeFalse();
        connect.HasPassword.ShouldBeTrue();
        connect.Password.ToArray().ShouldBe(new byte[] { 0x70 });
    }

    /// <summary>
    /// Proves an empty client id decodes; refusing it is the server's decision.
    /// </summary>
    [Fact]
    public void TryDecode_EmptyClientId_Succeeds()
    {
        // Arrange
        byte[] packet = Hex.Parse("10 0D 00 04 4D 51 54 54 05 02 00 00 00 00 00");

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        connect.ClientId.IsEmpty.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a Will is read and reported.
    /// </summary>
    [Fact]
    public void TryDecode_ConnectWithWill_ReportsTheWill()
    {
        // Arrange
        byte[] packet = KnownPackets.ConnectWithWill;

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        connect.HasWill.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a Will at QoS 1 with retain set is valid and reported.
    /// </summary>
    [Fact]
    public void TryDecode_WillAtQoS1WithRetain_ReportsTheWill()
    {
        // Arrange
        byte[] packet = Hex.Parse("10 15 00 04 4D 51 54 54 05 2E 00 00 00 00 01 63 00 00 01 77 00 01 78");

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        connect.HasWill.ShouldBeTrue();
    }

    /// <summary>
    /// Proves an Authentication Method is reported in the properties, not refused by the codec.
    /// </summary>
    [Fact]
    public void TryDecode_AuthenticationMethod_IsReportedNotRefused()
    {
        // Arrange
        byte[] packet = Hex.Parse("10 12 00 04 4D 51 54 54 05 02 00 00 04 15 00 01 61 00 01 63");

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        connect.Properties.Contains(PropertyId.AuthenticationMethod).ShouldBeTrue();
    }

    /// <summary>
    /// Proves a broken packet body is malformed and leaves the output at its default.
    /// </summary>
    /// <param name="hex">
    /// The broken packet.
    /// </param>
    [Theory]
    [InlineData("10 0E 00 04 4D 51 54 54 05 03 00 00 00 00 01 63")] // reserved flag
    [InlineData("10 0E 00 04 4D 51 54 54 05 0A 00 00 00 00 01 63")] // Will QoS without Will
    [InlineData("10 0E 00 04 4D 51 54 54 05 22 00 00 00 00 01 63")] // Will retain without Will
    [InlineData("10 15 00 04 4D 51 54 54 05 1E 00 00 00 00 01 63 00 00 01 77 00 01 78")] // Will QoS 3
    [InlineData("10 0E 00 04 4D 51 54 54 05 42 00 00 00 00 01 63")] // password flagged but missing
    [InlineData("10 0E 00 04 4D 51 54 54 05 82 00 00 00 00 01 63")] // user name flagged but missing
    [InlineData("10 0F 00 04 4D 51 54 54 05 02 00 00 00 00 01 63 00")] // a left-over byte
    [InlineData("10 03 00 04 4D")] // protocol name cut
    [InlineData("10 1A 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 05 11 00 00 00 01 00 01 77 00 01 78")] // Session Expiry among the Will properties
    public void TryDecode_BrokenConnect_IsMalformed(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
        connect.ClientId.IsEmpty.ShouldBeTrue();
        connect.HasUserName.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a Will topic that is not a topic name, empty or holding a wildcard, is refused as an invalid topic
    /// name and leaves the output at its default.
    /// </summary>
    /// <param name="hex">
    /// The packet with the invalid Will topic and no other fault.
    /// </param>
    [Theory]
    [InlineData("10 14 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 00 00 00 00 01 78")] // empty
    [InlineData("10 15 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 00 00 01 2B 00 01 78")] // "+"
    [InlineData("10 15 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 00 00 01 23 00 01 78")] // "#"
    [InlineData("10 17 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 00 00 03 61 2F 23 00 01 78")] // "a/#"
    [InlineData("10 19 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 00 00 05 61 2F 2B 2F 62 00 01 78")] // "a/+/b"
    public void TryDecode_WillTopicEmptyOrWithWildcard_IsTopicNameInvalid(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.TopicNameInvalid);
        connect.ClientId.IsEmpty.ShouldBeTrue();
        connect.HasWill.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a Will whose Response Topic property is not a topic name, empty or holding a wildcard, is a protocol
    /// error and leaves the output at its default.
    /// </summary>
    /// <param name="hex">
    /// The CONNECT with a valid Will topic and the invalid Response Topic among the Will properties.
    /// </param>
    [Theory]
    [InlineData("10 1F 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 0A 08 00 07 72 65 70 6C 79 2F 2B 00 01 77 00 01 78")] // "reply/+"
    [InlineData("10 1F 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 0A 08 00 07 72 65 70 6C 79 2F 23 00 01 77 00 01 78")] // "reply/#"
    [InlineData("10 18 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 03 08 00 00 00 01 77 00 01 78")] // empty
    public void TryDecode_WillResponseTopicNotATopicName_IsProtocolError(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.ProtocolError);
        connect.ClientId.IsEmpty.ShouldBeTrue();
        connect.HasWill.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a Will whose Response Topic property is a topic name is accepted.
    /// </summary>
    [Fact]
    public void TryDecode_WillResponseTopicThatIsATopicName_ReportsTheWill()
    {
        // Arrange: Will topic "w", Response Topic "reply/x".
        byte[] packet = Hex.Parse("10 1F 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 0A 08 00 07 72 65 70 6C 79 2F 78 00 01 77 00 01 78");

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        connect.HasWill.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a packet that is malformed after an invalid Will topic is reported as malformed, not as an invalid
    /// topic name.
    /// </summary>
    /// <param name="hex">
    /// The packet with the Will topic "+" and a later fault.
    /// </param>
    [Theory]
    [InlineData("10 16 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 00 00 01 2B 00 01 78 00")] // a left-over byte
    [InlineData("10 15 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 00 00 01 2B 00 02 78")] // Will payload cut
    [InlineData("10 15 00 04 4D 51 54 54 05 86 00 00 00 00 01 63 00 00 01 2B 00 01 78")] // user name flagged but missing
    public void TryDecode_InvalidWillTopicInAMalformedPacket_IsMalformed(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
    }

    /// <summary>
    /// Proves another protocol name or version is reported as unsupported, before the connect flags are checked
    /// (plan decision 3).
    /// </summary>
    /// <param name="hex">
    /// The packet with another protocol name or level.
    /// </param>
    [Theory]
    [InlineData("10 0F 00 06 4D 51 49 73 64 70 03 02 00 3C 00 01 63")] // "MQIsdp", level 3
    [InlineData("10 0E 00 04 4D 51 54 54 04 02 00 00 00 00 01 63")] // "MQTT", level 4
    [InlineData("10 0E 00 04 4D 51 54 54 04 03 00 00 00 00 01 63")] // "MQTT", level 4 and a reserved flag
    public void TryDecode_OtherProtocolOrVersion_IsUnsupported(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.UnsupportedProtocolVersion);
    }

    /// <summary>
    /// Proves Authentication Data without an Authentication Method is a protocol error.
    /// </summary>
    [Fact]
    public void TryDecode_AuthenticationDataWithoutMethod_IsProtocolError()
    {
        // Arrange
        byte[] packet = Hex.Parse("10 11 00 04 4D 51 54 54 05 02 00 00 03 16 00 00 00 01 63");

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.ProtocolError);
    }

    /// <summary>
    /// Proves a packet of another type, an incomplete one and one with trailing bytes are caller bugs.
    /// </summary>
    /// <param name="hex">
    /// The bytes that are not exactly one CONNECT.
    /// </param>
    [Theory]
    [InlineData("20 03 00 00 00")] // CONNACK
    [InlineData("10 0E 00 04 4D 51 54 54 05 02 00 00 00 00 01")] // cut short
    [InlineData("10 0E 00 04 4D 51 54 54 05 02 00 00 00 00 01 63 00")] // trailing byte
    [InlineData("")] // nothing
    public void TryDecode_NotExactlyOneConnect_Throws(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act & Assert
        Should.Throw<ArgumentException>(() => ConnectCodec.TryDecode(packet, out _));
    }

    /// <summary>
    /// Proves a destination smaller than the packet throws and nothing is written.
    /// </summary>
    [Fact]
    public void Write_DestinationTooSmall_ThrowsWithoutWriting()
    {
        // Arrange
        byte[] destination = new byte[25];

        // Act & Assert
        Should.Throw<ArgumentException>(() => ConnectCodec.Write(
            destination,
            new ConnectFields { ClientId = "c1", UserName = "u", Password = [0x70], KeepAliveSeconds = 15, ReceiveMaximum = 1024 }));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a string that MQTT cannot carry throws before the first byte is written.
    /// </summary>
    [Fact]
    public void Write_UserNameWithNul_ThrowsWithoutWriting()
    {
        // Arrange
        byte[] destination = new byte[64];

        // Act & Assert
        Should.Throw<ArgumentException>(() => ConnectCodec.Write(
            destination,
            new ConnectFields { ClientId = "c1", UserName = "u\0", KeepAliveSeconds = 15, ReceiveMaximum = 1024 }));
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a CONNECT that MQTTnet encodes decodes here to the same values.
    /// </summary>
    [Fact]
    public void TryDecode_ConnectMqttNetEncodes_DecodesToTheSameValues()
    {
        // Arrange
        MqttConnectPacket source = new()
        {
            ClientId = "c1",
            Username = "u",
            Password = [0x70],
            KeepAlivePeriod = 15,
            CleanSession = true,
            ReceiveMaximum = 1024,
            MaximumPacketSize = 65536,
            RequestProblemInformation = false,
            UserProperties = [new MqttUserProperty("k", "v"u8.ToArray())],
        };
        byte[] packet = MqttNetFormatter.Encode(source);

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        Encoding.UTF8.GetString(connect.ClientId).ShouldBe("c1");
        Encoding.UTF8.GetString(connect.UserName).ShouldBe("u");
        connect.Password.ToArray().ShouldBe(new byte[] { 0x70 });
        connect.KeepAliveSeconds.ShouldBe((ushort)15);
        connect.CleanStart.ShouldBeTrue();
        connect.Properties.TryGetInteger(PropertyId.MaximumPacketSize, out uint maximumPacketSize).ShouldBeTrue();
        maximumPacketSize.ShouldBe(65536u);
        connect.Properties.TryGetInteger(PropertyId.RequestProblemInformation, out uint requestProblemInformation).ShouldBeTrue();
        requestProblemInformation.ShouldBe(0u);
        int userProperties = 0;
        foreach (Utf8Pair pair in connect.Properties.UserProperties)
        {
            Encoding.UTF8.GetString(pair.Name).ShouldBe("k");
            Encoding.UTF8.GetString(pair.Value).ShouldBe("v");
            userProperties++;
        }

        userProperties.ShouldBe(1);
    }

    /// <summary>
    /// Proves a CONNECT this codec encodes decodes in MQTTnet to the same values.
    /// </summary>
    [Fact]
    public void Write_KnownFields_MqttNetDecodesTheSameValues()
    {
        // Arrange
        ConnectFields fields = new() { ClientId = "c1", UserName = "u", Password = [0x70], KeepAliveSeconds = 15, ReceiveMaximum = 1024 };
        byte[] destination = new byte[ConnectCodec.GetSize(fields)];
        ConnectCodec.Write(destination, fields);

        // Act
        MqttConnectPacket decoded = (MqttConnectPacket)MqttNetFormatter.Decode(destination);

        // Assert
        decoded.ClientId.ShouldBe("c1");
        decoded.Username.ShouldBe("u");
        decoded.Password.ShouldBe(new byte[] { 0x70 });
        decoded.KeepAlivePeriod.ShouldBe((ushort)15);
        decoded.CleanSession.ShouldBeTrue();
        decoded.ReceiveMaximum.ShouldBe((ushort)1024);
    }

    /// <summary>
    /// Proves a Maximum Packet Size is written as a four-byte property right after the Receive Maximum.
    /// </summary>
    [Fact]
    public void Write_MaximumPacketSize_WritesItAfterReceiveMaximum()
    {
        // Arrange
        ConnectFields fields = new()
        {
            ClientId = "c1",
            UserName = "u",
            Password = [0x70],
            KeepAliveSeconds = 15,
            ReceiveMaximum = 1024,
            MaximumPacketSize = 1_048_576,
        };
        byte[] destination = new byte[64];

        // Act
        int size = ConnectCodec.GetSize(fields);
        int written = ConnectCodec.Write(destination, fields);

        // Assert
        size.ShouldBe(KnownPackets.ConnectWithMaximumPacketSize.Length);
        written.ShouldBe(size);
        destination.AsSpan(0, written).ToArray().ShouldBe(KnownPackets.ConnectWithMaximumPacketSize);
    }

    /// <summary>
    /// Proves a CONNECT carrying a Maximum Packet Size decodes with the property readable.
    /// </summary>
    [Fact]
    public void TryDecode_ConnectWithMaximumPacketSize_ExposesIt()
    {
        // Arrange
        byte[] packet = KnownPackets.ConnectWithMaximumPacketSize;

        // Act
        MqttReasonCode result = ConnectCodec.TryDecode(packet, out ConnectPacket connect);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        connect.Properties.TryGetInteger(PropertyId.MaximumPacketSize, out uint maximumPacketSize).ShouldBeTrue();
        maximumPacketSize.ShouldBe(1_048_576u);
    }

    /// <summary>
    /// Proves a Maximum Packet Size of zero, which the standard forbids, throws from both GetSize and Write before
    /// anything is written.
    /// </summary>
    [Fact]
    public void Write_MaximumPacketSizeZero_ThrowsAndWritesNothing()
    {
        // Arrange
        byte[] destination = new byte[64];

        // Act
        Exception? sizeException = Record.Exception(() => ConnectCodec.GetSize(new ConnectFields { ClientId = "c1", ReceiveMaximum = 1024, MaximumPacketSize = 0 }));
        Exception? writeException = Record.Exception(() => ConnectCodec.Write(destination, new ConnectFields { ClientId = "c1", ReceiveMaximum = 1024, MaximumPacketSize = 0 }));

        // Assert
        sizeException.ShouldBeOfType<ArgumentOutOfRangeException>();
        writeException.ShouldBeOfType<ArgumentOutOfRangeException>();
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a CONNECT with a Maximum Packet Size that this codec encodes decodes in MQTTnet to the same value.
    /// </summary>
    [Fact]
    public void Write_MaximumPacketSize_MatchesMqttNet()
    {
        // Arrange
        ConnectFields fields = new()
        {
            ClientId = "c1",
            UserName = "u",
            Password = [0x70],
            KeepAliveSeconds = 15,
            ReceiveMaximum = 1024,
            MaximumPacketSize = 1_048_576,
        };
        byte[] destination = new byte[ConnectCodec.GetSize(fields)];
        ConnectCodec.Write(destination, fields);

        // Act
        MqttConnectPacket decoded = (MqttConnectPacket)MqttNetFormatter.Decode(destination);

        // Assert
        decoded.MaximumPacketSize.ShouldBe(1_048_576u);
        decoded.ReceiveMaximum.ShouldBe((ushort)1024);
        decoded.ClientId.ShouldBe("c1");
    }

    /// <summary>
    /// Proves a Receive Maximum of zero, which the standard forbids, throws from both GetSize and Write before anything is written.
    /// </summary>
    [Fact]
    public void Write_ReceiveMaximumZero_Throws()
    {
        // Arrange
        byte[] destination = new byte[64];

        // Act & Assert
        Should.Throw<ArgumentOutOfRangeException>(() => ConnectCodec.GetSize(new ConnectFields { ClientId = "c1", ReceiveMaximum = 0 }));
        Should.Throw<ArgumentOutOfRangeException>(() => ConnectCodec.Write(destination, new ConnectFields { ClientId = "c1", ReceiveMaximum = 0 }));
        destination.ShouldAllBe(b => b == 0);
    }
}
