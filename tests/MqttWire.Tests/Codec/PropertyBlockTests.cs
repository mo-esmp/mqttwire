using System.Text;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

public class PropertyBlockTests
{
    private const string ByteValue = "01";
    private const string TwoByteValue = "00 01";
    private const string FourByteValue = "00 00 00 01";
    private const string VariableValue = "01";
    private const string StringValue = "00 01 61";
    private const string BinaryValue = "00 01 61";
    private const string PairValue = "00 01 61 00 01 62";

    private const ushort Connect = 1;
    private const ushort Will = 2;
    private const ushort ConnAck = 4;
    private const ushort Publish = 8;
    private const ushort Ack = 16;
    private const ushort Subscribe = 32;
    private const ushort SubAck = 64;
    private const ushort Unsubscribe = 128;
    private const ushort UnsubAck = 256;
    private const ushort Disconnect = 512;
    private const ushort Auth = 1024;
    private const ushort Every = 2047;

    // The test's own copy of MQTT 5 section 2.2.2.2: id, a valid value of its type, the contexts that allow it.
    private static readonly (byte Id, string Value, ushort Contexts)[] s_table =
    [
        (0x01, ByteValue, Publish | Will),
        (0x02, FourByteValue, Publish | Will),
        (0x03, StringValue, Publish | Will),
        (0x08, StringValue, Publish | Will),
        (0x09, BinaryValue, Publish | Will),
        (0x0B, VariableValue, Publish | Subscribe),
        (0x11, FourByteValue, Connect | ConnAck | Disconnect),
        (0x12, StringValue, ConnAck),
        (0x13, TwoByteValue, ConnAck),
        (0x15, StringValue, Connect | ConnAck | Auth),
        (0x16, BinaryValue, Connect | ConnAck | Auth),
        (0x17, ByteValue, Connect),
        (0x18, FourByteValue, Will),
        (0x19, ByteValue, Connect),
        (0x1A, StringValue, ConnAck),
        (0x1C, StringValue, ConnAck | Disconnect),
        (0x1F, StringValue, ConnAck | Ack | SubAck | UnsubAck | Disconnect | Auth),
        (0x21, TwoByteValue, Connect | ConnAck),
        (0x22, TwoByteValue, Connect | ConnAck),
        (0x23, TwoByteValue, Publish),
        (0x24, ByteValue, ConnAck),
        (0x25, ByteValue, ConnAck),
        (0x26, PairValue, Every),
        (0x27, FourByteValue, Connect | ConnAck),
        (0x28, ByteValue, ConnAck),
        (0x29, ByteValue, ConnAck),
        (0x2A, ByteValue, ConnAck),
    ];

    private static readonly ushort[] s_contexts =
        [Connect, Will, ConnAck, Publish, Ack, Subscribe, SubAck, Unsubscribe, UnsubAck, Disconnect, Auth];

    public static IEnumerable<object[]> EveryPropertyInEveryContext()
    {
        foreach ((byte id, string value, ushort allowedIn) in s_table)
        {
            foreach (ushort context in s_contexts)
            {
                yield return [id, value, context, (allowedIn & context) != 0];
            }
        }
    }

    [Theory]
    [MemberData(nameof(EveryPropertyInEveryContext))]
    public void TryRead_PropertyInContext_IsAcceptedExactlyWhereTheStandardAllowsIt(byte id, string value, ushort context, bool allowed)
    {
        // Arrange
        byte[] valueBytes = Hex.Parse(value);
        byte[] source = [(byte)(1 + valueBytes.Length), id, .. valueBytes];
        MqttReader reader = new(source);

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, (PropertyContext)context, out _);

        // Assert
        result.ShouldBe(allowed ? MqttReasonCode.Success : MqttReasonCode.MalformedPacket);
    }

    [Fact]
    public void TryRead_EmptyBlock_SucceedsAndMovesPastTheLength()
    {
        // Arrange
        MqttReader reader = new(Hex.Parse("00 AA"));

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Connect, out PropertyBlock block);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        block.Contains(PropertyId.UserProperty).ShouldBeFalse();
        reader.Remaining.ShouldBe(1);
    }

    [Theory]
    [InlineData("02 05 00")]
    [InlineData("02 7F 00")]
    public void TryRead_UnknownProperty_IsMalformed(string block)
    {
        // Arrange
        MqttReader reader = new(Hex.Parse(block));

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Connect, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
    }

    [Theory]
    [InlineData("05 21 00")]
    [InlineData("02 21 00")]
    [InlineData("04 03 00 05 61")]
    public void TryRead_BlockOrValueRunningPastItsEnd_IsMalformed(string block)
    {
        // Arrange
        MqttReader reader = new(Hex.Parse(block));

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Publish | PropertyContext.Connect, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
    }

    [Fact]
    public void TryRead_OnceOnlyPropertyTwice_IsMalformed()
    {
        // Arrange
        MqttReader reader = new(Hex.Parse("06 21 00 0A 21 00 0A"));

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Connect, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
    }

    [Fact]
    public void TryRead_UserPropertiesRepeated_KeepsTheirOrder()
    {
        // Arrange
        MqttReader reader = new(Hex.Parse("0B 26 00 01 61 00 00 26 00 00 00 00"));

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Connect, out PropertyBlock block);
        List<(string, string)> pairs = [];
        foreach (Utf8Pair pair in block.UserProperties)
        {
            pairs.Add((Encoding.UTF8.GetString(pair.Name), Encoding.UTF8.GetString(pair.Value)));
        }

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        pairs.ShouldBe([("a", string.Empty), (string.Empty, string.Empty)]);
    }

    [Theory]
    [InlineData(Publish, MqttReasonCode.Success)]
    [InlineData(Subscribe, MqttReasonCode.MalformedPacket)]
    public void TryRead_SubscriptionIdentifierTwice_RepeatsOnlyInPublish(ushort context, MqttReasonCode expected)
    {
        // Arrange
        MqttReader reader = new(Hex.Parse("04 0B 01 0B 02"));

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, (PropertyContext)context, out _);

        // Assert
        result.ShouldBe(expected);
    }

    [Theory]
    [InlineData("03 21 00 00", Connect)]
    [InlineData("05 27 00 00 00 00", Connect)]
    [InlineData("02 0B 00", Publish)]
    [InlineData("03 23 00 00", Publish)]
    [InlineData("02 17 02", Connect)]
    [InlineData("02 24 02", ConnAck)]
    [InlineData("02 01 02", Publish)]
    public void TryRead_ValueTheStandardForbids_IsProtocolError(string block, ushort context)
    {
        // Arrange
        MqttReader reader = new(Hex.Parse(block));

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, (PropertyContext)context, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.ProtocolError);
    }

    [Fact]
    public void TryRead_ForbiddenValueBeforeAnUnknownProperty_IsProtocolError()
    {
        // Arrange
        MqttReader reader = new(Hex.Parse("05 21 00 00 05 00"));

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Connect, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.ProtocolError);
    }

    [Fact]
    public void TryRead_UnknownPropertyBeforeAForbiddenValue_IsMalformed()
    {
        // Arrange
        MqttReader reader = new(Hex.Parse("05 05 00 21 00 00"));

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Connect, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
    }

    [Fact]
    public void TryRead_InvalidUtf8InUserPropertyName_IsMalformed()
    {
        // Arrange
        MqttReader reader = new(Hex.Parse("06 26 00 01 FF 00 00"));

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Connect, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
    }

    [Fact]
    public void Accessors_BlockWithValues_ReadByIdentifier()
    {
        // Arrange
        MqttReader reader = new(Hex.Parse("07 21 04 00 1F 00 01 78"));
        PropertyBlock.TryRead(ref reader, PropertyContext.ConnAck, out PropertyBlock block);

        // Act
        bool hasReceiveMaximum = block.TryGetInteger(PropertyId.ReceiveMaximum, out uint receiveMaximum);
        bool hasReason = block.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> reason);

        // Assert
        hasReceiveMaximum.ShouldBeTrue();
        receiveMaximum.ShouldBe(1024u);
        hasReason.ShouldBeTrue();
        reason.ToArray().ShouldBe(Hex.Parse("78"));
        block.Contains(PropertyId.AuthenticationMethod).ShouldBeFalse();
        block.Contains(PropertyId.ReasonString).ShouldBeTrue();
        block.TryGetInteger(PropertyId.ReasonString, out _).ShouldBeFalse();
        block.TryGetBytes(PropertyId.ReceiveMaximum, out _).ShouldBeFalse();
    }

    [Fact]
    public void Accessors_EmptyBlock_FindNothing()
    {
        // Arrange
        PropertyBlock block = PropertyBlock.Empty;

        // Act & Assert
        block.Contains(PropertyId.UserProperty).ShouldBeFalse();
        block.TryGetInteger(PropertyId.ReceiveMaximum, out uint number).ShouldBeFalse();
        number.ShouldBe(0u);
        block.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> bytes).ShouldBeFalse();
        bytes.IsEmpty.ShouldBeTrue();
        block.UserProperties.MoveNext().ShouldBeFalse();
    }

    [Fact]
    public void PropertyWriter_EveryMethod_IsReadBackByTryRead()
    {
        // Arrange
        MqttWireUserProperty[] userProperties = [new("k", "v"), new("ü", string.Empty)];
        int size = PropertyWriter.ByteSize + PropertyWriter.UInt16Size + PropertyWriter.UInt32Size
            + PropertyWriter.StringSize("why") + PropertyWriter.UserPropertiesSize(userProperties);
        byte[] buffer = new byte[1 + size];
        MqttWriter writer = new(buffer);
        writer.WriteVariableByteInteger(size);

        // Act
        PropertyWriter.WriteByte(ref writer, PropertyId.MaximumQoS, 1);
        PropertyWriter.WriteUInt16(ref writer, PropertyId.ServerKeepAlive, 30);
        PropertyWriter.WriteUInt32(ref writer, PropertyId.MaximumPacketSize, 4096);
        PropertyWriter.WriteString(ref writer, PropertyId.ReasonString, "why");
        PropertyWriter.WriteUserProperties(ref writer, userProperties);
        MqttReader reader = new(buffer);
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.ConnAck, out PropertyBlock block);

        // Assert
        writer.Written.ShouldBe(buffer.Length);
        result.ShouldBe(MqttReasonCode.Success);
        reader.Remaining.ShouldBe(0);
        block.TryGetInteger(PropertyId.MaximumQoS, out uint maximumQoS).ShouldBeTrue();
        maximumQoS.ShouldBe(1u);
        block.TryGetInteger(PropertyId.ServerKeepAlive, out uint keepAlive).ShouldBeTrue();
        keepAlive.ShouldBe(30u);
        block.TryGetInteger(PropertyId.MaximumPacketSize, out uint maximumPacketSize).ShouldBeTrue();
        maximumPacketSize.ShouldBe(4096u);
        block.TryGetBytes(PropertyId.ReasonString, out ReadOnlySpan<byte> reason).ShouldBeTrue();
        Encoding.UTF8.GetString(reason).ShouldBe("why");
        int count = 0;
        foreach (Utf8Pair pair in block.UserProperties)
        {
            Encoding.UTF8.GetString(pair.Name).ShouldBe(userProperties[count].Name);
            Encoding.UTF8.GetString(pair.Value).ShouldBe(userProperties[count].Value);
            count++;
        }

        count.ShouldBe(2);
    }
}
