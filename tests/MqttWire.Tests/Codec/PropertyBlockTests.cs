using System.Text;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests validating, reading and writing MQTT 5 properties.
/// </summary>
public class PropertyBlockTests
{
    /// <summary>
    /// A valid one-byte value, accepted by every byte property.
    /// </summary>
    private const string ByteValue = "01";
    /// <summary>
    /// A valid two-byte value, accepted by every two-byte property.
    /// </summary>
    private const string TwoByteValue = "00 01";
    /// <summary>
    /// A valid four-byte value, accepted by every four-byte property.
    /// </summary>
    private const string FourByteValue = "00 00 00 01";
    /// <summary>
    /// A valid variable byte integer value.
    /// </summary>
    private const string VariableValue = "01";
    /// <summary>
    /// A valid UTF-8 string value.
    /// </summary>
    private const string StringValue = "00 01 61";
    /// <summary>
    /// A valid binary value.
    /// </summary>
    private const string BinaryValue = "00 01 61";
    /// <summary>
    /// A valid user property: a name and a value.
    /// </summary>
    private const string PairValue = "00 01 61 00 01 62";

    /// <summary>
    /// The PropertyContext.Connect flag.
    /// </summary>
    private const ushort Connect = 1;
    /// <summary>
    /// The PropertyContext.Will flag.
    /// </summary>
    private const ushort Will = 2;
    /// <summary>
    /// The PropertyContext.ConnAck flag.
    /// </summary>
    private const ushort ConnAck = 4;
    /// <summary>
    /// The PropertyContext.Publish flag.
    /// </summary>
    private const ushort Publish = 8;
    /// <summary>
    /// The PropertyContext.Ack flag.
    /// </summary>
    private const ushort Ack = 16;
    /// <summary>
    /// The PropertyContext.Subscribe flag.
    /// </summary>
    private const ushort Subscribe = 32;
    /// <summary>
    /// The PropertyContext.SubAck flag.
    /// </summary>
    private const ushort SubAck = 64;
    /// <summary>
    /// The PropertyContext.Unsubscribe flag.
    /// </summary>
    private const ushort Unsubscribe = 128;
    /// <summary>
    /// The PropertyContext.UnsubAck flag.
    /// </summary>
    private const ushort UnsubAck = 256;
    /// <summary>
    /// The PropertyContext.Disconnect flag.
    /// </summary>
    private const ushort Disconnect = 512;
    /// <summary>
    /// The PropertyContext.Auth flag.
    /// </summary>
    private const ushort Auth = 1024;
    /// <summary>
    /// Every context flag at once.
    /// </summary>
    private const ushort Every = 2047;

    /// <summary>
    /// The test's own copy of MQTT 5 section 2.2.2.2: id, a valid value of its type, the contexts that allow it.
    /// </summary>
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

    /// <summary>
    /// Every context a property can be read in.
    /// </summary>
    private static readonly ushort[] s_contexts =
        [Connect, Will, ConnAck, Publish, Ack, Subscribe, SubAck, Unsubscribe, UnsubAck, Disconnect, Auth];

    /// <summary>
    /// Yields every property in every context with whether the standard allows it there.
    /// </summary>
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

    /// <summary>
    /// A property is accepted exactly in the contexts the standard allows it in, and malformed elsewhere.
    /// </summary>
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

    /// <summary>
    /// An empty block succeeds and leaves the reader just after the length.
    /// </summary>
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

    /// <summary>
    /// An unknown property identifier is malformed.
    /// </summary>
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

    /// <summary>
    /// A block past the input, a cut two-byte value and a string past its block are malformed.
    /// </summary>
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

    /// <summary>
    /// A once-only property seen twice is malformed.
    /// </summary>
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

    /// <summary>
    /// User properties may repeat and are enumerated in wire order.
    /// </summary>
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

    /// <summary>
    /// A subscription identifier may repeat in a publish but not in a subscribe.
    /// </summary>
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

    /// <summary>
    /// A value the standard forbids is a protocol error.
    /// </summary>
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

    /// <summary>
    /// When a forbidden value comes first in byte order the result is a protocol error.
    /// </summary>
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

    /// <summary>
    /// When an unknown property comes first in byte order the result is malformed.
    /// </summary>
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

    /// <summary>
    /// Invalid UTF-8 in a user property name is malformed.
    /// </summary>
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

    /// <summary>
    /// The accessors read integers and bytes by identifier and refuse the wrong value type.
    /// </summary>
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

    /// <summary>
    /// The accessors on an empty block find nothing.
    /// </summary>
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

    /// <summary>
    /// What every property writer method writes, TryRead reads back, and the sizes add up.
    /// </summary>
    [Fact]
    public void PropertyWriter_EveryMethod_IsReadBackByTryRead()
    {
        // Arrange
        MqttWireUserProperty[] userProperties = [new("k", "v"), new("ü", string.Empty)];
        int size = PropertyWriter.ByteSize + PropertyWriter.UInt16Size + PropertyWriter.UInt32Size
            + PropertyWriter.StringSize("why") + (int)PropertyWriter.UserPropertiesSize(userProperties);
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

    /// <summary>
    /// Proves Contains is true for repeatable properties, however often they repeat, and false for absent ones.
    /// </summary>
    [Fact]
    public void Contains_RepeatableProperties_IsTrueForThemAndFalseForOthers()
    {
        // Arrange
        byte[] bytes = Hex.Parse("0B 0B 01 0B 02 26 00 01 6B 00 01 76");
        MqttReader reader = new(bytes);

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Publish, out PropertyBlock block);

        // Assert
        result.ShouldBe(MqttReasonCode.Success);
        block.Contains(PropertyId.SubscriptionIdentifier).ShouldBeTrue();
        block.Contains(PropertyId.UserProperty).ShouldBeTrue();
        block.Contains(PropertyId.TopicAlias).ShouldBeFalse();
        block.TryGetInteger(PropertyId.TopicAlias, out _).ShouldBeFalse();
        block.TryGetBytes(PropertyId.ContentType, out _).ShouldBeFalse();
        PropertyBlock.Empty.Contains(PropertyId.UserProperty).ShouldBeFalse();
    }
}
