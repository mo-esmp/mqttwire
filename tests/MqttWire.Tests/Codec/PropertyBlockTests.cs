using System.Text;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests validating, reading and writing MQTT 5 properties.
/// </summary>
public sealed class PropertyBlockTests
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
    /// Every context flag at once, where the user property is allowed.
    /// </summary>
    private const PropertyContext Every = PropertyContext.Connect | PropertyContext.Will | PropertyContext.ConnAck
        | PropertyContext.Publish | PropertyContext.Ack | PropertyContext.Subscribe | PropertyContext.SubAck
        | PropertyContext.Unsubscribe | PropertyContext.UnsubAck | PropertyContext.Disconnect | PropertyContext.Auth;

    /// <summary>
    /// The test's own copy of MQTT 5 section 2.2.2.2: id, a valid value of its type, the contexts that allow it.
    /// </summary>
    private static readonly (byte Id, string Value, PropertyContext Contexts)[] s_table =
    [
        (0x01, ByteValue, PropertyContext.Publish | PropertyContext.Will),
        (0x02, FourByteValue, PropertyContext.Publish | PropertyContext.Will),
        (0x03, StringValue, PropertyContext.Publish | PropertyContext.Will),
        (0x08, StringValue, PropertyContext.Publish | PropertyContext.Will),
        (0x09, BinaryValue, PropertyContext.Publish | PropertyContext.Will),
        (0x0B, VariableValue, PropertyContext.Publish | PropertyContext.Subscribe),
        (0x11, FourByteValue, PropertyContext.Connect | PropertyContext.ConnAck | PropertyContext.Disconnect),
        (0x12, StringValue, PropertyContext.ConnAck),
        (0x13, TwoByteValue, PropertyContext.ConnAck),
        (0x15, StringValue, PropertyContext.Connect | PropertyContext.ConnAck | PropertyContext.Auth),
        (0x16, BinaryValue, PropertyContext.Connect | PropertyContext.ConnAck | PropertyContext.Auth),
        (0x17, ByteValue, PropertyContext.Connect),
        (0x18, FourByteValue, PropertyContext.Will),
        (0x19, ByteValue, PropertyContext.Connect),
        (0x1A, StringValue, PropertyContext.ConnAck),
        (0x1C, StringValue, PropertyContext.ConnAck | PropertyContext.Disconnect),
        (0x1F, StringValue, PropertyContext.ConnAck | PropertyContext.Ack | PropertyContext.SubAck | PropertyContext.UnsubAck
            | PropertyContext.Disconnect | PropertyContext.Auth),
        (0x21, TwoByteValue, PropertyContext.Connect | PropertyContext.ConnAck),
        (0x22, TwoByteValue, PropertyContext.Connect | PropertyContext.ConnAck),
        (0x23, TwoByteValue, PropertyContext.Publish),
        (0x24, ByteValue, PropertyContext.ConnAck),
        (0x25, ByteValue, PropertyContext.ConnAck),
        (0x26, PairValue, Every),
        (0x27, FourByteValue, PropertyContext.Connect | PropertyContext.ConnAck),
        (0x28, ByteValue, PropertyContext.ConnAck),
        (0x29, ByteValue, PropertyContext.ConnAck),
        (0x2A, ByteValue, PropertyContext.ConnAck),
    ];

    /// <summary>
    /// The test's own list of the values the standard forbids (the property sections of MQTT 5, chapter 3): 0 for the
    /// properties that must not be 0, and values above 1 for those that must be 0 or 1. Each row is an id of
    /// <see cref="s_table"/> and one forbidden value.
    /// </summary>
    private static readonly (byte Id, string Value)[] s_forbidden =
    [
        (0x01, "02"),
        (0x01, "FF"),
        (0x0B, "00"),
        (0x17, "02"),
        (0x17, "FF"),
        (0x19, "02"),
        (0x19, "FF"),
        (0x21, "00 00"),
        (0x23, "00 00"),
        (0x24, "02"),
        (0x24, "FF"),
        (0x25, "02"),
        (0x25, "FF"),
        (0x27, "00 00 00 00"),
        (0x28, "02"),
        (0x28, "FF"),
        (0x29, "02"),
        (0x29, "FF"),
        (0x2A, "02"),
        (0x2A, "FF"),
    ];

    /// <summary>
    /// Every context a property can be read in.
    /// </summary>
    private static readonly PropertyContext[] s_contexts =
    [
        PropertyContext.Connect,
        PropertyContext.Will,
        PropertyContext.ConnAck,
        PropertyContext.Publish,
        PropertyContext.Ack,
        PropertyContext.Subscribe,
        PropertyContext.SubAck,
        PropertyContext.Unsubscribe,
        PropertyContext.UnsubAck,
        PropertyContext.Disconnect,
        PropertyContext.Auth,
    ];

    /// <summary>
    /// Yields every property in every context with whether the standard allows it there.
    /// </summary>
    /// <returns>
    /// Rows of id, value, context as its underlying type, and whether it is allowed.
    /// </returns>
    public static IEnumerable<object[]> EveryPropertyInEveryContext()
    {
        foreach ((byte id, string value, PropertyContext allowedIn) in s_table)
        {
            foreach (PropertyContext context in s_contexts)
            {
                yield return [id, value, (ushort)context, (allowedIn & context) != 0];
            }
        }
    }

    /// <summary>
    /// Yields every once-only property in every context that allows it. The user property may repeat everywhere and
    /// the Subscription Identifier may repeat in a PUBLISH (MQTT 5, 3.3.2.3.8), so those pairs are left out.
    /// </summary>
    /// <returns>
    /// Rows of id, value and context as its underlying type.
    /// </returns>
    public static IEnumerable<object[]> EveryOnceOnlyPropertyWhereAllowed()
    {
        foreach ((byte id, string value, PropertyContext allowedIn) in s_table)
        {
            foreach (PropertyContext context in s_contexts)
            {
                bool repeatable = id == (byte)PropertyId.UserProperty
                    || (id == (byte)PropertyId.SubscriptionIdentifier && context == PropertyContext.Publish);
                if ((allowedIn & context) != 0 && !repeatable)
                {
                    yield return [id, value, (ushort)context];
                }
            }
        }
    }

    /// <summary>
    /// Yields every forbidden value of <see cref="s_forbidden"/> in every context of <see cref="s_table"/> that
    /// allows its property.
    /// </summary>
    /// <returns>
    /// Rows of id, forbidden value and context as its underlying type.
    /// </returns>
    public static IEnumerable<object[]> EveryForbiddenValueWhereAllowed()
    {
        foreach ((byte id, string value) in s_forbidden)
        {
            PropertyContext allowedIn = Array.Find(s_table, row => row.Id == id).Contexts;
            foreach (PropertyContext context in s_contexts)
            {
                if ((allowedIn & context) != 0)
                {
                    yield return [id, value, (ushort)context];
                }
            }
        }
    }

    /// <summary>
    /// A property is accepted exactly in the contexts the standard allows it in, and malformed elsewhere.
    /// </summary>
    /// <param name="id">
    /// The property identifier.
    /// </param>
    /// <param name="value">
    /// A valid value of its type, as hex.
    /// </param>
    /// <param name="context">
    /// The context, as the underlying type of <see cref="PropertyContext"/>.
    /// </param>
    /// <param name="allowed">
    /// Whether the standard allows the property in the context.
    /// </param>
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
    [InlineData("04 15 00 05 61")]
    public void TryRead_BlockOrValueRunningPastItsEnd_IsMalformed(string block)
    {
        // Arrange
        MqttReader reader = new(Hex.Parse(block));

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Connect, out _);

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
    /// Every once-only property of the test's table, given twice in each context that allows it, is malformed.
    /// </summary>
    /// <param name="id">
    /// The property identifier.
    /// </param>
    /// <param name="value">
    /// A valid value of its type, as hex.
    /// </param>
    /// <param name="context">
    /// The context, as the underlying type of <see cref="PropertyContext"/>.
    /// </param>
    [Theory]
    [MemberData(nameof(EveryOnceOnlyPropertyWhereAllowed))]
    public void TryRead_EveryOnceOnlyPropertyTwice_IsMalformed(byte id, string value, ushort context)
    {
        // Arrange
        byte[] valueBytes = Hex.Parse(value);
        byte[] source = [(byte)(2 * (1 + valueBytes.Length)), id, .. valueBytes, id, .. valueBytes];
        MqttReader reader = new(source);

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, (PropertyContext)context, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
    }

    /// <summary>
    /// Every forbidden value of the test's list, in each context that allows its property, is a protocol error.
    /// </summary>
    /// <param name="id">
    /// The property identifier.
    /// </param>
    /// <param name="value">
    /// The forbidden value, as hex.
    /// </param>
    /// <param name="context">
    /// The context, as the underlying type of <see cref="PropertyContext"/>.
    /// </param>
    [Theory]
    [MemberData(nameof(EveryForbiddenValueWhereAllowed))]
    public void TryRead_EveryForbiddenValue_IsProtocolError(byte id, string value, ushort context)
    {
        // Arrange
        byte[] valueBytes = Hex.Parse(value);
        byte[] source = [(byte)(1 + valueBytes.Length), id, .. valueBytes];
        MqttReader reader = new(source);

        // Act
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, (PropertyContext)context, out _);

        // Assert
        result.ShouldBe(MqttReasonCode.ProtocolError);
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
    [InlineData((ushort)PropertyContext.Publish, MqttReasonCode.Success)]
    [InlineData((ushort)PropertyContext.Subscribe, MqttReasonCode.MalformedPacket)]
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
    [InlineData("03 21 00 00", (ushort)PropertyContext.Connect)]
    [InlineData("05 27 00 00 00 00", (ushort)PropertyContext.Connect)]
    [InlineData("02 0B 00", (ushort)PropertyContext.Publish)]
    [InlineData("03 23 00 00", (ushort)PropertyContext.Publish)]
    [InlineData("02 17 02", (ushort)PropertyContext.Connect)]
    [InlineData("02 24 02", (ushort)PropertyContext.ConnAck)]
    [InlineData("02 01 02", (ushort)PropertyContext.Publish)]
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
