namespace MqttWire.Codec;

/// <summary>
/// The wire type of a property value.
/// </summary>
internal enum PropertyType : byte
{
    /// <summary>
    /// One byte.
    /// </summary>
    Byte,

    /// <summary>
    /// Two-byte big-endian integer.
    /// </summary>
    UInt16,

    /// <summary>
    /// Four-byte big-endian integer.
    /// </summary>
    UInt32,

    /// <summary>
    /// Variable byte integer.
    /// </summary>
    VariableByteInteger,

    /// <summary>
    /// Length-prefixed UTF-8 string.
    /// </summary>
    String,

    /// <summary>
    /// Length-prefixed binary data.
    /// </summary>
    Binary,

    /// <summary>
    /// Two length-prefixed UTF-8 strings: a name and a value.
    /// </summary>
    StringPair,
}

/// <summary>
/// A validated MQTT 5 property block: the properties without their length prefix. The accessors walk the
/// already validated bytes again, so they have no error paths and allocate nothing.
/// </summary>
internal readonly ref struct PropertyBlock
{
    /// <summary>
    /// The number of table slots: one per identifier up to the highest one.
    /// </summary>
    private const int TableLength = (int)PropertyId.SharedSubscriptionAvailable + 1;

    /// <summary>
    /// Every context flag; the user property is allowed and repeatable in all of them.
    /// </summary>
    private const PropertyContext EveryContext = (PropertyContext)2047;

    /// <summary>
    /// The value rule: any value of the type is allowed.
    /// </summary>
    private const byte NoRule = 0;

    /// <summary>
    /// The value rule: 0 is a protocol error.
    /// </summary>
    private const byte NonZero = 1;

    /// <summary>
    /// The value rule: anything other than 0 or 1 is a protocol error.
    /// </summary>
    private const byte ZeroOrOne = 2;

    /// <summary>
    /// The property table (MQTT 5, 2.2.2.2) indexed by identifier; an unlisted identifier has no allowed context, so it is unknown.
    /// </summary>
    private static readonly Entry[] s_entries = BuildTable();

    /// <summary>
    /// The validated properties, without the length prefix.
    /// </summary>
    private readonly ReadOnlySpan<byte> _properties;

    /// <summary>
    /// One bit per identifier (bit n for identifier n) set for every property the block holds, repeatable ones
    /// included; it makes <see cref="Contains"/> a bit test.
    /// </summary>
    private readonly ulong _present;

    /// <summary>
    /// Wraps an already validated span.
    /// </summary>
    /// <param name="properties">The validated properties, without the length prefix.</param>
    /// <param name="present">The presence mask of the identifiers in <paramref name="properties"/>.</param>
    private PropertyBlock(ReadOnlySpan<byte> properties, ulong present)
    {
        _properties = properties;
        _present = present;
    }

    /// <summary>
    /// A block with no properties; the same as <c>default</c>.
    /// </summary>
    public static PropertyBlock Empty => default;

    /// <summary>
    /// The user properties in wire order; enumerating a block without one yields nothing and, as the presence mask
    /// tells, does not walk the block.
    /// </summary>
    public UserPropertyEnumerator UserProperties => Contains(PropertyId.UserProperty) ? new(_properties) : default;

    /// <summary>
    /// Reads the length and the whole block, validates it in one pass and moves the reader past it.
    /// </summary>
    /// <param name="reader">The reader, positioned at the property length; after a failure its position is unspecified.</param>
    /// <param name="context">The packet the block belongs to; a single flag.</param>
    /// <param name="block">The validated block; <see cref="Empty"/> unless the result is success.</param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>; <see cref="MqttReasonCode.MalformedPacket"/> for a block or value running
    /// past its end, an unknown property, one not allowed in the context, invalid UTF-8 or a repeated once-only property;
    /// <see cref="MqttReasonCode.ProtocolError"/> for a value the standard forbids. The first fault in byte order decides.
    /// </returns>
    public static MqttReasonCode TryRead(scoped ref MqttReader reader, PropertyContext context, out PropertyBlock block)
    {
        block = default;
        if (!reader.TryReadVariableByteInteger(out int length) || !reader.TryReadBytes(length, out ReadOnlySpan<byte> span))
        {
            return MqttReasonCode.MalformedPacket;
        }

        MqttReader properties = new(span);
        ulong present = 0;
        while (properties.Remaining > 0)
        {
            properties.TryReadByte(out byte id);
            if (id >= TableLength)
            {
                return MqttReasonCode.MalformedPacket;
            }

            ref readonly Entry entry = ref s_entries[id];
            if ((entry.Allowed & context) == 0 || !TryReadValue(ref properties, entry.Type, out uint number, out _))
            {
                return MqttReasonCode.MalformedPacket;
            }

            ulong bit = 1UL << id;
            if ((entry.Repeatable & context) == 0 && (present & bit) != 0)
            {
                return MqttReasonCode.MalformedPacket;
            }

            present |= bit;

            if ((entry.Rule == NonZero && number == 0) || (entry.Rule == ZeroOrOne && number > 1))
            {
                return MqttReasonCode.ProtocolError;
            }
        }

        block = new PropertyBlock(span, present);
        return MqttReasonCode.Success;
    }

    /// <summary>
    /// Tells whether the block holds the property, repeatable ones included. It is a bit test on the mask built
    /// while the block was validated: it does not walk the block.
    /// </summary>
    /// <param name="id">The property to look for.</param>
    /// <returns><see langword="true"/> if the property is present.</returns>
    public bool Contains(PropertyId id) => (uint)id < TableLength && (_present & (1UL << (int)id)) != 0;

    /// <summary>
    /// Gets the first occurrence of a byte, two-byte, four-byte or variable byte integer property.
    /// </summary>
    /// <param name="id">The property to look for.</param>
    /// <param name="value">The value, or 0 when the result is <see langword="false"/>.</param>
    /// <returns><see langword="false"/> if the property is absent or is not an integer property.</returns>
    public bool TryGetInteger(PropertyId id, out uint value)
    {
        if (Contains(id) && s_entries[(int)id].Type is not (PropertyType.String or PropertyType.Binary or PropertyType.StringPair) && TryFind(id, out value, out _))
        {
            return true;
        }

        value = 0;
        return false;
    }

    /// <summary>
    /// Gets the first occurrence of a string or binary property, without its length prefix.
    /// </summary>
    /// <param name="id">The property to look for.</param>
    /// <param name="value">The bytes, or empty when the result is <see langword="false"/>.</param>
    /// <returns><see langword="false"/> if the property is absent or is not a string or binary property.</returns>
    public bool TryGetBytes(PropertyId id, out ReadOnlySpan<byte> value)
    {
        if (Contains(id) && s_entries[(int)id].Type is (PropertyType.String or PropertyType.Binary) && TryFind(id, out _, out value))
        {
            return true;
        }

        value = default;
        return false;
    }

    /// <summary>
    /// Reads and validates one value of the given type; used only while a block is being validated.
    /// </summary>
    /// <param name="reader">The reader, positioned at the value; a failed read of a string pair may already have consumed its name.</param>
    /// <param name="type">The wire type of the value.</param>
    /// <param name="number">The value of an integer type, otherwise 0.</param>
    /// <param name="bytes">The data of a string or binary type, the value of a pair; otherwise empty.</param>
    /// <returns><see langword="false"/> if the value is cut short or invalid UTF-8.</returns>
    internal static bool TryReadValue(scoped ref MqttReader reader, PropertyType type, out uint number, out ReadOnlySpan<byte> bytes)
    {
        number = 0;
        bytes = default;
        switch (type)
        {
            case PropertyType.Byte:
                bool readByte = reader.TryReadByte(out byte single);
                number = single;
                return readByte;
            case PropertyType.UInt16:
                bool readShort = reader.TryReadUInt16(out ushort twoBytes);
                number = twoBytes;
                return readShort;
            case PropertyType.UInt32:
                return reader.TryReadUInt32(out number);
            case PropertyType.VariableByteInteger:
                bool readVariable = reader.TryReadVariableByteInteger(out int variable);
                number = (uint)variable;
                return readVariable;
            case PropertyType.String:
                return reader.TryReadUtf8String(out bytes);
            case PropertyType.Binary:
                return reader.TryReadBinary(out bytes);
            default:
                return reader.TryReadUtf8String(out _) && reader.TryReadUtf8String(out bytes);
        }
    }

    /// <summary>
    /// Reads one value of an already validated block without validating it again; it never fails and
    /// allocates nothing. The reader must be positioned at a value of the given type inside a validated block.
    /// </summary>
    /// <param name="reader">The reader, positioned at the value; it moves past it.</param>
    /// <param name="type">The wire type of the value.</param>
    /// <param name="number">The value of an integer type, otherwise 0.</param>
    /// <param name="bytes">The data of a string or binary type, the value of a pair; otherwise empty.</param>
    internal static void ReadValidated(scoped ref MqttReader reader, PropertyType type, out uint number, out ReadOnlySpan<byte> bytes)
    {
        number = 0;
        bytes = default;
        switch (type)
        {
            case PropertyType.Byte:
                reader.TryReadByte(out byte single);
                number = single;
                break;
            case PropertyType.UInt16:
                reader.TryReadUInt16(out ushort twoBytes);
                number = twoBytes;
                break;
            case PropertyType.UInt32:
                reader.TryReadUInt32(out number);
                break;
            case PropertyType.VariableByteInteger:
                reader.TryReadVariableByteInteger(out int variable);
                number = (uint)variable;
                break;
            case PropertyType.String:
            case PropertyType.Binary:
                reader.TryReadBinary(out bytes);
                break;
            default:
                reader.TryReadBinary(out _);
                reader.TryReadBinary(out bytes);
                break;
        }
    }

    /// <summary>
    /// Gets the wire type of a property that has already been validated.
    /// </summary>
    /// <param name="id">The identifier byte of a property in a validated block.</param>
    /// <returns>The wire type of the property.</returns>
    internal static PropertyType TypeOf(byte id) => s_entries[id].Type;

    /// <summary>
    /// Walks the validated block for the first occurrence of a property.
    /// </summary>
    /// <param name="id">The property to look for.</param>
    /// <param name="number">The value if it is an integer property, otherwise 0.</param>
    /// <param name="bytes">The data if it is a string or binary property, otherwise empty.</param>
    /// <returns><see langword="false"/> if the property is absent; an empty block returns at once.</returns>
    private bool TryFind(PropertyId id, out uint number, out ReadOnlySpan<byte> bytes)
    {
        number = 0;
        bytes = default;
        if (_properties.IsEmpty)
        {
            return false;
        }

        MqttReader reader = new(_properties);
        while (reader.Remaining > 0)
        {
            reader.TryReadByte(out byte current);
            ReadValidated(ref reader, s_entries[current].Type, out number, out bytes);
            if (current == (byte)id)
            {
                return true;
            }
        }

        number = 0;
        bytes = default;
        return false;
    }

    /// <summary>
    /// Builds the property table.
    /// </summary>
    /// <returns>The table indexed by identifier.</returns>
    private static Entry[] BuildTable()
    {
        Entry[] table = new Entry[TableLength];
        const PropertyContext PublishOrWill = PropertyContext.Publish | PropertyContext.Will;
        const PropertyContext ConnectOrConnAck = PropertyContext.Connect | PropertyContext.ConnAck;
        Set(table, PropertyId.PayloadFormatIndicator, PropertyType.Byte, ZeroOrOne, PublishOrWill);
        Set(table, PropertyId.MessageExpiryInterval, PropertyType.UInt32, NoRule, PublishOrWill);
        Set(table, PropertyId.ContentType, PropertyType.String, NoRule, PublishOrWill);
        Set(table, PropertyId.ResponseTopic, PropertyType.String, NoRule, PublishOrWill);
        Set(table, PropertyId.CorrelationData, PropertyType.Binary, NoRule, PublishOrWill);
        Set(table, PropertyId.SubscriptionIdentifier, PropertyType.VariableByteInteger, NonZero, PropertyContext.Publish | PropertyContext.Subscribe, PropertyContext.Publish);
        Set(table, PropertyId.SessionExpiryInterval, PropertyType.UInt32, NoRule, ConnectOrConnAck | PropertyContext.Disconnect);
        Set(table, PropertyId.AssignedClientIdentifier, PropertyType.String, NoRule, PropertyContext.ConnAck);
        Set(table, PropertyId.ServerKeepAlive, PropertyType.UInt16, NoRule, PropertyContext.ConnAck);
        Set(table, PropertyId.AuthenticationMethod, PropertyType.String, NoRule, ConnectOrConnAck | PropertyContext.Auth);
        Set(table, PropertyId.AuthenticationData, PropertyType.Binary, NoRule, ConnectOrConnAck | PropertyContext.Auth);
        Set(table, PropertyId.RequestProblemInformation, PropertyType.Byte, ZeroOrOne, PropertyContext.Connect);
        Set(table, PropertyId.WillDelayInterval, PropertyType.UInt32, NoRule, PropertyContext.Will);
        Set(table, PropertyId.RequestResponseInformation, PropertyType.Byte, ZeroOrOne, PropertyContext.Connect);
        Set(table, PropertyId.ResponseInformation, PropertyType.String, NoRule, PropertyContext.ConnAck);
        Set(table, PropertyId.ServerReference, PropertyType.String, NoRule, PropertyContext.ConnAck | PropertyContext.Disconnect);
        Set(
            table,
            PropertyId.ReasonString,
            PropertyType.String,
            NoRule,
            PropertyContext.ConnAck | PropertyContext.Ack | PropertyContext.SubAck | PropertyContext.UnsubAck | PropertyContext.Disconnect | PropertyContext.Auth);
        Set(table, PropertyId.ReceiveMaximum, PropertyType.UInt16, NonZero, ConnectOrConnAck);
        Set(table, PropertyId.TopicAliasMaximum, PropertyType.UInt16, NoRule, ConnectOrConnAck);
        Set(table, PropertyId.TopicAlias, PropertyType.UInt16, NonZero, PropertyContext.Publish);
        Set(table, PropertyId.MaximumQoS, PropertyType.Byte, ZeroOrOne, PropertyContext.ConnAck);
        Set(table, PropertyId.RetainAvailable, PropertyType.Byte, ZeroOrOne, PropertyContext.ConnAck);
        Set(table, PropertyId.UserProperty, PropertyType.StringPair, NoRule, EveryContext, EveryContext);
        Set(table, PropertyId.MaximumPacketSize, PropertyType.UInt32, NonZero, ConnectOrConnAck);
        Set(table, PropertyId.WildcardSubscriptionAvailable, PropertyType.Byte, ZeroOrOne, PropertyContext.ConnAck);
        Set(table, PropertyId.SubscriptionIdentifierAvailable, PropertyType.Byte, ZeroOrOne, PropertyContext.ConnAck);
        Set(table, PropertyId.SharedSubscriptionAvailable, PropertyType.Byte, ZeroOrOne, PropertyContext.ConnAck);
        return table;
    }

    /// <summary>
    /// Fills one table slot.
    /// </summary>
    /// <param name="table">The table to fill.</param>
    /// <param name="id">The property identifier, which is the slot.</param>
    /// <param name="type">The wire type of the value.</param>
    /// <param name="rule">The rule the value must obey.</param>
    /// <param name="allowed">The contexts the property is allowed in.</param>
    /// <param name="repeatable">The contexts the property may repeat in; none means once only.</param>
    private static void Set(Entry[] table, PropertyId id, PropertyType type, byte rule, PropertyContext allowed, PropertyContext repeatable = 0)
        => table[(int)id] = new Entry(type, rule, allowed, repeatable);

    /// <summary>
    /// One row of the property table.
    /// </summary>
    /// <param name="Type">The wire type of the value.</param>
    /// <param name="Rule">The rule the value must obey.</param>
    /// <param name="Allowed">The contexts the property is allowed in.</param>
    /// <param name="Repeatable">The contexts the property may repeat in.</param>
    private readonly record struct Entry(PropertyType Type, byte Rule, PropertyContext Allowed, PropertyContext Repeatable);
}

/// <summary>
/// The name and value of a user property, as UTF-8 bytes inside the block.
/// </summary>
internal readonly ref struct Utf8Pair
{
    /// <summary>
    /// Creates a pair.
    /// </summary>
    /// <param name="name">The UTF-8 name.</param>
    /// <param name="value">The UTF-8 value.</param>
    public Utf8Pair(ReadOnlySpan<byte> name, ReadOnlySpan<byte> value)
    {
        Name = name;
        Value = value;
    }

    /// <summary>
    /// The UTF-8 name, without its length prefix.
    /// </summary>
    public ReadOnlySpan<byte> Name { get; }

    /// <summary>
    /// The UTF-8 value, without its length prefix.
    /// </summary>
    public ReadOnlySpan<byte> Value { get; }
}

/// <summary>
/// Walks the user properties of a validated block in wire order, skipping the other properties.
/// </summary>
internal ref struct UserPropertyEnumerator
{
    /// <summary>
    /// The reader over the validated block; it only moves forward.
    /// </summary>
    private MqttReader _reader;

    /// <summary>
    /// Creates an enumerator over a validated block.
    /// </summary>
    /// <param name="properties">The validated properties, without the length prefix.</param>
    public UserPropertyEnumerator(ReadOnlySpan<byte> properties) => _reader = new MqttReader(properties);

    /// <summary>
    /// The pair found by the last successful <see cref="MoveNext"/>.
    /// </summary>
    public Utf8Pair Current { get; private set; }

    /// <summary>
    /// Returns this enumerator, so the struct works in <c>foreach</c>.
    /// </summary>
    /// <returns>A copy of this enumerator.</returns>
    public readonly UserPropertyEnumerator GetEnumerator() => this;

    /// <summary>
    /// Moves to the next user property.
    /// </summary>
    /// <returns><see langword="false"/> when there are no more user properties.</returns>
    public bool MoveNext()
    {
        while (_reader.Remaining > 0)
        {
            _reader.TryReadByte(out byte id);
            if (id == (byte)PropertyId.UserProperty)
            {
                _reader.TryReadBinary(out ReadOnlySpan<byte> name);
                _reader.TryReadBinary(out ReadOnlySpan<byte> value);
                Current = new Utf8Pair(name, value);
                return true;
            }

            PropertyBlock.ReadValidated(ref _reader, PropertyBlock.TypeOf(id), out _, out _);
        }

        return false;
    }
}
