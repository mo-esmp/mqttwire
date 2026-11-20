namespace MqttWire.Codec;

// The wire type of a property value.
internal enum PropertyType : byte
{
    Byte,
    UInt16,
    UInt32,
    VariableByteInteger,
    String,
    Binary,
    StringPair,
}

// A validated MQTT 5 property block: the properties without their length prefix.
internal readonly ref struct PropertyBlock
{
    private const int TableLength = (int)PropertyId.SharedSubscriptionAvailable + 1;

    private const PropertyContext EveryContext = (PropertyContext)2047;

    // The rule a value must obey on top of its type (MQTT 5, 2.2.2.2); a broken rule is a protocol error.
    private const byte NoRule = 0;
    private const byte NonZero = 1;
    private const byte ZeroOrOne = 2;

    // Indexed by property id; an unlisted id has no allowed context, so it is unknown.
    private static readonly Entry[] s_entries = BuildTable();

    private readonly ReadOnlySpan<byte> _properties;

    private PropertyBlock(ReadOnlySpan<byte> properties) => _properties = properties;

    public static PropertyBlock Empty => default;

    public UserPropertyEnumerator UserProperties => new(_properties);

    // Reads the length and the whole block, validates it in one pass and moves the reader past it.
    public static MqttReasonCode TryRead(ref MqttReader reader, PropertyContext context, out PropertyBlock block)
    {
        block = default;
        if (!reader.TryReadVariableByteInteger(out int length) || !reader.TryReadBytes(length, out ReadOnlySpan<byte> span))
        {
            return MqttReasonCode.MalformedPacket;
        }

        MqttReader properties = new(span);
        ulong seen = 0;
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

            if ((entry.Repeatable & context) == 0)
            {
                ulong bit = 1UL << id;
                if ((seen & bit) != 0)
                {
                    return MqttReasonCode.MalformedPacket;
                }

                seen |= bit;
            }

            if ((entry.Rule == NonZero && number == 0) || (entry.Rule == ZeroOrOne && number > 1))
            {
                return MqttReasonCode.ProtocolError;
            }
        }

        block = new PropertyBlock(span);
        return MqttReasonCode.Success;
    }

    public bool Contains(PropertyId id) => TryFind(id, out _, out _);

    public bool TryGetInteger(PropertyId id, out uint value)
    {
        if (TryFind(id, out value, out _) && s_entries[(int)id].Type is not (PropertyType.String or PropertyType.Binary or PropertyType.StringPair))
        {
            return true;
        }

        value = 0;
        return false;
    }

    public bool TryGetBytes(PropertyId id, out ReadOnlySpan<byte> value)
    {
        if (TryFind(id, out _, out value) && s_entries[(int)id].Type is PropertyType.String or PropertyType.Binary)
        {
            return true;
        }

        value = default;
        return false;
    }

    // Reads one value of the given type; a validated block never fails, a block under validation can.
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

    internal static PropertyType TypeOf(byte id) => s_entries[id].Type;

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
            TryReadValue(ref reader, s_entries[current].Type, out number, out bytes);
            if (current == (byte)id)
            {
                return true;
            }
        }

        number = 0;
        bytes = default;
        return false;
    }

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

    private static void Set(Entry[] table, PropertyId id, PropertyType type, byte rule, PropertyContext allowed, PropertyContext repeatable = 0)
        => table[(int)id] = new Entry(type, rule, allowed, repeatable);

    private readonly record struct Entry(PropertyType Type, byte Rule, PropertyContext Allowed, PropertyContext Repeatable);
}

internal readonly ref struct Utf8Pair
{
    public Utf8Pair(ReadOnlySpan<byte> name, ReadOnlySpan<byte> value)
    {
        Name = name;
        Value = value;
    }

    public ReadOnlySpan<byte> Name { get; }

    public ReadOnlySpan<byte> Value { get; }
}

internal ref struct UserPropertyEnumerator
{
    private MqttReader _reader;

    public UserPropertyEnumerator(ReadOnlySpan<byte> properties) => _reader = new MqttReader(properties);

    public Utf8Pair Current { get; private set; }

    public readonly UserPropertyEnumerator GetEnumerator() => this;

    public bool MoveNext()
    {
        while (_reader.Remaining > 0)
        {
            _reader.TryReadByte(out byte id);
            if (id == (byte)PropertyId.UserProperty)
            {
                _reader.TryReadUtf8String(out ReadOnlySpan<byte> name);
                _reader.TryReadUtf8String(out ReadOnlySpan<byte> value);
                Current = new Utf8Pair(name, value);
                return true;
            }

            PropertyBlock.TryReadValue(ref _reader, PropertyBlock.TypeOf(id), out _, out _);
        }

        return false;
    }
}
