namespace MqttWire.Codec;

/// <summary>
/// A decoded CONNECT packet: views over the bytes it was decoded from, so it is only valid while they are.
/// </summary>
internal readonly ref struct ConnectPacket
{
    /// <summary>
    /// The client identifier as UTF-8 bytes; empty when the client sent none, which the server decides about.
    /// </summary>
    public ReadOnlySpan<byte> ClientId { get; init; }

    /// <summary>
    /// Whether the user name flag was set; the name itself may be empty.
    /// </summary>
    public bool HasUserName { get; init; }

    /// <summary>
    /// The user name as UTF-8 bytes; empty when <see cref="HasUserName"/> is <see langword="false"/>.
    /// </summary>
    public ReadOnlySpan<byte> UserName { get; init; }

    /// <summary>
    /// Whether the password flag was set; the password itself may be empty.
    /// </summary>
    public bool HasPassword { get; init; }

    /// <summary>
    /// The password bytes; empty when <see cref="HasPassword"/> is <see langword="false"/>.
    /// </summary>
    public ReadOnlySpan<byte> Password { get; init; }

    /// <summary>
    /// The keep-alive interval in seconds; 0 means the client asks for none.
    /// </summary>
    public ushort KeepAliveSeconds { get; init; }

    /// <summary>
    /// Whether the client asks to start a new session rather than resume one.
    /// </summary>
    public bool CleanStart { get; init; }

    /// <summary>
    /// Whether the packet carries a Will; its content is validated but not exposed.
    /// </summary>
    public bool HasWill { get; init; }

    /// <summary>
    /// The validated CONNECT properties.
    /// </summary>
    public PropertyBlock Properties { get; init; }
}

/// <summary>
/// What the client writes into a CONNECT: protocol 5, clean start, no Will, and Receive Maximum as the only property.
/// </summary>
internal readonly ref struct ConnectFields
{
    /// <summary>
    /// The client identifier.
    /// </summary>
    public required string ClientId { get; init; }

    /// <summary>
    /// The user name, or <see langword="null"/> to write neither the flag nor the field.
    /// </summary>
    public string? UserName { get; init; }

    /// <summary>
    /// The password, or <see langword="null"/> to write neither the flag nor the field.
    /// </summary>
    public byte[]? Password { get; init; }

    /// <summary>
    /// The keep-alive interval in seconds.
    /// </summary>
    public ushort KeepAliveSeconds { get; init; }

    /// <summary>
    /// The Receive Maximum property: how many QoS 1 and 2 publications the client accepts at once. The client
    /// sends 1,024 (MQTT 5, 3.1.2.11.3). The standard forbids 0, so the encoder refuses it.
    /// </summary>
    public ushort ReceiveMaximum { get; init; }
}

/// <summary>
/// Decodes and encodes the MQTT 5 CONNECT packet (MQTT 5, 3.1).
/// </summary>
internal static class ConnectCodec
{
    /// <summary>
    /// The protocol name every CONNECT starts with.
    /// </summary>
    private static ReadOnlySpan<byte> ProtocolName => "MQTT"u8;

    /// <summary>
    /// The protocol level of MQTT 5.
    /// </summary>
    private const byte ProtocolLevel = 5;

    /// <summary>
    /// The reserved connect flag; it must be 0.
    /// </summary>
    private const byte ReservedFlag = 0b0000_0001;

    /// <summary>
    /// The Clean Start connect flag.
    /// </summary>
    private const byte CleanStartFlag = 0b0000_0010;

    /// <summary>
    /// The Will connect flag.
    /// </summary>
    private const byte WillFlag = 0b0000_0100;

    /// <summary>
    /// The Will QoS bits of the connect flags.
    /// </summary>
    private const byte WillQosMask = 0b0001_1000;

    /// <summary>
    /// The Will QoS bits all set, the forbidden QoS 3.
    /// </summary>
    private const byte WillQosThree = 0b0001_1000;

    /// <summary>
    /// The Will Retain connect flag.
    /// </summary>
    private const byte WillRetainFlag = 0b0010_0000;

    /// <summary>
    /// The Password connect flag.
    /// </summary>
    private const byte PasswordFlag = 0b0100_0000;

    /// <summary>
    /// The User Name connect flag.
    /// </summary>
    private const byte UserNameFlag = 0b1000_0000;

    /// <summary>
    /// The size of the property block the encoder writes: its length byte and the Receive Maximum property.
    /// </summary>
    private const int PropertiesSize = 1 + PropertyWriter.UInt16Size;

    /// <summary>
    /// The size of the variable header up to the properties: protocol name, level, flags and keep-alive.
    /// </summary>
    private const int FixedVariableHeaderSize = FieldSize.LengthPrefix + 4 + 1 + 1 + sizeof(ushort);

    /// <summary>
    /// Decodes one CONNECT packet. Nothing allocates; the packet's fields are slices of the input.
    /// </summary>
    /// <param name="packet">
    /// Exactly one framed CONNECT packet, fixed header included.
    /// </param>
    /// <param name="connect">
    /// The decoded packet when the result is success; otherwise the default value.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>; <see cref="MqttReasonCode.UnsupportedProtocolVersion"/> for another
    /// protocol name or level; <see cref="MqttReasonCode.ProtocolError"/> for Authentication Data without an
    /// Authentication Method or a forbidden property value; <see cref="MqttReasonCode.MalformedPacket"/> for any
    /// other fault in the body, including bytes left over after the last field.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The buffer is not exactly one framed CONNECT packet (a caller bug, see <see cref="PacketHeader.ReadExact(ReadOnlySpan{byte}, PacketType)"/>).
    /// </exception>
    public static MqttReasonCode TryDecode(ReadOnlySpan<byte> packet, out ConnectPacket connect)
    {
        connect = default;
        PacketHeader header = PacketHeader.ReadExact(packet, PacketType.Connect);
        MqttReader reader = new(packet[header.HeaderLength..]);

        if (!reader.TryReadUtf8String(out ReadOnlySpan<byte> name) || !reader.TryReadByte(out byte level))
        {
            return MqttReasonCode.MalformedPacket;
        }

        if (!name.SequenceEqual(ProtocolName) || level != ProtocolLevel)
        {
            return MqttReasonCode.UnsupportedProtocolVersion;
        }

        if (!reader.TryReadByte(out byte flags) || !AreFlagsValid(flags) || !reader.TryReadUInt16(out ushort keepAlive))
        {
            return MqttReasonCode.MalformedPacket;
        }

        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Connect, out PropertyBlock properties);
        if (result != MqttReasonCode.Success)
        {
            return result;
        }

        if (properties.Contains(PropertyId.AuthenticationData) && !properties.Contains(PropertyId.AuthenticationMethod))
        {
            return MqttReasonCode.ProtocolError;
        }

        if (!reader.TryReadUtf8String(out ReadOnlySpan<byte> clientId))
        {
            return MqttReasonCode.MalformedPacket;
        }

        bool hasWill = (flags & WillFlag) != 0;
        if (hasWill)
        {
            result = PropertyBlock.TryRead(ref reader, PropertyContext.Will, out _);
            if (result != MqttReasonCode.Success)
            {
                return result;
            }

            if (!reader.TryReadUtf8String(out _) || !reader.TryReadBinary(out _))
            {
                return MqttReasonCode.MalformedPacket;
            }
        }

        ReadOnlySpan<byte> userName = default;
        bool hasUserName = (flags & UserNameFlag) != 0;
        if (hasUserName && !reader.TryReadUtf8String(out userName))
        {
            return MqttReasonCode.MalformedPacket;
        }

        ReadOnlySpan<byte> password = default;
        bool hasPassword = (flags & PasswordFlag) != 0;
        if (hasPassword && !reader.TryReadBinary(out password))
        {
            return MqttReasonCode.MalformedPacket;
        }

        if (reader.Remaining != 0)
        {
            return MqttReasonCode.MalformedPacket;
        }

        connect = new ConnectPacket
        {
            ClientId = clientId,
            HasUserName = hasUserName,
            UserName = userName,
            HasPassword = hasPassword,
            Password = password,
            KeepAliveSeconds = keepAlive,
            CleanStart = (flags & CleanStartFlag) != 0,
            HasWill = hasWill,
            Properties = properties,
        };
        return MqttReasonCode.Success;
    }

    /// <summary>
    /// Gets the size of the whole CONNECT packet for the fields, fixed header included.
    /// </summary>
    /// <param name="fields">
    /// The fields to encode.
    /// </param>
    /// <returns>
    /// The size in bytes.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// The client id is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// A string or the password cannot be carried: too long, or a string containing U+0000 or an unpaired surrogate.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ConnectFields.ReceiveMaximum"/> is 0, which the standard forbids.
    /// </exception>
    public static int GetSize(in ConnectFields fields)
    {
        int remainingLength = GetRemainingLength(fields);
        return PacketHeader.GetLength(remainingLength) + remainingLength;
    }

    /// <summary>
    /// Writes a CONNECT packet: protocol 5, clean start, no Will, Receive Maximum as the only property.
    /// </summary>
    /// <param name="destination">
    /// Where to write; it must hold at least <see cref="GetSize"/> bytes.
    /// </param>
    /// <param name="fields">
    /// The fields to encode.
    /// </param>
    /// <returns>
    /// The number of bytes written, equal to <see cref="GetSize"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// The client id is <see langword="null"/>. Nothing is written.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// A string or the password cannot be carried, or the destination is too small. Everything is checked before the
    /// first byte is written, so nothing is written.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ConnectFields.ReceiveMaximum"/> is 0, which the standard forbids. Nothing is written.
    /// </exception>
    public static int Write(Span<byte> destination, in ConnectFields fields)
    {
        int remainingLength = GetRemainingLength(fields);
        int size = PacketHeader.GetLength(remainingLength) + remainingLength;
        if (destination.Length < size)
        {
            CodecThrow.DestinationTooSmall();
        }

        int headerLength = PacketHeader.Write(destination, PacketType.Connect, 0, remainingLength);
        MqttWriter writer = new(destination[headerLength..]);

        byte flags = CleanStartFlag;
        if (fields.UserName is not null)
        {
            flags |= UserNameFlag;
        }

        if (fields.Password is not null)
        {
            flags |= PasswordFlag;
        }

        writer.WriteUtf8String(ProtocolName);
        writer.WriteByte(ProtocolLevel);
        writer.WriteByte(flags);
        writer.WriteUInt16(fields.KeepAliveSeconds);
        writer.WriteVariableByteInteger(PropertiesSize - 1);
        PropertyWriter.WriteUInt16(ref writer, PropertyId.ReceiveMaximum, fields.ReceiveMaximum);
        writer.WriteUtf8String(fields.ClientId);

        if (fields.UserName is not null)
        {
            writer.WriteUtf8String(fields.UserName);
        }

        if (fields.Password is not null)
        {
            writer.WriteBinary(fields.Password);
        }

        return headerLength + writer.Written;
    }

    /// <summary>
    /// Gets the remaining length of the packet for the fields, checking every string and the password.
    /// </summary>
    /// <param name="fields">
    /// The fields to measure.
    /// </param>
    /// <returns>
    /// The number of bytes after the fixed header.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// The client id is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// A string or the password cannot be carried.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The Receive Maximum is 0.
    /// </exception>
    private static int GetRemainingLength(in ConnectFields fields)
    {
        ArgumentOutOfRangeException.ThrowIfZero(fields.ReceiveMaximum, "ReceiveMaximum");

        int length = FixedVariableHeaderSize + PropertiesSize + FieldSize.Utf8String(fields.ClientId);

        if (fields.UserName is not null)
        {
            length += FieldSize.Utf8String(fields.UserName);
        }

        if (fields.Password is not null)
        {
            length += FieldSize.Binary(fields.Password.Length);
        }

        return length;
    }

    /// <summary>
    /// Checks the connect flags (MQTT 5, 3.1.2.3): the reserved bit is 0, Will QoS and Will Retain need the Will flag,
    /// and Will QoS is not 3.
    /// </summary>
    /// <param name="flags">
    /// The connect flags byte.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the flags are not allowed.
    /// </returns>
    private static bool AreFlagsValid(byte flags)
    {
        if ((flags & ReservedFlag) != 0 || (flags & WillQosMask) == WillQosThree)
        {
            return false;
        }

        return (flags & WillFlag) != 0 || (flags & (WillQosMask | WillRetainFlag)) == 0;
    }
}
