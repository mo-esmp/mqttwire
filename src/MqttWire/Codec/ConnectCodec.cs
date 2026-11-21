namespace MqttWire.Codec;

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
    /// Authentication Method or a forbidden property value; <see cref="MqttReasonCode.TopicNameInvalid"/> for a Will
    /// topic that is empty or holds a wildcard, in a packet with no other fault;
    /// <see cref="MqttReasonCode.MalformedPacket"/> for any other fault in the body, including bytes left over after
    /// the last field.
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

        result = TryReadPayload(ref reader, flags, out ReadOnlySpan<byte> clientId, out ReadOnlySpan<byte> userName, out ReadOnlySpan<byte> password);
        if (result != MqttReasonCode.Success)
        {
            return result;
        }

        connect = new ConnectPacket
        {
            ClientId = clientId,
            HasUserName = (flags & UserNameFlag) != 0,
            UserName = userName,
            HasPassword = (flags & PasswordFlag) != 0,
            Password = password,
            KeepAliveSeconds = keepAlive,
            CleanStart = (flags & CleanStartFlag) != 0,
            HasWill = (flags & WillFlag) != 0,
            Properties = properties,
        };
        return MqttReasonCode.Success;
    }

    /// <summary>
    /// Reads the payload of a CONNECT (MQTT 5, 3.1.3) in wire order: the client identifier, the Will when its flag is
    /// set, then the user name and the password when theirs are, and checks that nothing is left after them.
    /// </summary>
    /// <param name="reader">
    /// The reader, positioned at the client identifier; it moves to the end of the packet. After a failure its
    /// position is unspecified.
    /// </param>
    /// <param name="flags">
    /// The connect flags, already checked by <see cref="AreFlagsValid"/>; they say which fields are present.
    /// </param>
    /// <param name="clientId">
    /// The client identifier as UTF-8 bytes, a view of the input; empty when it cannot be read.
    /// </param>
    /// <param name="userName">
    /// The user name as UTF-8 bytes, a view of the input; empty when its flag is clear or the payload is faulty
    /// before it.
    /// </param>
    /// <param name="password">
    /// The password bytes, a view of the input; empty when its flag is clear or the payload is faulty before it.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>; the error of <see cref="TryReadWill"/> for a faulty Will;
    /// <see cref="MqttReasonCode.MalformedPacket"/> for a field that cannot be read or bytes left over after the
    /// last field; <see cref="MqttReasonCode.TopicNameInvalid"/> for a Will topic that is empty or holds a wildcard,
    /// in a payload with no other fault.
    /// </returns>
    private static MqttReasonCode TryReadPayload(
        scoped ref MqttReader reader,
        byte flags,
        out ReadOnlySpan<byte> clientId,
        out ReadOnlySpan<byte> userName,
        out ReadOnlySpan<byte> password)
    {
        userName = default;
        password = default;
        if (!reader.TryReadUtf8String(out clientId))
        {
            return MqttReasonCode.MalformedPacket;
        }

        bool isWillTopicValid = true;
        if ((flags & WillFlag) != 0)
        {
            MqttReasonCode result = TryReadWill(ref reader, out isWillTopicValid);
            if (result != MqttReasonCode.Success)
            {
                return result;
            }
        }

        if ((flags & UserNameFlag) != 0 && !reader.TryReadUtf8String(out userName))
        {
            return MqttReasonCode.MalformedPacket;
        }

        if ((flags & PasswordFlag) != 0 && !reader.TryReadBinary(out password))
        {
            return MqttReasonCode.MalformedPacket;
        }

        if (reader.Remaining != 0)
        {
            return MqttReasonCode.MalformedPacket;
        }

        // Reported last, as PUBLISH does for its topic: a packet that is also malformed is malformed first.
        return isWillTopicValid ? MqttReasonCode.Success : MqttReasonCode.TopicNameInvalid;
    }

    /// <summary>
    /// Reads and validates the Will of a CONNECT (MQTT 5, 3.1.3.2 to 3.1.3.4): its properties, topic and payload.
    /// The content is checked but not kept.
    /// </summary>
    /// <param name="reader">
    /// The reader, positioned at the Will properties; it moves past the Will payload. After a failure its position
    /// is unspecified.
    /// </param>
    /// <param name="isTopicValid">
    /// <see langword="false"/> when the Will topic was read but is not a topic name (MQTT 5, 4.7): it is empty or
    /// holds a wildcard. The caller reports it after the rest of the payload; <see langword="true"/> otherwise,
    /// also when the result is an error.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>, also for a topic that is not a topic name; the error of
    /// <see cref="PropertyBlock.TryRead"/> for a faulty property block; <see cref="MqttReasonCode.MalformedPacket"/>
    /// for a Will topic or payload that cannot be read.
    /// </returns>
    private static MqttReasonCode TryReadWill(scoped ref MqttReader reader, out bool isTopicValid)
    {
        isTopicValid = true;
        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Will, out _);
        if (result != MqttReasonCode.Success)
        {
            return result;
        }

        if (!reader.TryReadUtf8String(out ReadOnlySpan<byte> topic) || !reader.TryReadBinary(out _))
        {
            return MqttReasonCode.MalformedPacket;
        }

        isTopicValid = Topics.IsValidName(topic);
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
    /// <see cref="ConnectFields.ReceiveMaximum"/> or <see cref="ConnectFields.MaximumPacketSize"/> is 0, which the
    /// standard forbids.
    /// </exception>
    public static int GetSize(in ConnectFields fields)
    {
        int remainingLength = GetRemainingLength(fields, out _);
        return PacketHeader.GetLength(remainingLength) + remainingLength;
    }

    /// <summary>
    /// Writes a CONNECT packet: protocol 5, clean start, no Will, a Receive Maximum and, when given, a Maximum Packet
    /// Size after it.
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
    /// <see cref="ConnectFields.ReceiveMaximum"/> or <see cref="ConnectFields.MaximumPacketSize"/> is 0, which the
    /// standard forbids. Nothing is written.
    /// </exception>
    public static int Write(Span<byte> destination, in ConnectFields fields)
    {
        int remainingLength = GetRemainingLength(fields, out int propertiesLength);
        int size = PacketHeader.GetLength(remainingLength) + remainingLength;
        if (destination.Length < size)
        {
            CodecThrow.DestinationTooSmall(nameof(destination));
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
        writer.WriteVariableByteInteger(propertiesLength);
        PropertyWriter.WriteUInt16(ref writer, PropertyId.ReceiveMaximum, fields.ReceiveMaximum);
        if (fields.MaximumPacketSize is uint maximumPacketSize)
        {
            PropertyWriter.WriteUInt32(ref writer, PropertyId.MaximumPacketSize, maximumPacketSize);
        }

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
    /// <param name="propertiesLength">
    /// The length of the property block without its length prefix.
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
    /// The Receive Maximum or the Maximum Packet Size is 0.
    /// </exception>
    private static int GetRemainingLength(in ConnectFields fields, out int propertiesLength)
    {
        if (fields.ReceiveMaximum == 0)
        {
            CodecThrow.OutOfRange("The Receive Maximum cannot be 0.", nameof(ConnectFields.ReceiveMaximum));
        }

        if (fields.MaximumPacketSize == 0)
        {
            CodecThrow.OutOfRange("The Maximum Packet Size cannot be 0.", nameof(ConnectFields.MaximumPacketSize));
        }

        propertiesLength = PropertyWriter.UInt16Size;
        if (fields.MaximumPacketSize is not null)
        {
            propertiesLength += PropertyWriter.UInt32Size;
        }

        // The property block is short, so its length prefix is one byte.
        int length = FixedVariableHeaderSize + 1 + propertiesLength + FieldSize.Utf8String(fields.ClientId);

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
