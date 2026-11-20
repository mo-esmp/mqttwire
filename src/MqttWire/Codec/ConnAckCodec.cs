namespace MqttWire.Codec;

/// <summary>
/// Decodes and encodes the MQTT 5 CONNACK packet (MQTT 5, 3.2).
/// </summary>
internal static class ConnAckCodec
{
    /// <summary>
    /// The connect acknowledge flags that must be 0: every bit but session present.
    /// </summary>
    private const byte ReservedFlags = 0b1111_1110;

    /// <summary>
    /// The Session Present bit of the connect acknowledge flags.
    /// </summary>
    private const byte SessionPresentFlag = 0b0000_0001;

    /// <summary>
    /// The highest Maximum QoS that can be carried.
    /// </summary>
    private const byte HighestMaximumQoS = 1;

    /// <summary>
    /// The size of the variable header before the properties: the flags and the reason code.
    /// </summary>
    private const int FlagsAndReasonSize = 2;

    /// <summary>
    /// Decodes one CONNACK packet. Nothing allocates; the properties are views of the input.
    /// </summary>
    /// <param name="packet">
    /// Exactly one framed CONNACK packet, fixed header included.
    /// </param>
    /// <param name="connAck">
    /// The decoded packet when the result is success; otherwise the default value.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>; <see cref="MqttReasonCode.ProtocolError"/> for a forbidden property
    /// value; <see cref="MqttReasonCode.MalformedPacket"/> for any other fault in the body, including a missing
    /// property length and bytes left over after the properties.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The buffer is not exactly one framed CONNACK packet (a caller bug, see <see cref="PacketHeader.ReadExact(ReadOnlySpan{byte}, PacketType)"/>).
    /// </exception>
    public static MqttReasonCode TryDecode(ReadOnlySpan<byte> packet, out ConnAckPacket connAck)
    {
        connAck = default;
        PacketHeader header = PacketHeader.ReadExact(packet, PacketType.ConnAck);
        MqttReader reader = new(packet[header.HeaderLength..]);

        if (!reader.TryReadByte(out byte flags) || (flags & ReservedFlags) != 0 || !reader.TryReadByte(out byte reasonCode))
        {
            return MqttReasonCode.MalformedPacket;
        }

        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.ConnAck, out PropertyBlock properties);
        if (result != MqttReasonCode.Success)
        {
            return result;
        }

        if (reader.Remaining != 0)
        {
            return MqttReasonCode.MalformedPacket;
        }

        connAck = new ConnAckPacket
        {
            SessionPresent = (flags & SessionPresentFlag) != 0,
            ReasonCode = (MqttReasonCode)reasonCode,
            Properties = properties,
        };
        return MqttReasonCode.Success;
    }

    /// <summary>
    /// Gets the size of the whole CONNACK packet for the fields, fixed header included.
    /// </summary>
    /// <param name="fields">
    /// The fields to encode.
    /// </param>
    /// <returns>
    /// The size in bytes.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// A string cannot be carried: too long, or containing U+0000 or an unpaired surrogate; or the remaining length
    /// would pass <see cref="VariableByteInteger.MaxValue"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ConnAckFields.ReceiveMaximum"/> is 0 or <see cref="ConnAckFields.MaximumQoS"/> is above 1.
    /// </exception>
    public static int GetSize(in ConnAckFields fields)
    {
        int remainingLength = GetRemainingLength(fields, out _);
        return PacketHeader.GetLength(remainingLength) + remainingLength;
    }

    /// <summary>
    /// Writes a CONNACK packet with session present 0 and the properties that are set, in the order Session Expiry
    /// Interval, Receive Maximum, Maximum QoS, Subscription Identifier Available, Server Keep Alive, Reason String,
    /// user properties.
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
    /// <exception cref="ArgumentException">
    /// A string cannot be carried, the remaining length would pass <see cref="VariableByteInteger.MaxValue"/>, or the
    /// destination is too small. Everything is checked before the first byte is written, so nothing is written.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ConnAckFields.ReceiveMaximum"/> is 0 or <see cref="ConnAckFields.MaximumQoS"/> is above 1. Nothing
    /// is written.
    /// </exception>
    public static int Write(Span<byte> destination, in ConnAckFields fields)
    {
        int remainingLength = GetRemainingLength(fields, out int propertiesLength);
        int size = PacketHeader.GetLength(remainingLength) + remainingLength;
        if (destination.Length < size)
        {
            CodecThrow.DestinationTooSmall(nameof(destination));
        }

        int headerLength = PacketHeader.Write(destination, PacketType.ConnAck, 0, remainingLength);
        MqttWriter writer = new(destination[headerLength..]);

        writer.WriteByte(0);
        writer.WriteByte((byte)fields.ReasonCode);
        writer.WriteVariableByteInteger(propertiesLength);

        if (fields.SessionExpiryInterval is uint sessionExpiryInterval)
        {
            PropertyWriter.WriteUInt32(ref writer, PropertyId.SessionExpiryInterval, sessionExpiryInterval);
        }

        if (fields.ReceiveMaximum is ushort receiveMaximum)
        {
            PropertyWriter.WriteUInt16(ref writer, PropertyId.ReceiveMaximum, receiveMaximum);
        }

        if (fields.MaximumQoS is byte maximumQoS)
        {
            PropertyWriter.WriteByte(ref writer, PropertyId.MaximumQoS, maximumQoS);
        }

        if (fields.SubscriptionIdentifiersAvailable is bool available)
        {
            PropertyWriter.WriteByte(ref writer, PropertyId.SubscriptionIdentifierAvailable, available ? (byte)1 : (byte)0);
        }

        if (fields.ServerKeepAlive is ushort serverKeepAlive)
        {
            PropertyWriter.WriteUInt16(ref writer, PropertyId.ServerKeepAlive, serverKeepAlive);
        }

        if (fields.ReasonString is not null)
        {
            PropertyWriter.WriteString(ref writer, PropertyId.ReasonString, fields.ReasonString);
        }

        PropertyWriter.WriteUserProperties(ref writer, fields.UserProperties);

        return headerLength + writer.Written;
    }

    /// <summary>
    /// Gets the remaining length of the packet for the fields, checking every value and string.
    /// </summary>
    /// <param name="fields">
    /// The fields to measure.
    /// </param>
    /// <param name="propertiesLength">
    /// The length of the properties, not counting their own length field.
    /// </param>
    /// <returns>
    /// The number of bytes after the fixed header.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// A string cannot be carried, or the remaining length would pass <see cref="VariableByteInteger.MaxValue"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The Receive Maximum is 0 or the Maximum QoS is above 1.
    /// </exception>
    private static int GetRemainingLength(in ConnAckFields fields, out int propertiesLength)
    {
        if (fields.ReceiveMaximum is 0)
        {
            CodecThrow.OutOfRange("The Receive Maximum cannot be 0.", nameof(ConnAckFields.ReceiveMaximum));
        }

        if (fields.MaximumQoS is > HighestMaximumQoS)
        {
            CodecThrow.OutOfRange("The Maximum QoS cannot be above 1.", nameof(ConnAckFields.MaximumQoS));
        }

        // Summed in a long: many large user properties can pass int.MaxValue.
        long length = PropertyWriter.UserPropertiesSize(fields.UserProperties);
        if (fields.SessionExpiryInterval is not null)
        {
            length += PropertyWriter.UInt32Size;
        }

        if (fields.ReceiveMaximum is not null)
        {
            length += PropertyWriter.UInt16Size;
        }

        if (fields.MaximumQoS is not null)
        {
            length += PropertyWriter.ByteSize;
        }

        if (fields.SubscriptionIdentifiersAvailable is not null)
        {
            length += PropertyWriter.ByteSize;
        }

        if (fields.ServerKeepAlive is not null)
        {
            length += PropertyWriter.UInt16Size;
        }

        if (fields.ReasonString is not null)
        {
            length += PropertyWriter.StringSize(fields.ReasonString);
        }

        // The properties alone past the largest value also make the remaining length pass it.
        if (length > VariableByteInteger.MaxValue
            || FlagsAndReasonSize + VariableByteInteger.GetLength((int)length) + length > VariableByteInteger.MaxValue)
        {
            CodecThrow.Invalid("The packet is longer than the largest remaining length.", "UserProperties");
        }

        propertiesLength = (int)length;
        return FlagsAndReasonSize + VariableByteInteger.GetLength(propertiesLength) + propertiesLength;
    }
}
