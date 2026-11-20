namespace MqttWire.Codec;

/// <summary>
/// Decodes and encodes the MQTT 5 SUBSCRIBE packet (MQTT 5, 3.8). The encoder writes one filter per packet.
/// </summary>
internal static class SubscribeCodec
{
    /// <summary>
    /// Selects the QoS bits of the options byte.
    /// </summary>
    internal const byte QosMask = 0b11;

    /// <summary>
    /// The No Local bit of the options byte.
    /// </summary>
    internal const byte NoLocalFlag = 0b100;

    /// <summary>
    /// The Retain As Published bit of the options byte.
    /// </summary>
    internal const byte RetainAsPublishedFlag = 0b1000;

    /// <summary>
    /// The position of the Retain Handling bits in the options byte.
    /// </summary>
    internal const int RetainHandlingShift = 4;

    /// <summary>
    /// Selects the Retain Handling bits once shifted down.
    /// </summary>
    internal const byte RetainHandlingMask = 0b11;

    /// <summary>
    /// The bits 6 and 7 of the options byte, which must be 0.
    /// </summary>
    private const byte ReservedOptionBits = 0b1100_0000;

    /// <summary>
    /// The flags SUBSCRIBE must carry.
    /// </summary>
    private const byte Flags = 0b0010;

    /// <summary>
    /// The size of the packet identifier.
    /// </summary>
    private const int IdentifierSize = sizeof(ushort);

    /// <summary>
    /// The size of the property length written by the encoder, which writes no properties.
    /// </summary>
    private const int NoPropertiesSize = 1;

    /// <summary>
    /// The size of the options byte.
    /// </summary>
    private const int OptionsSize = 1;

    /// <summary>
    /// Decodes one SUBSCRIBE packet, validating every entry in one pass. Nothing allocates; the properties and
    /// filters are views of the input.
    /// </summary>
    /// <param name="packet">
    /// Exactly one framed SUBSCRIBE packet, fixed header included.
    /// </param>
    /// <param name="subscribe">
    /// The decoded packet when the result is success; otherwise the default value.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>; <see cref="MqttReasonCode.ProtocolError"/> for a packet identifier of 0,
    /// a forbidden property value, no entry, a QoS of 3, a Retain Handling of 3, or No Local on a shared
    /// subscription; <see cref="MqttReasonCode.MalformedPacket"/> for a reserved option bit or any other fault in
    /// the body. Whether a filter is a valid topic filter is not checked.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The buffer is not exactly one framed SUBSCRIBE packet (a caller bug, see
    /// <see cref="PacketHeader.ReadExact(ReadOnlySpan{byte}, PacketType)"/>).
    /// </exception>
    public static MqttReasonCode TryDecode(ReadOnlySpan<byte> packet, out SubscribePacket subscribe)
    {
        subscribe = default;
        PacketHeader header = PacketHeader.ReadExact(packet, PacketType.Subscribe);
        MqttReader reader = new(packet[header.HeaderLength..]);

        if (!reader.TryReadUInt16(out ushort packetIdentifier))
        {
            return MqttReasonCode.MalformedPacket;
        }

        if (packetIdentifier == 0)
        {
            return MqttReasonCode.ProtocolError;
        }

        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Subscribe, out PropertyBlock properties);
        if (result != MqttReasonCode.Success)
        {
            return result;
        }

        if (reader.Remaining == 0)
        {
            return MqttReasonCode.ProtocolError;
        }

        ReadOnlySpan<byte> entries = reader.ReadToEnd();
        reader = new MqttReader(entries);
        int count = 0;
        while (reader.Remaining > 0)
        {
            if (!reader.TryReadUtf8String(out ReadOnlySpan<byte> filter) || !reader.TryReadByte(out byte options))
            {
                return MqttReasonCode.MalformedPacket;
            }

            if ((options & ReservedOptionBits) != 0)
            {
                return MqttReasonCode.MalformedPacket;
            }

            if ((options & QosMask) == QosMask
                || ((options >> RetainHandlingShift) & RetainHandlingMask) == RetainHandlingMask
                || ((options & NoLocalFlag) != 0 && Topics.IsShared(filter)))
            {
                return MqttReasonCode.ProtocolError;
            }

            count++;
        }

        subscribe = new SubscribePacket(packetIdentifier, properties, entries, count);
        return MqttReasonCode.Success;
    }

    /// <summary>
    /// Gets the size of the whole SUBSCRIBE packet for one filter, fixed header included. The packet identifier and
    /// QoS do not change it, so they are checked by <see cref="Write"/> only.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter as UTF-8.
    /// </param>
    /// <returns>
    /// The size in bytes.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The filter is over 65,535 bytes, is not well-formed UTF-8 or contains U+0000.
    /// </exception>
    public static int GetSize(ReadOnlySpan<byte> topicFilter)
    {
        int remainingLength = GetRemainingLength(topicFilter);
        return PacketHeader.GetLength(remainingLength) + remainingLength;
    }

    /// <summary>
    /// Writes a SUBSCRIBE packet with one filter; its options are the maximum QoS only and it has no properties.
    /// The encoder refuses a filter its decoder refuses: one that is not well-formed UTF-8 or holds U+0000. Like the
    /// decoder, it does not check that the filter is a valid topic filter (see <see cref="Topics.IsValidFilter"/>),
    /// so an empty filter is written.
    /// </summary>
    /// <param name="destination">
    /// Where to write; it must hold at least <see cref="GetSize"/> bytes.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier, from 1 to 65,535.
    /// </param>
    /// <param name="topicFilter">
    /// The topic filter as UTF-8.
    /// </param>
    /// <param name="maximumQoS">
    /// The maximum QoS, 0 to 2.
    /// </param>
    /// <returns>
    /// The number of bytes written, equal to <see cref="GetSize"/>.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The packet identifier is 0, the QoS is above 2, the filter is over 65,535 bytes, is not well-formed UTF-8 or
    /// contains U+0000, or the destination is too small. Everything is checked before the first byte is written, so
    /// nothing is written.
    /// </exception>
    public static int Write(Span<byte> destination, ushort packetIdentifier, ReadOnlySpan<byte> topicFilter, MqttQoS maximumQoS)
    {
        if (packetIdentifier == 0)
        {
            CodecThrow.Invalid("The packet identifier cannot be 0.", nameof(packetIdentifier));
        }

        if (maximumQoS > MqttQoS.ExactlyOnce)
        {
            CodecThrow.Invalid("The QoS is not 0, 1 or 2.", nameof(maximumQoS));
        }

        int remainingLength = GetRemainingLength(topicFilter);
        if (destination.Length < PacketHeader.GetLength(remainingLength) + remainingLength)
        {
            CodecThrow.DestinationTooSmall(nameof(destination));
        }

        int headerLength = PacketHeader.Write(destination, PacketType.Subscribe, Flags, remainingLength);
        MqttWriter writer = new(destination[headerLength..]);

        writer.WriteUInt16(packetIdentifier);
        writer.WriteVariableByteInteger(0);
        writer.WriteUtf8String(topicFilter);
        writer.WriteByte((byte)maximumQoS);
        return headerLength + writer.Written;
    }

    /// <summary>
    /// Measures the remaining length of a SUBSCRIBE with one filter, which also checks the filter as the decoder
    /// does.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter as UTF-8.
    /// </param>
    /// <returns>
    /// The remaining length.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The filter is over 65,535 bytes, is not well-formed UTF-8 or contains U+0000.
    /// </exception>
    private static int GetRemainingLength(ReadOnlySpan<byte> topicFilter)
        => IdentifierSize + NoPropertiesSize + FieldSize.CheckedUtf8String(topicFilter, nameof(topicFilter)) + OptionsSize;
}
