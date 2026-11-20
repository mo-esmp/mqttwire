namespace MqttWire.Codec;

/// <summary>
/// Decodes and encodes the MQTT 5 UNSUBSCRIBE packet (MQTT 5, 3.10). The encoder writes one filter per packet.
/// </summary>
internal static class UnsubscribeCodec
{
    /// <summary>
    /// The flags UNSUBSCRIBE must carry.
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
    /// Decodes one UNSUBSCRIBE packet, validating every filter in one pass. Nothing allocates; the properties and
    /// filters are views of the input.
    /// </summary>
    /// <param name="packet">
    /// Exactly one framed UNSUBSCRIBE packet, fixed header included.
    /// </param>
    /// <param name="unsubscribe">
    /// The decoded packet when the result is success; otherwise the default value.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>; <see cref="MqttReasonCode.ProtocolError"/> for a packet identifier of 0,
    /// a forbidden property value or no filter; <see cref="MqttReasonCode.MalformedPacket"/> for any other fault in
    /// the body. Whether a filter is a valid topic filter is not checked.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The buffer is not exactly one framed UNSUBSCRIBE packet (a caller bug, see
    /// <see cref="PacketHeader.ReadExact(ReadOnlySpan{byte}, PacketType)"/>).
    /// </exception>
    public static MqttReasonCode TryDecode(ReadOnlySpan<byte> packet, out UnsubscribePacket unsubscribe)
    {
        unsubscribe = default;
        PacketHeader header = PacketHeader.ReadExact(packet, PacketType.Unsubscribe);
        MqttReader reader = new(packet[header.HeaderLength..]);

        if (!reader.TryReadUInt16(out ushort packetIdentifier))
        {
            return MqttReasonCode.MalformedPacket;
        }

        if (packetIdentifier == 0)
        {
            return MqttReasonCode.ProtocolError;
        }

        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Unsubscribe, out PropertyBlock properties);
        if (result != MqttReasonCode.Success)
        {
            return result;
        }

        if (reader.Remaining == 0)
        {
            return MqttReasonCode.ProtocolError;
        }

        ReadOnlySpan<byte> filters = reader.ReadToEnd();
        reader = new MqttReader(filters);
        int count = 0;
        while (reader.Remaining > 0)
        {
            if (!reader.TryReadUtf8String(out _))
            {
                return MqttReasonCode.MalformedPacket;
            }

            count++;
        }

        unsubscribe = new UnsubscribePacket(packetIdentifier, properties, filters, count);
        return MqttReasonCode.Success;
    }

    /// <summary>
    /// Gets the size of the whole UNSUBSCRIBE packet for one filter, fixed header included. The packet identifier
    /// does not change it, so it is checked by <see cref="Write"/> only.
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
    /// Writes an UNSUBSCRIBE packet with one filter and no properties. The encoder refuses a filter its decoder
    /// refuses: one that is not well-formed UTF-8 or holds U+0000. Like the decoder, it does not check that the
    /// filter is a valid topic filter (see <see cref="Topics.IsValidFilter"/>), so an empty filter is written.
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
    /// <returns>
    /// The number of bytes written, equal to <see cref="GetSize"/>.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The packet identifier is 0, the filter is over 65,535 bytes, is not well-formed UTF-8 or contains U+0000, or
    /// the destination is too small. Everything is checked before the first byte is written, so nothing is written.
    /// </exception>
    public static int Write(Span<byte> destination, ushort packetIdentifier, ReadOnlySpan<byte> topicFilter)
    {
        if (packetIdentifier == 0)
        {
            CodecThrow.Invalid("The packet identifier cannot be 0.", nameof(packetIdentifier));
        }

        int remainingLength = GetRemainingLength(topicFilter);
        if (destination.Length < PacketHeader.GetLength(remainingLength) + remainingLength)
        {
            CodecThrow.DestinationTooSmall(nameof(destination));
        }

        int headerLength = PacketHeader.Write(destination, PacketType.Unsubscribe, Flags, remainingLength);
        MqttWriter writer = new(destination[headerLength..]);

        writer.WriteUInt16(packetIdentifier);
        writer.WriteVariableByteInteger(0);
        writer.WriteUtf8String(topicFilter);
        return headerLength + writer.Written;
    }

    /// <summary>
    /// Measures the remaining length of an UNSUBSCRIBE with one filter, which also checks the filter as the
    /// decoder does.
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
        => IdentifierSize + NoPropertiesSize + FieldSize.CheckedUtf8String(topicFilter, nameof(topicFilter));
}
