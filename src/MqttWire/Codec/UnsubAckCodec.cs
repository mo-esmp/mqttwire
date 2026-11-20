namespace MqttWire.Codec;

/// <summary>
/// A decoded UNSUBACK packet: views over the bytes it was decoded from, so it is only valid while they are.
/// </summary>
internal readonly ref struct UnsubAckPacket
{
    /// <summary>
    /// The identifier of the UNSUBSCRIBE this answers; never 0 in a decoded packet.
    /// </summary>
    public ushort PacketIdentifier { get; init; }

    /// <summary>
    /// The validated UNSUBACK properties.
    /// </summary>
    public PropertyBlock Properties { get; init; }

    /// <summary>
    /// One reason code per filter of the UNSUBSCRIBE, at least one, as sent: each is a <see cref="MqttReasonCode"/>
    /// value, not checked against what an UNSUBACK may carry.
    /// </summary>
    public ReadOnlySpan<byte> ReasonCodes { get; init; }
}

/// <summary>
/// Decodes and encodes the MQTT 5 UNSUBACK packet (MQTT 5, 3.11).
/// </summary>
internal static class UnsubAckCodec
{
    /// <summary>
    /// Decodes one UNSUBACK packet. Nothing allocates; the properties and reason codes are views of the input.
    /// </summary>
    /// <param name="packet">
    /// Exactly one framed UNSUBACK packet, fixed header included.
    /// </param>
    /// <param name="unsubAck">
    /// The decoded packet when the result is success; otherwise the default value.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>; <see cref="MqttReasonCode.ProtocolError"/> for a packet identifier of 0,
    /// a forbidden property value or no reason code; <see cref="MqttReasonCode.MalformedPacket"/> for any other
    /// fault in the body.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The buffer is not exactly one framed UNSUBACK packet (a caller bug, see
    /// <see cref="PacketHeader.ReadExact(ReadOnlySpan{byte}, PacketType)"/>).
    /// </exception>
    public static MqttReasonCode TryDecode(ReadOnlySpan<byte> packet, out UnsubAckPacket unsubAck)
    {
        unsubAck = default;
        MqttReasonCode result = ReasonCodeList.TryDecode(
            packet, PacketType.UnsubAck, PropertyContext.UnsubAck, out ushort identifier, out PropertyBlock properties, out ReadOnlySpan<byte> reasonCodes);
        if (result != MqttReasonCode.Success)
        {
            return result;
        }

        unsubAck = new UnsubAckPacket
        {
            PacketIdentifier = identifier,
            Properties = properties,
            ReasonCodes = reasonCodes,
        };
        return MqttReasonCode.Success;
    }

    /// <summary>
    /// Gets the size of the whole UNSUBACK packet, fixed header included.
    /// </summary>
    /// <param name="count">
    /// The number of reason codes, at least 1.
    /// </param>
    /// <param name="reasonString">
    /// The Reason String property, or <see langword="null"/> to write none.
    /// </param>
    /// <returns>
    /// The size in bytes.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The count is below 1 or makes the packet too long, or the reason string cannot be carried.
    /// </exception>
    public static int GetSize(int count, string? reasonString) => ReasonCodeList.GetSize(count, reasonString);

    /// <summary>
    /// Writes an UNSUBACK packet; the property length is always written, 0 without a reason string.
    /// </summary>
    /// <param name="destination">
    /// Where to write; it must hold at least <see cref="GetSize"/> bytes.
    /// </param>
    /// <param name="packetIdentifier">
    /// The identifier of the UNSUBSCRIBE this answers, from 1 to 65,535.
    /// </param>
    /// <param name="reasonCodes">
    /// One reason code per filter, at least one.
    /// </param>
    /// <param name="reasonString">
    /// The Reason String property, or <see langword="null"/> to write none.
    /// </param>
    /// <returns>
    /// The number of bytes written, equal to <see cref="GetSize"/>.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The packet identifier is 0, there are no reason codes, the reason string cannot be carried, or the
    /// destination is too small. Everything is checked before the first byte is written, so nothing is written.
    /// </exception>
    public static int Write(Span<byte> destination, ushort packetIdentifier, ReadOnlySpan<MqttReasonCode> reasonCodes, string? reasonString)
        => ReasonCodeList.Write(destination, PacketType.UnsubAck, packetIdentifier, reasonCodes, reasonString);
}
