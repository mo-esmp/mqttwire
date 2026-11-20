namespace MqttWire.Codec;

/// <summary>
/// A decoded PUBACK, PUBREC, PUBREL or PUBCOMP packet: views over the bytes it was decoded from, so it is only
/// valid while they are.
/// </summary>
internal readonly ref struct AckPacket
{
    /// <summary>
    /// Which of the four acknowledgements this is.
    /// </summary>
    public PacketType Type { get; init; }

    /// <summary>
    /// The identifier of the publish exchange; never 0 in a decoded packet.
    /// </summary>
    public ushort PacketIdentifier { get; init; }

    /// <summary>
    /// The reason code as sent; <see cref="MqttReasonCode.Success"/> when the packet is in its short form. Its value
    /// is not checked against the packet type.
    /// </summary>
    public MqttReasonCode ReasonCode { get; init; }

    /// <summary>
    /// The validated acknowledgement properties; empty in the short forms.
    /// </summary>
    public PropertyBlock Properties { get; init; }
}

/// <summary>
/// Decodes and encodes the MQTT 5 acknowledgements PUBACK, PUBREC, PUBREL and PUBCOMP (MQTT 5, 3.4 to 3.7). They
/// share one layout, so one codec handles the four.
/// </summary>
internal static class AckCodec
{
    /// <summary>
    /// The flags PUBREL must carry; the other three carry 0.
    /// </summary>
    private const byte PubRelFlags = 0b0010;

    /// <summary>
    /// The size of the packet identifier, the part of the variable header every form has.
    /// </summary>
    private const int PacketIdentifierSize = 2;

    /// <summary>
    /// Decodes one PUBACK, PUBREC, PUBREL or PUBCOMP packet. Nothing allocates; the properties are views of the input.
    /// </summary>
    /// <param name="packet">
    /// Exactly one framed acknowledgement packet, fixed header included.
    /// </param>
    /// <param name="ack">
    /// The decoded packet when the result is success; otherwise the default value.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>; <see cref="MqttReasonCode.ProtocolError"/> for a packet identifier of 0
    /// or a forbidden property value; <see cref="MqttReasonCode.MalformedPacket"/> for any other fault in the body,
    /// including a remaining length below 2 and bytes left over after the properties.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The buffer is not exactly one framed acknowledgement packet (a caller bug, see
    /// <see cref="PacketHeader.ReadExact(ReadOnlySpan{byte}, PacketType, PacketType)"/>).
    /// </exception>
    public static MqttReasonCode TryDecode(ReadOnlySpan<byte> packet, out AckPacket ack)
    {
        ack = default;
        PacketHeader header = PacketHeader.ReadExact(packet, PacketType.PubAck, PacketType.PubComp);
        MqttReader reader = new(packet[header.HeaderLength..]);

        if (!reader.TryReadUInt16(out ushort packetIdentifier))
        {
            return MqttReasonCode.MalformedPacket;
        }

        if (packetIdentifier == 0)
        {
            return MqttReasonCode.ProtocolError;
        }

        MqttReasonCode result = ReasonTail.TryRead(ref reader, PropertyContext.Ack, out MqttReasonCode reasonCode, out PropertyBlock properties);
        if (result != MqttReasonCode.Success)
        {
            return result;
        }

        ack = new AckPacket
        {
            Type = header.Type,
            PacketIdentifier = packetIdentifier,
            ReasonCode = reasonCode,
            Properties = properties,
        };
        return MqttReasonCode.Success;
    }

    /// <summary>
    /// Gets the size of the whole acknowledgement packet for a reason code and reason string, fixed header included.
    /// The packet type does not change it.
    /// </summary>
    /// <param name="reasonCode">
    /// The reason code.
    /// </param>
    /// <param name="reasonString">
    /// The Reason String property, or <see langword="null"/> to write none.
    /// </param>
    /// <returns>
    /// The size in bytes: 4 for success without a reason string, 5 for another reason code without one, more with a
    /// reason string.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The reason string cannot be carried: too long, or containing U+0000 or an unpaired surrogate.
    /// </exception>
    public static int GetSize(MqttReasonCode reasonCode, string? reasonString)
    {
        int remainingLength = PacketIdentifierSize + ReasonTail.GetSize(reasonCode, reasonString, out _);
        return PacketHeader.GetLength(remainingLength) + remainingLength;
    }

    /// <summary>
    /// Writes a PUBACK, PUBREC, PUBREL or PUBCOMP packet. Success without a reason string is written in the short
    /// form (the packet identifier only); another reason code adds that code, and a reason string adds a property
    /// block holding it. PUBREL gets the flags 0b0010 and the others 0.
    /// </summary>
    /// <param name="destination">
    /// Where to write; it must hold at least <see cref="GetSize"/> bytes.
    /// </param>
    /// <param name="type">
    /// <see cref="PacketType.PubAck"/>, <see cref="PacketType.PubRec"/>, <see cref="PacketType.PubRel"/> or
    /// <see cref="PacketType.PubComp"/>.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier, from 1 to 65,535.
    /// </param>
    /// <param name="reasonCode">
    /// The reason code.
    /// </param>
    /// <param name="reasonString">
    /// The Reason String property, or <see langword="null"/> to write none.
    /// </param>
    /// <returns>
    /// The number of bytes written, equal to <see cref="GetSize"/>.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The type is not one of the four, the packet identifier is 0, the reason string cannot be carried, or the
    /// destination is too small. Everything is checked before the first byte is written, so nothing is written.
    /// </exception>
    public static int Write(Span<byte> destination, PacketType type, ushort packetIdentifier, MqttReasonCode reasonCode, string? reasonString)
    {
        if (type is < PacketType.PubAck or > PacketType.PubComp)
        {
            CodecThrow.Invalid("The type is not PUBACK, PUBREC, PUBREL or PUBCOMP.", nameof(type));
        }

        if (packetIdentifier == 0)
        {
            CodecThrow.Invalid("The packet identifier cannot be 0.", nameof(packetIdentifier));
        }

        int tailLength = ReasonTail.GetSize(reasonCode, reasonString, out int propertiesLength);
        int remainingLength = PacketIdentifierSize + tailLength;
        int size = PacketHeader.GetLength(remainingLength) + remainingLength;
        if (destination.Length < size)
        {
            CodecThrow.DestinationTooSmall();
        }

        int headerLength = PacketHeader.Write(destination, type, type == PacketType.PubRel ? PubRelFlags : (byte)0, remainingLength);
        MqttWriter writer = new(destination[headerLength..]);

        writer.WriteUInt16(packetIdentifier);
        ReasonTail.Write(ref writer, reasonCode, reasonString, propertiesLength);

        return headerLength + writer.Written;
    }
}
