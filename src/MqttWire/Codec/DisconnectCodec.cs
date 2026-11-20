namespace MqttWire.Codec;

/// <summary>
/// A decoded DISCONNECT packet: views over the bytes it was decoded from, so it is only valid while they are.
/// </summary>
internal readonly ref struct DisconnectPacket
{
    /// <summary>
    /// The reason code as sent; <see cref="MqttReasonCode.Success"/> when the packet is in its short form. Its value
    /// is not checked against the sender.
    /// </summary>
    public MqttReasonCode ReasonCode { get; init; }

    /// <summary>
    /// The validated DISCONNECT properties; empty in the short forms.
    /// </summary>
    public PropertyBlock Properties { get; init; }
}

/// <summary>
/// Decodes and encodes the MQTT 5 DISCONNECT packet (MQTT 5, 3.14).
/// </summary>
internal static class DisconnectCodec
{
    /// <summary>
    /// Decodes one DISCONNECT packet. Nothing allocates; the properties are views of the input.
    /// </summary>
    /// <param name="packet">
    /// Exactly one framed DISCONNECT packet, fixed header included.
    /// </param>
    /// <param name="disconnect">
    /// The decoded packet when the result is success; otherwise the default value.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>; <see cref="MqttReasonCode.ProtocolError"/> for a forbidden property
    /// value; <see cref="MqttReasonCode.MalformedPacket"/> for any other fault in the body, including bytes left
    /// over after the properties.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The buffer is not exactly one framed DISCONNECT packet (a caller bug, see
    /// <see cref="PacketHeader.ReadExact(ReadOnlySpan{byte}, PacketType)"/>).
    /// </exception>
    public static MqttReasonCode TryDecode(ReadOnlySpan<byte> packet, out DisconnectPacket disconnect)
    {
        disconnect = default;
        PacketHeader header = PacketHeader.ReadExact(packet, PacketType.Disconnect);
        MqttReader reader = new(packet[header.HeaderLength..]);

        MqttReasonCode result = ReasonTail.TryRead(ref reader, PropertyContext.Disconnect, out MqttReasonCode reasonCode, out PropertyBlock properties);
        if (result != MqttReasonCode.Success)
        {
            return result;
        }

        disconnect = new DisconnectPacket { ReasonCode = reasonCode, Properties = properties };
        return MqttReasonCode.Success;
    }

    /// <summary>
    /// Gets the size of the whole DISCONNECT packet for a reason code and reason string, fixed header included.
    /// </summary>
    /// <param name="reasonCode">
    /// The reason code.
    /// </param>
    /// <param name="reasonString">
    /// The Reason String property, or <see langword="null"/> to write none.
    /// </param>
    /// <returns>
    /// The size in bytes: 2 for success without a reason string, 3 for another reason code without one, more with a
    /// reason string.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The reason string cannot be carried: too long, or containing U+0000 or an unpaired surrogate.
    /// </exception>
    public static int GetSize(MqttReasonCode reasonCode, string? reasonString)
    {
        int remainingLength = ReasonTail.GetSize(reasonCode, reasonString, out _);
        return PacketHeader.GetLength(remainingLength) + remainingLength;
    }

    /// <summary>
    /// Writes a DISCONNECT packet. Success without a reason string is written as <c>E0 00</c>; another reason code
    /// adds that code, and a reason string adds a property block holding it.
    /// </summary>
    /// <param name="destination">
    /// Where to write; it must hold at least <see cref="GetSize"/> bytes.
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
    /// The reason string cannot be carried, or the destination is too small. Everything is checked before the first
    /// byte is written, so nothing is written.
    /// </exception>
    public static int Write(Span<byte> destination, MqttReasonCode reasonCode, string? reasonString)
    {
        int remainingLength = ReasonTail.GetSize(reasonCode, reasonString, out int propertiesLength);
        int size = PacketHeader.GetLength(remainingLength) + remainingLength;
        if (destination.Length < size)
        {
            CodecThrow.DestinationTooSmall();
        }

        int headerLength = PacketHeader.Write(destination, PacketType.Disconnect, 0, remainingLength);
        MqttWriter writer = new(destination[headerLength..]);

        ReasonTail.Write(ref writer, reasonCode, reasonString, propertiesLength);

        return headerLength + writer.Written;
    }
}
