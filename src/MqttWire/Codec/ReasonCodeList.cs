using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// The layout SUBACK and UNSUBACK share: a packet identifier, a property block and one reason code for each filter
/// of the request (MQTT 5, 3.9 and 3.11). The two codecs differ only in the packet type and the property context, so
/// they call this once.
/// </summary>
internal static class ReasonCodeList
{
    /// <summary>
    /// The size of the packet identifier.
    /// </summary>
    private const int IdentifierSize = sizeof(ushort);

    /// <summary>
    /// Decodes a SUBACK or UNSUBACK.
    /// </summary>
    /// <param name="packet">
    /// Exactly one framed packet of the given type, fixed header included.
    /// </param>
    /// <param name="type">
    /// <see cref="PacketType.SubAck"/> or <see cref="PacketType.UnsubAck"/>.
    /// </param>
    /// <param name="context">
    /// The property context of that type.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier; 0 unless the result is success.
    /// </param>
    /// <param name="properties">
    /// The validated properties; empty unless the result is success.
    /// </param>
    /// <param name="reasonCodes">
    /// One reason code per filter, at least one; empty unless the result is success.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>; <see cref="MqttReasonCode.ProtocolError"/> for a packet identifier of 0,
    /// a forbidden property value or no reason code; <see cref="MqttReasonCode.MalformedPacket"/> for any other
    /// fault in the body.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The buffer is not exactly one framed packet of the given type (a caller bug).
    /// </exception>
    public static MqttReasonCode TryDecode(
        ReadOnlySpan<byte> packet,
        PacketType type,
        PropertyContext context,
        out ushort packetIdentifier,
        out PropertyBlock properties,
        out ReadOnlySpan<byte> reasonCodes)
    {
        packetIdentifier = 0;
        properties = PropertyBlock.Empty;
        reasonCodes = default;
        PacketHeader header = PacketHeader.ReadExact(packet, type);
        MqttReader reader = new(packet[header.HeaderLength..]);

        if (!reader.TryReadUInt16(out ushort identifier))
        {
            return MqttReasonCode.MalformedPacket;
        }

        if (identifier == 0)
        {
            return MqttReasonCode.ProtocolError;
        }

        MqttReasonCode result = PropertyBlock.TryRead(ref reader, context, out PropertyBlock block);
        if (result != MqttReasonCode.Success)
        {
            return result;
        }

        if (reader.Remaining == 0)
        {
            return MqttReasonCode.ProtocolError;
        }

        packetIdentifier = identifier;
        properties = block;
        reasonCodes = reader.ReadToEnd();
        return MqttReasonCode.Success;
    }

    /// <summary>
    /// Gets the size of the whole packet, fixed header included.
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
    /// The count is below 1 or makes the packet longer than the largest remaining length, or the reason string
    /// cannot be carried: too long, or containing U+0000 or an unpaired surrogate.
    /// </exception>
    public static int GetSize(int count, string? reasonString)
    {
        int remainingLength = GetRemainingLength(count, reasonString, out _);
        return PacketHeader.GetLength(remainingLength) + remainingLength;
    }

    /// <summary>
    /// Writes a SUBACK or UNSUBACK. The property length is always written; it is 0 without a reason string.
    /// </summary>
    /// <param name="destination">
    /// Where to write; it must hold at least <see cref="GetSize"/> bytes.
    /// </param>
    /// <param name="type">
    /// <see cref="PacketType.SubAck"/> or <see cref="PacketType.UnsubAck"/>.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier, from 1 to 65,535.
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
    public static int Write(
        Span<byte> destination, PacketType type, ushort packetIdentifier, ReadOnlySpan<MqttReasonCode> reasonCodes, string? reasonString)
    {
        if (packetIdentifier == 0)
        {
            CodecThrow.Invalid("The packet identifier cannot be 0.", nameof(packetIdentifier));
        }

        int remainingLength = GetRemainingLength(reasonCodes.Length, reasonString, out int propertiesLength);
        if (destination.Length < PacketHeader.GetLength(remainingLength) + remainingLength)
        {
            CodecThrow.DestinationTooSmall();
        }

        int headerLength = PacketHeader.Write(destination, type, 0, remainingLength);
        MqttWriter writer = new(destination[headerLength..]);

        writer.WriteUInt16(packetIdentifier);
        writer.WriteVariableByteInteger(propertiesLength);
        if (reasonString is not null)
        {
            PropertyWriter.WriteString(ref writer, PropertyId.ReasonString, reasonString);
        }

        writer.WriteBytes(MemoryMarshal.AsBytes(reasonCodes));
        return headerLength + writer.Written;
    }

    /// <summary>
    /// Checks the count and the reason string and measures the packet once, for the size and the writer to share.
    /// </summary>
    /// <param name="count">
    /// The number of reason codes.
    /// </param>
    /// <param name="reasonString">
    /// The Reason String property, or <see langword="null"/> for none.
    /// </param>
    /// <param name="propertiesLength">
    /// The length of the properties, not counting their own length field; 0 without a reason string.
    /// </param>
    /// <returns>
    /// The remaining length.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// See <see cref="GetSize"/>.
    /// </exception>
    private static int GetRemainingLength(int count, string? reasonString, out int propertiesLength)
    {
        if (count < 1)
        {
            CodecThrow.Invalid("There must be at least one reason code.", nameof(count));
        }

        propertiesLength = reasonString is null ? 0 : PropertyWriter.StringSize(reasonString);

        // Summed in a long: the count may be as large as int.MaxValue.
        long length = IdentifierSize + VariableByteInteger.GetLength(propertiesLength) + propertiesLength + (long)count;
        if (length > VariableByteInteger.MaxValue)
        {
            CodecThrow.Invalid("The packet is longer than the largest remaining length.", nameof(count));
        }

        return (int)length;
    }
}
