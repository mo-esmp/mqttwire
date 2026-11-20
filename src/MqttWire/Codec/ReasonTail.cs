namespace MqttWire.Codec;

/// <summary>
/// The tail PUBACK, PUBREC, PUBREL, PUBCOMP and DISCONNECT share after what comes first in their variable header:
/// an optional reason code, then an optional property block. The short forms drop the property block, and a
/// <see cref="MqttReasonCode.Success"/> with nothing else drops the reason code too.
/// </summary>
internal static class ReasonTail
{
    /// <summary>
    /// Gets the size of the tail an encoder writes.
    /// </summary>
    /// <param name="reasonCode">
    /// The reason code.
    /// </param>
    /// <param name="reasonString">
    /// The Reason String property, or <see langword="null"/> to write none.
    /// </param>
    /// <param name="propertiesLength">
    /// The length of the properties, not counting their own length field; 0 when there is no reason string.
    /// </param>
    /// <returns>
    /// 0 for success without a reason string; 1 for another reason code without one; otherwise the reason code, the
    /// property length and the property.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The reason string cannot be carried: too long, or containing U+0000 or an unpaired surrogate.
    /// </exception>
    public static int GetSize(MqttReasonCode reasonCode, string? reasonString, out int propertiesLength)
    {
        if (reasonString is null)
        {
            propertiesLength = 0;
            return reasonCode == MqttReasonCode.Success ? 0 : 1;
        }

        propertiesLength = PropertyWriter.StringSize(reasonString);
        return 1 + VariableByteInteger.GetLength(propertiesLength) + propertiesLength;
    }

    /// <summary>
    /// Writes the tail in the form <see cref="GetSize"/> measured.
    /// </summary>
    /// <param name="writer">
    /// The writer to append to; it must have room for the size <see cref="GetSize"/> returned.
    /// </param>
    /// <param name="reasonCode">
    /// The reason code.
    /// </param>
    /// <param name="reasonString">
    /// The Reason String property, or <see langword="null"/> to write none.
    /// </param>
    /// <param name="propertiesLength">
    /// The properties length <see cref="GetSize"/> returned for the same arguments.
    /// </param>
    /// <exception cref="ArgumentException">
    /// The writer has too little room.
    /// </exception>
    public static void Write(ref MqttWriter writer, MqttReasonCode reasonCode, string? reasonString, int propertiesLength)
    {
        if (reasonString is null && reasonCode == MqttReasonCode.Success)
        {
            return;
        }

        writer.WriteByte((byte)reasonCode);
        if (reasonString is null)
        {
            return;
        }

        writer.WriteVariableByteInteger(propertiesLength);
        PropertyWriter.WriteString(ref writer, PropertyId.ReasonString, reasonString);
    }

    /// <summary>
    /// Reads the tail from what is left of a packet body, which it must consume entirely.
    /// </summary>
    /// <param name="reader">
    /// The reader, positioned after what precedes the tail; after a failure its position is unspecified.
    /// </param>
    /// <param name="context">
    /// The packet the properties belong to.
    /// </param>
    /// <param name="reasonCode">
    /// The reason code as sent, unchecked; <see cref="MqttReasonCode.Success"/> when the tail is empty or the result
    /// is not success.
    /// </param>
    /// <param name="properties">
    /// The validated properties; <see cref="PropertyBlock.Empty"/> when there are none or the result is not success.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>; the result of <see cref="PropertyBlock.TryRead"/> for a bad property
    /// block; <see cref="MqttReasonCode.MalformedPacket"/> for bytes left over after the block.
    /// </returns>
    public static MqttReasonCode TryRead(
        scoped ref MqttReader reader, PropertyContext context, out MqttReasonCode reasonCode, out PropertyBlock properties)
    {
        reasonCode = MqttReasonCode.Success;
        properties = PropertyBlock.Empty;

        if (reader.Remaining == 0)
        {
            return MqttReasonCode.Success;
        }

        reader.TryReadByte(out byte code);
        if (reader.Remaining != 0)
        {
            MqttReasonCode result = PropertyBlock.TryRead(ref reader, context, out properties);
            if (result != MqttReasonCode.Success)
            {
                return result;
            }

            if (reader.Remaining != 0)
            {
                properties = PropertyBlock.Empty;
                return MqttReasonCode.MalformedPacket;
            }
        }

        reasonCode = (MqttReasonCode)code;
        return MqttReasonCode.Success;
    }
}
