namespace MqttWire.Codec;

/// <summary>
/// Sizes and writes MQTT 5 properties; every size and write includes the identifier byte. The writers throw
/// <see cref="ArgumentException"/> for a string MQTT cannot carry or a destination that is too small (caller bugs).
/// </summary>
internal static class PropertyWriter
{
    /// <summary>
    /// The size of a byte property: the identifier and one byte.
    /// </summary>
    public const int ByteSize = 2;

    /// <summary>
    /// The size of a two-byte integer property: the identifier and two bytes.
    /// </summary>
    public const int UInt16Size = 3;

    /// <summary>
    /// The size of a four-byte integer property: the identifier and four bytes.
    /// </summary>
    public const int UInt32Size = 5;

    /// <summary>
    /// Gets the size of a string property: the identifier, the length prefix and the UTF-8 bytes.
    /// </summary>
    /// <param name="value">The string value.</param>
    /// <returns>The size in bytes.</returns>
    /// <exception cref="ArgumentException">The string cannot be carried: too long or containing U+0000.</exception>
    public static int StringSize(string value) => 1 + FieldSize.Utf8String(value);

    /// <summary>
    /// Gets the combined size of user properties: per property the identifier, the name field and the value field.
    /// </summary>
    /// <param name="properties">The user properties.</param>
    /// <returns>The size in bytes; 0 for none. Summed in a <see cref="long"/>, since many properties can pass <see cref="int.MaxValue"/>.</returns>
    /// <exception cref="ArgumentException">A name or value cannot be carried.</exception>
    public static long UserPropertiesSize(ReadOnlySpan<MqttWireUserProperty> properties)
    {
        long size = 0;
        foreach (ref readonly MqttWireUserProperty property in properties)
        {
            size += 1 + FieldSize.Utf8String(property.Name) + FieldSize.Utf8String(property.Value);
        }

        return size;
    }

    /// <summary>
    /// Writes a byte property.
    /// </summary>
    /// <param name="writer">The writer to append to.</param>
    /// <param name="id">The property identifier.</param>
    /// <param name="value">The value; the limits the standard puts on it are not checked.</param>
    /// <exception cref="ArgumentException">The destination is too small.</exception>
    public static void WriteByte(ref MqttWriter writer, PropertyId id, byte value)
    {
        writer.WriteByte((byte)id);
        writer.WriteByte(value);
    }

    /// <summary>
    /// Writes a two-byte integer property.
    /// </summary>
    /// <param name="writer">The writer to append to.</param>
    /// <param name="id">The property identifier.</param>
    /// <param name="value">The value; the limits the standard puts on it are not checked.</param>
    /// <exception cref="ArgumentException">The destination is too small.</exception>
    public static void WriteUInt16(ref MqttWriter writer, PropertyId id, ushort value)
    {
        writer.WriteByte((byte)id);
        writer.WriteUInt16(value);
    }

    /// <summary>
    /// Writes a four-byte integer property.
    /// </summary>
    /// <param name="writer">The writer to append to.</param>
    /// <param name="id">The property identifier.</param>
    /// <param name="value">The value; the limits the standard puts on it are not checked.</param>
    /// <exception cref="ArgumentException">The destination is too small.</exception>
    public static void WriteUInt32(ref MqttWriter writer, PropertyId id, uint value)
    {
        writer.WriteByte((byte)id);
        writer.WriteUInt32(value);
    }

    /// <summary>
    /// Writes a string property.
    /// </summary>
    /// <param name="writer">The writer to append to.</param>
    /// <param name="id">The property identifier.</param>
    /// <param name="value">The string value.</param>
    /// <exception cref="ArgumentException">The string cannot be carried, or the destination is too small.</exception>
    public static void WriteString(ref MqttWriter writer, PropertyId id, string value)
    {
        writer.WriteByte((byte)id);
        writer.WriteUtf8String(value);
    }

    /// <summary>
    /// Writes user properties in order.
    /// </summary>
    /// <param name="writer">The writer to append to.</param>
    /// <param name="properties">The user properties; none writes nothing.</param>
    /// <exception cref="ArgumentException">A name or value cannot be carried, or the destination is too small.</exception>
    public static void WriteUserProperties(ref MqttWriter writer, ReadOnlySpan<MqttWireUserProperty> properties)
    {
        foreach (ref readonly MqttWireUserProperty property in properties)
        {
            writer.WriteByte((byte)PropertyId.UserProperty);
            writer.WriteUtf8String(property.Name);
            writer.WriteUtf8String(property.Value);
        }
    }
}
