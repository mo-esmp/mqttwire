namespace MqttWire.Codec;

internal static class PropertyWriter
{
    public const int ByteSize = 2;

    public const int UInt16Size = 3;

    public const int UInt32Size = 5;

    public static int StringSize(string value) => 1 + FieldSize.Utf8String(value);

    public static int UserPropertiesSize(ReadOnlySpan<MqttWireUserProperty> properties)
    {
        int size = 0;
        foreach (ref readonly MqttWireUserProperty property in properties)
        {
            size += 1 + FieldSize.Utf8String(property.Name) + FieldSize.Utf8String(property.Value);
        }

        return size;
    }

    public static void WriteByte(ref MqttWriter writer, PropertyId id, byte value)
    {
        writer.WriteByte((byte)id);
        writer.WriteByte(value);
    }

    public static void WriteUInt16(ref MqttWriter writer, PropertyId id, ushort value)
    {
        writer.WriteByte((byte)id);
        writer.WriteUInt16(value);
    }

    public static void WriteUInt32(ref MqttWriter writer, PropertyId id, uint value)
    {
        writer.WriteByte((byte)id);
        writer.WriteUInt32(value);
    }

    public static void WriteString(ref MqttWriter writer, PropertyId id, string value)
    {
        writer.WriteByte((byte)id);
        writer.WriteUtf8String(value);
    }

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
