using System.Text;

namespace MqttWire.Codec;

internal static class FieldSize
{
    public static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public const int MaxDataLength = ushort.MaxValue;

    public const int LengthPrefix = sizeof(ushort);

    public static int Utf8String(string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        // The UTF-8 length is at least the number of chars, so a string this long can never fit.
        if (value.Length > MaxDataLength)
        {
            throw new ArgumentException("The string is longer than 65,535 bytes in UTF-8.", nameof(value));
        }

        if (value.Contains('\0'))
        {
            throw new ArgumentException("The string contains U+0000, which MQTT does not allow.", nameof(value));
        }

        return Binary(StrictUtf8.GetByteCount(value));
    }

    public static int Utf8String(ReadOnlySpan<byte> utf8) => Binary(utf8.Length);

    public static int Binary(int length)
    {
        if ((uint)length > MaxDataLength)
        {
            throw new ArgumentException("The data is longer than 65,535 bytes.", nameof(length));
        }

        return LengthPrefix + length;
    }
}
