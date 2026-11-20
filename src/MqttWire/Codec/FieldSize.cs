using System.Text;

namespace MqttWire.Codec;

/// <summary>
/// Measures the length-prefixed MQTT fields and checks they fit the two-byte length prefix.
/// </summary>
internal static class FieldSize
{
    /// <summary>
    /// UTF-8 without a byte order mark that throws on an unpaired surrogate instead of replacing it.
    /// </summary>
    public static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>
    /// The most bytes a length-prefixed field can hold, 65,535.
    /// </summary>
    public const int MaxDataLength = ushort.MaxValue;

    /// <summary>
    /// The size in bytes of the length prefix.
    /// </summary>
    public const int LengthPrefix = sizeof(ushort);

    /// <summary>
    /// Gets the encoded size of a string: its length prefix plus its UTF-8 bytes.
    /// </summary>
    /// <param name="value">
    /// The text to measure.
    /// </param>
    /// <returns>
    /// The number of bytes the field takes.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// The string is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The string is longer than 65,535 bytes in UTF-8, contains U+0000 or has an unpaired surrogate.
    /// </exception>
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

    /// <summary>
    /// Gets the encoded size of UTF-8 bytes: the length prefix plus the bytes. Only the length is checked, not
    /// that the bytes are valid UTF-8.
    /// </summary>
    /// <param name="utf8">
    /// The bytes to measure.
    /// </param>
    /// <returns>
    /// The number of bytes the field takes.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// There are more than 65,535 bytes.
    /// </exception>
    public static int Utf8String(ReadOnlySpan<byte> utf8) => Binary(utf8.Length);

    /// <summary>
    /// Gets the encoded size of binary data: the length prefix plus the data.
    /// </summary>
    /// <param name="length">
    /// The number of data bytes.
    /// </param>
    /// <returns>
    /// The number of bytes the field takes.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The length is negative or above 65,535.
    /// </exception>
    public static int Binary(int length)
    {
        if ((uint)length > MaxDataLength)
        {
            throw new ArgumentException("The data is longer than 65,535 bytes.", nameof(length));
        }

        return LengthPrefix + length;
    }
}
