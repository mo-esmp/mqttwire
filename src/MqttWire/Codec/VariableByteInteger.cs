using System.Buffers;

namespace MqttWire.Codec;

/// <summary>
/// The MQTT variable byte integer: seven bits per byte, least significant group first, with the high bit set
/// on every byte but the last. It takes one to four bytes and the shortest form is the only valid one.
/// </summary>
internal static class VariableByteInteger
{
    /// <summary>
    /// The largest value a variable byte integer can hold, 268,435,455.
    /// </summary>
    public const int MaxValue = 268_435_455;

    /// <summary>
    /// The most bytes a variable byte integer takes.
    /// </summary>
    public const int MaxLength = 4;

    /// <summary>
    /// Selects the seven value bits of an encoded byte.
    /// </summary>
    private const int PayloadMask = 0x7F;

    /// <summary>
    /// Set on an encoded byte when another byte follows.
    /// </summary>
    private const int ContinuationBit = 0x80;

    /// <summary>
    /// The number of value bits in each encoded byte.
    /// </summary>
    private const int PayloadBits = 7;

    /// <summary>
    /// Reads a variable byte integer from the start of a buffer.
    /// </summary>
    /// <param name="source">
    /// The bytes to read from; anything after the value is ignored.
    /// </param>
    /// <param name="value">
    /// The decoded value, or 0 when the status is not <see cref="OperationStatus.Done"/>.
    /// </param>
    /// <param name="length">
    /// The number of bytes the value took, or 0 when the status is not <see cref="OperationStatus.Done"/>.
    /// </param>
    /// <returns>
    /// <see cref="OperationStatus.Done"/> when a value was read; <see cref="OperationStatus.NeedMoreData"/> when the
    /// buffer ends before the value does and more bytes could complete it; <see cref="OperationStatus.InvalidData"/>
    /// when the value can never be valid: four bytes without an end, or a longer form than needed.
    /// </returns>
    public static OperationStatus TryRead(ReadOnlySpan<byte> source, out int value, out int length)
    {
        int result = 0;
        int count = Math.Min(source.Length, MaxLength);

        for (int i = 0; i < count; i++)
        {
            int current = source[i];
            result |= (current & PayloadMask) << (i * PayloadBits);

            if ((current & ContinuationBit) != 0)
            {
                continue;
            }

            // A last byte of 0x00 after the first means the value was written in a longer form than needed.
            if (current == 0 && i > 0)
            {
                value = 0;
                length = 0;
                return OperationStatus.InvalidData;
            }

            value = result;
            length = i + 1;
            return OperationStatus.Done;
        }

        value = 0;
        length = 0;

        // Four bytes without an end can never become valid; fewer may still be completed.
        return count == MaxLength
            ? OperationStatus.InvalidData
            : OperationStatus.NeedMoreData;
    }

    /// <summary>
    /// Gets the number of bytes a value takes when written.
    /// </summary>
    /// <param name="value">
    /// The value to measure. Not validated: a value above <see cref="MaxValue"/> still answers 4 and a
    /// negative one answers 1; only <see cref="Write"/> refuses them.
    /// </param>
    /// <returns>
    /// One to four.
    /// </returns>
    public static int GetLength(int value) => value switch
    {
        < 128 => 1,
        < 16_384 => 2,
        < 2_097_152 => 3,
        _ => 4,
    };

    /// <summary>
    /// Writes a value in its shortest form.
    /// </summary>
    /// <param name="destination">
    /// Where to write the bytes; only the first <see cref="GetLength"/> bytes are touched.
    /// </param>
    /// <param name="value">
    /// The value to write, from 0 to <see cref="MaxValue"/>.
    /// </param>
    /// <returns>
    /// The number of bytes written.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The value is negative or above <see cref="MaxValue"/>. Nothing is written.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The destination is shorter than the value needs. Nothing is written.
    /// </exception>
    public static int Write(Span<byte> destination, int value)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(value);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(value, MaxValue);

        int length = GetLength(value);
        if (destination.Length < length)
        {
            throw new ArgumentException("The destination is too small for the value.", nameof(destination));
        }

        for (int i = 0; i < length - 1; i++)
        {
            destination[i] = (byte)((value & PayloadMask) | ContinuationBit);
            value >>= PayloadBits;
        }

        destination[length - 1] = (byte)value;
        return length;
    }
}
