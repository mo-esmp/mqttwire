using System.Buffers;

namespace MqttWire.Codec;

internal static class VariableByteInteger
{
    public const int MaxValue = 268_435_455;

    public const int MaxLength = 4;

    private const int PayloadMask = 0x7F;

    private const int ContinuationBit = 0x80;

    private const int PayloadBits = 7;

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

    public static int GetLength(int value) => value switch
    {
        < 128 => 1,
        < 16_384 => 2,
        < 2_097_152 => 3,
        _ => 4,
    };

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
