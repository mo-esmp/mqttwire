using System.Buffers;

namespace MqttWire.Codec;

internal readonly struct PacketHeader
{
    private const int FlagsMask = 0x0F;

    private const int TypeShift = 4;

    private const int RequiredFlags = 0b0010;

    private const int PublishQosMask = 0b0110;

    private readonly byte _firstByte;

    private PacketHeader(byte firstByte, int remainingLength)
    {
        _firstByte = firstByte;
        RemainingLength = remainingLength;
    }

    public PacketType Type => (PacketType)(_firstByte >> TypeShift);

    public byte Flags => (byte)(_firstByte & FlagsMask);

    public int RemainingLength { get; }

    public int HeaderLength => 1 + VariableByteInteger.GetLength(RemainingLength);

    public int TotalLength => HeaderLength + RemainingLength;

    public static OperationStatus TryRead(ReadOnlySpan<byte> buffer, out PacketHeader header)
    {
        header = default;

        if (buffer.IsEmpty)
        {
            return OperationStatus.NeedMoreData;
        }

        byte firstByte = buffer[0];

        if (!AreFlagsValid(firstByte >> TypeShift, firstByte & FlagsMask))
        {
            return OperationStatus.InvalidData;
        }

        OperationStatus status = VariableByteInteger.TryRead(buffer[1..], out int remainingLength, out _);

        if (status == OperationStatus.Done)
        {
            header = new PacketHeader(firstByte, remainingLength);
        }

        return status;
    }

    public static OperationStatus TryReadPacket(ReadOnlySpan<byte> buffer, out PacketHeader header)
    {
        OperationStatus status = TryRead(buffer, out header);

        if (status != OperationStatus.Done)
        {
            return status;
        }

        return buffer.Length >= header.TotalLength
            ? OperationStatus.Done
            : OperationStatus.NeedMoreData;
    }

    public static int GetLength(int remainingLength) => 1 + VariableByteInteger.GetLength(remainingLength);

    public static int Write(Span<byte> destination, PacketType type, byte flags, int remainingLength)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(flags, FlagsMask);

        // The remaining length is checked before anything is written, so a bad call leaves the destination untouched.
        int length = VariableByteInteger.Write(destination[1..], remainingLength);
        destination[0] = (byte)(((int)type << TypeShift) | flags);
        return 1 + length;
    }

    // MQTT 2.1.3: the flags of every type except PUBLISH are fixed; PUBLISH may not use QoS 3.
    private static bool AreFlagsValid(int type, int flags) => type switch
    {
        0 => false,
        (int)PacketType.Publish => (flags & PublishQosMask) != PublishQosMask,
        (int)PacketType.PubRel or (int)PacketType.Subscribe or (int)PacketType.Unsubscribe => flags == RequiredFlags,
        _ => flags == 0,
    };
}
