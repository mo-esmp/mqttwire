using System.Buffers;
using System.Buffers.Binary;
using System.Text.Unicode;

namespace MqttWire.Codec;

internal ref struct MqttReader
{
    private ReadOnlySpan<byte> _remaining;

    public MqttReader(ReadOnlySpan<byte> source) => _remaining = source;

    public readonly int Remaining => _remaining.Length;

    public bool TryReadByte(out byte value)
    {
        if (_remaining.IsEmpty)
        {
            value = 0;
            return false;
        }

        value = _remaining[0];
        _remaining = _remaining[1..];
        return true;
    }

    public bool TryReadUInt16(out ushort value)
    {
        if (_remaining.Length < sizeof(ushort))
        {
            value = 0;
            return false;
        }

        value = BinaryPrimitives.ReadUInt16BigEndian(_remaining);
        _remaining = _remaining[sizeof(ushort)..];
        return true;
    }

    public bool TryReadUInt32(out uint value)
    {
        if (_remaining.Length < sizeof(uint))
        {
            value = 0;
            return false;
        }

        value = BinaryPrimitives.ReadUInt32BigEndian(_remaining);
        _remaining = _remaining[sizeof(uint)..];
        return true;
    }

    public bool TryReadVariableByteInteger(out int value)
    {
        // The reader only runs over a complete packet, so a cut value is as malformed as an invalid one.
        if (VariableByteInteger.TryRead(_remaining, out value, out int length) != OperationStatus.Done)
        {
            value = 0;
            return false;
        }

        _remaining = _remaining[length..];
        return true;
    }

    public bool TryReadUtf8String(out ReadOnlySpan<byte> value)
    {
        if (!TryPeekBinary(out ReadOnlySpan<byte> data) || !Utf8.IsValid(data) || data.Contains((byte)0))
        {
            value = default;
            return false;
        }

        value = data;
        _remaining = _remaining[(FieldSize.LengthPrefix + data.Length)..];
        return true;
    }

    public bool TryReadBinary(out ReadOnlySpan<byte> value)
    {
        if (!TryPeekBinary(out value))
        {
            return false;
        }

        _remaining = _remaining[(FieldSize.LengthPrefix + value.Length)..];
        return true;
    }

    public bool TryReadBytes(int count, out ReadOnlySpan<byte> value)
    {
        if ((uint)count > (uint)_remaining.Length)
        {
            value = default;
            return false;
        }

        value = _remaining[..count];
        _remaining = _remaining[count..];
        return true;
    }

    public ReadOnlySpan<byte> ReadToEnd()
    {
        ReadOnlySpan<byte> rest = _remaining;
        _remaining = default;
        return rest;
    }

    private readonly bool TryPeekBinary(out ReadOnlySpan<byte> data)
    {
        if (_remaining.Length < FieldSize.LengthPrefix)
        {
            data = default;
            return false;
        }

        int length = BinaryPrimitives.ReadUInt16BigEndian(_remaining);
        if (_remaining.Length - FieldSize.LengthPrefix < length)
        {
            data = default;
            return false;
        }

        data = _remaining.Slice(FieldSize.LengthPrefix, length);
        return true;
    }
}
