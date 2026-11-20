using System.Buffers.Binary;

namespace MqttWire.Codec;

internal ref struct MqttWriter
{
    private Span<byte> _remaining;

    public MqttWriter(Span<byte> destination) => _remaining = destination;

    public int Written { get; private set; }

    public void WriteByte(byte value) => Reserve(sizeof(byte))[0] = value;

    public void WriteUInt16(ushort value) => BinaryPrimitives.WriteUInt16BigEndian(Reserve(sizeof(ushort)), value);

    public void WriteUInt32(uint value) => BinaryPrimitives.WriteUInt32BigEndian(Reserve(sizeof(uint)), value);

    public void WriteVariableByteInteger(int value)
    {
        int length = VariableByteInteger.Write(_remaining, value);
        Advance(length);
    }

    public void WriteUtf8String(string value)
    {
        int size = FieldSize.Utf8String(value);
        Span<byte> field = Reserve(size);
        BinaryPrimitives.WriteUInt16BigEndian(field, (ushort)(size - FieldSize.LengthPrefix));
        FieldSize.StrictUtf8.GetBytes(value, field[FieldSize.LengthPrefix..]);
    }

    public void WriteUtf8String(ReadOnlySpan<byte> utf8)
    {
        int size = FieldSize.Utf8String(utf8);
        Span<byte> field = Reserve(size);
        BinaryPrimitives.WriteUInt16BigEndian(field, (ushort)utf8.Length);
        utf8.CopyTo(field[FieldSize.LengthPrefix..]);
    }

    public void WriteBinary(ReadOnlySpan<byte> data)
    {
        int size = FieldSize.Binary(data.Length);
        Span<byte> field = Reserve(size);
        BinaryPrimitives.WriteUInt16BigEndian(field, (ushort)data.Length);
        data.CopyTo(field[FieldSize.LengthPrefix..]);
    }

    public void WriteBytes(ReadOnlySpan<byte> data) => data.CopyTo(Reserve(data.Length));

    private Span<byte> Reserve(int count)
    {
        if (count > _remaining.Length)
        {
            throw new ArgumentException("The destination is too small for the value.");
        }

        Span<byte> field = _remaining[..count];
        Advance(count);
        return field;
    }

    private void Advance(int count)
    {
        _remaining = _remaining[count..];
        Written += count;
    }
}
