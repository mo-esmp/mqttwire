using System.Buffers.Binary;

namespace MqttWire.Codec;

/// <summary>
/// Writes MQTT fields forward into a span of bytes, never allocating. Every method checks that the field
/// fits and throws before it writes anything, so a failed call writes nothing and leaves
/// <see cref="Written"/> as it was. The caller sizes the destination with <see cref="FieldSize"/>.
/// </summary>
internal ref struct MqttWriter
{
    /// <summary>
    /// The part of the destination not yet written.
    /// </summary>
    private Span<byte> _remaining;

    /// <summary>
    /// Starts a writer at the first byte of a buffer.
    /// </summary>
    /// <param name="destination">
    /// Where to write; the writer keeps a reference to it.
    /// </param>
    public MqttWriter(Span<byte> destination) => _remaining = destination;

    /// <summary>
    /// The number of bytes written so far.
    /// </summary>
    public int Written { get; private set; }

    /// <summary>
    /// Writes one byte.
    /// </summary>
    /// <param name="value">
    /// The byte to write.
    /// </param>
    /// <exception cref="ArgumentException">
    /// No room is left.
    /// </exception>
    public void WriteByte(byte value) => Reserve(sizeof(byte))[0] = value;

    /// <summary>
    /// Writes a two-byte big-endian integer.
    /// </summary>
    /// <param name="value">
    /// The integer to write.
    /// </param>
    /// <exception cref="ArgumentException">
    /// Fewer than two bytes are left.
    /// </exception>
    public void WriteUInt16(ushort value) => BinaryPrimitives.WriteUInt16BigEndian(Reserve(sizeof(ushort)), value);

    /// <summary>
    /// Writes a four-byte big-endian integer.
    /// </summary>
    /// <param name="value">
    /// The integer to write.
    /// </param>
    /// <exception cref="ArgumentException">
    /// Fewer than four bytes are left.
    /// </exception>
    public void WriteUInt32(uint value) => BinaryPrimitives.WriteUInt32BigEndian(Reserve(sizeof(uint)), value);

    /// <summary>
    /// Writes a variable byte integer in its shortest form.
    /// </summary>
    /// <param name="value">
    /// The value to write, from 0 to <see cref="VariableByteInteger.MaxValue"/>.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The value is negative or above <see cref="VariableByteInteger.MaxValue"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Too little room is left.
    /// </exception>
    public void WriteVariableByteInteger(int value)
    {
        int length = VariableByteInteger.Write(_remaining, value);
        Advance(length);
    }

    /// <summary>
    /// Writes a string as a two-byte length followed by its UTF-8 bytes.
    /// </summary>
    /// <param name="value">
    /// The text to write.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// The string is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The string is longer than 65,535 bytes in UTF-8, contains U+0000 or an unpaired surrogate, or too little
    /// room is left.
    /// </exception>
    public void WriteUtf8String(string value)
    {
        int size = FieldSize.Utf8String(value);
        Span<byte> field = Reserve(size);
        BinaryPrimitives.WriteUInt16BigEndian(field, (ushort)(size - FieldSize.LengthPrefix));
        FieldSize.StrictUtf8.GetBytes(value, field[FieldSize.LengthPrefix..]);
    }

    /// <summary>
    /// Writes UTF-8 bytes as a two-byte length followed by the bytes. The bytes are copied as they are:
    /// their validity as UTF-8 and the absence of U+0000 are the caller's to ensure.
    /// </summary>
    /// <param name="utf8">
    /// The bytes to write.
    /// </param>
    /// <exception cref="ArgumentException">
    /// There are more than 65,535 bytes or too little room is left.
    /// </exception>
    public void WriteUtf8String(ReadOnlySpan<byte> utf8)
    {
        int size = FieldSize.Utf8String(utf8);
        Span<byte> field = Reserve(size);
        BinaryPrimitives.WriteUInt16BigEndian(field, (ushort)utf8.Length);
        utf8.CopyTo(field[FieldSize.LengthPrefix..]);
    }

    /// <summary>
    /// Writes binary data as a two-byte length followed by the bytes.
    /// </summary>
    /// <param name="data">
    /// The data to write.
    /// </param>
    /// <exception cref="ArgumentException">
    /// There are more than 65,535 bytes or too little room is left.
    /// </exception>
    public void WriteBinary(ReadOnlySpan<byte> data)
    {
        int size = FieldSize.Binary(data.Length);
        Span<byte> field = Reserve(size);
        BinaryPrimitives.WriteUInt16BigEndian(field, (ushort)data.Length);
        data.CopyTo(field[FieldSize.LengthPrefix..]);
    }

    /// <summary>
    /// Writes bytes with no length prefix.
    /// </summary>
    /// <param name="data">
    /// The bytes to write.
    /// </param>
    /// <exception cref="ArgumentException">
    /// Too little room is left.
    /// </exception>
    public void WriteBytes(ReadOnlySpan<byte> data) => data.CopyTo(Reserve(data.Length));

    /// <summary>
    /// Claims the next bytes of the destination and moves past them.
    /// </summary>
    /// <param name="count">
    /// The number of bytes to claim.
    /// </param>
    /// <returns>
    /// The claimed bytes, for the caller to fill.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Fewer than <paramref name="count"/> bytes are left. Nothing is claimed.
    /// </exception>
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

    /// <summary>
    /// Moves past bytes that have been written.
    /// </summary>
    /// <param name="count">
    /// The number of bytes to skip.
    /// </param>
    private void Advance(int count)
    {
        _remaining = _remaining[count..];
        Written += count;
    }
}
