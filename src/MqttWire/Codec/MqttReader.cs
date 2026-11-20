using System.Buffers;
using System.Buffers.Binary;
using System.Text.Unicode;

namespace MqttWire.Codec;

/// <summary>
/// Reads MQTT fields forward from a span of bytes. Every <c>TryRead</c> method returns <see langword="false"/>
/// for a malformed or cut-off field, sets its output to the default and leaves the position where it was.
/// The reader never allocates: strings and binary data come back as slices of the source.
/// </summary>
internal ref struct MqttReader
{
    /// <summary>
    /// The bytes not yet read.
    /// </summary>
    private ReadOnlySpan<byte> _remaining;

    /// <summary>
    /// Starts a reader at the first byte of a buffer.
    /// </summary>
    /// <param name="source">
    /// The bytes to read; the reader keeps a reference to them and does not copy.
    /// </param>
    public MqttReader(ReadOnlySpan<byte> source) => _remaining = source;

    /// <summary>
    /// The number of bytes not yet read.
    /// </summary>
    public readonly int Remaining => _remaining.Length;

    /// <summary>
    /// Reads one byte.
    /// </summary>
    /// <param name="value">
    /// The byte, or 0 when there is none.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when no byte is left.
    /// </returns>
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

    /// <summary>
    /// Reads a two-byte big-endian integer.
    /// </summary>
    /// <param name="value">
    /// The integer, or 0 when fewer than two bytes are left.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when fewer than two bytes are left.
    /// </returns>
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

    /// <summary>
    /// Reads a four-byte big-endian integer.
    /// </summary>
    /// <param name="value">
    /// The integer, or 0 when fewer than four bytes are left.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when fewer than four bytes are left.
    /// </returns>
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

    /// <summary>
    /// Reads a variable byte integer.
    /// </summary>
    /// <param name="value">
    /// The integer, or 0 when it could not be read.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the integer is invalid or cut off; the two are not told apart, because the
    /// reader only runs over a complete packet.
    /// </returns>
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

    /// <summary>
    /// Reads a two-byte length followed by that many bytes of UTF-8 text, checking the text against the MQTT rules.
    /// </summary>
    /// <param name="value">
    /// The UTF-8 bytes without the length prefix, a slice of the source; empty when the read fails.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the length prefix is missing, the length runs past the end, the bytes are not
    /// well-formed UTF-8 (this includes unpaired surrogates and overlong forms) or they contain U+0000.
    /// </returns>
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

    /// <summary>
    /// Reads a two-byte length followed by that many bytes of binary data. The content is not checked.
    /// </summary>
    /// <param name="value">
    /// The data without the length prefix, a slice of the source; empty when the read fails.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the length prefix is missing or the length runs past the end.
    /// </returns>
    public bool TryReadBinary(out ReadOnlySpan<byte> value)
    {
        if (!TryPeekBinary(out value))
        {
            return false;
        }

        _remaining = _remaining[(FieldSize.LengthPrefix + value.Length)..];
        return true;
    }

    /// <summary>
    /// Reads a fixed number of bytes with no length prefix.
    /// </summary>
    /// <param name="count">
    /// The number of bytes to read.
    /// </param>
    /// <param name="value">
    /// The bytes, a slice of the source; empty when the read fails.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when <paramref name="count"/> is negative or more than are left.
    /// </returns>
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

    /// <summary>
    /// Takes everything that is left.
    /// </summary>
    /// <returns>
    /// The remaining bytes, a slice of the source; empty when none are left. The reader is then at its end.
    /// </returns>
    public ReadOnlySpan<byte> ReadToEnd()
    {
        ReadOnlySpan<byte> rest = _remaining;
        _remaining = default;
        return rest;
    }

    /// <summary>
    /// Reads a length-prefixed field without moving the position.
    /// </summary>
    /// <param name="data">
    /// The field content without the length prefix; empty when the read fails.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the length prefix is missing or the length runs past the end.
    /// </returns>
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
