using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace MqttWire.Codec;

/// <summary>
/// The fixed header every MQTT packet starts with: a first byte holding the packet type and its flags,
/// then the remaining length as a variable byte integer.
/// </summary>
internal readonly struct PacketHeader
{
    /// <summary>
    /// Selects the flags in the low nibble of the first byte.
    /// </summary>
    private const int FlagsMask = 0x0F;

    /// <summary>
    /// The position of the packet type in the first byte.
    /// </summary>
    private const int TypeShift = 4;

    /// <summary>
    /// The flags PUBREL, SUBSCRIBE and UNSUBSCRIBE must carry.
    /// </summary>
    private const int RequiredFlags = 0b0010;

    /// <summary>
    /// The QoS bits of the PUBLISH flags; all set is the forbidden QoS 3.
    /// </summary>
    private const int PublishQosMask = 0b0110;

    /// <summary>
    /// The first byte of the packet: the type in the high nibble, the flags in the low.
    /// </summary>
    private readonly byte _firstByte;

    /// <summary>
    /// The size of the fixed header, two to five, stored when it is read so the lengths need no recomputing. It
    /// fits the struct's padding, which stays 8 bytes.
    /// </summary>
    private readonly byte _headerLength;

    /// <summary>
    /// Creates a header from values already validated by <see cref="TryRead"/>.
    /// </summary>
    /// <param name="firstByte">
    /// The first byte of the packet.
    /// </param>
    /// <param name="headerLength">
    /// The size of the fixed header: the first byte and the remaining length as it was written.
    /// </param>
    /// <param name="remainingLength">
    /// The remaining length.
    /// </param>
    private PacketHeader(byte firstByte, byte headerLength, int remainingLength)
    {
        _firstByte = firstByte;
        _headerLength = headerLength;
        RemainingLength = remainingLength;
    }

    /// <summary>
    /// The packet type. Only meaningful for a header that <see cref="TryRead"/> returned as done.
    /// </summary>
    public PacketType Type => (PacketType)(_firstByte >> TypeShift);

    /// <summary>
    /// The four flag bits of the first byte.
    /// </summary>
    public byte Flags => (byte)(_firstByte & FlagsMask);

    /// <summary>
    /// The number of bytes after the fixed header: the variable header and the payload.
    /// </summary>
    public int RemainingLength { get; }

    /// <summary>
    /// The size of the fixed header in bytes: the first byte and the remaining length, two to five.
    /// </summary>
    public int HeaderLength => _headerLength;

    /// <summary>
    /// The size of the whole packet in bytes: the fixed header plus the remaining length.
    /// </summary>
    public int TotalLength => _headerLength + RemainingLength;

    /// <summary>
    /// Reads the fixed header from the start of a buffer. The buffer need not hold the whole packet.
    /// </summary>
    /// <param name="buffer">
    /// The bytes to read from.
    /// </param>
    /// <param name="header">
    /// The header when the status is done; otherwise the default value.
    /// </param>
    /// <returns>
    /// <see cref="OperationStatus.Done"/> when a valid header was read; <see cref="OperationStatus.NeedMoreData"/>
    /// when the header is cut off; <see cref="OperationStatus.InvalidData"/> when the type is 0, the flags are not
    /// allowed for the type, or the remaining length can never be valid. The first byte is checked first, so a
    /// bad one is reported even when the length is not yet complete.
    /// </returns>
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

        OperationStatus status = VariableByteInteger.TryRead(buffer[1..], out int remainingLength, out int lengthLength);

        if (status == OperationStatus.Done)
        {
            header = new PacketHeader(firstByte, (byte)(1 + lengthLength), remainingLength);
        }

        return status;
    }

    /// <summary>
    /// Reads the fixed header and checks the buffer holds the whole packet.
    /// </summary>
    /// <param name="buffer">
    /// The bytes to read from; bytes after the packet are ignored.
    /// </param>
    /// <param name="header">
    /// The header when the status is done or when only the body is missing; otherwise the default value.
    /// </param>
    /// <returns>
    /// The status of <see cref="TryRead"/>, or <see cref="OperationStatus.NeedMoreData"/> when the header is valid
    /// but fewer than <see cref="TotalLength"/> bytes are in the buffer.
    /// </returns>
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

    /// <summary>
    /// Reads the fixed header of a buffer that must hold exactly one whole packet of the expected type. Packet
    /// codecs call this first: anything else is a bug in the caller, which framed the packet.
    /// </summary>
    /// <param name="packet">
    /// The bytes of one framed packet: the fixed header and the remaining length bytes after it, nothing more.
    /// </param>
    /// <param name="expected">
    /// The packet type the codec handles.
    /// </param>
    /// <returns>
    /// The header; the body is <c>packet[header.HeaderLength..]</c>.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The header is invalid or cut off, the type is not <paramref name="expected"/>, or the buffer is shorter or
    /// longer than <see cref="TotalLength"/>.
    /// </exception>
    public static PacketHeader ReadExact(ReadOnlySpan<byte> packet, PacketType expected)
        => ReadExact(packet, expected, expected);

    /// <summary>
    /// Reads the fixed header of a buffer that must hold exactly one whole packet whose type is in a range of
    /// adjacent types, such as PUBACK to PUBCOMP. The same rule as <see cref="ReadExact(ReadOnlySpan{byte}, PacketType)"/>,
    /// with the type check widened.
    /// </summary>
    /// <param name="packet">
    /// The bytes of one framed packet: the fixed header and the remaining length bytes after it, nothing more.
    /// </param>
    /// <param name="first">
    /// The lowest packet type the codec handles.
    /// </param>
    /// <param name="last">
    /// The highest packet type the codec handles.
    /// </param>
    /// <returns>
    /// The header; the body is <c>packet[header.HeaderLength..]</c>, and <see cref="Type"/> says which type it was.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The header is invalid or cut off, the type is outside <paramref name="first"/> to <paramref name="last"/>,
    /// or the buffer is shorter or longer than <see cref="TotalLength"/>.
    /// </exception>
    public static PacketHeader ReadExact(ReadOnlySpan<byte> packet, PacketType first, PacketType last)
    {
        if (TryReadPacket(packet, out PacketHeader header) != OperationStatus.Done
            || header.Type < first
            || header.Type > last
            || packet.Length != header.TotalLength)
        {
            ThrowNotExactPacket(first, last);
        }

        return header;
    }

    /// <summary>
    /// Throws for <see cref="ReadExact(ReadOnlySpan{byte}, PacketType, PacketType)"/>; kept out of line so the
    /// success path stays small enough to inline.
    /// </summary>
    /// <param name="first">
    /// The lowest packet type that was expected.
    /// </param>
    /// <param name="last">
    /// The highest packet type that was expected.
    /// </param>
    /// <exception cref="ArgumentException">
    /// Always.
    /// </exception>
    [DoesNotReturn]
    private static void ThrowNotExactPacket(PacketType first, PacketType last)
        => throw new ArgumentException(
            first == last
                ? $"The buffer is not exactly one framed {first} packet."
                : $"The buffer is not exactly one framed packet of type {first} to {last}.",
            "packet");

    /// <summary>
    /// Gets the size of the fixed header for a remaining length.
    /// </summary>
    /// <param name="remainingLength">
    /// The remaining length; not validated here, <see cref="Write"/> refuses an invalid one.
    /// </param>
    /// <returns>
    /// The size of the fixed header in bytes, two to five.
    /// </returns>
    public static int GetLength(int remainingLength) => 1 + VariableByteInteger.GetLength(remainingLength);

    /// <summary>
    /// Writes a fixed header. The flags are not checked against the type; that is the packet writer's job.
    /// </summary>
    /// <param name="destination">
    /// Where to write the header.
    /// </param>
    /// <param name="type">
    /// The packet type.
    /// </param>
    /// <param name="flags">
    /// The flags, from 0 to 15.
    /// </param>
    /// <param name="remainingLength">
    /// The remaining length, from 0 to <see cref="VariableByteInteger.MaxValue"/>.
    /// </param>
    /// <returns>
    /// The number of bytes written.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The flags are above 15 or the remaining length is out of range. Nothing is written.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The destination is too small. Nothing is written.
    /// </exception>
    public static int Write(Span<byte> destination, PacketType type, byte flags, int remainingLength)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(flags, FlagsMask);

        // The remaining length is checked before anything is written, so a bad call leaves the destination untouched.
        int length = VariableByteInteger.Write(destination[1..], remainingLength);
        destination[0] = (byte)(((int)type << TypeShift) | flags);
        return 1 + length;
    }

    // MQTT 2.1.3: the flags of every type except PUBLISH are fixed; PUBLISH may not use QoS 3.
    /// <summary>
    /// Checks the flags allowed for a packet type.
    /// </summary>
    /// <param name="type">
    /// The type nibble.
    /// </param>
    /// <param name="flags">
    /// The flags nibble.
    /// </param>
    /// <returns>
    /// <see langword="false"/> for type 0, for PUBLISH with QoS 3, and for any other type whose flags differ
    /// from the fixed value.
    /// </returns>
    private static bool AreFlagsValid(int type, int flags) => type switch
    {
        0 => false,
        (int)PacketType.Publish => (flags & PublishQosMask) != PublishQosMask,
        (int)PacketType.PubRel or (int)PacketType.Subscribe or (int)PacketType.Unsubscribe => flags == RequiredFlags,
        _ => flags == 0,
    };
}
