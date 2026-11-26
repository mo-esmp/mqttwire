using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using MqttWire.Codec;

namespace MqttWire.Client;

/// <summary>
/// The answers (PUBACK, PUBREC, PUBREL, PUBCOMP) the read loop has decided on and not yet written, encoded one
/// after the other in one pooled buffer, so that they go out in a single write. The buffer is rented when the first
/// answer is added and handed over by <see cref="Take"/>. Used by the read loop only; not thread-safe.
/// </summary>
/// <remarks>
/// A mutable struct, so that it lives inline in the client and constructing a client allocates nothing for it; it
/// holds no pool either, and the members that rent or return take the client's. It must be used in place, from a
/// field that is not <see langword="readonly"/>, and never copied: a copy would hold the same buffer, and both could
/// return it. The default value holds no answer.
/// </remarks>
[StructLayout(LayoutKind.Auto)]
internal struct PendingAnswers
{
    /// <summary>
    /// The rented buffer, or <see langword="null"/> when no answer waits.
    /// </summary>
    private byte[]? _buffer;

    /// <summary>
    /// The number of bytes of encoded answers in <see cref="_buffer"/>, at most
    /// <see cref="ClientLimits.AnswerBufferBytes"/>; a byte, like the two counts, so the struct adds little to the
    /// client it lives in.
    /// </summary>
    private byte _length;

    /// <summary>
    /// The number of packets counted by <see cref="CountPacket"/> since the oldest waiting answer was added.
    /// </summary>
    private byte _packets;

    /// <summary>
    /// The number of answers waiting, at most <see cref="ClientLimits.AnswerPacketBound"/>.
    /// </summary>
    private byte _count;

    /// <summary>
    /// Gets the number of answers waiting.
    /// </summary>
    public readonly int Count => _count;

    /// <summary>
    /// Gets a value that tells whether the answers must be written before the next packet is handled: some answer
    /// waits and <see cref="ClientLimits.AnswerPacketBound"/> minus one packets have been counted since the oldest
    /// one was added. It bounds how long an answer waits and keeps the buffer from overflowing.
    /// </summary>
    public readonly bool MustWriteBeforeNextPacket => _count > 0 && _packets >= ClientLimits.AnswerPacketBound - 1;

    /// <summary>
    /// Encodes an answer after the ones waiting, renting the buffer for the first. Adding to a full buffer is a
    /// client bug: it is asserted in debug builds and has no other path.
    /// </summary>
    /// <param name="pool">
    /// The pool the buffer is rented from when this is the first answer; the one <see cref="Release"/> is given.
    /// </param>
    /// <param name="type">
    /// The answer's packet type.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier it answers.
    /// </param>
    /// <param name="reasonCode">
    /// The reason code; a success is written without the reason byte.
    /// </param>
    public void Add(ArrayPool<byte> pool, PacketType type, ushort packetIdentifier, MqttReasonCode reasonCode)
    {
        Debug.Assert(_count < ClientLimits.AnswerPacketBound, "The answers must be written before the buffer is full.");
        _buffer ??= pool.Rent(minimumLength: ClientLimits.AnswerBufferBytes);
        int written = AckCodec.Write(
            destination: _buffer.AsSpan(start: _length),
            type: type,
            packetIdentifier: packetIdentifier,
            reasonCode: reasonCode,
            reasonString: null);

        // At most AnswerPacketBound answers of at most MaxAnswerBytes each: the sum fits in a byte.
        _length = (byte)(_length + written);
        _count++;
    }

    /// <summary>
    /// Counts a packet about to be handled, but only while an answer waits: the bound is on how many packets pass
    /// after the oldest answer, not on all of them.
    /// </summary>
    public void CountPacket()
    {
        if (_count > 0)
        {
            _packets++;
        }
    }

    /// <summary>
    /// Hands over the buffer with the encoded answers. The caller owns it and returns it to the pool, through
    /// the send path; this type holds nothing afterwards, and the next <see cref="Add"/> rents a fresh buffer.
    /// Taking with no answer waiting is a client bug: it is asserted in debug builds and has no other path.
    /// </summary>
    /// <param name="length">
    /// The number of bytes of encoded answers at the start of the buffer.
    /// </param>
    /// <returns>
    /// The rented buffer.
    /// </returns>
    public byte[] Take(out int length)
    {
        Debug.Assert(_buffer is not null, "There must be an answer waiting to take.");

        // An answer waits, so the buffer is held; a take with none waiting is a client bug.
        byte[] buffer = _buffer;
        length = _length;
        _buffer = null;
        _length = 0;
        _packets = 0;
        _count = 0;
        return buffer;
    }

    /// <summary>
    /// Returns the buffer if one is held, and forgets the answers in it. Does nothing when none is held, so it can
    /// be called again, and from the end of the read loop whatever the way out.
    /// </summary>
    /// <param name="pool">
    /// The pool the buffer was rented from, the one <see cref="Add"/> was given.
    /// </param>
    public void Release(ArrayPool<byte> pool)
    {
        byte[]? buffer = _buffer;
        _buffer = null;
        _length = 0;
        _packets = 0;
        _count = 0;
        if (buffer is not null)
        {
            pool.Return(array: buffer);
        }
    }
}
