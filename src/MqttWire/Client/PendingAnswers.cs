using System.Buffers;
using System.Diagnostics;
using MqttWire.Codec;

namespace MqttWire.Client;

/// <summary>
/// The answers (PUBACK, PUBREC, PUBREL, PUBCOMP) the read loop has decided on and not yet written, encoded one
/// after the other in one pooled buffer, so that they go out in a single write. The buffer is rented when the first
/// answer is added and handed over by <see cref="Take"/>. Used by the read loop only; not thread-safe.
/// </summary>
/// <param name="pool">
/// The pool the buffer is rented from.
/// </param>
internal sealed class PendingAnswers(ArrayPool<byte> pool)
{
    /// <summary>
    /// The rented buffer, or <see langword="null"/> when no answer waits.
    /// </summary>
    private byte[]? _buffer;

    /// <summary>
    /// The number of bytes of encoded answers in <see cref="_buffer"/>.
    /// </summary>
    private int _length;

    /// <summary>
    /// The number of packets counted by <see cref="CountPacket"/> since the oldest waiting answer was added.
    /// </summary>
    private int _packets;

    /// <summary>
    /// Gets the number of answers waiting.
    /// </summary>
    public int Count { get; private set; }

    /// <summary>
    /// Gets a value that tells whether the answers must be written before the next packet is handled: some answer
    /// waits and <see cref="ClientLimits.AnswerPacketBound"/> minus one packets have been counted since the oldest
    /// one was added. It bounds how long an answer waits and keeps the buffer from overflowing.
    /// </summary>
    public bool MustWriteBeforeNextPacket => Count > 0 && _packets >= ClientLimits.AnswerPacketBound - 1;

    /// <summary>
    /// Encodes an answer after the ones waiting, renting the buffer for the first. Adding to a full buffer is a
    /// client bug: it is asserted in debug builds and has no other path.
    /// </summary>
    /// <param name="type">
    /// The answer's packet type.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier it answers.
    /// </param>
    /// <param name="reasonCode">
    /// The reason code; a success is written without the reason byte.
    /// </param>
    public void Add(PacketType type, ushort packetIdentifier, MqttReasonCode reasonCode)
    {
        Debug.Assert(Count < ClientLimits.AnswerPacketBound, "The answers must be written before the buffer is full.");
        _buffer ??= pool.Rent(minimumLength: ClientLimits.AnswerBufferBytes);
        _length += AckCodec.Write(
            destination: _buffer.AsSpan(start: _length),
            type: type,
            packetIdentifier: packetIdentifier,
            reasonCode: reasonCode,
            reasonString: null);
        Count++;
    }

    /// <summary>
    /// Counts a packet about to be handled, but only while an answer waits: the bound is on how many packets pass
    /// after the oldest answer, not on all of them.
    /// </summary>
    public void CountPacket()
    {
        if (Count > 0)
        {
            _packets++;
        }
    }

    /// <summary>
    /// Hands over the buffer with the encoded answers. The caller owns it and returns it to the pool, through
    /// <c>SendAsync</c>; this type holds nothing afterwards, and the next <see cref="Add"/> rents a fresh buffer.
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
        byte[] buffer = _buffer!;
        length = _length;
        _buffer = null;
        _length = 0;
        _packets = 0;
        Count = 0;
        return buffer;
    }

    /// <summary>
    /// Returns the buffer if one is held, and forgets the answers in it. Does nothing when none is held, so it can
    /// be called again, and from the end of the read loop whatever the way out.
    /// </summary>
    public void Release()
    {
        byte[]? buffer = _buffer;
        _buffer = null;
        _length = 0;
        _packets = 0;
        Count = 0;
        if (buffer is not null)
        {
            pool.Return(array: buffer);
        }
    }
}
