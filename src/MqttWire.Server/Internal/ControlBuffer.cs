using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace MqttWire.Server.Internal;

/// <summary>
/// The control packets of one connection that wait behind a write in progress, already encoded, one behind the
/// other in the order they were produced (plan decision 9). It holds a rented array only while packets wait, so an
/// idle connection holds none. It is a mutable struct held in a field of the connection, never copied while it is
/// in use, and not thread-safe: the connection's lock guards it. How many packets and bytes may wait is the
/// connection's limit; this type only never rents more than <see cref="ServerLimits.MaxEncodingBufferBytes"/>.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal struct ControlBuffer
{
    /// <summary>
    /// The largest array ever rented, in bytes: one encoding buffer.
    /// </summary>
    private const int LargestArray = ServerLimits.MaxEncodingBufferBytes;

    /// <summary>
    /// The rented array, or <see langword="null"/> when no packet waits.
    /// </summary>
    private byte[]? _array;

    /// <summary>
    /// Gets the number of packets that wait.
    /// </summary>
    public int PacketCount { get; private set; }

    /// <summary>
    /// Gets the bytes of the packets that wait, together.
    /// </summary>
    public int ByteCount { get; private set; }

    /// <summary>
    /// Gets a value indicating whether a PINGRESP is among the packets that wait.
    /// </summary>
    public bool HasPingResp { get; private set; }

    /// <summary>
    /// Gets the number of waiting packets that are the last answer to a QoS 1 or 2 publish of the client: PUBACK,
    /// PUBCOMP success, or a failing PUBREC. They count against the receive maximum the server announced until they
    /// are handed to the link (<see cref="Detach"/>) or dropped (<see cref="Release"/>).
    /// </summary>
    public int PublishAnswerCount { get; private set; }

    /// <summary>
    /// Makes room for one more packet behind those that wait and hands the room out. The first packet rents the
    /// array; when the array is too small it grows by renting the next size, copying the packets that wait and
    /// returning the old array. Nothing waits more until <see cref="Commit"/>.
    /// </summary>
    /// <param name="pool">
    /// The pool the arrays come from and go back to.
    /// </param>
    /// <param name="size">
    /// The packet's size in bytes; with <see cref="ByteCount"/> at most
    /// <see cref="ServerLimits.MaxEncodingBufferBytes"/>, which the caller has checked.
    /// </param>
    /// <returns>
    /// Exactly <paramref name="size"/> bytes to write the packet into, valid until the next call that changes the
    /// buffer.
    /// </returns>
    public Span<byte> Reserve(ArrayPool<byte> pool, int size)
    {
        int needed = ByteCount + size;
        Debug.Assert(needed <= LargestArray, "The caller keeps the control buffer within 64 KiB.");
        if (_array is null)
        {
            _array = pool.Rent(needed);
        }
        else if (_array.Length < needed)
        {
            byte[] bigger = pool.Rent(Math.Min(Math.Max(needed, _array.Length * 2), LargestArray));
            _array.AsSpan(start: 0, ByteCount).CopyTo(bigger);
            byte[] old = _array;
            _array = bigger;
            pool.Return(old);
        }

        return _array.AsSpan(ByteCount, size);
    }

    /// <summary>
    /// Counts the packet written into the room <see cref="Reserve"/> handed out as waiting.
    /// </summary>
    /// <param name="size">
    /// The packet's size in bytes, as reserved.
    /// </param>
    /// <param name="isPingResp">
    /// Whether the packet is a PINGRESP.
    /// </param>
    /// <param name="isPublishAnswer">
    /// Whether the packet is the last answer to a QoS 1 or 2 publish of the client, counted in
    /// <see cref="PublishAnswerCount"/>.
    /// </param>
    public void Commit(int size, bool isPingResp, bool isPublishAnswer)
    {
        ByteCount += size;
        PacketCount++;
        HasPingResp |= isPingResp;
        PublishAnswerCount += isPublishAnswer ? 1 : 0;
    }

    /// <summary>
    /// Hands the array with every waiting packet to the writer and forgets it: the buffer is empty afterwards, and
    /// the array belongs to the write, whose completion returns it (design 6.8).
    /// </summary>
    /// <param name="length">
    /// The bytes of the packets in the array.
    /// </param>
    /// <returns>
    /// The rented array.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// No packet waits (a caller bug); nothing changes.
    /// </exception>
    public byte[] Detach(out int length)
    {
        if (_array is null || PacketCount == 0)
        {
            throw new InvalidOperationException("No control packet waits.");
        }

        byte[] array = _array;
        length = ByteCount;
        this = default;
        return array;
    }

    /// <summary>
    /// Returns an array that was never handed to the link and forgets it with every packet in it. Calling it when
    /// no array is held does nothing.
    /// </summary>
    /// <param name="pool">
    /// The pool the array came from.
    /// </param>
    public void Release(ArrayPool<byte> pool)
    {
        byte[]? array = _array;
        this = default;
        if (array is not null)
        {
            pool.Return(array);
        }
    }
}
