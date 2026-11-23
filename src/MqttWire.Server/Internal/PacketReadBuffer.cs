using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace MqttWire.Server.Internal;

/// <summary>
/// A connection's read buffer: an array rented when bytes arrive and returned when they are handled, so an idle
/// connection holds none. It is a mutable struct held in a field of the connection, never copied; only the code
/// that reads uses it, so it is not thread-safe. Modelled on the client's <c>ReceiveBuffer</c>, with the sizes left
/// to the caller, because the server's limits depend on the packet's type.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal struct PacketReadBuffer
{
    /// <summary>
    /// The rented array, or <see langword="null"/> when none is held.
    /// </summary>
    private byte[]? _array;

    /// <summary>
    /// Where the bytes not yet consumed start in <see cref="_array"/>.
    /// </summary>
    private int _start;

    /// <summary>
    /// Where the bytes read end in <see cref="_array"/>; the free space starts here.
    /// </summary>
    private int _end;

    /// <summary>
    /// Gets a value indicating whether an array is held.
    /// </summary>
    public readonly bool IsHeld => _array is not null;

    /// <summary>
    /// Gets the number of bytes read and not yet consumed.
    /// </summary>
    public readonly int Count => _end - _start;

    /// <summary>
    /// Gets the length of the array held; 0 when none is held.
    /// </summary>
    public readonly int Capacity => _array?.Length ?? 0;

    /// <summary>
    /// Gets the bytes read and not yet consumed.
    /// </summary>
    /// <returns>
    /// A view valid until the next call that changes the buffer; empty when no array is held.
    /// </returns>
    public readonly ReadOnlySpan<byte> GetData() => _array.AsSpan(_start, Count);

    /// <summary>
    /// Rents the array, which must not be held yet.
    /// </summary>
    /// <param name="pool">
    /// The pool to rent from.
    /// </param>
    /// <param name="size">
    /// The smallest length the array must have.
    /// </param>
    public void Rent(ArrayPool<byte> pool, int size)
    {
        Debug.Assert(_array is null, "The read buffer is already held.");
        _array = pool.Rent(size);
        _start = 0;
        _end = 0;
    }

    /// <summary>
    /// Gets the free space after the bytes read, for the next read.
    /// </summary>
    /// <returns>
    /// The memory the next read fills. It belongs to that read until the read has ended (design 6.8).
    /// </returns>
    public readonly Memory<byte> GetReadMemory() => _array.AsMemory(_end);

    /// <summary>
    /// Counts bytes a read placed in the memory from <see cref="GetReadMemory"/>.
    /// </summary>
    /// <param name="count">
    /// The number of bytes read.
    /// </param>
    public void Advance(int count) => _end += count;

    /// <summary>
    /// Replaces the array with a larger one, keeping the bytes not yet consumed at its start: a new array is
    /// rented, the bytes are copied, and the old array is returned. The caller must not have a read in progress
    /// into the old array.
    /// </summary>
    /// <param name="pool">
    /// The pool the arrays come from and go back to.
    /// </param>
    /// <param name="size">
    /// The smallest length the new array must have.
    /// </param>
    /// <param name="clear">
    /// Whether every byte read into the old array is cleared before the array goes back: <see langword="true"/> for
    /// the buffer of a CONNECT, whose password the pool would otherwise hand to its next renter;
    /// <see langword="false"/> on the read loop of an Active connection, a hot path.
    /// </param>
    public void Grow(ArrayPool<byte> pool, int size, bool clear)
    {
        Debug.Assert(_array is not null, "Only a held read buffer grows.");
        byte[] bigger = pool.Rent(size);
        int count = Count;
        GetData().CopyTo(bigger);
        byte[]? old = _array;
        int read = _end;
        _array = bigger;
        _start = 0;
        _end = count;
        if (old is not null)
        {
            if (clear)
            {
                // Every read into it has ended and was counted, so nothing lies beyond what was read.
                old.AsSpan(start: 0, read).Clear();
            }

            pool.Return(old);
        }
    }

    /// <summary>
    /// Moves the bytes not yet consumed to the start of the array, so the free space after them is as large as the
    /// array allows. The caller must not have a read in progress into the array.
    /// </summary>
    public void Compact()
    {
        if (_start == 0)
        {
            return;
        }

        int count = Count;
        _array.AsSpan(_start, count).CopyTo(_array);
        _start = 0;
        _end = count;
    }

    /// <summary>
    /// Drops a handled packet from the start of the bytes not yet consumed.
    /// </summary>
    /// <param name="length">
    /// The packet's total length.
    /// </param>
    public void Consume(int length) => _start += length;

    /// <summary>
    /// Drops a handled packet from the start of the bytes not yet consumed, as <see cref="Consume"/> does, and
    /// clears its bytes in the array first: for a CONNECT, so that its password is in the array no longer than the
    /// decoding needs it, whoever sees the array next. The bytes behind the packet are left as they are.
    /// </summary>
    /// <param name="length">
    /// The packet's total length.
    /// </param>
    public void ConsumeCleared(int length)
    {
        _array.AsSpan(_start, length).Clear();
        _start += length;
    }

    /// <summary>
    /// Returns the array, if one is held, and forgets it with every byte in it, so no reference to it remains. The
    /// caller must not have a read in progress into it. Calling it when no array is held does nothing.
    /// </summary>
    /// <param name="pool">
    /// The pool the array came from.
    /// </param>
    public void Return(ArrayPool<byte> pool)
    {
        byte[]? array = _array;
        _array = null;
        _start = 0;
        _end = 0;
        if (array is not null)
        {
            pool.Return(array);
        }
    }

    /// <summary>
    /// Returns the array as <see cref="Return"/> does, cleared first, all of it: for the buffer of a connect phase
    /// that ends without an Active connection, which may hold part of a CONNECT. The whole array is cleared, not
    /// only what was counted as read, because a read that ended with an error (the link was aborted at a deadline)
    /// may have placed bytes it never reported.
    /// </summary>
    /// <param name="pool">
    /// The pool the array came from.
    /// </param>
    public void ReturnCleared(ArrayPool<byte> pool)
    {
        _array?.AsSpan().Clear();
        Return(pool);
    }
}
