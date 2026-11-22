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
    public void Grow(ArrayPool<byte> pool, int size)
    {
        Debug.Assert(_array is not null, "Only a held read buffer grows.");
        byte[] bigger = pool.Rent(size);
        int count = Count;
        GetData().CopyTo(bigger);
        byte[]? old = _array;
        _array = bigger;
        _start = 0;
        _end = count;
        if (old is not null)
        {
            pool.Return(old);
        }
    }

    /// <summary>
    /// Drops a handled packet from the start of the bytes not yet consumed.
    /// </summary>
    /// <param name="length">
    /// The packet's total length.
    /// </param>
    public void Consume(int length) => _start += length;

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
}
