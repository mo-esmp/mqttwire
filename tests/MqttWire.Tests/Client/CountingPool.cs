using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace MqttWire.Tests.Client;

/// <summary>
/// An <see cref="ArrayPool{T}"/> over the shared pool that counts what is out and refuses a return it did not
/// expect, so a test can prove that every rented buffer comes back exactly once.
/// </summary>
internal sealed class CountingPool : ArrayPool<byte>
{
    private readonly Lock _gate = new();
    private readonly HashSet<byte[]> _rented = [];
    private int _largestRent;

    /// <summary>
    /// Gets the number of buffers rented and not yet returned.
    /// </summary>
    public int Outstanding
    {
        get
        {
            lock (_gate)
            {
                return _rented.Count;
            }
        }
    }

    /// <summary>
    /// Gets the largest minimum length any rent asked for; 0 before the first rent.
    /// </summary>
    public int LargestRent
    {
        get
        {
            lock (_gate)
            {
                return _largestRent;
            }
        }
    }

    /// <summary>
    /// Tells whether a span lies inside an array this pool has rented and not had returned: the span is a view
    /// into a pooled buffer, not a copy.
    /// </summary>
    /// <param name="span">
    /// The span to look for.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the span's first byte is inside a buffer that is out.
    /// </returns>
    public bool IsViewIntoRentedBuffer(ReadOnlySpan<byte> span)
    {
        ref byte start = ref MemoryMarshal.GetReference(span);
        lock (_gate)
        {
            foreach (byte[] array in _rented)
            {
                nint offset = Unsafe.ByteOffset(ref MemoryMarshal.GetArrayDataReference(array), ref start);
                if (offset >= 0 && offset < array.Length)
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Rents a buffer from the shared pool and records it.
    /// </summary>
    /// <param name="minimumLength">
    /// The smallest length the buffer must have.
    /// </param>
    /// <returns>
    /// A buffer at least <paramref name="minimumLength"/> long.
    /// </returns>
    public override byte[] Rent(int minimumLength)
    {
        byte[] array = Shared.Rent(minimumLength);
        lock (_gate)
        {
            _rented.Add(array);
            _largestRent = Math.Max(_largestRent, minimumLength);
        }

        return array;
    }

    /// <summary>
    /// Takes a rented buffer back and hands it to the shared pool.
    /// </summary>
    /// <param name="array">
    /// The buffer.
    /// </param>
    /// <param name="clearArray">
    /// Whether the shared pool clears it.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// The buffer was not rented from this pool, or was already returned.
    /// </exception>
    public override void Return(byte[] array, bool clearArray = false)
    {
        lock (_gate)
        {
            if (!_rented.Remove(array))
            {
                throw new InvalidOperationException("The buffer was not rented from this pool, or was returned twice.");
            }
        }

        Shared.Return(array, clearArray);
    }
}
