using System.Buffers;
using System.Runtime.InteropServices;

namespace MqttWire.Tests.Client;

/// <summary>
/// An <see cref="ArrayPool{T}"/> that makes memory used after its return visible at once (6.13 item 4): it fills
/// every returned array with <see cref="Filler"/> and hands it out again on the next <see cref="Rent"/> of a size
/// it fits, the most recently returned first. It counts what is out and refuses a return it did not expect.
/// </summary>
internal sealed class OverwritingPool : ArrayPool<byte>
{
    /// <summary>
    /// The byte every returned array is filled with.
    /// </summary>
    public const byte Filler = 0xEE;

    /// <summary>
    /// Guards <see cref="_free"/>, <see cref="_rented"/> and <see cref="_faultyReturns"/>.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// The returned arrays, oldest first, waiting to be handed out again.
    /// </summary>
    private readonly List<byte[]> _free = [];

    /// <summary>
    /// The arrays rented and not yet returned, compared by reference.
    /// </summary>
    private readonly HashSet<byte[]> _rented = [];

    /// <summary>
    /// The number of returns refused.
    /// </summary>
    private int _faultyReturns;

    /// <summary>
    /// Gets or sets a callback that sees each array as it comes back, before it is overwritten and while it is still
    /// counted as rented, so a test can act at the very moment of a return; or <see langword="null"/>.
    /// </summary>
    public Action<byte[]>? Returning { get; set; }

    /// <summary>
    /// Gets the number of arrays rented and not yet returned.
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
    /// Gets the number of returns this pool refused: an array returned twice, or one it never rented.
    /// </summary>
    public int FaultyReturns
    {
        get
        {
            lock (_gate)
            {
                return _faultyReturns;
            }
        }
    }

    /// <summary>
    /// Tells whether memory lies in an array that is rented from this pool and not yet returned.
    /// </summary>
    /// <param name="memory">
    /// The memory to look for.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the memory's array is out; <see langword="false"/> when it was returned, was
    /// never rented here, or the memory is not over an array.
    /// </returns>
    public bool IsRented(ReadOnlyMemory<byte> memory)
    {
        if (!MemoryMarshal.TryGetArray(memory, out ArraySegment<byte> segment) || segment.Array is null)
        {
            return false;
        }

        lock (_gate)
        {
            return _rented.Contains(segment.Array);
        }
    }

    /// <summary>
    /// Hands out the most recently returned array that is long enough, or a new one when none is.
    /// </summary>
    /// <param name="minimumLength">
    /// The smallest length the array must have.
    /// </param>
    /// <returns>
    /// An array at least <paramref name="minimumLength"/> long; a reused one is full of <see cref="Filler"/>.
    /// </returns>
    public override byte[] Rent(int minimumLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);
        lock (_gate)
        {
            byte[]? array = null;
            for (int i = _free.Count - 1; i >= 0; i--)
            {
                if (_free[i].Length >= minimumLength)
                {
                    array = _free[i];
                    _free.RemoveAt(i);
                    break;
                }
            }

            array ??= new byte[minimumLength];
            _rented.Add(array);
            return array;
        }
    }

    /// <summary>
    /// Takes an array back, fills it with <see cref="Filler"/> and keeps it for the next fitting rent.
    /// </summary>
    /// <param name="array">
    /// The array.
    /// </param>
    /// <param name="clearArray">
    /// Ignored: every returned array is overwritten.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// The array was not rented from this pool, or was already returned; it is counted in
    /// <see cref="FaultyReturns"/> and not kept.
    /// </exception>
    public override void Return(byte[] array, bool clearArray = false)
    {
        ArgumentNullException.ThrowIfNull(array);
        Returning?.Invoke(array);
        lock (_gate)
        {
            if (!_rented.Remove(array))
            {
                _faultyReturns++;
                throw new InvalidOperationException("The array was not rented from this pool, or was returned twice.");
            }

            array.AsSpan().Fill(Filler);
            _free.Add(array);
        }
    }
}
