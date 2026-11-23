using System.Buffers;
using MqttWire.Tests.Client;

namespace MqttWire.Tests.Server;

/// <summary>
/// A pool over a <see cref="CountingPool"/> that throws on one chosen rent, as a pool may that cannot allocate, or on
/// one chosen return, as a pool may that refuses a buffer.
/// </summary>
internal sealed class ThrowingPool : ArrayPool<byte>
{
    /// <summary>
    /// How many rents are left before the one that throws; 0 when none is to throw.
    /// </summary>
    private int _rentsLeft;

    /// <summary>
    /// How many returns are left before the one that throws; 0 when none is to throw.
    /// </summary>
    private int _returnsLeft;

    /// <summary>
    /// Gets the pool every rent and return goes to, which counts what is out.
    /// </summary>
    public CountingPool Inner { get; } = new();

    /// <summary>
    /// Gets the exception the chosen rent throws.
    /// </summary>
    public InvalidOperationException Thrown { get; } = new("The pool cannot rent.");

    /// <summary>
    /// Gets a value indicating whether the chosen rent has thrown.
    /// </summary>
    public bool HasThrown { get; private set; }

    /// <summary>
    /// Gets the exception the chosen return throws.
    /// </summary>
    public InvalidOperationException ThrownOnReturn { get; } = new("The pool refuses the buffer.");

    /// <summary>
    /// Gets a value indicating whether the chosen return has thrown.
    /// </summary>
    public bool HasThrownOnReturn { get; private set; }

    /// <summary>
    /// Chooses the rent that throws, counted from the next one; only that one throws.
    /// </summary>
    /// <param name="which">
    /// 1 for the next rent, 2 for the one after it, and so on.
    /// </param>
    public void ThrowOnRent(int which) => Volatile.Write(ref _rentsLeft, which);

    /// <summary>
    /// Rents from <see cref="Inner"/>, unless this is the chosen rent.
    /// </summary>
    /// <param name="minimumLength">
    /// The smallest length the buffer must have.
    /// </param>
    /// <returns>
    /// A buffer at least <paramref name="minimumLength"/> long.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// This is the chosen rent; nothing is rented.
    /// </exception>
    public override byte[] Rent(int minimumLength)
    {
        if (Volatile.Read(ref _rentsLeft) > 0 && Interlocked.Decrement(ref _rentsLeft) == 0)
        {
            HasThrown = true;
            throw Thrown;
        }

        return Inner.Rent(minimumLength);
    }

    /// <summary>
    /// Chooses the return that throws, counted from the next one; only that one throws, and its buffer is not taken
    /// back, as a pool may that refuses a buffer: it is left to the garbage collector.
    /// </summary>
    /// <param name="which">
    /// 1 for the next return, 2 for the one after it, and so on.
    /// </param>
    public void ThrowOnReturn(int which) => Volatile.Write(ref _returnsLeft, which);

    /// <summary>
    /// Returns a buffer to <see cref="Inner"/>, unless this is the chosen return.
    /// </summary>
    /// <param name="array">
    /// The buffer.
    /// </param>
    /// <param name="clearArray">
    /// Whether the buffer is cleared.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// This is the chosen return; the buffer is not taken back.
    /// </exception>
    public override void Return(byte[] array, bool clearArray = false)
    {
        if (Volatile.Read(ref _returnsLeft) > 0 && Interlocked.Decrement(ref _returnsLeft) == 0)
        {
            HasThrownOnReturn = true;
            throw ThrownOnReturn;
        }

        Inner.Return(array, clearArray);
    }
}
