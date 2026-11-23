namespace MqttWire.Tests.Client;

/// <summary>
/// What a test pool tells about the buffers it lent, so that a harness can assert the same bounds whichever pool the
/// code under test rents from.
/// </summary>
internal interface IPoolCounts
{
    /// <summary>
    /// Gets the number of buffers rented and not yet returned.
    /// </summary>
    int Outstanding { get; }

    /// <summary>
    /// Gets the number of returns the pool refused: a buffer returned twice, or one it never rented.
    /// </summary>
    int FaultyReturns { get; }

    /// <summary>
    /// Gets the largest minimum length any rent asked for; 0 before the first rent. It is what the renter asked
    /// for, not the length of the array it was handed, which a pool may round up.
    /// </summary>
    int LargestRent { get; }
}
