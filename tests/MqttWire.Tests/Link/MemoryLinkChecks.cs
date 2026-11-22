namespace MqttWire.Tests.Link;

/// <summary>
/// Counts, for all the <see cref="MemoryLink"/>s that share it, what a correct user of a link never does. A harness
/// gives one to every link it opens and asserts that the counts stay 0, without having to keep the links alive.
/// Safe to report to from several threads at once.
/// </summary>
internal sealed class MemoryLinkChecks
{
    /// <summary>
    /// The number of aborts issued after the run over the link had ended.
    /// </summary>
    private int _abortsAfterEnd;

    /// <summary>
    /// Gets the number of calls of <see cref="MemoryLink.Abort"/> made after the link's
    /// <see cref="MemoryLink.Run"/> had completed: over a WebSocket such an abort throws.
    /// </summary>
    public int AbortsAfterEnd => Volatile.Read(ref _abortsAfterEnd);

    /// <summary>
    /// Counts an abort issued after the run over the link had ended.
    /// </summary>
    public void NoteAbortAfterEnd() => Interlocked.Increment(ref _abortsAfterEnd);
}
