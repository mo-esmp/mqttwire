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
    /// The number of writes and closes begun while another had not ended.
    /// </summary>
    private int _overlappingWrites;

    /// <summary>
    /// The number of writes and closes begun after the link was aborted, the first on each link aside.
    /// </summary>
    private int _furtherWritesAfterAbort;

    /// <summary>
    /// Gets the number of writes and closes begun while another write or close had not ended
    /// (<see cref="MemoryLink.OverlappingWrites"/>), over every link that reports here.
    /// </summary>
    public int OverlappingWrites => Volatile.Read(ref _overlappingWrites);

    /// <summary>
    /// Gets the number of writes and closes begun after a link was aborted, the first on each link aside, over
    /// every link that reports here. The first may be the one write that the holder of the send path had decided on
    /// before another thread closed the connection and aborted the link: it is begun after the abort and fails.
    /// Nothing may follow it.
    /// </summary>
    public int FurtherWritesAfterAbort => Volatile.Read(ref _furtherWritesAfterAbort);

    /// <summary>
    /// Gets the number of calls of <see cref="MemoryLink.Abort"/> made after the link's
    /// <see cref="MemoryLink.Run"/> had completed: over a WebSocket such an abort throws.
    /// </summary>
    public int AbortsAfterEnd => Volatile.Read(ref _abortsAfterEnd);

    /// <summary>
    /// Counts a write or a close begun while another had not ended.
    /// </summary>
    public void NoteOverlappingWrite() => Interlocked.Increment(ref _overlappingWrites);

    /// <summary>
    /// Counts a write or a close begun after the link was aborted that is not the first on its link.
    /// </summary>
    public void NoteFurtherWriteAfterAbort() => Interlocked.Increment(ref _furtherWritesAfterAbort);

    /// <summary>
    /// Counts an abort issued after the run over the link had ended.
    /// </summary>
    public void NoteAbortAfterEnd() => Interlocked.Increment(ref _abortsAfterEnd);
}
