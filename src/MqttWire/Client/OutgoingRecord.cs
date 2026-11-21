namespace MqttWire.Client;

/// <summary>
/// One request sent to the server and not yet acknowledged. Instances are pooled by <see cref="OutgoingRecords"/>:
/// a record must not be used after it has been removed.
/// </summary>
internal sealed class OutgoingRecord
{
    /// <summary>
    /// Gets the packet identifier of the request.
    /// </summary>
    public ushort PacketIdentifier { get; internal set; }

    /// <summary>
    /// Gets what the request is waiting for.
    /// </summary>
    public RecordKind Kind { get; internal set; }

    /// <summary>
    /// Gets the <see cref="TimeProvider.GetTimestamp"/> value at which the current phase began.
    /// </summary>
    public long PhaseStartedAt { get; internal set; }

    /// <summary>
    /// Gets the completion the caller awaits; it is never <see langword="null"/> while the record is in use.
    /// </summary>
    public RequestCompletion Completion { get; internal set; } = null!; // Set by OutgoingRecords.TryAdd before the record is handed out.
}
