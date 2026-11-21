using System.Diagnostics.CodeAnalysis;

namespace MqttWire.Client;

/// <summary>
/// The requests sent to the server and not yet acknowledged, keyed by packet identifier, with the pools of
/// records and completions. Not thread-safe except for the completion pool: the client guards the table with
/// its state lock.
/// </summary>
/// <param name="publishLimit">
/// The most QoS 1 and 2 publishes in flight: the smaller of the server's Receive Maximum and 1,024.
/// </param>
internal sealed class OutgoingRecords(int publishLimit)
{
    /// <summary>
    /// The records in use, by packet identifier.
    /// </summary>
    private readonly Dictionary<ushort, OutgoingRecord> _records = [];

    /// <summary>
    /// Records that were removed and wait for reuse.
    /// </summary>
    private readonly Stack<OutgoingRecord> _recordPool = [];

    /// <summary>
    /// Completions whose result was read and wait for reuse; guarded by <see cref="_completionGate"/>.
    /// </summary>
    private readonly Stack<RequestCompletion> _completionPool = [];

    /// <summary>
    /// Guards the completion pool, which is touched from the caller's thread.
    /// </summary>
    private readonly Lock _completionGate = new();

    /// <summary>
    /// The identifier given last; 0 before the first.
    /// </summary>
    private ushort _lastIdentifier;

    /// <summary>
    /// Gets the number of QoS 1 and 2 publishes that have not been acknowledged.
    /// </summary>
    public int PublishesInFlight { get; private set; }

    /// <summary>
    /// Gets the phase start of the oldest record, or <see langword="null"/> when there is none.
    /// </summary>
    public long? OldestPhaseStartedAt
    {
        get
        {
            long? oldest = null;
#pragma warning disable S3267 // LINQ is banned on the hot paths.
            foreach (OutgoingRecord record in _records.Values)
#pragma warning restore S3267
            {
                if (oldest is null || record.PhaseStartedAt < oldest)
                {
                    oldest = record.PhaseStartedAt;
                }
            }

            return oldest;
        }
    }

    /// <summary>
    /// Takes a completion from this table's pool, creating one when the pool is empty. Safe to call from any
    /// thread.
    /// </summary>
    /// <returns>
    /// A completion ready to be given to <see cref="TryAdd"/>.
    /// </returns>
    public RequestCompletion RentCompletion()
    {
        lock (_completionGate)
        {
            if (_completionPool.TryPop(out RequestCompletion? completion))
            {
                return completion;
            }
        }

        return new RequestCompletion(this);
    }

    /// <summary>
    /// Returns a completion to the pool. Called by the completion itself when its result has been read.
    /// </summary>
    /// <param name="completion">
    /// The completion to keep for reuse.
    /// </param>
    internal void ReturnCompletion(RequestCompletion completion)
    {
        lock (_completionGate)
        {
            _completionPool.Push(completion);
        }
    }

    /// <summary>
    /// Adds a record under the next free packet identifier. Nothing changes when it fails.
    /// </summary>
    /// <param name="kind">
    /// What the request waits for.
    /// </param>
    /// <param name="now">
    /// The current <see cref="TimeProvider.GetTimestamp"/> value.
    /// </param>
    /// <param name="completion">
    /// The completion the caller awaits.
    /// </param>
    /// <param name="record">
    /// The new record when the call succeeds.
    /// </param>
    /// <param name="failure">
    /// <see cref="MqttRequestStatus.ReceiveMaximumReached"/> when a publish would pass the limit, or
    /// <see cref="MqttRequestStatus.NoFreePacketIdentifier"/> when all 65,535 identifiers are in use.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the record was added.
    /// </returns>
    public bool TryAdd(
        RecordKind kind,
        long now,
        RequestCompletion completion,
        [NotNullWhen(true)] out OutgoingRecord? record,
        out MqttRequestStatus failure)
    {
        record = null;
        failure = MqttRequestStatus.Completed;
        bool isPublish = kind >= RecordKind.PublishQoS1;
        if (isPublish && PublishesInFlight >= publishLimit)
        {
            failure = MqttRequestStatus.ReceiveMaximumReached;
            return false;
        }

        if (_records.Count >= ushort.MaxValue)
        {
            failure = MqttRequestStatus.NoFreePacketIdentifier;
            return false;
        }

        ushort identifier = _lastIdentifier;
        do
        {
            identifier = identifier == ushort.MaxValue ? (ushort)1 : (ushort)(identifier + 1);
        }
        while (_records.ContainsKey(identifier));

        _lastIdentifier = identifier;
        if (!_recordPool.TryPop(out record))
        {
            record = new OutgoingRecord();
        }

        record.PacketIdentifier = identifier;
        record.Kind = kind;
        record.PhaseStartedAt = now;
        record.Completion = completion;
        _records.Add(identifier, record);
        if (isPublish)
        {
            PublishesInFlight++;
        }

        return true;
    }

    /// <summary>
    /// Finds the record of a packet identifier.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The identifier an acknowledgement carries.
    /// </param>
    /// <param name="record">
    /// The record when found.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when no request is waiting under that identifier.
    /// </returns>
    public bool TryGet(ushort packetIdentifier, [NotNullWhen(true)] out OutgoingRecord? record)
        => _records.TryGetValue(packetIdentifier, out record);

    /// <summary>
    /// Moves a record to its next phase, for example from waiting for PUBREC to waiting for PUBCOMP. The
    /// record still counts against the publish limit.
    /// </summary>
    /// <param name="record">
    /// A record of this table.
    /// </param>
    /// <param name="next">
    /// What it now waits for.
    /// </param>
    /// <param name="now">
    /// The current <see cref="TimeProvider.GetTimestamp"/> value.
    /// </param>
#pragma warning disable S2325 // Kept an instance method: the client advances records through the table that owns them.
    public void Advance(OutgoingRecord record, RecordKind next, long now)
    {
        record.Kind = next;
        record.PhaseStartedAt = now;
    }
#pragma warning restore S2325

    /// <summary>
    /// Removes a record, frees its identifier and publish slot, and keeps the record for reuse. The caller
    /// reads the record's completion before calling this and must not touch the record afterwards. Removing a
    /// record that is not in the table does nothing.
    /// </summary>
    /// <param name="record">
    /// A record of this table.
    /// </param>
    public void Remove(OutgoingRecord record)
    {
        if (_records.Remove(record.PacketIdentifier))
        {
            Release(record);
        }
    }

    /// <summary>
    /// Hands every record to a callback, then empties the table. Used once, when the connection ends. The
    /// callback must not change the table.
    /// </summary>
    /// <param name="each">
    /// Called for each record before it is recycled.
    /// </param>
    public void DrainAll(Action<OutgoingRecord> each)
    {
        foreach (OutgoingRecord record in _records.Values)
        {
            each(record);
            Release(record);
        }

        _records.Clear();
    }

    /// <summary>
    /// Updates the publish count and puts a record back in the pool.
    /// </summary>
    /// <param name="record">
    /// The record leaving the table.
    /// </param>
    private void Release(OutgoingRecord record)
    {
        if (record.Kind >= RecordKind.PublishQoS1)
        {
            PublishesInFlight--;
        }

        record.Completion = null!; // The record is idle in the pool; TryAdd sets it again before it is handed out.
        _recordPool.Push(record);
    }
}
