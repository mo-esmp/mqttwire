namespace MqttWire.Server.Internal;

/// <summary>
/// The records of one connection's QoS 1 and 2 exchanges (design 6.6, plan decision 13): two arrays of 16-byte
/// <see cref="ExchangeRecord"/>s, one for the client's QoS 2 publishes waiting for PUBREL and one for the server's
/// deliveries, each searched linearly and holding at most <see cref="ServerLimits.ReceiveMaximum"/>. A connection
/// takes one from the server's pool with its first exchange and gives it back with the last (plan decision 12), so
/// an idle connection holds none. It is not thread-safe: the connection's lock guards it. The arrays grow by
/// doubling and keep their storage while the object is pooled; a record holds no reference, so a cleared object
/// keeps nothing alive. The packet identifier counter of the deliveries is not here but the owner's, passed to
/// <see cref="AddDelivery"/>, so that it outlives the pooled object and is never carried to another owner.
/// </summary>
internal sealed class ExchangeRecords
{
    /// <summary>
    /// The number of records an array starts with when its first record is added.
    /// </summary>
    private const int InitialCapacity = 16;

    /// <summary>
    /// The most records each array holds.
    /// </summary>
    private const int MaxRecords = ServerLimits.ReceiveMaximum;

    /// <summary>
    /// The records of the client's QoS 2 publishes, in no order; the first <see cref="IncomingCount"/> are in use.
    /// </summary>
    private ExchangeRecord[] _incoming = [];

    /// <summary>
    /// The records of the server's deliveries, in no order; the first <see cref="DeliveryCount"/> are in use.
    /// </summary>
    private ExchangeRecord[] _deliveries = [];

    /// <summary>
    /// Gets the number of the client's QoS 2 publishes that wait for their PUBREL.
    /// </summary>
    public int IncomingCount { get; private set; }

    /// <summary>
    /// Gets the number of the server's deliveries that wait for an acknowledgement.
    /// </summary>
    public int DeliveryCount { get; private set; }

    /// <summary>
    /// Gets a value indicating whether no record is held.
    /// </summary>
    public bool IsEmpty => IncomingCount == 0 && DeliveryCount == 0;

    /// <summary>
    /// Gets the earliest <see cref="ExchangeRecord.EnteredAt"/> of every record, in both arrays; 0 when no record is
    /// held. It looks at every record, at most 512.
    /// </summary>
    public long OldestEnteredAt
    {
        get
        {
            long oldest = long.MaxValue;
            foreach (ExchangeRecord record in _incoming.AsSpan(start: 0, IncomingCount))
            {
                oldest = Math.Min(oldest, record.EnteredAt);
            }

            foreach (ExchangeRecord record in _deliveries.AsSpan(start: 0, DeliveryCount))
            {
                oldest = Math.Min(oldest, record.EnteredAt);
            }

            return oldest == long.MaxValue ? 0 : oldest;
        }
    }

    /// <summary>
    /// Tells whether a QoS 2 publish of the client with this identifier is remembered.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when it waits for its PUBREL; otherwise <see langword="false"/>.
    /// </returns>
    public bool ContainsIncoming(ushort packetIdentifier) => IndexOf(_incoming, IncomingCount, packetIdentifier) >= 0;

    /// <summary>
    /// Remembers a QoS 2 publish of the client, waiting for its PUBREL. The caller has made sure the identifier is
    /// not remembered already.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier, from 1 to 65,535.
    /// </param>
    /// <param name="now">
    /// The time, a timestamp of the server's clock.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// <see cref="ServerLimits.ReceiveMaximum"/> records are held already (a caller bug: the connection closes before
    /// that); nothing changes.
    /// </exception>
    public void AddIncoming(ushort packetIdentifier, long now)
    {
        _incoming = Add(_incoming, IncomingCount);
        _incoming[IncomingCount++] = new ExchangeRecord
        {
            PacketIdentifier = packetIdentifier, Phase = ExchangePhase.IncomingAwaitingPubRel, EnteredAt = now,
        };
    }

    /// <summary>
    /// Forgets a QoS 2 publish of the client.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when it was remembered and is forgotten now; <see langword="false"/> when it was not
    /// remembered, and nothing changed.
    /// </returns>
    public bool RemoveIncoming(ushort packetIdentifier)
    {
        int index = IndexOf(_incoming, IncomingCount, packetIdentifier);
        if (index < 0)
        {
            return false;
        }

        _incoming[index] = _incoming[--IncomingCount];
        return true;
    }

    /// <summary>
    /// Finds the record of a delivery by its identifier.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier.
    /// </param>
    /// <param name="index">
    /// The record's index for <see cref="DeliveryAt"/> and <see cref="RemoveDeliveryAt"/> when the result is
    /// <see langword="true"/>, valid until the next call that adds or removes a record; otherwise -1.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the delivery has a record; otherwise <see langword="false"/>.
    /// </returns>
    public bool TryFindDelivery(ushort packetIdentifier, out int index)
    {
        index = IndexOf(_deliveries, DeliveryCount, packetIdentifier);
        return index >= 0;
    }

    /// <summary>
    /// Gets the record of a delivery, to read or change in place.
    /// </summary>
    /// <param name="index">
    /// The index <see cref="TryFindDelivery"/> gave.
    /// </param>
    /// <returns>
    /// A reference to the record, valid until the next call that adds or removes a record.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The index is not that of a record (a caller bug); nothing changes.
    /// </exception>
    public ref ExchangeRecord DeliveryAt(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, DeliveryCount);
        return ref _deliveries[index];
    }

    /// <summary>
    /// Adds the record of a delivery with the next free packet identifier after the owner's last one: the counter
    /// goes from 1 to 65,535 and wraps to 1, and skips the identifiers that have a delivery record.
    /// </summary>
    /// <param name="phase">
    /// The delivery's phase: <see cref="ExchangePhase.DeliveryAwaitingPubAck"/> or
    /// <see cref="ExchangePhase.DeliveryAwaitingPubRec"/>.
    /// </param>
    /// <param name="now">
    /// The time, a timestamp of the server's clock.
    /// </param>
    /// <param name="lastIdentifier">
    /// The owner's counter: the last identifier it handed out, or 0 before the first, so that the first is 1. Set
    /// to the identifier handed out; unchanged when this throws.
    /// </param>
    /// <returns>
    /// The delivery's packet identifier.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// <see cref="ServerLimits.ReceiveMaximum"/> records are held already (a caller bug: the client's window is full
    /// before that); nothing changes.
    /// </exception>
    public ushort AddDelivery(ExchangePhase phase, long now, ref ushort lastIdentifier)
    {
        _deliveries = Add(_deliveries, DeliveryCount);
        ushort identifier = Next(lastIdentifier);
        while (IndexOf(_deliveries, DeliveryCount, identifier) >= 0)
        {
            identifier = Next(identifier);
        }

        lastIdentifier = identifier;
        _deliveries[DeliveryCount++] = new ExchangeRecord
        {
            PacketIdentifier = identifier, Phase = phase, EnteredAt = now,
        };
        return identifier;
    }

    /// <summary>
    /// Removes the record of a delivery. The record that was last takes its index.
    /// </summary>
    /// <param name="index">
    /// The index <see cref="TryFindDelivery"/> gave.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The index is not that of a record (a caller bug); nothing changes.
    /// </exception>
    public void RemoveDeliveryAt(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, DeliveryCount);
        _deliveries[index] = _deliveries[--DeliveryCount];
    }

    /// <summary>
    /// Forgets every record; the arrays are kept.
    /// </summary>
    public void Clear()
    {
        IncomingCount = 0;
        DeliveryCount = 0;
    }

    /// <summary>
    /// Finds a record by its identifier among the first records of an array.
    /// </summary>
    /// <param name="records">
    /// The array.
    /// </param>
    /// <param name="count">
    /// The number of records in use.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// The record's index, or -1 when none has the identifier.
    /// </returns>
    private static int IndexOf(ExchangeRecord[] records, int count, ushort packetIdentifier)
    {
        for (int i = 0; i < count; i++)
        {
            if (records[i].PacketIdentifier == packetIdentifier)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Makes room for one more record in an array: the first record allocates <see cref="InitialCapacity"/>, a full
    /// array doubles, up to <see cref="MaxRecords"/>.
    /// </summary>
    /// <param name="records">
    /// The array.
    /// </param>
    /// <param name="count">
    /// The number of records in use.
    /// </param>
    /// <returns>
    /// The array to add to: the same one, or a larger copy.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The array holds <see cref="MaxRecords"/> records already.
    /// </exception>
    private static ExchangeRecord[] Add(ExchangeRecord[] records, int count)
    {
        if (count < records.Length)
        {
            return records;
        }

        if (count >= MaxRecords)
        {
            throw new InvalidOperationException("The exchange records are full.");
        }

        ExchangeRecord[] bigger = new ExchangeRecord[records.Length == 0 ? InitialCapacity : records.Length * 2];
        Array.Copy(records, bigger, count);
        return bigger;
    }

    /// <summary>
    /// Gets the identifier after one: 1 to 65,535, wrapping to 1; after 0, which stands for none yet, 1.
    /// </summary>
    /// <param name="identifier">
    /// The identifier.
    /// </param>
    /// <returns>
    /// The next one.
    /// </returns>
    private static ushort Next(ushort identifier) => identifier == ushort.MaxValue ? (ushort)1 : (ushort)(identifier + 1);
}
