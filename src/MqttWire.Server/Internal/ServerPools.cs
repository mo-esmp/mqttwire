using Microsoft.Extensions.ObjectPool;

namespace MqttWire.Server.Internal;

/// <summary>
/// The pools of the objects a busy connection needs, one set per server (plan decision 12): a connection takes an
/// object when it first needs it and gives it back, cleared, when it no longer does, so an idle connection holds
/// none and steady traffic allocates none. They are the queues of waiting deliveries, the exchange records, and the
/// objects that wait for a write that did not complete at once.
/// </summary>
internal sealed class ServerPools
{
    /// <summary>
    /// Creates the server's pools, each keeping at most <see cref="ServerLimits.MaxPooledObjects"/>.
    /// </summary>
    public ServerPools()
        : this(
            new DefaultObjectPool<WaitingDeliveries>(new WaitingDeliveriesPolicy(), ServerLimits.MaxPooledObjects),
            new DefaultObjectPool<ExchangeRecords>(new ExchangeRecordsPolicy(), ServerLimits.MaxPooledObjects))
    {
    }

    /// <summary>
    /// Creates the pools from the two given, with the server's own pool of pending writes, for the tests of a pool
    /// that fails.
    /// </summary>
    /// <param name="waiting">
    /// The pool of the queues of waiting deliveries.
    /// </param>
    /// <param name="records">
    /// The pool of the exchange records.
    /// </param>
    public ServerPools(ObjectPool<WaitingDeliveries> waiting, ObjectPool<ExchangeRecords> records)
        : this(waiting, records, CreatePendingWrites())
    {
    }

    /// <summary>
    /// Creates the pools from the three given, for the tests of a pool that fails or that counts.
    /// </summary>
    /// <param name="waiting">
    /// The pool of the queues of waiting deliveries.
    /// </param>
    /// <param name="records">
    /// The pool of the exchange records.
    /// </param>
    /// <param name="pendingWrites">
    /// The pool of the objects that wait for a write that did not complete at once.
    /// </param>
    public ServerPools(ObjectPool<WaitingDeliveries> waiting, ObjectPool<ExchangeRecords> records,
        ObjectPool<PendingWrite> pendingWrites)
    {
        Waiting = waiting;
        Records = records;
        PendingWrites = pendingWrites;
    }

    /// <summary>
    /// Gets the pool of the queues of waiting deliveries; by default it keeps at most
    /// <see cref="ServerLimits.MaxPooledObjects"/>, a queue that comes back holds no delivery, and one grown past
    /// <see cref="ServerLimits.MaxPooledQueueCapacity"/> slots is not kept.
    /// </summary>
    public ObjectPool<WaitingDeliveries> Waiting { get; }

    /// <summary>
    /// Gets the pool of the exchange records; by default it keeps at most <see cref="ServerLimits.MaxPooledObjects"/>,
    /// and an object that comes back holds no record.
    /// </summary>
    public ObjectPool<ExchangeRecords> Records { get; }

    /// <summary>
    /// Gets the pool of the objects that wait for a write that did not complete at once; by default it keeps at most
    /// <see cref="ServerLimits.MaxPooledObjects"/>, and an object that comes back awaits nothing and holds no
    /// reference to a connection or a buffer. A connection takes one only while a write of its own is pending, and
    /// the object is back before the next write begins, so a server holds as many as it has writes pending at one
    /// moment.
    /// </summary>
    public ObjectPool<PendingWrite> PendingWrites { get; }

    /// <summary>
    /// Creates the server's own pool of pending writes, which keeps at most
    /// <see cref="ServerLimits.MaxPooledObjects"/>.
    /// </summary>
    /// <returns>
    /// The pool.
    /// </returns>
    public static ObjectPool<PendingWrite> CreatePendingWrites() =>
        new DefaultObjectPool<PendingWrite>(new PendingWritePolicy(), ServerLimits.MaxPooledObjects);

    /// <summary>
    /// Creates the objects that wait for a pending write, and keeps only those that come back idle.
    /// </summary>
    private sealed class PendingWritePolicy : IPooledObjectPolicy<PendingWrite>
    {
        /// <summary>
        /// Creates an object that awaits nothing.
        /// </summary>
        /// <returns>
        /// The object.
        /// </returns>
        public PendingWrite Create() => new();

        /// <summary>
        /// Takes back an object, which cleared itself when its write ended.
        /// </summary>
        /// <param name="obj">
        /// The object.
        /// </param>
        /// <returns>
        /// <see langword="true"/> when the pool keeps the object, which awaits nothing; <see langword="false"/> for
        /// one that still awaits a write, which is then left to that write and never handed out again.
        /// </returns>
        public bool Return(PendingWrite obj) => obj.IsIdle;
    }

    /// <summary>
    /// Creates the exchange records and clears each object that comes back.
    /// </summary>
    private sealed class ExchangeRecordsPolicy : IPooledObjectPolicy<ExchangeRecords>
    {
        /// <summary>
        /// Creates an object that holds no record.
        /// </summary>
        /// <returns>
        /// The records.
        /// </returns>
        public ExchangeRecords Create() => new();

        /// <summary>
        /// Clears an object that comes back, so that the next connection starts with no record and its identifier
        /// counter at 1.
        /// </summary>
        /// <param name="obj">
        /// The records.
        /// </param>
        /// <returns>
        /// <see langword="true"/>, always: no object is refused.
        /// </returns>
        public bool Return(ExchangeRecords obj)
        {
            obj.Clear();
            return true;
        }
    }

    /// <summary>
    /// Creates the queues of waiting deliveries, clears each one that comes back, and keeps only those that stayed
    /// small; internal for its tests.
    /// </summary>
    internal sealed class WaitingDeliveriesPolicy : IPooledObjectPolicy<WaitingDeliveries>
    {
        /// <summary>
        /// Creates an empty queue.
        /// </summary>
        /// <returns>
        /// The queue.
        /// </returns>
        public WaitingDeliveries Create() => new();

        /// <summary>
        /// Clears a queue that comes back, so that it keeps no reference to a host's topic or payload, and keeps it
        /// only while its storage is at most <see cref="ServerLimits.MaxPooledQueueCapacity"/> slots.
        /// </summary>
        /// <param name="obj">
        /// The queue.
        /// </param>
        /// <returns>
        /// <see langword="true"/> when the pool keeps the queue; <see langword="false"/> when a burst grew it past
        /// <see cref="ServerLimits.MaxPooledQueueCapacity"/> slots, so that it is dropped, cleared, to the garbage
        /// collector.
        /// </returns>
        public bool Return(WaitingDeliveries obj)
        {
            obj.Clear();
            return obj.Capacity <= ServerLimits.MaxPooledQueueCapacity;
        }
    }
}
