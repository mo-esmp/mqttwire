using Microsoft.Extensions.ObjectPool;

namespace MqttWire.Server.Internal;

/// <summary>
/// The pools of the objects a busy connection needs, one set per server (plan decision 12): a connection takes an
/// object when it first needs it and gives it back, cleared, when it no longer does, so an idle connection holds
/// none and steady traffic allocates none.
/// </summary>
internal sealed class ServerPools
{
    /// <summary>
    /// Gets the pool of the queues of waiting deliveries; it keeps at most
    /// <see cref="ServerLimits.MaxPooledObjects"/>, and a queue that comes back holds no delivery.
    /// </summary>
    public ObjectPool<WaitingDeliveries> Waiting { get; } =
        new DefaultObjectPool<WaitingDeliveries>(new WaitingDeliveriesPolicy(), ServerLimits.MaxPooledObjects);

    /// <summary>
    /// Creates the queues of waiting deliveries and clears each one that comes back.
    /// </summary>
    private sealed class WaitingDeliveriesPolicy : IPooledObjectPolicy<WaitingDeliveries>
    {
        /// <summary>
        /// Creates an empty queue.
        /// </summary>
        /// <returns>
        /// The queue.
        /// </returns>
        public WaitingDeliveries Create() => new();

        /// <summary>
        /// Clears a queue that comes back, so that it keeps no reference to a host's topic or payload.
        /// </summary>
        /// <param name="obj">
        /// The queue.
        /// </param>
        /// <returns>
        /// <see langword="true"/>, always: no queue is refused.
        /// </returns>
        public bool Return(WaitingDeliveries obj)
        {
            obj.Clear();
            return true;
        }
    }
}
