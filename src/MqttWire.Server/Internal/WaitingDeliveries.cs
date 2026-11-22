namespace MqttWire.Server.Internal;

/// <summary>
/// The deliveries of one connection that wait behind a write in progress or for room in the client's window, oldest
/// first: a ring of <see cref="PendingDelivery"/>. A connection takes one from the server's pool with its first
/// waiting delivery and gives it back with the last (plan decision 12), so an idle connection holds none. It is not
/// thread-safe: the connection's lock guards it. The ring grows by doubling and keeps its storage while it is
/// pooled; how many deliveries may wait is the connection's limit, not this type's.
/// </summary>
internal sealed class WaitingDeliveries
{
    /// <summary>
    /// The number of slots the ring starts with; always a power of two, so a position wraps with a mask.
    /// </summary>
    private const int InitialCapacity = 16;

    /// <summary>
    /// The slots; empty until the first delivery. Its length is 0 or a power of two.
    /// </summary>
    private PendingDelivery[] _items = [];

    /// <summary>
    /// The position of the oldest delivery in <see cref="_items"/>.
    /// </summary>
    private int _head;

    /// <summary>
    /// Gets the number of deliveries that wait.
    /// </summary>
    public int Count { get; private set; }

    /// <summary>
    /// Gets the payload bytes of the deliveries that wait, together.
    /// </summary>
    public long PayloadBytes { get; private set; }

    /// <summary>
    /// Adds a delivery behind those that wait.
    /// </summary>
    /// <param name="delivery">
    /// The delivery.
    /// </param>
    public void Enqueue(in PendingDelivery delivery)
    {
        if (Count == _items.Length)
        {
            Grow();
        }

        _items[(_head + Count) & (_items.Length - 1)] = delivery;
        Count++;
        PayloadBytes += delivery.Payload.Length;
    }

    /// <summary>
    /// Gets the oldest delivery without removing it.
    /// </summary>
    /// <returns>
    /// A reference valid until the next call that changes the ring.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// No delivery waits (a caller bug); nothing changes.
    /// </exception>
    public ref readonly PendingDelivery Peek()
    {
        ThrowIfEmpty();
        return ref _items[_head];
    }

    /// <summary>
    /// Removes the oldest delivery and drops its references to the topic and the payload.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// No delivery waits (a caller bug); nothing changes.
    /// </exception>
    public void Dequeue()
    {
        ThrowIfEmpty();
        ref PendingDelivery oldest = ref _items[_head];
        PayloadBytes -= oldest.Payload.Length;
        oldest = default;
        _head = (_head + 1) & (_items.Length - 1);
        Count--;
    }

    /// <summary>
    /// Removes every delivery and drops every payload and topic reference; the storage is kept.
    /// </summary>
    public void Clear()
    {
        int first = Math.Min(Count, _items.Length - _head);
        Array.Clear(_items, _head, first);
        Array.Clear(_items, index: 0, Count - first);
        _head = 0;
        Count = 0;
        PayloadBytes = 0;
    }

    /// <summary>
    /// Doubles the ring, moving the deliveries to the start of the new storage in order.
    /// </summary>
    private void Grow()
    {
        PendingDelivery[] bigger = new PendingDelivery[_items.Length == 0 ? InitialCapacity : _items.Length * 2];
        int first = Math.Min(Count, _items.Length - _head);
        Array.Copy(_items, _head, bigger, destinationIndex: 0, first);
        Array.Copy(_items, sourceIndex: 0, bigger, first, Count - first);
        _items = bigger;
        _head = 0;
    }

    /// <summary>
    /// Throws when no delivery waits.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// No delivery waits.
    /// </exception>
    private void ThrowIfEmpty()
    {
        if (Count == 0)
        {
            throw new InvalidOperationException("No delivery waits.");
        }
    }
}
