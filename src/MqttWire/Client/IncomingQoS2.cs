namespace MqttWire.Client;

/// <summary>
/// The packet identifiers of QoS 2 messages the client has handed on and the server has not yet released with
/// PUBREL. Not thread-safe: only the read loop uses it.
/// </summary>
internal sealed class IncomingQoS2
{
    /// <summary>
    /// The identifiers in use, in the first <see cref="Count"/> slots; allocated on the first QoS 2 delivery.
    /// </summary>
    private ushort[]? _identifiers;

    /// <summary>
    /// Gets the number of identifiers remembered.
    /// </summary>
    public int Count { get; private set; }

    /// <summary>
    /// Gets a value saying whether <see cref="ClientLimits.MaxIncomingQoS2"/> identifiers are remembered, so that
    /// no new one can be added.
    /// </summary>
    public bool IsFull => Count >= ClientLimits.MaxIncomingQoS2;

    /// <summary>
    /// Tells whether an identifier is remembered.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The identifier.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when it was added and not removed.
    /// </returns>
    public bool Contains(ushort packetIdentifier)
        => _identifiers is not null && Array.IndexOf(_identifiers, packetIdentifier, 0, Count) >= 0;

    /// <summary>
    /// Remembers an identifier.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The identifier.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when it was added; <see langword="false"/> when it was already remembered, or when
    /// <see cref="IsFull"/> is true, and nothing changed.
    /// </returns>
    public bool TryAdd(ushort packetIdentifier)
    {
        if (IsFull || Contains(packetIdentifier))
        {
            return false;
        }

        _identifiers ??= new ushort[ClientLimits.MaxIncomingQoS2];
        _identifiers[Count++] = packetIdentifier;
        return true;
    }

    /// <summary>
    /// Forgets an identifier.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The identifier.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when it was remembered; <see langword="false"/> when it was not, and nothing changed.
    /// </returns>
    public bool Remove(ushort packetIdentifier)
    {
        if (_identifiers is null)
        {
            return false;
        }

        int index = Array.IndexOf(_identifiers, packetIdentifier, 0, Count);
        if (index < 0)
        {
            return false;
        }

        _identifiers[index] = _identifiers[--Count];
        return true;
    }
}
