using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// A decoded UNSUBSCRIBE packet: views over the bytes it was decoded from, so it is only valid while they are.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly ref struct UnsubscribePacket
{
    /// <summary>
    /// The topic filters, already validated by <see cref="UnsubscribeCodec.TryDecode"/>: each length-prefixed.
    /// </summary>
    private readonly ReadOnlySpan<byte> _filters;

    /// <summary>
    /// Creates a packet over validated filters.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier.
    /// </param>
    /// <param name="properties">
    /// The validated properties.
    /// </param>
    /// <param name="filters">
    /// The validated filters.
    /// </param>
    /// <param name="count">
    /// The number of filters.
    /// </param>
    public UnsubscribePacket(ushort packetIdentifier, PropertyBlock properties, ReadOnlySpan<byte> filters, int count)
    {
        PacketIdentifier = packetIdentifier;
        Properties = properties;
        _filters = filters;
        Count = count;
    }

    /// <summary>
    /// The packet identifier; never 0 in a decoded packet.
    /// </summary>
    public ushort PacketIdentifier { get; }

    /// <summary>
    /// The validated UNSUBSCRIBE properties.
    /// </summary>
    public PropertyBlock Properties { get; }

    /// <summary>
    /// The number of topic filters, at least 1.
    /// </summary>
    public int Count { get; }

    /// <summary>
    /// Gets an enumerator over the topic filters, which cannot fail.
    /// </summary>
    /// <returns>
    /// The enumerator.
    /// </returns>
    public TopicFilterEnumerator GetTopicFilters() => new(_filters);
}
