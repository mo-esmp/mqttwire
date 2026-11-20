using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// A decoded SUBSCRIBE packet: views over the bytes it was decoded from, so it is only valid while they are.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly ref struct SubscribePacket
{
    /// <summary>
    /// The entries, already validated by <see cref="SubscribeCodec.TryDecode"/>: for each, a length-prefixed topic
    /// filter and the options byte.
    /// </summary>
    private readonly ReadOnlySpan<byte> _entries;

    /// <summary>
    /// Creates a packet over validated entries.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier.
    /// </param>
    /// <param name="properties">
    /// The validated properties.
    /// </param>
    /// <param name="entries">
    /// The validated entries.
    /// </param>
    /// <param name="count">
    /// The number of entries.
    /// </param>
    public SubscribePacket(ushort packetIdentifier, PropertyBlock properties, ReadOnlySpan<byte> entries, int count)
    {
        PacketIdentifier = packetIdentifier;
        Properties = properties;
        _entries = entries;
        Count = count;
    }

    /// <summary>
    /// The packet identifier; never 0 in a decoded packet.
    /// </summary>
    public ushort PacketIdentifier { get; }

    /// <summary>
    /// The validated SUBSCRIBE properties. A Subscription Identifier is allowed here; the server refuses it with
    /// <see cref="MqttReasonCode.SubscriptionIdentifiersNotSupported"/>, so it is only reported.
    /// </summary>
    public PropertyBlock Properties { get; }

    /// <summary>
    /// The number of subscriptions, at least 1.
    /// </summary>
    public int Count { get; }

    /// <summary>
    /// Gets an enumerator over the subscriptions, which cannot fail.
    /// </summary>
    /// <returns>
    /// The enumerator.
    /// </returns>
    public SubscriptionEnumerator GetSubscriptions() => new(_entries);
}
