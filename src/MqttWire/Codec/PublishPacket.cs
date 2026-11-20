using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// A decoded PUBLISH packet: views over the bytes it was decoded from, so it is only valid while they are.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly ref struct PublishPacket
{
    /// <summary>
    /// The quality of service of the delivery.
    /// </summary>
    public MqttQoS QoS { get; init; }

    /// <summary>
    /// Whether this is a re-delivery of an earlier attempt; the flag as the sender set it.
    /// </summary>
    public bool Dup { get; init; }

    /// <summary>
    /// Whether the message is to be retained.
    /// </summary>
    public bool Retain { get; init; }

    /// <summary>
    /// The topic name as UTF-8, checked to be well-formed and free of wildcards. It is empty only when
    /// <see cref="HasTopicAlias"/> is <see langword="true"/>.
    /// </summary>
    public ReadOnlySpan<byte> Topic { get; init; }

    /// <summary>
    /// The packet identifier; 0 at QoS 0, where the packet has none.
    /// </summary>
    public ushort PacketIdentifier { get; init; }

    /// <summary>
    /// The validated PUBLISH properties.
    /// </summary>
    public PropertyBlock Properties { get; init; }

    /// <summary>
    /// The application message, which may be empty.
    /// </summary>
    public ReadOnlySpan<byte> Payload { get; init; }

    /// <summary>
    /// Whether the properties hold a Topic Alias, found once while decoding. The decoder reports it and does not
    /// resolve it.
    /// </summary>
    public bool HasTopicAlias { get; init; }
}
