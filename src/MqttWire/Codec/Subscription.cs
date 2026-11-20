using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// One subscription of a SUBSCRIBE: a topic filter and its options. The filter is a view of the packet's bytes.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly ref struct Subscription
{
    /// <summary>
    /// The topic filter as UTF-8, checked to be well-formed but not to be a valid topic filter (see
    /// <see cref="Topics.IsValidFilter"/>); the server answers an invalid one in the SUBACK.
    /// </summary>
    public ReadOnlySpan<byte> TopicFilter { get; init; }

    /// <summary>
    /// The maximum QoS the subscriber accepts, 0 to 2.
    /// </summary>
    public MqttQoS MaximumQoS { get; init; }

    /// <summary>
    /// Whether the server must not forward the subscriber's own publications to it; never set on a shared
    /// subscription.
    /// </summary>
    public bool NoLocal { get; init; }

    /// <summary>
    /// Whether forwarded messages keep the retain flag they were published with.
    /// </summary>
    public bool RetainAsPublished { get; init; }

    /// <summary>
    /// When retained messages are sent: 0 at subscribe, 1 at subscribe if the subscription is new, 2 never.
    /// </summary>
    public byte RetainHandling { get; init; }
}
