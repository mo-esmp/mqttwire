namespace MqttWire.Codec;

/// <summary>
/// What a server writes into a CONNACK. Session present is always written as 0, and a <see langword="null"/> field
/// writes no property.
/// </summary>
internal readonly ref struct ConnAckFields
{
    /// <summary>
    /// The reason code.
    /// </summary>
    public MqttReasonCode ReasonCode { get; init; }

    /// <summary>
    /// The Session Expiry Interval property in seconds, or <see langword="null"/> to write none.
    /// </summary>
    public uint? SessionExpiryInterval { get; init; }

    /// <summary>
    /// The Receive Maximum property, or <see langword="null"/> to write none. The standard forbids 0, so the encoder
    /// refuses it.
    /// </summary>
    public ushort? ReceiveMaximum { get; init; }

    /// <summary>
    /// The Maximum QoS property, or <see langword="null"/> to write none. Only 0 and 1 can be carried, so the encoder
    /// refuses anything above 1.
    /// </summary>
    public byte? MaximumQoS { get; init; }

    /// <summary>
    /// The Subscription Identifier Available property, or <see langword="null"/> to write none.
    /// </summary>
    public bool? SubscriptionIdentifiersAvailable { get; init; }

    /// <summary>
    /// The Server Keep Alive property in seconds, or <see langword="null"/> to write none.
    /// </summary>
    public ushort? ServerKeepAlive { get; init; }

    /// <summary>
    /// The Reason String property, or <see langword="null"/> to write none.
    /// </summary>
    public string? ReasonString { get; init; }

    /// <summary>
    /// The user properties, written last; empty writes none.
    /// </summary>
    public ReadOnlySpan<MqttWireUserProperty> UserProperties { get; init; }
}
