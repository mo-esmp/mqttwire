namespace MqttWire.Server.Internal;

/// <summary>
/// The kind and phase of one QoS 1 or 2 exchange of a connection (design 6.6, "QoS 1 and 2 inside a connection"):
/// what the exchange waits for.
/// </summary>
internal enum ExchangePhase : byte
{
    /// <summary>
    /// A publish from the client at QoS 2, accepted and answered with PUBREC, waiting for the client's PUBREL.
    /// </summary>
    IncomingAwaitingPubRel,

    /// <summary>
    /// A delivery to the client at QoS 1, waiting for the client's PUBACK.
    /// </summary>
    DeliveryAwaitingPubAck,

    /// <summary>
    /// A delivery to the client at QoS 2, waiting for the client's PUBREC.
    /// </summary>
    DeliveryAwaitingPubRec,

    /// <summary>
    /// A delivery to the client at QoS 2, answered with PUBREL, waiting for the client's PUBCOMP.
    /// </summary>
    DeliveryAwaitingPubComp,
}
