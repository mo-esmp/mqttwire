namespace MqttWire.Client;

/// <summary>
/// What an outgoing record is waiting for.
/// </summary>
internal enum RecordKind : byte
{
    /// <summary>
    /// A SUBSCRIBE waiting for its SUBACK.
    /// </summary>
    Subscribe,

    /// <summary>
    /// An UNSUBSCRIBE waiting for its UNSUBACK.
    /// </summary>
    Unsubscribe,

    /// <summary>
    /// A QoS 1 PUBLISH waiting for its PUBACK.
    /// </summary>
    PublishQoS1,

    /// <summary>
    /// A QoS 2 PUBLISH waiting for its PUBREC.
    /// </summary>
    PublishQoS2AwaitingPubRec,

    /// <summary>
    /// A QoS 2 PUBLISH whose PUBREC came and whose PUBREL was sent, waiting for the PUBCOMP.
    /// </summary>
    PublishQoS2AwaitingPubComp,
}
