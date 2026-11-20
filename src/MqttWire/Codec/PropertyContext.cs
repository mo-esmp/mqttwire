namespace MqttWire.Codec;

/// <summary>
/// The kind of packet or property set a block belongs to; decides which properties are allowed in it.
/// </summary>
/// <summary>
/// The kind of packet or property set a block belongs to; decides which properties are allowed in it.
/// </summary>
[Flags]
internal enum PropertyContext : ushort
{
    /// <summary>
    /// CONNECT.
    /// </summary>
    /// <summary>
    /// CONNECT.
    /// </summary>
    Connect = 1,

    /// <summary>
    /// The will properties of CONNECT.
    /// </summary>
    /// <summary>
    /// The will properties of CONNECT.
    /// </summary>
    Will = 2,

    /// <summary>
    /// CONNACK.
    /// </summary>
    /// <summary>
    /// CONNACK.
    /// </summary>
    ConnAck = 4,

    /// <summary>
    /// PUBLISH.
    /// </summary>
    /// <summary>
    /// PUBLISH.
    /// </summary>
    Publish = 8,

    /// <summary>
    /// PUBACK, PUBREC, PUBREL and PUBCOMP.
    /// </summary>
    /// <summary>
    /// PUBACK, PUBREC, PUBREL and PUBCOMP.
    /// </summary>
    Ack = 16,

    /// <summary>
    /// SUBSCRIBE.
    /// </summary>
    /// <summary>
    /// SUBSCRIBE.
    /// </summary>
    Subscribe = 32,

    /// <summary>
    /// SUBACK.
    /// </summary>
    /// <summary>
    /// SUBACK.
    /// </summary>
    SubAck = 64,

    /// <summary>
    /// UNSUBSCRIBE.
    /// </summary>
    /// <summary>
    /// UNSUBSCRIBE.
    /// </summary>
    Unsubscribe = 128,

    /// <summary>
    /// UNSUBACK.
    /// </summary>
    /// <summary>
    /// UNSUBACK.
    /// </summary>
    UnsubAck = 256,

    /// <summary>
    /// DISCONNECT.
    /// </summary>
    /// <summary>
    /// DISCONNECT.
    /// </summary>
    Disconnect = 512,

    /// <summary>
    /// AUTH.
    /// </summary>
    /// <summary>
    /// AUTH.
    /// </summary>
    Auth = 1024,
}
