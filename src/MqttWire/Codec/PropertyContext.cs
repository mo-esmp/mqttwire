namespace MqttWire.Codec;

/// <summary>
/// The kind of packet or property set a block belongs to; decides which properties are allowed in it.
/// </summary>
[Flags]
internal enum PropertyContext : ushort
{
    /// <summary>
    /// No context: a property allowed or repeatable in none.
    /// </summary>
    None = 0,

    /// <summary>
    /// CONNECT.
    /// </summary>
    Connect = 1,

    /// <summary>
    /// The will properties of CONNECT.
    /// </summary>
    Will = 2,

    /// <summary>
    /// CONNACK.
    /// </summary>
    ConnAck = 4,

    /// <summary>
    /// PUBLISH.
    /// </summary>
    Publish = 8,

    /// <summary>
    /// PUBACK, PUBREC, PUBREL and PUBCOMP.
    /// </summary>
    Ack = 16,

    /// <summary>
    /// SUBSCRIBE.
    /// </summary>
    Subscribe = 32,

    /// <summary>
    /// SUBACK.
    /// </summary>
    SubAck = 64,

    /// <summary>
    /// UNSUBSCRIBE.
    /// </summary>
    Unsubscribe = 128,

    /// <summary>
    /// UNSUBACK.
    /// </summary>
    UnsubAck = 256,

    /// <summary>
    /// DISCONNECT.
    /// </summary>
    Disconnect = 512,

    /// <summary>
    /// AUTH.
    /// </summary>
    Auth = 1024,
}
