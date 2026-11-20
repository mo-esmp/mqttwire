namespace MqttWire.Codec;

[Flags]
internal enum PropertyContext : ushort
{
    Connect = 1,
    Will = 2,
    ConnAck = 4,
    Publish = 8,

    // PUBACK, PUBREC, PUBREL and PUBCOMP.
    Ack = 16,
    Subscribe = 32,
    SubAck = 64,
    Unsubscribe = 128,
    UnsubAck = 256,
    Disconnect = 512,
    Auth = 1024,
}
