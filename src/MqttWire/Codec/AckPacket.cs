using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// A decoded PUBACK, PUBREC, PUBREL or PUBCOMP packet: views over the bytes it was decoded from, so it is only
/// valid while they are.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly ref struct AckPacket
{
    /// <summary>
    /// Which of the four acknowledgements this is.
    /// </summary>
    public PacketType Type { get; init; }

    /// <summary>
    /// The identifier of the publish exchange; never 0 in a decoded packet.
    /// </summary>
    public ushort PacketIdentifier { get; init; }

    /// <summary>
    /// The reason code as sent; <see cref="MqttReasonCode.Success"/> when the packet is in its short form. Its value
    /// is not checked against the packet type.
    /// </summary>
    public MqttReasonCode ReasonCode { get; init; }

    /// <summary>
    /// The validated acknowledgement properties; empty in the short forms.
    /// </summary>
    public PropertyBlock Properties { get; init; }
}
