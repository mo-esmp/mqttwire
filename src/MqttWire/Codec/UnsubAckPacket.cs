using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// A decoded UNSUBACK packet: views over the bytes it was decoded from, so it is only valid while they are.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly ref struct UnsubAckPacket
{
    /// <summary>
    /// The identifier of the UNSUBSCRIBE this answers; never 0 in a decoded packet.
    /// </summary>
    public ushort PacketIdentifier { get; init; }

    /// <summary>
    /// The validated UNSUBACK properties.
    /// </summary>
    public PropertyBlock Properties { get; init; }

    /// <summary>
    /// One reason code per filter of the UNSUBSCRIBE, at least one, as sent: each is a <see cref="MqttReasonCode"/>
    /// value, not checked against what an UNSUBACK may carry.
    /// </summary>
    public ReadOnlySpan<byte> ReasonCodes { get; init; }
}
