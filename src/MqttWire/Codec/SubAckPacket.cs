using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// A decoded SUBACK packet: views over the bytes it was decoded from, so it is only valid while they are.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly ref struct SubAckPacket
{
    /// <summary>
    /// The identifier of the SUBSCRIBE this answers; never 0 in a decoded packet.
    /// </summary>
    public ushort PacketIdentifier { get; init; }

    /// <summary>
    /// The validated SUBACK properties.
    /// </summary>
    public PropertyBlock Properties { get; init; }

    /// <summary>
    /// One reason code per filter of the SUBSCRIBE, at least one, as sent: each is a <see cref="MqttReasonCode"/>
    /// value, not checked against what a SUBACK may carry.
    /// </summary>
    public ReadOnlySpan<byte> ReasonCodes { get; init; }
}
