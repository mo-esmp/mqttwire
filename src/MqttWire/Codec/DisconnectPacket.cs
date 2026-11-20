using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// A decoded DISCONNECT packet: views over the bytes it was decoded from, so it is only valid while they are.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly ref struct DisconnectPacket
{
    /// <summary>
    /// The reason code as sent; <see cref="MqttReasonCode.Success"/> when the packet is in its short form. Its value
    /// is not checked against the sender.
    /// </summary>
    public MqttReasonCode ReasonCode { get; init; }

    /// <summary>
    /// The validated DISCONNECT properties; empty in the short forms.
    /// </summary>
    public PropertyBlock Properties { get; init; }
}
