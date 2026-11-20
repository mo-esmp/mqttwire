using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// A decoded CONNACK packet: views over the bytes it was decoded from, so it is only valid while they are.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly ref struct ConnAckPacket
{
    /// <summary>
    /// Whether the server holds a session for the client; the client closes the connection on it (MQTT 5, 3.2.2.1.1).
    /// </summary>
    public bool SessionPresent { get; init; }

    /// <summary>
    /// The reason code as the server sent it; its value is not checked against the packet type.
    /// </summary>
    public MqttReasonCode ReasonCode { get; init; }

    /// <summary>
    /// The validated CONNACK properties.
    /// </summary>
    public PropertyBlock Properties { get; init; }
}
