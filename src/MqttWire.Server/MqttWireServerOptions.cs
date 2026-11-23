namespace MqttWire.Server;

/// <summary>
/// The options of the MQTT server. An invalid value throws
/// <see cref="Microsoft.Extensions.Options.OptionsValidationException"/> when the server is first resolved (in a
/// <c>WebApplication</c>, that is the call to <c>MapMqttWire</c>) or, at the latest, when the host starts, which then
/// does not start.
/// </summary>
public sealed class MqttWireServerOptions
{
    /// <summary>
    /// Gets or sets the highest quality of service the server accepts from clients and uses towards them.
    /// <see cref="MqttQoS.AtLeastOnce"/> or <see cref="MqttQoS.ExactlyOnce"/>; the default is
    /// <see cref="MqttQoS.ExactlyOnce"/>. A subscription granted above it, and a message sent above it, are lowered
    /// to it; a client that publishes above it is disconnected with 0x9B (QoS not supported).
    /// </summary>
    public MqttQoS MaxQoS { get; set; } = MqttQoS.ExactlyOnce;

    /// <summary>
    /// Gets or sets the largest payload of a PUBLISH packet the server accepts, in bytes. From 1 to 16,777,216; the
    /// default is 65,536. A publish with a larger payload is refused with 0x97 (quota exceeded), in the form its QoS
    /// calls for (nothing at QoS 0), without the host being called, and the connection stays open; only a PUBLISH
    /// packet of more than 16 times this limit ends the connection, with 0x95 (packet too large).
    /// </summary>
    public int MaxPublishPayloadBytes { get; set; } = 65_536;

    /// <summary>
    /// Gets or sets the most messages that may wait to be sent to one client. At least 1; the default is 1,000. A
    /// client that falls further behind is disconnected with 0x97 (quota exceeded).
    /// </summary>
    public int MaxOutgoingMessages { get; set; } = 1_000;

    /// <summary>
    /// Gets or sets the most bytes that may wait to be sent to one client. At least 65,536; the default is
    /// 1,048,576. What is counted is what the server keeps alive for the client on the way out: for every message it
    /// has taken and not yet written, the UTF-8 bytes of the topic and the bytes of the payload (for each message,
    /// also when several share one topic or one payload), and the bytes of the acknowledgements and other control
    /// packets that wait. A client that falls further behind is disconnected with 0x97 (quota exceeded). A single
    /// message whose topic and payload are together larger is never sent: <see cref="MqttWireConnection.TrySend"/>
    /// answers <see cref="MqttSendResult.TooLarge"/> and the connection is closed with 0x95 (packet too large).
    /// </summary>
    public int MaxOutgoingBytes { get; set; } = 1_048_576;
}
