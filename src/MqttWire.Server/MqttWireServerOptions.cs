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
    /// Gets or sets the highest quality of service the server grants. <see cref="MqttQoS.AtLeastOnce"/> or
    /// <see cref="MqttQoS.ExactlyOnce"/>; the default is <see cref="MqttQoS.ExactlyOnce"/>.
    /// </summary>
    public MqttQoS MaxQoS { get; set; } = MqttQoS.ExactlyOnce;

    /// <summary>
    /// Gets or sets the largest payload of a PUBLISH packet the server accepts, in bytes. From 1 to 16,777,216; the
    /// default is 65,536. A larger packet ends the connection.
    /// </summary>
    public int MaxPublishPayloadBytes { get; set; } = 65_536;

    /// <summary>
    /// Gets or sets the most messages that may wait to be sent to one client. At least 1; the default is 1,000. A
    /// client that falls further behind is disconnected.
    /// </summary>
    public int MaxOutgoingMessages { get; set; } = 1_000;

    /// <summary>
    /// Gets or sets the most bytes that may wait to be sent to one client. At least 65,536; the default is
    /// 1,048,576. A client that falls further behind is disconnected.
    /// </summary>
    public int MaxOutgoingBytes { get; set; } = 1_048_576;
}
