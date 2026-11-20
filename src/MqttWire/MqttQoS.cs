namespace MqttWire;

/// <summary>
/// The MQTT quality of service levels.
/// </summary>
public enum MqttQoS : byte
{
    /// <summary>
    /// The message is delivered at most once, with no acknowledgement.
    /// </summary>
    AtMostOnce = 0,

    /// <summary>
    /// The message is delivered at least once, and may be delivered again.
    /// </summary>
    AtLeastOnce = 1,

    /// <summary>
    /// The message is delivered exactly once.
    /// </summary>
    ExactlyOnce = 2,
}
