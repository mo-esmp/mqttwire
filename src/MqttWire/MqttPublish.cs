using System.Runtime.InteropServices;
using System.Text;

namespace MqttWire;

/// <summary>
/// A received message: a view over the packet's buffer, valid only while the handler that received it runs.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public readonly ref struct MqttPublish
{
    /// <summary>
    /// Creates a view of a message.
    /// </summary>
    /// <param name="topic">
    /// The topic, as validated UTF-8.
    /// </param>
    /// <param name="payload">
    /// The payload.
    /// </param>
    /// <param name="qos">
    /// The QoS of the message.
    /// </param>
    /// <param name="retain">
    /// Whether the message is retained.
    /// </param>
    internal MqttPublish(ReadOnlySpan<byte> topic, ReadOnlySpan<byte> payload, MqttQoS qos, bool retain)
    {
        Topic = topic;
        Payload = payload;
        QoS = qos;
        Retain = retain;
    }

    /// <summary>
    /// Gets the topic as UTF-8 bytes.
    /// </summary>
    public ReadOnlySpan<byte> Topic { get; }

    /// <summary>
    /// Gets the payload.
    /// </summary>
    public ReadOnlySpan<byte> Payload { get; }

    /// <summary>
    /// Gets the QoS the message was delivered with.
    /// </summary>
    public MqttQoS QoS { get; }

    /// <summary>
    /// Gets a value indicating whether the message is retained.
    /// </summary>
    public bool Retain { get; }

    /// <summary>
    /// Gets the topic as a string. Allocates; prefer <see cref="Topic"/> on hot paths.
    /// </summary>
    /// <returns>
    /// The topic.
    /// </returns>
    public string GetTopic() => Encoding.UTF8.GetString(bytes: Topic);
}
