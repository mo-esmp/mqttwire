namespace MqttWire.Client;

/// <summary>
/// The fixed limits of the client.
/// </summary>
internal static class ClientLimits
{
    /// <summary>
    /// The Receive Maximum the client announces: the most unacknowledged QoS 1 and 2 messages the server may send.
    /// </summary>
    public const int ReceiveMaximum = 1024;

    /// <summary>
    /// The most QoS 1 and 2 publishes the client has in flight, whatever larger Receive Maximum the server allows.
    /// </summary>
    public const int MaxPublishesInFlight = 1024;

    /// <summary>
    /// The size of the first read buffer, in bytes.
    /// </summary>
    public const int FirstReadBufferBytes = 4096;

    /// <summary>
    /// The most incoming QoS 2 packet identifiers remembered between PUBLISH and PUBREL.
    /// </summary>
    public const int MaxIncomingQoS2 = 1024;

    /// <summary>
    /// The smallest allowed <c>MaxIncomingPacketBytes</c>.
    /// </summary>
    public const int MinIncomingPacketBytes = 4096;

    /// <summary>
    /// The largest allowed <c>MaxIncomingPacketBytes</c>: the largest MQTT packet, 268,435,455 bytes of
    /// remaining length plus a five-byte fixed header.
    /// </summary>
    public const int MaxIncomingPacketBytes = 268_435_460;

    /// <summary>
    /// How long a connect may wait for the CONNACK.
    /// </summary>
    public static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(seconds: 10);

    /// <summary>
    /// How long a close may take before the link is aborted.
    /// </summary>
    public static readonly TimeSpan CloseTimeout = TimeSpan.FromSeconds(seconds: 2);
}
