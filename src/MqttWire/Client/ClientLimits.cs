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
    /// The most PUBLISH bytes queued for the send lock and not yet written, 1 MiB. A publish that would go over it
    /// fails with <c>SendQueueFull</c>, except that one publish is always taken when nothing is queued, so a packet
    /// larger than this can still be sent.
    /// </summary>
    public const long MaxPendingPublishBytes = 1_048_576;

    /// <summary>
    /// The most idle request completions kept for reuse. A caller that holds completed results before reading them
    /// can return many at once; those above this are left to the garbage collector.
    /// </summary>
    public const int MaxIdleCompletions = 32;

    /// <summary>
    /// The size of the first read buffer, in bytes.
    /// </summary>
    public const int FirstReadBufferBytes = 4096;

    /// <summary>
    /// The most incoming QoS 2 packet identifiers remembered between PUBLISH and PUBREL.
    /// </summary>
    public const int MaxIncomingQoS2 = 1024;

    /// <summary>
    /// The most answers the read loop keeps before it writes them. It writes before it handles the sixteenth packet
    /// after the one whose answer waits longest, so at most fifteen packets pass in between.
    /// </summary>
    public const int AnswerPacketBound = 16;

    /// <summary>
    /// The most bytes one answer takes: a PUBACK, PUBREC, PUBREL or PUBCOMP with a reason code and no properties.
    /// </summary>
    public const int MaxAnswerBytes = 5;

    /// <summary>
    /// The size of the buffer that holds the waiting answers, in bytes.
    /// </summary>
    public const int AnswerBufferBytes = AnswerPacketBound * MaxAnswerBytes;

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
