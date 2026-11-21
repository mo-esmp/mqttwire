namespace MqttWire;

/// <summary>
/// How a publish, subscribe or unsubscribe request ended.
/// </summary>
public enum MqttRequestStatus : byte
{
    /// <summary>
    /// The exchange completed; the reason code is the server's.
    /// </summary>
    Completed = 0,

    /// <summary>
    /// The client is not connected; nothing was sent.
    /// </summary>
    NotConnected,

    /// <summary>
    /// The connection ended before the exchange completed. The server may or may not have received the request.
    /// </summary>
    ConnectionLost,

    /// <summary>
    /// The server's Receive Maximum of unacknowledged messages is reached; nothing was sent.
    /// </summary>
    ReceiveMaximumReached,

    /// <summary>
    /// The packet is larger than the server accepts; nothing was sent.
    /// </summary>
    PacketTooLarge,

    /// <summary>
    /// The server does not support the requested QoS; nothing was sent.
    /// </summary>
    QoSNotSupported,

    /// <summary>
    /// All 65,535 packet identifiers are in use; nothing was sent.
    /// </summary>
    NoFreePacketIdentifier,

    /// <summary>
    /// The publishes already queued and not yet written reach the client's limit of 1 MiB, because the link is
    /// slower than the caller; nothing was sent. Trying again once earlier writes have ended may succeed.
    /// </summary>
    SendQueueFull,
}
