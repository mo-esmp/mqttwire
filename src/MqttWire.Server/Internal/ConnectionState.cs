namespace MqttWire.Server.Internal;

/// <summary>
/// The state of a server connection (design 6.6, "States"). A connection moves forward only, and enters
/// <see cref="Closing"/> exactly once.
/// </summary>
internal enum ConnectionState : byte
{
    /// <summary>
    /// The WebSocket is accepted and the connection waits for a complete CONNECT; it may hold one read buffer.
    /// </summary>
    Accepted,

    /// <summary>
    /// The CONNECT is valid so far and the host's <see cref="IMqttWireHandler.OnConnectAsync"/> is deciding.
    /// </summary>
    Authenticating,

    /// <summary>
    /// The admission step accepted the connection; MQTT traffic flows.
    /// </summary>
    Active,

    /// <summary>
    /// The decision to end was made; no new work is accepted.
    /// </summary>
    Closing,

    /// <summary>
    /// The run has ended and no write is in progress; the connection holds nothing.
    /// </summary>
    Closed,
}
