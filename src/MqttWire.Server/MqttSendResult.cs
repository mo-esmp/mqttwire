namespace MqttWire.Server;

/// <summary>
/// What became of a message handed to <see cref="MqttWireConnection.TrySend"/>.
/// </summary>
public enum MqttSendResult
{
    /// <summary>
    /// The server took the message: accepted, not delivered. It is written when the socket allows, and dropped if
    /// the connection ends first.
    /// </summary>
    Taken,

    /// <summary>
    /// The connection is not open: it has not opened yet, or it is closing or has closed. Nothing changed.
    /// </summary>
    NotOpen,

    /// <summary>
    /// The message is larger than the client accepts, or than the server can hold for the connection; it was not
    /// taken, and the connection is being closed with 0x95 (packet too large).
    /// </summary>
    TooLarge,

    /// <summary>
    /// The connection was just evicted because it could not keep up; the message was not taken, and the connection
    /// is being closed with 0x97 (quota exceeded).
    /// </summary>
    Evicted,
}
