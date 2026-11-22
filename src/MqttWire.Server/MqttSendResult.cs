namespace MqttWire.Server;

/// <summary>
/// What became of a message handed to <see cref="MqttWireConnection.TrySend"/>.
/// </summary>
public enum MqttSendResult
{
    /// <summary>
    /// The server took the message and will deliver it.
    /// </summary>
    Taken,

    /// <summary>
    /// The connection is not open: it has not opened yet, or it has closed.
    /// </summary>
    NotOpen,

    /// <summary>
    /// The message is larger than the client accepts, or than the server can hold for the connection; it was not
    /// taken.
    /// </summary>
    TooLarge,

    /// <summary>
    /// The connection was just evicted because it could not keep up; the message was not taken.
    /// </summary>
    Evicted,
}
