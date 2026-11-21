namespace MqttWire;

/// <summary>
/// Why a client connection ended.
/// </summary>
public enum MqttClientCloseReason : byte
{
    /// <summary>
    /// No reason recorded: the zero value, which only a default <see cref="MqttClientCloseInfo"/> carries. The
    /// client never reports it.
    /// </summary>
    None,

    /// <summary>
    /// The application disposed the client.
    /// </summary>
    Disposed,

    /// <summary>
    /// The server sent DISCONNECT.
    /// </summary>
    ServerDisconnected,

    /// <summary>
    /// The link ended or a read failed.
    /// </summary>
    ConnectionLost,

    /// <summary>
    /// A write failed.
    /// </summary>
    WriteFailed,

    /// <summary>
    /// The server could not be reached.
    /// </summary>
    Unreachable,

    /// <summary>
    /// No CONNACK arrived in time.
    /// </summary>
    ConnectTimedOut,

    /// <summary>
    /// The server refused the connection in its CONNACK.
    /// </summary>
    ConnectRefused,

    /// <summary>
    /// The server sent a packet the client cannot accept, or set a Maximum Packet Size too small for a packet the
    /// connection needs, such as an acknowledgement; the client ended the connection.
    /// </summary>
    ProtocolError,

    /// <summary>
    /// The server did not answer a PINGREQ in time.
    /// </summary>
    KeepAliveTimedOut,

    /// <summary>
    /// The server left a request unacknowledged for too long.
    /// </summary>
    AcknowledgementTimedOut,

    /// <summary>
    /// The application's message handler threw.
    /// </summary>
    MessageHandlerThrew,
}
