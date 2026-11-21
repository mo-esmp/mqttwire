namespace MqttWire;

/// <summary>
/// What the client knows about the end of a connection.
/// </summary>
/// <param name="Reason">
/// Why the connection ended.
/// </param>
/// <param name="ReasonCode">
/// The code of the DISCONNECT received (<see cref="MqttClientCloseReason.ServerDisconnected"/>) or sent
/// (<see cref="MqttClientCloseReason.ProtocolError"/>, <see cref="MqttClientCloseReason.MessageHandlerThrew"/>
/// and <see cref="MqttClientCloseReason.Disposed"/>); otherwise <see cref="MqttReasonCode.Success"/>.
/// </param>
/// <param name="ReasonString">
/// The reason string of the DISCONNECT received, or <see langword="null"/>.
/// </param>
/// <param name="Exception">
/// The exception behind the end, such as a failed read or write or the one the message handler threw, or
/// <see langword="null"/>.
/// </param>
public readonly record struct MqttClientCloseInfo(
    MqttClientCloseReason Reason,
    MqttReasonCode ReasonCode,
    string? ReasonString,
    Exception? Exception);
