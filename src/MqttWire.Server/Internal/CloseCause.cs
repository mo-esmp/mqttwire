namespace MqttWire.Server.Internal;

/// <summary>
/// Why a connection ended, as the server records it for its log (design 6.12): every member of
/// <see cref="MqttCloseReason"/> with the same value, and four ends the public enum does not name. Three of them end
/// a connection that was never admitted, so no <see cref="IMqttWireHandler.OnClosed"/> reports them; the fourth is
/// reported as <see cref="MqttCloseReason.ConnectionLost"/>. It is narrowed to <see cref="MqttCloseReason"/> only
/// where <see cref="IMqttWireHandler.OnClosed"/> is called.
/// </summary>
internal enum CloseCause : byte
{
    /// <summary>
    /// The client sent DISCONNECT.
    /// </summary>
    ClientDisconnected = (byte)MqttCloseReason.ClientDisconnected,

    /// <summary>
    /// The link ended without a DISCONNECT.
    /// </summary>
    ConnectionLost = (byte)MqttCloseReason.ConnectionLost,

    /// <summary>
    /// The client sent a malformed packet.
    /// </summary>
    MalformedPacket = (byte)MqttCloseReason.MalformedPacket,

    /// <summary>
    /// The client broke the protocol.
    /// </summary>
    ProtocolError = (byte)MqttCloseReason.ProtocolError,

    /// <summary>
    /// The client sent nothing within one and a half times its keep-alive interval.
    /// </summary>
    KeepAliveTimedOut = (byte)MqttCloseReason.KeepAliveTimedOut,

    /// <summary>
    /// A packet stayed incomplete for too long.
    /// </summary>
    PartialPacketTimedOut = (byte)MqttCloseReason.PartialPacketTimedOut,

    /// <summary>
    /// A write to the client took too long.
    /// </summary>
    WriteTimedOut = (byte)MqttCloseReason.WriteTimedOut,

    /// <summary>
    /// The client did not acknowledge a packet in time.
    /// </summary>
    AcknowledgementTimedOut = (byte)MqttCloseReason.AcknowledgementTimedOut,

    /// <summary>
    /// More messages or bytes waited for the client than the options allow.
    /// </summary>
    OutgoingLimitPassed = (byte)MqttCloseReason.OutgoingLimitPassed,

    /// <summary>
    /// The client sent more unacknowledged QoS 1 and 2 publishes than the receive maximum allows.
    /// </summary>
    ReceiveMaximumPassed = (byte)MqttCloseReason.ReceiveMaximumPassed,

    /// <summary>
    /// A message was larger than the client accepts.
    /// </summary>
    MessageTooLargeToSend = (byte)MqttCloseReason.MessageTooLargeToSend,

    /// <summary>
    /// A new connection with the same client id took the place of this one.
    /// </summary>
    TakenOver = (byte)MqttCloseReason.TakenOver,

    /// <summary>
    /// The host closed the connection with <see cref="MqttWireConnection.Close"/>.
    /// </summary>
    ClosedByHost = (byte)MqttCloseReason.ClosedByHost,

    /// <summary>
    /// A method of the handler threw.
    /// </summary>
    HandlerThrew = (byte)MqttCloseReason.HandlerThrew,

    /// <summary>
    /// The server is stopping.
    /// </summary>
    ServerStopping = (byte)MqttCloseReason.ServerStopping,

    /// <summary>
    /// The host refused the CONNECT, in <see cref="IMqttWireHandler.OnConnectAsync"/> or in
    /// <see cref="IMqttWireHandler.OnAdmit"/>; the connection was never admitted.
    /// </summary>
    RefusedByHost,

    /// <summary>
    /// The host's <see cref="IMqttWireHandler.OnConnectAsync"/> did not answer within the authentication deadline;
    /// the connection was never admitted.
    /// </summary>
    AuthenticationTimedOut,

    /// <summary>
    /// No complete CONNECT came within the connect deadline; the connection was never admitted and has no client id.
    /// </summary>
    ConnectTimedOut,

    /// <summary>
    /// Something threw inside MqttWire while it sent to the client; reported to the host as
    /// <see cref="MqttCloseReason.ConnectionLost"/>.
    /// </summary>
    SendPathFaulted,
}
