namespace MqttWire.Server;

/// <summary>
/// Why a connection ended, as <see cref="IMqttWireHandler.OnClosed"/> reports it.
/// </summary>
public enum MqttCloseReason
{
    /// <summary>
    /// The client sent DISCONNECT.
    /// </summary>
    ClientDisconnected,

    /// <summary>
    /// The link ended without a DISCONNECT.
    /// </summary>
    ConnectionLost,

    /// <summary>
    /// The client sent a malformed packet.
    /// </summary>
    MalformedPacket,

    /// <summary>
    /// The client broke the protocol.
    /// </summary>
    ProtocolError,

    /// <summary>
    /// The client sent nothing within one and a half times its keep-alive interval.
    /// </summary>
    KeepAliveTimedOut,

    /// <summary>
    /// A packet stayed incomplete for too long.
    /// </summary>
    PartialPacketTimedOut,

    /// <summary>
    /// A write to the client took too long.
    /// </summary>
    WriteTimedOut,

    /// <summary>
    /// The client did not acknowledge a packet in time.
    /// </summary>
    AcknowledgementTimedOut,

    /// <summary>
    /// More messages or bytes waited for the client than the options allow.
    /// </summary>
    OutgoingLimitPassed,

    /// <summary>
    /// The client sent more unacknowledged QoS 1 and 2 publishes than the receive maximum allows.
    /// </summary>
    ReceiveMaximumPassed,

    /// <summary>
    /// A message was larger than the client accepts.
    /// </summary>
    MessageTooLargeToSend,

    /// <summary>
    /// A new connection with the same client id took the place of this one.
    /// </summary>
    TakenOver,

    /// <summary>
    /// The host closed the connection with <see cref="MqttWireConnection.Close"/>.
    /// </summary>
    ClosedByHost,

    /// <summary>
    /// A method of the handler threw.
    /// </summary>
    HandlerThrew,

    /// <summary>
    /// The server is stopping.
    /// </summary>
    ServerStopping,
}
