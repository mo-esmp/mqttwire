using Microsoft.Extensions.Logging;

namespace MqttWire.Server.Internal;

/// <summary>
/// The server's log events (design 6.12), source-generated so that a level that is off costs a check and nothing
/// else: no string is built and no value boxed unless the level is on. Every end of a connection the server decides
/// on has its own event; a reason code is logged as its <see cref="MqttReasonCode"/> name. A line names the client by
/// its client id; a connection that has none yet (it sent no CONNECT that was read) is named by its remote end point
/// instead, through a variant of the same event (the same id, name and level; the methods ending in <c>From</c>),
/// whose caller checks the level before it reads the end point. No line has an empty client id. No event carries a
/// password, a user name, a payload, a topic, user properties or a reason string from the host.
/// </summary>
internal static partial class ServerLog
{
    /// <summary>
    /// Event 1 for a connection that has no client id yet: the same id, name and level, naming the remote end point.
    /// </summary>
    private static readonly Action<ILogger, string, MqttReasonCode, Exception?> s_malformedPacketFrom =
        LoggerMessage.Define<string, MqttReasonCode>(LogLevel.Debug, new EventId(1, nameof(MalformedPacket)),
            "Connection from {RemoteEndPoint} sent a malformed packet; the connection was ended with reason code {ReasonCode}.",
            new LogDefineOptions { SkipEnabledCheck = true });

    /// <summary>
    /// Event 2 for a connection that has no client id yet: the same id, name and level, naming the remote end point.
    /// </summary>
    private static readonly Action<ILogger, string, MqttReasonCode, Exception?> s_protocolErrorFrom =
        LoggerMessage.Define<string, MqttReasonCode>(LogLevel.Debug, new EventId(2, nameof(ProtocolError)),
            "Connection from {RemoteEndPoint} broke the protocol; the connection was ended with reason code {ReasonCode}.",
            new LogDefineOptions { SkipEnabledCheck = true });

    /// <summary>
    /// Event 16 for a connection that has no client id yet: the same id, name and level, naming the remote end point.
    /// </summary>
    private static readonly Action<ILogger, string, Exception?> s_serverStoppingFrom =
        LoggerMessage.Define<string>(LogLevel.Debug, new EventId(16, nameof(ServerStopping)),
            "Connection from {RemoteEndPoint} was ended because the server is stopping.",
            new LogDefineOptions { SkipEnabledCheck = true });

    /// <summary>
    /// Event 18 for a connection that has no client id yet: the same id, name and level, naming the remote end point.
    /// </summary>
    private static readonly Action<ILogger, string, Exception?> s_connectionLostFrom =
        LoggerMessage.Define<string>(LogLevel.Debug, new EventId(18, nameof(ConnectionLost)),
            "Connection from {RemoteEndPoint} was lost.",
            new LogDefineOptions { SkipEnabledCheck = true });

    /// <summary>
    /// Event 20 for a connection that has no client id yet: the same id, name and level, naming the remote end point.
    /// </summary>
    private static readonly Action<ILogger, string, Exception?> s_sendPathFaultedFrom =
        LoggerMessage.Define<string>(LogLevel.Error, new EventId(20, nameof(SendPathFaulted)),
            "Sending to the connection from {RemoteEndPoint} failed inside MqttWire; the connection was ended.",
            new LogDefineOptions { SkipEnabledCheck = true });

    /// <summary>
    /// Logs event 1, a malformed packet, for a connection that has no client id yet. The caller has checked that
    /// <see cref="LogLevel.Debug"/> is enabled.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="remoteEndPoint">
    /// The remote end point, address and port, or "unknown".
    /// </param>
    /// <param name="reasonCode">
    /// The reason code of the end; no packet carries it when the first packet was refused.
    /// </param>
    public static void MalformedPacketFrom(ILogger logger, string remoteEndPoint, MqttReasonCode reasonCode) =>
        s_malformedPacketFrom(logger, remoteEndPoint, reasonCode, arg4: null);

    /// <summary>
    /// Logs event 2, a protocol error, for a connection that has no client id yet. The caller has checked that
    /// <see cref="LogLevel.Debug"/> is enabled.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="remoteEndPoint">
    /// The remote end point, address and port, or "unknown".
    /// </param>
    /// <param name="reasonCode">
    /// The reason code of the end; no packet carries it when the first packet was refused.
    /// </param>
    public static void ProtocolErrorFrom(ILogger logger, string remoteEndPoint, MqttReasonCode reasonCode) =>
        s_protocolErrorFrom(logger, remoteEndPoint, reasonCode, arg4: null);

    /// <summary>
    /// Logs event 16, the server's stop, for a connection that has no client id yet. The caller has checked that
    /// <see cref="LogLevel.Debug"/> is enabled.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="remoteEndPoint">
    /// The remote end point, address and port, or "unknown".
    /// </param>
    public static void ServerStoppingFrom(ILogger logger, string remoteEndPoint) =>
        s_serverStoppingFrom(logger, remoteEndPoint, arg3: null);

    /// <summary>
    /// Logs event 18, a lost link, for a connection that has no client id yet. The caller has checked that
    /// <see cref="LogLevel.Debug"/> is enabled.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="remoteEndPoint">
    /// The remote end point, address and port, or "unknown".
    /// </param>
    public static void ConnectionLostFrom(ILogger logger, string remoteEndPoint) =>
        s_connectionLostFrom(logger, remoteEndPoint, arg3: null);

    /// <summary>
    /// Logs event 20, a fault of the send path, for a connection that has no client id yet. The caller has checked
    /// that <see cref="LogLevel.Error"/> is enabled.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="remoteEndPoint">
    /// The remote end point, address and port, or "unknown".
    /// </param>
    /// <param name="exception">
    /// What was thrown.
    /// </param>
    public static void SendPathFaultedFrom(ILogger logger, string remoteEndPoint, Exception exception) =>
        s_sendPathFaultedFrom(logger, remoteEndPoint, exception);

    /// <summary>
    /// Logs that the client sent a malformed packet, and the connection was ended.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection; empty when the CONNECT itself was malformed.
    /// </param>
    /// <param name="reasonCode">
    /// The reason code the end carries to the client.
    /// </param>
    [LoggerMessage(EventId = 1, Level = LogLevel.Debug,
        Message = "Client {ClientId} sent a malformed packet; the connection was ended with reason code {ReasonCode}.")]
    public static partial void MalformedPacket(ILogger logger, string clientId, MqttReasonCode reasonCode);

    /// <summary>
    /// Logs that the client broke the protocol, or sent what MqttWire does not take, and the connection was ended.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection; empty when the CONNECT itself was refused before it was read.
    /// </param>
    /// <param name="reasonCode">
    /// The reason code the end carries to the client.
    /// </param>
    [LoggerMessage(EventId = 2, Level = LogLevel.Debug,
        Message = "Client {ClientId} broke the protocol; the connection was ended with reason code {ReasonCode}.")]
    public static partial void ProtocolError(ILogger logger, string clientId, MqttReasonCode reasonCode);

    /// <summary>
    /// Logs that the host refused the CONNECT, in <see cref="IMqttWireHandler.OnConnectAsync"/> or in
    /// <see cref="IMqttWireHandler.OnAdmit"/>. The host's reason string is not logged.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    /// <param name="reasonCode">
    /// The host's reason code, which the CONNACK carries.
    /// </param>
    [LoggerMessage(EventId = 3, Level = LogLevel.Debug,
        Message = "The host refused client {ClientId} with reason code {ReasonCode}.")]
    public static partial void RefusedByHost(ILogger logger, string clientId, MqttReasonCode reasonCode);

    /// <summary>
    /// Logs that the host's <see cref="IMqttWireHandler.OnConnectAsync"/> did not answer within the authentication
    /// deadline, so the connection was refused with CONNACK 0x88.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    [LoggerMessage(EventId = 4, Level = LogLevel.Error,
        Message = "OnConnectAsync did not answer for client {ClientId} within the authentication deadline; the connection was refused with 0x88.")]
    public static partial void AuthenticationTimedOut(ILogger logger, string clientId);

    /// <summary>
    /// Logs that no complete CONNECT came within the connect deadline, so the connection was aborted. There is no
    /// client id yet: the remote end point names the connection. The caller checks the level first, so that the end
    /// point is read and turned into text only for a line that is written.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="remoteEndPoint">
    /// The remote end point of the request, address and port, or "unknown" when it has none.
    /// </param>
    [LoggerMessage(EventId = 5, Level = LogLevel.Debug, SkipEnabledCheck = true,
        Message = "Connection from {RemoteEndPoint} sent no complete CONNECT within the connect deadline; it was aborted.")]
    public static partial void ConnectTimedOut(ILogger logger, string remoteEndPoint);

    /// <summary>
    /// Logs that the client sent nothing within one and a half times its keep-alive, so the connection was ended.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    [LoggerMessage(EventId = 6, Level = LogLevel.Debug,
        Message = "Client {ClientId} sent nothing within one and a half times its keep-alive; the connection was ended.")]
    public static partial void KeepAliveTimedOut(ILogger logger, string clientId);

    /// <summary>
    /// Logs that a packet from the client stayed incomplete past the partial-packet deadline, so the connection was
    /// ended.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    [LoggerMessage(EventId = 7, Level = LogLevel.Debug,
        Message = "A packet from client {ClientId} stayed incomplete past the partial-packet deadline; the connection was ended.")]
    public static partial void PartialPacketTimedOut(ILogger logger, string clientId);

    /// <summary>
    /// Logs that a write to the client did not complete within the write deadline, so the connection was aborted.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    [LoggerMessage(EventId = 8, Level = LogLevel.Debug,
        Message = "A write to client {ClientId} did not complete within the write deadline; the connection was aborted.")]
    public static partial void WriteTimedOut(ILogger logger, string clientId);

    /// <summary>
    /// Logs that the client did not acknowledge a packet within the acknowledgement deadline, so the connection was
    /// ended.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    [LoggerMessage(EventId = 9, Level = LogLevel.Debug,
        Message = "Client {ClientId} did not acknowledge a packet within the acknowledgement deadline; the connection was ended.")]
    public static partial void AcknowledgementTimedOut(ILogger logger, string clientId);

    /// <summary>
    /// Logs that more messages or bytes waited for the client than the options allow, so the connection was evicted.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    [LoggerMessage(EventId = 10, Level = LogLevel.Debug,
        Message = "More waited for client {ClientId} than the outgoing limits allow; the connection was evicted.")]
    public static partial void OutgoingLimitPassed(ILogger logger, string clientId);

    /// <summary>
    /// Logs that the client sent more unacknowledged QoS 1 and 2 publishes than the receive maximum allows, so the
    /// connection was ended.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    [LoggerMessage(EventId = 11, Level = LogLevel.Debug,
        Message = "Client {ClientId} sent more unacknowledged QoS 1 and 2 publishes than the receive maximum allows; the connection was ended.")]
    public static partial void ReceiveMaximumPassed(ILogger logger, string clientId);

    /// <summary>
    /// Logs that a message the host sent was too large for the client or for the options, so the connection was
    /// ended. Only the payload's length is logged, never the payload.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    /// <param name="payloadLength">
    /// The length of the message's payload, in bytes.
    /// </param>
    [LoggerMessage(EventId = 12, Level = LogLevel.Debug,
        Message = "A message with a payload of {PayloadLength} bytes was too large to send to client {ClientId}; the connection was ended.")]
    public static partial void MessageTooLargeToSend(ILogger logger, string clientId, int payloadLength);

    /// <summary>
    /// Logs that a new connection with the same client id took the place of this one.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    [LoggerMessage(EventId = 13, Level = LogLevel.Debug,
        Message = "The connection of client {ClientId} was taken over by a new one with the same client id.")]
    public static partial void TakenOver(ILogger logger, string clientId);

    /// <summary>
    /// Logs that the host closed the connection with <see cref="MqttWireConnection.Close"/>. The host's reason string
    /// is not logged.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    /// <param name="reasonCode">
    /// The host's reason code, which the DISCONNECT carries.
    /// </param>
    [LoggerMessage(EventId = 14, Level = LogLevel.Debug,
        Message = "The host closed the connection of client {ClientId} with reason code {ReasonCode}.")]
    public static partial void ClosedByHost(ILogger logger, string clientId, MqttReasonCode reasonCode);

    /// <summary>
    /// Logs that a method of the host's handler threw; the connection is closed with 0x83, or, for
    /// <see cref="IMqttWireHandler.OnClosed"/>, nothing else happens.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    /// <param name="method">
    /// The name of the handler method that threw.
    /// </param>
    /// <param name="exception">
    /// What it threw.
    /// </param>
    [LoggerMessage(EventId = 15, Level = LogLevel.Error, Message = "The handler's {Method} threw for client {ClientId}.")]
    public static partial void HandlerThrew(ILogger logger, string clientId, string method, Exception exception);

    /// <summary>
    /// Logs that the connection was ended because the server is stopping.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection; empty when it had sent no CONNECT yet.
    /// </param>
    [LoggerMessage(EventId = 16, Level = LogLevel.Debug,
        Message = "The connection of client {ClientId} was ended because the server is stopping.")]
    public static partial void ServerStopping(ILogger logger, string clientId);

    /// <summary>
    /// Logs that the client sent DISCONNECT.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    /// <param name="reasonCode">
    /// The reason code of the client's DISCONNECT.
    /// </param>
    [LoggerMessage(EventId = 17, Level = LogLevel.Debug,
        Message = "Client {ClientId} disconnected with reason code {ReasonCode}.")]
    public static partial void ClientDisconnected(ILogger logger, string clientId, MqttReasonCode reasonCode);

    /// <summary>
    /// Logs that the link of the connection ended without a DISCONNECT.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection; empty when it had sent no CONNECT yet.
    /// </param>
    [LoggerMessage(EventId = 18, Level = LogLevel.Debug,
        Message = "The connection of client {ClientId} was lost.")]
    public static partial void ConnectionLost(ILogger logger, string clientId);

    /// <summary>
    /// Logs how the host's <see cref="IMqttWireHandler.OnConnectAsync"/> ended once the connection had stopped waiting
    /// for it (its deadline had passed, or its connection had gone): whether it completed, was cancelled or threw,
    /// nothing it returned is used.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="level">
    /// <see cref="LogLevel.Debug"/> when the late task completed or was cancelled; <see cref="LogLevel.Error"/> when
    /// it threw.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    /// <param name="exception">
    /// What the late task threw, or <see langword="null"/> when it did not throw.
    /// </param>
    [LoggerMessage(EventId = 19, Message = "OnConnectAsync for client {ClientId} ended after the connection had stopped waiting for it; nothing it returned was used.")]
    public static partial void LateConnectResult(ILogger logger, LogLevel level, string clientId, Exception? exception);

    /// <summary>
    /// Logs that something threw inside MqttWire while it sent to a client (an encoding, the pool, the link): the
    /// connection was ended, reported to the host as <see cref="MqttCloseReason.ConnectionLost"/>, and its link
    /// aborted.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    /// <param name="exception">
    /// What was thrown.
    /// </param>
    [LoggerMessage(EventId = 20, Level = LogLevel.Error, Message = "Sending to client {ClientId} failed inside MqttWire; the connection was ended.")]
    public static partial void SendPathFaulted(ILogger logger, string clientId, Exception exception);
}
