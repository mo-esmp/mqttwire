using Microsoft.Extensions.Logging;

namespace MqttWire.Server.Internal;

/// <summary>
/// The server's log events, source-generated so that a level that is off costs a check and nothing else. No event
/// carries a password or a payload.
/// </summary>
internal static partial class ServerLog
{
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
    /// Logs what the host's <see cref="IMqttWireHandler.OnConnectAsync"/> did after its deadline had passed or its
    /// connection had gone: the answer is discarded either way.
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
    [LoggerMessage(EventId = 19, Message = "OnConnectAsync for client {ClientId} ended after the connection had let go of it; its outcome was discarded.")]
    public static partial void LateConnectResult(ILogger logger, LogLevel level, string clientId, Exception? exception);
}
