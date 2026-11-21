namespace MqttWire;

/// <summary>
/// The options of an <see cref="MqttWireReconnectingClient"/>. They are checked when the client is created.
/// </summary>
public sealed class MqttWireReconnectOptions
{
    /// <summary>
    /// Gets the callback that builds the options of each connection. It is called before every attempt, so a token
    /// that has expired can be replaced. The options it returns must leave <see cref="MqttWireClientOptions.OnMessage"/>
    /// and <see cref="MqttWireClientOptions.OnClosed"/> <see langword="null"/>: the reconnecting client has its own.
    /// </summary>
    /// <remarks>
    /// Its token is cancelled when the reconnecting client stops. An exception it throws, options that
    /// <see cref="MqttWireClient"/> refuses, or options that set <c>OnMessage</c> or <c>OnClosed</c>, are reported
    /// to <see cref="OnCallbackError"/> and count as a failed attempt, which is retried after the back-off.
    /// </remarks>
    public required Func<CancellationToken, ValueTask<MqttWireClientOptions>> ClientOptions { get; init; }

    /// <summary>
    /// Gets the delay before the first retry; each later retry doubles it, up to <see cref="MaxDelay"/>. Greater
    /// than zero. A random part of ±20 % is added to each delay, so that many clients do not come back in the same
    /// instant.
    /// </summary>
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Gets the longest delay between two attempts, random part included. At least <see cref="InitialDelay"/>,
    /// and at most 4,294,967,294 milliseconds (about 49 days), the longest delay a timer supports.
    /// </summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Gets the callback for each connection made, after the remembered subscriptions were made again, or
    /// <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// It must not wait synchronously for a request to the reconnecting client, such as
    /// <c>PublishAsync(…).GetAwaiter().GetResult()</c>: the callbacks run one at a time, so no message is
    /// delivered, and no acknowledgement read, while it runs, and the wait may never end.
    /// </remarks>
    public Action<MqttConnectedInfo>? OnConnected { get; init; }

    /// <summary>
    /// Gets the callback for the end of a connection that <see cref="OnConnected"/> reported, or
    /// <see langword="null"/>. It is called once for each such connection, before the next
    /// <see cref="OnConnected"/>, also when the reconnecting client is disposed.
    /// </summary>
    public Action<MqttClientCloseInfo>? OnDisconnected { get; init; }

    /// <summary>
    /// Gets the callback for a remembered subscription the server refused when it was made again on a new
    /// connection, with the topic filter and the outcome, or <see langword="null"/>. The subscription is no longer
    /// remembered.
    /// </summary>
    /// <remarks>
    /// It must not wait synchronously for a request to the reconnecting client, such as
    /// <c>PublishAsync(…).GetAwaiter().GetResult()</c>: the callbacks run one at a time, so no message is
    /// delivered, and no acknowledgement read, while it runs, and the wait may never end.
    /// </remarks>
    public Action<string, MqttRequestResult>? OnSubscriptionRefused { get; init; }

    /// <summary>
    /// Gets the callback for the refusal that ended the reconnecting client for good, or <see langword="null"/>:
    /// a CONNACK with unsupported protocol version (0x84), client identifier not valid (0x85), bad user name or
    /// password (0x86), not authorized (0x87) or banned (0x8A), which the same CONNECT would meet again. It is
    /// called once, and no attempt follows it.
    /// </summary>
    public Action<MqttConnectAck>? OnStopped { get; init; }

    /// <summary>
    /// Gets the handler for received messages, or <see langword="null"/> to ignore them. A handler that throws ends
    /// that connection, as it does on <see cref="MqttWireClient"/>: <see cref="OnDisconnected"/> reports
    /// <see cref="MqttClientCloseReason.MessageHandlerThrew"/> with the exception, and the reconnecting client
    /// connects again.
    /// </summary>
    /// <remarks>
    /// It must not wait synchronously for a request to the reconnecting client, such as
    /// <c>PublishAsync(…).GetAwaiter().GetResult()</c>: the callbacks run one at a time, so no message is
    /// delivered, and no acknowledgement read, while it runs, and the wait may never end.
    /// </remarks>
    public MqttMessageHandler? OnMessage { get; init; }

    /// <summary>
    /// Gets the callback for an exception thrown by another callback or by <see cref="ClientOptions"/>, or
    /// <see langword="null"/> to ignore them. The reconnecting client carries on after it; an exception this
    /// callback throws is ignored.
    /// </summary>
    public Action<Exception>? OnCallbackError { get; init; }
}
