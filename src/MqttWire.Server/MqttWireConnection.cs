namespace MqttWire.Server;

/// <summary>
/// One client connected to the server, as the host sees it. Methods may be called from any thread.
/// </summary>
public sealed class MqttWireConnection
{
    /// <summary>
    /// Creates a connection.
    /// </summary>
    /// <param name="clientId">
    /// The client identifier.
    /// </param>
    /// <param name="state">
    /// The object the host returned from <see cref="IMqttWireHandler.OnConnectAsync"/>, or
    /// <see langword="null"/>.
    /// </param>
    internal MqttWireConnection(string clientId, object? state)
    {
        ClientId = clientId;
        State = state;
    }

    /// <summary>
    /// Gets the client identifier.
    /// </summary>
    public string ClientId { get; }

    /// <summary>
    /// Gets the object the host returned from <see cref="IMqttWireHandler.OnConnectAsync"/>; the server never
    /// looks at it.
    /// </summary>
    public object? State { get; }

    /// <summary>
    /// Hands a message for this connection to the server and returns at once; it never waits for the network.
    /// Safe to call from any thread. The topic and the payload are not copied: the caller must not change them
    /// afterwards, and one payload may be handed to any number of connections.
    /// </summary>
    /// <param name="topic">
    /// The topic of the message.
    /// </param>
    /// <param name="payload">
    /// The payload; it is referenced, not copied.
    /// </param>
    /// <param name="qos">
    /// The QoS to deliver with; the server lowers it to what the client was granted if needed.
    /// </param>
    /// <returns>
    /// <see cref="MqttSendResult.Taken"/> when the server took the message; otherwise why it did not: the
    /// connection is not open, the message is too large, or the connection was just evicted. A message that was
    /// not taken is not queued and nothing changes.
    /// </returns>
#pragma warning disable S2325 // The shell has no state yet; the next task makes this an instance member in earnest.
    public MqttSendResult TrySend(MqttTopic topic, ReadOnlyMemory<byte> payload, MqttQoS qos) => MqttSendResult.NotOpen;
#pragma warning restore S2325

    /// <summary>
    /// Ends the connection. Safe from any thread and any number of times; a call after the first changes
    /// nothing.
    /// </summary>
    /// <param name="reason">
    /// The reason code to send to the client in DISCONNECT.
    /// </param>
    /// <param name="reasonString">
    /// A human-readable reason, or <see langword="null"/>; it is cut to 256 UTF-8 bytes.
    /// </param>
    public void Close(MqttReasonCode reason, string? reasonString = null)
    {
        // A shell: the next task gives the connection a link to close.
    }
}
