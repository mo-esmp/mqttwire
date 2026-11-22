namespace MqttWire.Server;

/// <summary>
/// The one interface a host implements to drive the server: who may connect, what a client may subscribe to and
/// publish, and what to do when a connection ends. MqttWire keeps no record of subscriptions; what a subscription
/// means, and remembering it, is the host's.
/// </summary>
/// <remarks>
/// <see cref="OnSubscribe"/>, <see cref="OnUnsubscribe"/> and <see cref="OnPublish"/> are called from the
/// connection's read path: for one connection one at a time, in the order the packets arrived; for different
/// connections at the same time. They are synchronous and must return quickly; work that takes time is queued by
/// the host. <see cref="OnAdmit"/> runs inside the admission step of the server and must not block or call back
/// into the server. <see cref="OnClosed"/> is called once, after the last other call for that connection has
/// returned, and only for a connection that <see cref="OnAdmit"/> accepted: a connection refused in
/// <see cref="OnConnectAsync"/> or <see cref="OnAdmit"/> gets none. A handler method that throws is the host's
/// fault: the exception is logged as an error and the connection is closed with reason code 0x83 (implementation
/// specific error); an exception from <see cref="OnClosed"/> is logged only.
/// </remarks>
public interface IMqttWireHandler
{
    /// <summary>
    /// Decides who a connecting client is. May take time and may do I/O; it has a deadline, and a connection
    /// that overruns it is refused.
    /// </summary>
    /// <param name="request">
    /// The CONNECT as the host needs it. It owns copies of what it holds, so it may be used across awaits.
    /// </param>
    /// <param name="cancellationToken">
    /// Canceled when the deadline passes, the client goes away or the server stops.
    /// </param>
    /// <returns>
    /// Accept, with the state object of the host for the connection and optional CONNACK user properties; or
    /// refuse, with a reason code and a reason string.
    /// </returns>
    ValueTask<MqttConnectResult> OnConnectAsync(MqttConnectRequest request, CancellationToken cancellationToken);

    /// <summary>
    /// Decides whether the client may come in now. Called inside the admission step: synchronous and quick; it
    /// must not block or call back into the server.
    /// </summary>
    /// <param name="connection">
    /// The new connection, which is not open yet.
    /// </param>
    /// <param name="replaces">
    /// The connection that currently has this client id, or <see langword="null"/> when there is none.
    /// </param>
    /// <returns>
    /// Accept or refuse. When <paramref name="replaces"/> is not <see langword="null"/> and the host accepts, the
    /// new connection takes over and the old one is closed with the reason "session taken over".
    /// </returns>
    MqttAdmitResult OnAdmit(MqttWireConnection connection, MqttWireConnection? replaces);

    /// <summary>
    /// Decides a subscription. Called from the read path of the connection: synchronous, and it must return
    /// quickly.
    /// </summary>
    /// <param name="connection">
    /// The subscribing connection.
    /// </param>
    /// <param name="topicFilter">
    /// The topic filter the client asked for.
    /// </param>
    /// <param name="requested">
    /// The QoS the client asked for.
    /// </param>
    /// <returns>
    /// Grant at a QoS (the server lowers it to the highest QoS it supports if needed), or refuse with a reason
    /// code and a reason string.
    /// </returns>
    MqttSubscribeResult OnSubscribe(MqttWireConnection connection, string topicFilter, MqttQoS requested);

    /// <summary>
    /// Tells the host that a client removed a subscription. Called from the read path of the connection:
    /// synchronous, and it must return quickly.
    /// </summary>
    /// <param name="connection">
    /// The connection that unsubscribed.
    /// </param>
    /// <param name="topicFilter">
    /// The topic filter the client removed.
    /// </param>
    void OnUnsubscribe(MqttWireConnection connection, string topicFilter);

    /// <summary>
    /// Receives a message a client published. Called from the read path of the connection: synchronous, and it
    /// must return quickly.
    /// </summary>
    /// <param name="connection">
    /// The publishing connection.
    /// </param>
    /// <param name="publish">
    /// The message: a view that is valid only during the call. A host that keeps the topic or the payload copies
    /// it.
    /// </param>
    /// <returns>
    /// Accept, or refuse with a reason code and a reason string. The server sends the answer in the form the QoS
    /// calls for.
    /// </returns>
    MqttPublishResult OnPublish(MqttWireConnection connection, in MqttPublish publish);

    /// <summary>
    /// Tells the host that a connection ended. Called exactly once for every connection that
    /// <see cref="OnAdmit"/> accepted, after the last other call for that connection has returned.
    /// </summary>
    /// <param name="connection">
    /// The connection that ended.
    /// </param>
    /// <param name="reason">
    /// Why it ended.
    /// </param>
    void OnClosed(MqttWireConnection connection, MqttCloseReason reason);
}
