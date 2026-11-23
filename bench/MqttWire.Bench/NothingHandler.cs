using MqttWire.Server;

namespace MqttWire.Bench;

/// <summary>
/// A handler that accepts everything and does nothing (design 6.14 item 2): it keeps no state for a connection
/// and no table of its own, so what the benchmark measures above the floor is MqttWire's alone.
/// </summary>
internal sealed class NothingHandler : IMqttWireHandler
{
    /// <summary>
    /// Accepts every client, with no state.
    /// </summary>
    /// <param name="request">
    /// The CONNECT; not looked at.
    /// </param>
    /// <param name="cancellationToken">
    /// Not used: the answer is immediate.
    /// </param>
    /// <returns>
    /// The acceptance, completed.
    /// </returns>
    public ValueTask<MqttConnectResult> OnConnectAsync(MqttConnectRequest request, CancellationToken cancellationToken) =>
        ValueTask.FromResult(MqttConnectResult.Accept());

    /// <summary>
    /// Lets every connection in.
    /// </summary>
    /// <param name="connection">
    /// The new connection.
    /// </param>
    /// <param name="replaces">
    /// The connection that has the client id now, or <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see cref="MqttAdmitResult.Accepted"/>, always.
    /// </returns>
    public MqttAdmitResult OnAdmit(MqttWireConnection connection, MqttWireConnection? replaces) =>
        MqttAdmitResult.Accepted;

    /// <summary>
    /// Grants every subscription and remembers nothing.
    /// </summary>
    /// <param name="connection">
    /// The subscribing connection.
    /// </param>
    /// <param name="topicFilter">
    /// The filter; not looked at.
    /// </param>
    /// <param name="requested">
    /// The QoS asked for, which is granted.
    /// </param>
    /// <returns>
    /// The grant.
    /// </returns>
    public MqttSubscribeResult OnSubscribe(MqttWireConnection connection, string topicFilter, MqttQoS requested) =>
        MqttSubscribeResult.Granted(requested);

    /// <summary>
    /// Does nothing.
    /// </summary>
    /// <param name="connection">
    /// The connection that unsubscribed.
    /// </param>
    /// <param name="topicFilter">
    /// The filter; not looked at.
    /// </param>
    public void OnUnsubscribe(MqttWireConnection connection, string topicFilter)
    {
        // Nothing is kept, so there is nothing to forget.
    }

    /// <summary>
    /// Accepts every publish and sends it nowhere.
    /// </summary>
    /// <param name="connection">
    /// The publishing connection.
    /// </param>
    /// <param name="publish">
    /// The message; not looked at.
    /// </param>
    /// <returns>
    /// <see cref="MqttPublishResult.Accepted"/>, always.
    /// </returns>
    public MqttPublishResult OnPublish(MqttWireConnection connection, in MqttPublish publish) =>
        MqttPublishResult.Accepted;

    /// <summary>
    /// Does nothing.
    /// </summary>
    /// <param name="connection">
    /// The connection that ended.
    /// </param>
    /// <param name="reason">
    /// Why it ended; not looked at.
    /// </param>
    public void OnClosed(MqttWireConnection connection, MqttCloseReason reason)
    {
        // Nothing is kept, so there is nothing to clean up.
    }
}
