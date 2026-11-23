using System.Buffers;
using Microsoft.Extensions.Options;
using MqttWire.Server;

namespace MqttWire.TestHost;

/// <summary>
/// A small host for the tests: every client may connect except the user name <c>refuse-me</c>, a subscription is to
/// an exact topic, and a message published to a topic goes to every connection subscribed to it, the sender
/// included, at the lower of the publish's QoS and the QoS granted to the subscription (design 6.10).
/// </summary>
/// <remarks>
/// The server calls <see cref="OnSubscribe"/>, <see cref="OnUnsubscribe"/> and <see cref="OnPublish"/> for different
/// connections at the same time, so the table of subscriptions, and each connection's own list in its
/// <see cref="MqttWireConnection.State"/>, are guarded by one lock.
/// </remarks>
/// <param name="options">
/// The server's options, for the highest QoS the server grants.
/// </param>
public sealed class ExactTopicHandler(IOptions<MqttWireServerOptions> options) : IMqttWireHandler
{
    /// <summary>
    /// The user name that is refused with <see cref="MqttReasonCode.BadUserNameOrPassword"/>.
    /// </summary>
    public const string RefusedUserName = "refuse-me";

    /// <summary>
    /// The highest QoS the server grants: a subscription asked for above it is granted, and remembered, at it.
    /// </summary>
    private readonly MqttQoS _maxQoS = options.Value.MaxQoS;

    /// <summary>
    /// Guards <see cref="_subscribers"/> and every connection's own subscriptions.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// For each topic, the connections subscribed to it and the QoS each was granted.
    /// </summary>
    private readonly Dictionary<string, Dictionary<MqttWireConnection, MqttQoS>> _subscribers =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Accepts every client except one with the user name <see cref="RefusedUserName"/>, which is refused with
    /// <see cref="MqttReasonCode.BadUserNameOrPassword"/>. An accepted connection's state is its own list of
    /// subscriptions, empty.
    /// </summary>
    /// <param name="request">
    /// The CONNECT.
    /// </param>
    /// <param name="cancellationToken">
    /// Not used: the answer is immediate.
    /// </param>
    /// <returns>
    /// The answer, completed.
    /// </returns>
    public ValueTask<MqttConnectResult> OnConnectAsync(MqttConnectRequest request, CancellationToken cancellationToken)
    {
        if (string.Equals(request.UserName, RefusedUserName, StringComparison.Ordinal))
        {
            return ValueTask.FromResult(MqttConnectResult.Refuse(MqttReasonCode.BadUserNameOrPassword));
        }

        Dictionary<string, MqttQoS> subscriptions = new(StringComparer.Ordinal);
        return ValueTask.FromResult(MqttConnectResult.Accept(subscriptions));
    }

    /// <summary>
    /// Lets every connection in; one that replaces another takes over its client id.
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
    /// Grants a subscription to an exact topic at the QoS asked for, or at the server's highest when that is lower,
    /// and remembers the QoS granted, in the table and in the connection's own list; subscribing again to the same
    /// topic replaces the QoS. A filter with a wildcard is refused with
    /// <see cref="MqttReasonCode.WildcardSubscriptionsNotSupported"/> and changes nothing.
    /// </summary>
    /// <param name="connection">
    /// The subscribing connection.
    /// </param>
    /// <param name="topicFilter">
    /// The filter, which must be an exact topic.
    /// </param>
    /// <param name="requested">
    /// The QoS asked for.
    /// </param>
    /// <returns>
    /// The grant at the lower of <paramref name="requested"/> and the server's highest QoS, or the refusal of a
    /// wildcard.
    /// </returns>
    public MqttSubscribeResult OnSubscribe(MqttWireConnection connection, string topicFilter, MqttQoS requested)
    {
        if (topicFilter.AsSpan().IndexOfAny('+', '#') >= 0)
        {
            return MqttSubscribeResult.Refuse(MqttReasonCode.WildcardSubscriptionsNotSupported,
                "Only exact topics can be subscribed to.");
        }

        // What the server will grant, which is what a later publish may be sent at.
        MqttQoS granted = requested > _maxQoS ? _maxQoS : requested;
        lock (_gate)
        {
            if (!_subscribers.TryGetValue(topicFilter, out Dictionary<MqttWireConnection, MqttQoS>? members))
            {
                members = [];
                _subscribers.Add(topicFilter, members);
            }

            members[connection] = granted;
            GetSubscriptions(connection)[topicFilter] = granted;
        }

        return MqttSubscribeResult.Granted(granted);
    }

    /// <summary>
    /// Forgets the connection's subscription to the topic; a topic it was not subscribed to changes nothing.
    /// </summary>
    /// <param name="connection">
    /// The connection that unsubscribed.
    /// </param>
    /// <param name="topicFilter">
    /// The topic.
    /// </param>
    public void OnUnsubscribe(MqttWireConnection connection, string topicFilter)
    {
        lock (_gate)
        {
            GetSubscriptions(connection).Remove(topicFilter);
            RemoveMember(topicFilter, connection);
        }
    }

    /// <summary>
    /// Copies the payload once and sends it to every connection subscribed to the publish's topic, the sender
    /// included, at the lower of the publish's QoS and the granted QoS. The recipients are copied out of the table
    /// under the lock and sent to after it is released: <c>TrySend</c> may begin a recipient's close, and that
    /// recipient's <see cref="OnClosed"/>, which runs on another thread and never inside the call, takes the lock to
    /// change the table; it must not have to wait for a whole fan-out. A recipient that closed meanwhile answers "not
    /// open"; one that cannot take the message (too large, evicted) is skipped: what that means for it is the
    /// server's.
    /// </summary>
    /// <param name="connection">
    /// The publishing connection.
    /// </param>
    /// <param name="publish">
    /// The message, valid only during this call.
    /// </param>
    /// <returns>
    /// <see cref="MqttPublishResult.Accepted"/>, always.
    /// </returns>
    public MqttPublishResult OnPublish(MqttWireConnection connection, in MqttPublish publish)
    {
        string topicName = publish.GetTopic();
        Recipient[] recipients;
        int count = 0;
        lock (_gate)
        {
            if (!_subscribers.TryGetValue(topicName, out Dictionary<MqttWireConnection, MqttQoS>? members))
            {
                return MqttPublishResult.Accepted;
            }

            recipients = ArrayPool<Recipient>.Shared.Rent(members.Count);
            foreach (KeyValuePair<MqttWireConnection, MqttQoS> member in members)
            {
                recipients[count++] = new Recipient(member.Key, member.Value);
            }
        }

        try
        {
            // One copy, shared by every recipient; the view is valid only during this call.
            byte[] payload = publish.Payload.ToArray();
            MqttTopic topic = MqttTopic.Create(publish.Topic);
            for (int i = 0; i < count; i++)
            {
                MqttQoS qos = publish.QoS < recipients[i].QoS ? publish.QoS : recipients[i].QoS;
                recipients[i].Connection.TrySend(topic, payload, qos);
            }
        }
        finally
        {
            // Cleared, so the pool keeps no connection alive.
            ArrayPool<Recipient>.Shared.Return(recipients, clearArray: true);
        }

        return MqttPublishResult.Accepted;
    }

    /// <summary>
    /// Forgets every subscription of the connection that ended.
    /// </summary>
    /// <param name="connection">
    /// The connection that ended.
    /// </param>
    /// <param name="reason">
    /// Why it ended; not used.
    /// </param>
    public void OnClosed(MqttWireConnection connection, MqttCloseReason reason)
    {
        lock (_gate)
        {
            Dictionary<string, MqttQoS> subscriptions = GetSubscriptions(connection);
            foreach (string topicFilter in subscriptions.Keys)
            {
                RemoveMember(topicFilter, connection);
            }

            subscriptions.Clear();
        }
    }

    /// <summary>
    /// Counts the connections subscribed to a topic, for a test that asserts on the table itself: a connection that
    /// ended is no longer counted once its <see cref="OnClosed"/> has run.
    /// </summary>
    /// <param name="topic">
    /// The exact topic.
    /// </param>
    /// <returns>
    /// The number of subscribed connections; 0 for a topic nobody is subscribed to.
    /// </returns>
    public int CountSubscribers(string topic)
    {
        lock (_gate)
        {
            return _subscribers.TryGetValue(topic, out Dictionary<MqttWireConnection, MqttQoS>? members) ? members.Count : 0;
        }
    }

    /// <summary>
    /// Gets the connection's own list of subscriptions, which <see cref="OnConnectAsync"/> made its state.
    /// </summary>
    /// <param name="connection">
    /// A connection this handler accepted.
    /// </param>
    /// <returns>
    /// The list, guarded by <see cref="_gate"/>.
    /// </returns>
    private static Dictionary<string, MqttQoS> GetSubscriptions(MqttWireConnection connection) =>
        (Dictionary<string, MqttQoS>)connection.State!; // OnConnectAsync accepts only with this state, and only an accepted connection reaches the other calls.

    /// <summary>
    /// Removes a connection from a topic's members, and the topic from the table when it has none left. The caller
    /// holds <see cref="_gate"/>.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic.
    /// </param>
    /// <param name="connection">
    /// The connection.
    /// </param>
    private void RemoveMember(string topicFilter, MqttWireConnection connection)
    {
        if (_subscribers.TryGetValue(topicFilter, out Dictionary<MqttWireConnection, MqttQoS>? members)
            && members.Remove(connection) && members.Count == 0)
        {
            _subscribers.Remove(topicFilter);
        }
    }

    /// <summary>
    /// A connection a publish is sent to, copied out of the table.
    /// </summary>
    /// <param name="Connection">
    /// The connection.
    /// </param>
    /// <param name="QoS">
    /// The QoS granted to its subscription.
    /// </param>
    private readonly record struct Recipient(MqttWireConnection Connection, MqttQoS QoS);
}
