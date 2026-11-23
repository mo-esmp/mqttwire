using MqttWire;
using MqttWire.Server;

namespace GroupChat.Server;

/// <summary>
/// The chat's host code, all of it: a group is a topic of the form <c>groups/&lt;name&gt;</c>, joining a group is
/// subscribing to its topic, and a message published to a group goes to every member, the sender included.
/// MqttWire handles the connections and the protocol; who may connect, what a subscription means and where a
/// message goes is decided here (design 6.10).
/// </summary>
/// <param name="logger">
/// Where joining and leaving is logged.
/// </param>
internal sealed class ChatHandler(ILogger<ChatHandler> logger) : IMqttWireHandler
{
    /// <summary>
    /// What every group's topic starts with.
    /// </summary>
    private const string GroupPrefix = "groups/";

    /// <summary>
    /// The longest group name, in characters. Without a bound a client could make the table hold topics of up to
    /// 64 KB each, and one too long to send to.
    /// </summary>
    private const int MaxGroupNameLength = 64;

    /// <summary>
    /// Who is in which group.
    /// </summary>
    private readonly GroupTable _groups = new();

    /// <summary>
    /// Accepts every client that gives a name as its user name; a real host would check a password or a token
    /// here, and may take its time doing so. The connection's state is its <see cref="ChatMember"/>.
    /// </summary>
    /// <param name="request">
    /// The CONNECT.
    /// </param>
    /// <param name="cancellationToken">
    /// Not used: the answer is immediate.
    /// </param>
    /// <returns>
    /// The acceptance with the member as the connection's state, or a refusal with
    /// <see cref="MqttReasonCode.BadUserNameOrPassword"/> when the name is missing or blank.
    /// </returns>
    public ValueTask<MqttConnectResult> OnConnectAsync(MqttConnectRequest request, CancellationToken cancellationToken) =>
        ValueTask.FromResult(string.IsNullOrWhiteSpace(request.UserName)
            ? MqttConnectResult.Refuse(MqttReasonCode.BadUserNameOrPassword, "A name is required.")
            : MqttConnectResult.Accept(state: new ChatMember(request.UserName)));

    /// <summary>
    /// Lets every connection in; a new connection with a client id that is in use takes the old one over.
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
    /// Joins a group: grants a subscription to a group's topic and adds the connection to the group. Every other
    /// filter, a wildcard included, is refused and changes nothing.
    /// </summary>
    /// <param name="connection">
    /// The subscribing connection.
    /// </param>
    /// <param name="topicFilter">
    /// The filter the client asked for.
    /// </param>
    /// <param name="requested">
    /// The QoS the client asked for. The server lowers a grant above its own highest QoS, and lowers the QoS of
    /// a later <c>TrySend</c> the same way, so the handler can remember what was asked.
    /// </param>
    /// <returns>
    /// The grant, or <see cref="MqttSubscribeResult.NotAuthorized"/> for a filter that is not a group's topic.
    /// </returns>
    public MqttSubscribeResult OnSubscribe(MqttWireConnection connection, string topicFilter, MqttQoS requested)
    {
        if (!IsGroupTopic(topicFilter))
        {
            return MqttSubscribeResult.NotAuthorized;
        }

        ChatMember member = GetMember(connection);
        member.Groups.Add(topicFilter);
        _groups.Join(topicFilter, connection, requested);
        logger.LogInformation("{Name} joined {Group}.", member.Name, topicFilter);
        return MqttSubscribeResult.Granted(requested);
    }

    /// <summary>
    /// Leaves a group; a filter the connection had not joined changes nothing.
    /// </summary>
    /// <param name="connection">
    /// The connection that unsubscribed.
    /// </param>
    /// <param name="topicFilter">
    /// The filter it unsubscribed from.
    /// </param>
    public void OnUnsubscribe(MqttWireConnection connection, string topicFilter)
    {
        ChatMember member = GetMember(connection);
        if (member.Groups.Remove(topicFilter))
        {
            _groups.Leave(topicFilter, connection);
            logger.LogInformation("{Name} left {Group}.", member.Name, topicFilter);
        }
    }

    /// <summary>
    /// Sends a message to every member of the group it was published to, the sender included. This shows the two
    /// rules a host most easily gets wrong: the payload is copied, once, because it is valid only during this call
    /// and <c>TrySend</c> keeps what it is given; and each member gets the message at no higher QoS than its
    /// subscription was granted.
    /// </summary>
    /// <param name="connection">
    /// The publishing connection.
    /// </param>
    /// <param name="publish">
    /// The message, valid only during this call.
    /// </param>
    /// <returns>
    /// <see cref="MqttPublishResult.Accepted"/>, or a refusal with <see cref="MqttReasonCode.NotAuthorized"/> when
    /// the sender has not joined the group; nothing is sent then.
    /// </returns>
    public MqttPublishResult OnPublish(MqttWireConnection connection, in MqttPublish publish)
    {
        // One string per publish, to look the group up. A sample can afford it; a host that cannot keeps its
        // table by the topic's bytes.
        string group = publish.GetTopic();
        if (!GetMember(connection).Groups.Contains(group))
        {
            return MqttPublishResult.Refuse(MqttReasonCode.NotAuthorized, "Join the group before you send to it.");
        }

        // The group as it is now: an immutable snapshot of its members with its topic already encoded, which the
        // table replaces when someone joins or leaves. So nothing is copied here but the payload, and the members
        // are sent to with no lock held: a member that has fallen too far behind is disconnected by TrySend, and
        // its OnClosed takes the table's lock on another thread.
        Group? found = _groups.Find(group);
        if (found is null)
        {
            // The sender is a member, so the group is there; a group is only ever gone once its last member is.
            return MqttPublishResult.Accepted;
        }

        // One copy, shared by every member: TrySend does not copy it again.
        byte[] payload = publish.Payload.ToArray();
        foreach (GroupMember member in found.Members)
        {
            // TrySend returns at once, whatever the member's network does, so a slow member delays nobody. It
            // never gets a higher QoS than its subscription asked for.
            MqttQoS qos = publish.QoS < member.QoS ? publish.QoS : member.QoS;
            member.Connection.TrySend(found.Topic, payload, qos);
        }

        return MqttPublishResult.Accepted;
    }

    /// <summary>
    /// Takes the connection that ended out of every group it had joined.
    /// </summary>
    /// <param name="connection">
    /// The connection that ended.
    /// </param>
    /// <param name="reason">
    /// Why it ended.
    /// </param>
    public void OnClosed(MqttWireConnection connection, MqttCloseReason reason)
    {
        ChatMember member = GetMember(connection);
        foreach (string group in member.Groups)
        {
            _groups.Leave(group, connection);
        }

        member.Groups.Clear();
        logger.LogInformation("{Name} disconnected: {Reason}.", member.Name, reason);
    }

    /// <summary>
    /// Tells whether a topic filter is a group's topic: <c>groups/</c> followed by a name of 1 to
    /// <see cref="MaxGroupNameLength"/> characters that holds no further level and no wildcard. Such a filter is
    /// also a topic a message can be sent to, which <see cref="GroupTable.Join"/> relies on.
    /// </summary>
    /// <param name="topicFilter">
    /// The filter, which the server has already checked to be a valid MQTT topic filter.
    /// </param>
    /// <returns>
    /// <see langword="true"/> for a group's topic.
    /// </returns>
    private static bool IsGroupTopic(string topicFilter) =>
        topicFilter.StartsWith(GroupPrefix, StringComparison.Ordinal)
        && topicFilter.Length > GroupPrefix.Length
        && topicFilter.Length <= GroupPrefix.Length + MaxGroupNameLength
        && topicFilter.AsSpan(GroupPrefix.Length).IndexOfAny('/', '+', '#') < 0;

    /// <summary>
    /// Gets the member a connection stands for, which <see cref="OnConnectAsync"/> made its state.
    /// </summary>
    /// <param name="connection">
    /// A connection this handler accepted.
    /// </param>
    /// <returns>
    /// The member.
    /// </returns>
    private static ChatMember GetMember(MqttWireConnection connection) =>
        (ChatMember)connection.State!; // OnConnectAsync accepts only with this state, and only an accepted connection reaches the other calls.
}
