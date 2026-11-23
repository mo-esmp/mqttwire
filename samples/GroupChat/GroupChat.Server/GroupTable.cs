using MqttWire;
using MqttWire.Server;

namespace GroupChat.Server;

/// <summary>
/// The chat's own table of who is in which group: for each group's topic, the connections subscribed to it and
/// the QoS each was granted. MqttWire keeps no such table; routing is the host's (design 6.10).
/// </summary>
/// <remarks>
/// The server calls the handler for different connections at the same time, so every member takes one lock.
/// </remarks>
internal sealed class GroupTable
{
    /// <summary>
    /// Guards <see cref="_groups"/>.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// The members of each group, by the group's topic. A group with no member has no entry.
    /// </summary>
    private readonly Dictionary<string, List<(MqttWireConnection Connection, MqttQoS Granted)>> _groups =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Adds a connection to a group, or replaces the QoS it was granted when it is a member already.
    /// </summary>
    /// <param name="group">
    /// The group's topic.
    /// </param>
    /// <param name="connection">
    /// The connection that subscribed.
    /// </param>
    /// <param name="granted">
    /// The QoS its subscription was granted.
    /// </param>
    public void Join(string group, MqttWireConnection connection, MqttQoS granted)
    {
        lock (_gate)
        {
            if (!_groups.TryGetValue(group, out List<(MqttWireConnection Connection, MqttQoS Granted)>? members))
            {
                members = [];
                _groups.Add(group, members);
            }

            int index = members.FindIndex(member => member.Connection == connection);
            if (index >= 0)
            {
                members[index] = (connection, granted);
            }
            else
            {
                members.Add((connection, granted));
            }
        }
    }

    /// <summary>
    /// Removes a connection from a group, and the group from the table when it has no member left. A connection
    /// that is not a member changes nothing.
    /// </summary>
    /// <param name="group">
    /// The group's topic.
    /// </param>
    /// <param name="connection">
    /// The connection that unsubscribed or ended.
    /// </param>
    public void Leave(string group, MqttWireConnection connection)
    {
        lock (_gate)
        {
            if (_groups.TryGetValue(group, out List<(MqttWireConnection Connection, MqttQoS Granted)>? members)
                && members.RemoveAll(member => member.Connection == connection) > 0
                && members.Count == 0)
            {
                _groups.Remove(group);
            }
        }
    }

    /// <summary>
    /// Copies the members of a group out of the table, so that the caller can send to them after the lock is
    /// released: <c>TrySend</c> can end a connection, and that connection's <c>OnClosed</c>, which runs on another
    /// thread, takes this lock to leave its groups.
    /// </summary>
    /// <param name="group">
    /// The group's topic.
    /// </param>
    /// <returns>
    /// The members and the QoS each was granted; empty for a group with no member. One array per call, which a
    /// sample can afford; a host with large groups would rent it.
    /// </returns>
    public (MqttWireConnection Connection, MqttQoS Granted)[] GetMembers(string group)
    {
        lock (_gate)
        {
            return _groups.TryGetValue(group, out List<(MqttWireConnection Connection, MqttQoS Granted)>? members)
                ? [.. members]
                : [];
        }
    }
}
