using System.Collections.Immutable;
using MqttWire;
using MqttWire.Server;

namespace GroupChat.Server;

/// <summary>
/// The chat's own table of who is in which group. MqttWire keeps no such table; routing is the host's (design
/// 6.10).
/// </summary>
/// <remarks>
/// The server calls the handler for different connections at the same time, so every member takes one lock. The
/// lock is held only to read or replace one entry: a group's members are an immutable snapshot
/// (<see cref="Group"/>), which a join or a leave replaces. Membership changes far less often than messages are
/// sent, so a publish finds the snapshot ready and copies nothing.
/// </remarks>
internal sealed class GroupTable
{
    /// <summary>
    /// Guards <see cref="_groups"/>.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// Each group as it is now, by its topic. A group with no member has no entry.
    /// </summary>
    private readonly Dictionary<string, Group> _groups = new(StringComparer.Ordinal);

    /// <summary>
    /// Adds a connection to a group, or replaces the QoS it asked for when it is a member already.
    /// </summary>
    /// <param name="group">
    /// The group's topic.
    /// </param>
    /// <param name="connection">
    /// The connection that subscribed.
    /// </param>
    /// <param name="qos">
    /// The QoS its subscription asked for.
    /// </param>
    /// <exception cref="ArgumentException">
    /// The topic is not one a message can be sent to: see <see cref="MqttTopic.Create(string)"/>. The handler
    /// lets only topics through that are.
    /// </exception>
    public void Join(string group, MqttWireConnection connection, MqttQoS qos)
    {
        GroupMember member = new(connection, qos);
        lock (_gate)
        {
            if (!_groups.TryGetValue(group, out Group? current))
            {
                // The topic is encoded here, once for the life of the group.
                _groups.Add(group, new Group(MqttTopic.Create(group), [member]));
                return;
            }

            int index = IndexOf(current.Members, connection);
            ImmutableArray<GroupMember> members = index >= 0
                ? current.Members.SetItem(index, member)
                : current.Members.Add(member);
            _groups[group] = new Group(current.Topic, members);
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
            if (!_groups.TryGetValue(group, out Group? current))
            {
                return;
            }

            int index = IndexOf(current.Members, connection);
            if (index < 0)
            {
                return;
            }

            if (current.Members.Length == 1)
            {
                _groups.Remove(group);
                return;
            }

            _groups[group] = new Group(current.Topic, current.Members.RemoveAt(index));
        }
    }

    /// <summary>
    /// Finds a group as it is now.
    /// </summary>
    /// <param name="group">
    /// The group's topic.
    /// </param>
    /// <returns>
    /// The group's snapshot, which the caller sends to after this returns, with no lock held: <c>TrySend</c> can
    /// end a connection, and that connection's <c>OnClosed</c>, which runs on another thread, takes the lock to
    /// leave its groups. <see langword="null"/> for a group with no member.
    /// </returns>
    public Group? Find(string group)
    {
        lock (_gate)
        {
            return _groups.GetValueOrDefault(group);
        }
    }

    /// <summary>
    /// Finds a connection among a group's members.
    /// </summary>
    /// <param name="members">
    /// The members.
    /// </param>
    /// <param name="connection">
    /// The connection.
    /// </param>
    /// <returns>
    /// Its index, or -1 when it is not a member.
    /// </returns>
    private static int IndexOf(ImmutableArray<GroupMember> members, MqttWireConnection connection)
    {
        for (int i = 0; i < members.Length; i++)
        {
            if (ReferenceEquals(members[i].Connection, connection))
            {
                return i;
            }
        }

        return -1;
    }
}
