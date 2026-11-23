using System.Collections.Immutable;
using MqttWire;
using MqttWire.Server;

namespace GroupChat.Server;

/// <summary>
/// A connection in a group and the QoS it asked for when it subscribed, which a message to it must not pass.
/// </summary>
/// <param name="Connection">
/// The connection.
/// </param>
/// <param name="QoS">
/// The QoS its subscription asked for. The server lowers a grant, and a later <c>TrySend</c>, to its own highest
/// QoS, so what was asked is all the table has to keep.
/// </param>
internal readonly record struct GroupMember(MqttWireConnection Connection, MqttQoS QoS);

/// <summary>
/// A group as it is at one moment: its topic, encoded once, and its members. An instance never changes. When a
/// member joins or leaves, <see cref="GroupTable"/> puts a new instance in its place, so whoever holds one can
/// send to its members with no lock held and nothing copied.
/// </summary>
/// <param name="topic">
/// The group's topic, already encoded and checked, so that no publish has to encode it again.
/// </param>
/// <param name="members">
/// The members.
/// </param>
internal sealed class Group(MqttTopic topic, ImmutableArray<GroupMember> members)
{
    /// <summary>
    /// Gets the group's topic, ready to send to.
    /// </summary>
    public MqttTopic Topic { get; } = topic;

    /// <summary>
    /// Gets the members; never empty, because a group with no member is taken out of the table.
    /// </summary>
    public ImmutableArray<GroupMember> Members { get; } = members;
}
