namespace GroupChat.Server;

/// <summary>
/// What the chat keeps for one connection, in the connection's state slot: the member's name and the groups it
/// joined.
/// </summary>
/// <remarks>
/// The server calls the handler for one connection one call at a time, and <c>OnClosed</c> after the last of them,
/// so nothing here needs a lock.
/// </remarks>
/// <param name="name">
/// The member's name, which is the user name of its CONNECT.
/// </param>
internal sealed class ChatMember(string name)
{
    /// <summary>
    /// Gets the member's name.
    /// </summary>
    public string Name { get; } = name;

    /// <summary>
    /// Gets the topics of the groups the member joined, which are the only topics it may publish to.
    /// </summary>
    public HashSet<string> Groups { get; } = new(StringComparer.Ordinal);
}
