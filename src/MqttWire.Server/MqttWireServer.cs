using System.Diagnostics.CodeAnalysis;

namespace MqttWire.Server;

/// <summary>
/// The running server: lets the host look up the connections that are open. It has no public constructor; the
/// host gets it from dependency injection.
/// </summary>
public sealed class MqttWireServer
{
    /// <summary>
    /// Creates the server.
    /// </summary>
    internal MqttWireServer()
    {
    }

    /// <summary>
    /// Gets the number of open connections.
    /// </summary>
#pragma warning disable S2325 // The shell has no state yet; the next task makes these instance members in earnest.
    public int ConnectionCount => 0;

    /// <summary>
    /// Looks up an open connection by client identifier.
    /// </summary>
    /// <param name="clientId">
    /// The client identifier.
    /// </param>
    /// <param name="connection">
    /// The connection when found; otherwise <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when a connection with this client identifier is open; <see langword="false"/>
    /// otherwise.
    /// </returns>
    public bool TryGetConnection(string clientId, [NotNullWhen(true)] out MqttWireConnection? connection)
    {
        connection = null;

        return false;
    }
#pragma warning restore S2325
}
