using System.Net.WebSockets;

namespace MqttWire;

/// <summary>
/// The options of an <see cref="MqttWireClient"/>. They are checked when the client is created.
/// </summary>
public sealed class MqttWireClientOptions
{
    /// <summary>
    /// Gets the address of the server: an absolute <c>ws</c> or <c>wss</c> URI.
    /// </summary>
    public required Uri Uri { get; init; }

    /// <summary>
    /// Gets the client identifier: not empty, at most 65,535 bytes in UTF-8, and free of U+0000.
    /// </summary>
    public required string ClientId { get; init; }

    /// <summary>
    /// Gets the user name sent in CONNECT, or <see langword="null"/> for none.
    /// </summary>
    public string? UserName { get; init; }

    /// <summary>
    /// Gets the password sent in CONNECT, as UTF-8, or <see langword="null"/> for none.
    /// </summary>
    public string? Password { get; init; }

    /// <summary>
    /// Gets the keep-alive interval, from 1 to 65,535 seconds. A fraction is rounded up to whole seconds.
    /// </summary>
    public TimeSpan KeepAlive { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Gets the largest packet the client accepts, from 4,096 to 268,435,460 bytes. Announced to the server.
    /// </summary>
    public int MaxIncomingPacketBytes { get; init; } = 1_048_576;

    /// <summary>
    /// Gets a callback that configures the WebSocket before it connects, or <see langword="null"/> for none.
    /// </summary>
    public Action<ClientWebSocketOptions>? ConfigureWebSocket { get; init; }

    /// <summary>
    /// Gets the handler for received messages, or <see langword="null"/> to ignore them.
    /// </summary>
    public MqttMessageHandler? OnMessage { get; init; }

    /// <summary>
    /// Gets the callback for the end of a connection that was reported as connected, or <see langword="null"/>.
    /// </summary>
    public Action<MqttClientCloseInfo>? OnClosed { get; init; }
}
