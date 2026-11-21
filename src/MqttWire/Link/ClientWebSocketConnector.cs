using System.Net.WebSockets;

namespace MqttWire.Link;

/// <summary>
/// Opens a <see cref="WebSocketLink"/> over a <see cref="ClientWebSocket"/> with the <c>mqtt</c> sub-protocol.
/// </summary>
internal sealed class ClientWebSocketConnector : IMqttLinkConnector
{
    /// <summary>
    /// The WebSocket sub-protocol of MQTT.
    /// </summary>
    private const string SubProtocol = "mqtt";

    /// <summary>
    /// Creates the connector; there is one, <see cref="Instance"/>.
    /// </summary>
    private ClientWebSocketConnector()
    {
    }

    /// <summary>
    /// Gets the connector.
    /// </summary>
    public static ClientWebSocketConnector Instance { get; } = new();

    /// <summary>
    /// Creates a <see cref="ClientWebSocket"/>, runs <see cref="MqttWireClientOptions.ConfigureWebSocket"/>, then
    /// asks for the <c>mqtt</c> sub-protocol, turns the WebSocket's own keep-alive off (MQTT has its own) and
    /// connects.
    /// </summary>
    /// <param name="options">
    /// The client's options.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the connect.
    /// </param>
    /// <returns>
    /// The open link; or, with no link, the <see cref="WebSocketException"/> of a server that could not be reached
    /// or did not answer with the <c>mqtt</c> sub-protocol. Such a server's socket is closed and disposed.
    /// </returns>
    /// <exception cref="OperationCanceledException">
    /// The token was cancelled. The socket is disposed.
    /// </exception>
    /// <remarks>
    /// An exception from <see cref="MqttWireClientOptions.ConfigureWebSocket"/> propagates after the socket is
    /// disposed; nothing is dialled. The returned link does not dispose the socket: the client ends every link with
    /// <see cref="IMqttLink.Abort"/>, which disposes it.
    /// </remarks>
    public async ValueTask<LinkConnectResult> ConnectAsync(MqttWireClientOptions options,
        CancellationToken cancellationToken)
    {
        ClientWebSocket socket = new();
        try
        {
            options.ConfigureWebSocket?.Invoke(obj: socket.Options);
            socket.Options.AddSubProtocol(subProtocol: SubProtocol);

            // The browser manages its own WebSocket keep-alive and does not let it be set.
            if (!OperatingSystem.IsBrowser())
            {
                socket.Options.KeepAliveInterval = TimeSpan.Zero;
            }
        }
        catch
        {
            socket.Dispose();
            throw;
        }

        try
        {
            await socket.ConnectAsync(uri: options.Uri, cancellationToken: cancellationToken)
                .ConfigureAwait(continueOnCapturedContext: false);
        }
        catch (WebSocketException ex)
        {
            socket.Dispose();
            return new LinkConnectResult(Link: null, Error: ex);
        }
        catch
        {
            socket.Dispose();
            throw;
        }

        if (string.Equals(a: socket.SubProtocol, b: SubProtocol, comparisonType: StringComparison.Ordinal))
        {
            return new LinkConnectResult(Link: new WebSocketLink(webSocket: socket), Error: null);
        }

        await RefuseAsync(socket: socket, cancellationToken: cancellationToken)
            .ConfigureAwait(continueOnCapturedContext: false);
        return new LinkConnectResult(
            Link: null,
            Error: new WebSocketException(error: WebSocketError.UnsupportedProtocol,
                message: "The server did not answer with the mqtt sub-protocol."));
    }

    /// <summary>
    /// Closes and disposes the socket of a server that did not answer with the <c>mqtt</c> sub-protocol.
    /// </summary>
    /// <param name="socket">
    /// The connected socket.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the close.
    /// </param>
    /// <returns>
    /// A task that completes when the socket is disposed.
    /// </returns>
    /// <exception cref="OperationCanceledException">
    /// The token was cancelled. The socket is disposed.
    /// </exception>
    private static async ValueTask RefuseAsync(ClientWebSocket socket, CancellationToken cancellationToken)
    {
        try
        {
            await socket.CloseOutputAsync(closeStatus: WebSocketCloseStatus.ProtocolError,
                    statusDescription: "The mqtt sub-protocol is required.",
                    cancellationToken: cancellationToken)
                .ConfigureAwait(continueOnCapturedContext: false);
        }
        catch (WebSocketException)
        {
            // The socket failed while closing; disposing it below is all that is left to do.
        }
        finally
        {
            socket.Dispose();
        }
    }
}
