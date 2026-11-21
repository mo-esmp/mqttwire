namespace MqttWire.Link;

/// <summary>
/// Opens the link a client connection runs over.
/// </summary>
internal interface IMqttLinkConnector
{
    /// <summary>
    /// Opens a link to the server the options name.
    /// </summary>
    /// <param name="options">
    /// The client's options: the URI and the callback that configures the WebSocket.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the attempt; the caller's token and the connect deadline together.
    /// </param>
    /// <returns>
    /// The open link, or the network error that kept it from opening. A failure to reach the server is returned,
    /// never thrown.
    /// </returns>
    /// <exception cref="OperationCanceledException">
    /// The token was cancelled. Nothing is left open.
    /// </exception>
    /// <remarks>
    /// An exception from <see cref="MqttWireClientOptions.ConfigureWebSocket"/> propagates to the caller, since it
    /// is the application's own code that failed. Nothing is left open.
    /// </remarks>
    ValueTask<LinkConnectResult> ConnectAsync(MqttWireClientOptions options, CancellationToken cancellationToken);
}
