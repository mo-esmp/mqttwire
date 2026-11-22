using System.Net.WebSockets;
using System.Runtime.CompilerServices;

namespace MqttWire.Link;

/// <summary>
/// An <see cref="IMqttLink"/> over a <see cref="WebSocket"/>: each packet is one binary message, and incoming
/// messages are read as one continuous stream of bytes.
/// </summary>
/// <param name="webSocket">
/// The connected socket, which this link uses but does not dispose.
/// </param>
internal sealed class WebSocketLink(WebSocket webSocket) : IMqttLink
{
    /// <summary>
    /// Whether the zero-byte receive of <see cref="WaitForDataAsync"/> reported a text message. An empty text
    /// message is consumed whole by that receive, so the next read would see only what follows it; the next
    /// <see cref="ReadAsync"/> closes with 1003 instead.
    /// </summary>
    private bool _textReceived;

    /// <summary>
    /// Waits with a zero-byte receive, which completes when data is available and consumes nothing but the type of
    /// the message it reports, which is remembered when it is text. It uses the pooling builder, so a wait that
    /// suspends, as every idle wait does, allocates nothing in steady state.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels the wait.
    /// </param>
    /// <returns>
    /// A task that completes when a read would not wait. A close from the peer also completes it: the socket then
    /// records the close, and the next <see cref="ReadAsync"/> returns 0.
    /// </returns>
    [AsyncMethodBuilder(builderType: typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask WaitForDataAsync(CancellationToken cancellationToken)
    {
        ValueWebSocketReceiveResult result =
            await webSocket.ReceiveAsync(buffer: Memory<byte>.Empty, cancellationToken: cancellationToken)
                .ConfigureAwait(continueOnCapturedContext: false);
        if (result.MessageType == WebSocketMessageType.Text)
        {
            _textReceived = true;
        }
    }

    /// <summary>
    /// Reads the next bytes, skipping empty frames. It uses the pooling builder, so a read that suspends allocates
    /// nothing in steady state.
    /// </summary>
    /// <param name="buffer">
    /// Receives the bytes.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the read.
    /// </param>
    /// <returns>
    /// The number of bytes read, or 0 when the peer closed or sent a text message (the link then closes with 1003).
    /// A close the peer sent is not answered here: the caller answers it with <see cref="CloseAsync"/>.
    /// </returns>
    /// <exception cref="WebSocketException">
    /// The socket failed.
    /// </exception>
    [AsyncMethodBuilder(builderType: typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        if (_textReceived)
        {
            await webSocket.CloseOutputAsync(closeStatus: WebSocketCloseStatus.InvalidMessageType,
                    statusDescription: null, cancellationToken: cancellationToken)
                .ConfigureAwait(continueOnCapturedContext: false);
            return 0;
        }

        while (true)
        {
            if (webSocket.State is WebSocketState.CloseReceived or WebSocketState.Closed)
            {
                // The peer's close was already taken, by the zero-byte receive of WaitForDataAsync or by an earlier
                // read; receiving again would throw instead of reporting the end.
                return 0;
            }

            ValueWebSocketReceiveResult result =
                await webSocket.ReceiveAsync(buffer: buffer, cancellationToken: cancellationToken)
                    .ConfigureAwait(continueOnCapturedContext: false);

            switch (result.MessageType)
            {
                case WebSocketMessageType.Close:
                    return 0;
                case WebSocketMessageType.Text:
                    await webSocket.CloseOutputAsync(closeStatus: WebSocketCloseStatus.InvalidMessageType,
                            statusDescription: null, cancellationToken: cancellationToken)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    return 0;
                default:
                    if (result.Count > 0 || buffer.IsEmpty)
                    {
                        return result.Count;
                    }

                    break;
            }
        }
    }

    /// <summary>
    /// Sends the packet as one binary message.
    /// </summary>
    /// <param name="packet">
    /// The encoded packet.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the write.
    /// </param>
    /// <returns>
    /// A task that completes when the message has been sent.
    /// </returns>
    public ValueTask WriteAsync(ReadOnlyMemory<byte> packet, CancellationToken cancellationToken)
        => webSocket.SendAsync(buffer: packet, messageType: WebSocketMessageType.Binary, endOfMessage: true,
            cancellationToken: cancellationToken);

    /// <summary>
    /// Sends a close message and does not wait for the peer's reply, so it is safe beside a pending read. When a close
    /// message has already gone out, which is what <see cref="ReadAsync"/> does for a text message, nothing is sent
    /// and the close completes at once: a WebSocket sends one close message, and a second would fail, which would make
    /// the caller abort the link and could reset the connection before the peer has read the first.
    /// </summary>
    /// <param name="status">
    /// The close status the peer sees; ignored when a close message has already gone out.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the close.
    /// </param>
    /// <returns>
    /// A task that completes when the close message has been sent, or at once when one had been.
    /// </returns>
    public async ValueTask CloseAsync(WebSocketCloseStatus status, CancellationToken cancellationToken)
    {
        if (webSocket.State is WebSocketState.CloseSent or WebSocketState.Closed)
        {
            return;
        }

        await webSocket
            .CloseOutputAsync(closeStatus: status, statusDescription: null, cancellationToken: cancellationToken)
            .ConfigureAwait(continueOnCapturedContext: false);
    }

    /// <summary>
    /// Aborts the socket.
    /// </summary>
    public void Abort() => webSocket.Abort();
}
