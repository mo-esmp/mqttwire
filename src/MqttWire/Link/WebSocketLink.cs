using System.Net.WebSockets;
using System.Runtime.CompilerServices;

namespace MqttWire.Link;

/// <summary>
/// An <see cref="IMqttLink"/> over a <see cref="WebSocket"/>: each write is one binary message, which holds one
/// packet or several, and incoming messages are read as one continuous stream of bytes. Once the link has started
/// its one close frame, writes are dropped: nothing may follow the close frame, and a send after it would fail
/// and make the caller abort the link during the close handshake.
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
    /// 1 once this link has started its one close frame: its own close with 1003 for a text message
    /// (<see cref="ReadAsync"/>) or the caller's (<see cref="CloseAsync"/>), whichever came first; 0 before. Taken
    /// with <see cref="Interlocked.Exchange(ref int, int)"/>, so exactly one of them sends a close frame. Once it is 1,
    /// <see cref="WriteAsync"/> sends nothing.
    /// </summary>
    private int _closeStarted;

    /// <summary>
    /// Waits with a zero-byte receive, which completes when data is available and consumes nothing but the type of
    /// the message it reports, which is remembered when it is text. An empty binary message is consumed whole by
    /// that receive and brings no bytes, so it does not end the wait: the wait goes on, and a peer cannot make the
    /// caller rent a buffer for a read that then has nothing to read. It uses the pooling builder, so a wait that
    /// suspends, as every idle wait does, allocates nothing in steady state.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels the wait.
    /// </param>
    /// <returns>
    /// A task that completes when a message has begun to arrive: a read then finds its bytes, or waits for them when
    /// the peer sent the start of a message and withholds the rest, which the link cannot tell apart. A close from
    /// the peer also completes it: the socket then records the close, and the next <see cref="ReadAsync"/> returns
    /// 0.
    /// </returns>
    [AsyncMethodBuilder(builderType: typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask WaitForDataAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            ValueWebSocketReceiveResult result =
                await webSocket.ReceiveAsync(buffer: Memory<byte>.Empty, cancellationToken: cancellationToken)
                    .ConfigureAwait(continueOnCapturedContext: false);
            if (result.MessageType == WebSocketMessageType.Text)
            {
                _textReceived = true;
                return;
            }

            // A zero-byte receive says "end of message" for a binary message only when the message is empty.
            if (result.MessageType != WebSocketMessageType.Binary || !result.EndOfMessage)
            {
                return;
            }
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
    /// The number of bytes read, or 0 when the peer closed or sent a text message (the link then closes with 1003,
    /// unless it has started a close already; see <see cref="CloseAsync"/>).
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
            await CloseForTextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
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
                    await CloseForTextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
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
    /// Sends the bytes, one packet or several, as one binary message, unless this link has started its close frame
    /// (<see cref="ReadAsync"/> for a text message, or <see cref="CloseAsync"/>): then nothing is sent and this
    /// completes at once. The read loop writes the answers it owes while a read is not complete, and a read that met
    /// a text message is not complete while its close with 1003 is being sent; nothing may follow a close frame, and
    /// a send after it fails, which would make the caller abort the link during the close handshake. A send that
    /// fails because the close started meanwhile, which the socket's own send order can put first, is dropped the
    /// same way. A send that completes at once returns the socket's task as it is, so the common write builds no
    /// state machine.
    /// </summary>
    /// <param name="packet">
    /// The encoded packets.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the write.
    /// </param>
    /// <returns>
    /// A task that completes when the message has been sent, or when the write was dropped because the link's close
    /// has started.
    /// </returns>
    /// <exception cref="WebSocketException">
    /// The socket failed while this link had not started its close.
    /// </exception>
    public ValueTask WriteAsync(ReadOnlyMemory<byte> packet, CancellationToken cancellationToken)
    {
        if (Volatile.Read(location: ref _closeStarted) != 0)
        {
            return ValueTask.CompletedTask;
        }

        ValueTask sending;
        try
        {
            sending = webSocket.SendAsync(buffer: packet, messageType: WebSocketMessageType.Binary,
                endOfMessage: true, cancellationToken: cancellationToken);
        }
        catch (Exception) when (Volatile.Read(location: ref _closeStarted) != 0)
        {
            // The close started after the check above and the socket refused the send: the write is dropped.
            return ValueTask.CompletedTask;
        }

        return sending.IsCompletedSuccessfully ? sending : AwaitSendAsync(sending: sending);
    }

    /// <summary>
    /// Starts the close of the link with the given status and does not wait for the peer's reply, so it is safe beside
    /// a pending read. This link starts at most one close frame, whichever side starts first: when its own close for
    /// a text message (<see cref="ReadAsync"/>) has started, also while that frame still waits behind a write in
    /// progress, nothing more is sent and this completes at once; the peer then sees the close with 1003, not
    /// <paramref name="status"/>. Nothing is sent either when the WebSocket is already in a state in which no close
    /// can be sent (<see cref="WebSocketState.CloseSent"/> or <see cref="WebSocketState.Closed"/>). A second close
    /// frame would fail, and a failed close makes the caller abort the link, which can reset the connection before the
    /// peer has read the first.
    /// </summary>
    /// <param name="status">
    /// The close status the peer sees, when this call is the one that starts the close.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the close.
    /// </param>
    /// <returns>
    /// A task that completes when this call's close message has been sent, or at once when it sends none.
    /// </returns>
    public async ValueTask CloseAsync(WebSocketCloseStatus status, CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _closeStarted, value: 1) != 0
            || webSocket.State is WebSocketState.CloseSent or WebSocketState.Closed)
        {
            return;
        }

        await webSocket
            .CloseOutputAsync(closeStatus: status, statusDescription: null, cancellationToken: cancellationToken)
            .ConfigureAwait(continueOnCapturedContext: false);
    }

    /// <summary>
    /// Closes the link with 1003 because the peer sent a text message, unless this link has started a close already
    /// (<see cref="CloseAsync"/> came first): then nothing more is sent, and the peer sees that close instead.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels the close.
    /// </param>
    /// <returns>
    /// A task that completes when the close message has been sent, or at once when none is sent.
    /// </returns>
    private async ValueTask CloseForTextAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _closeStarted, value: 1) != 0)
        {
            return;
        }

        await webSocket.CloseOutputAsync(closeStatus: WebSocketCloseStatus.InvalidMessageType,
                statusDescription: null, cancellationToken: cancellationToken)
            .ConfigureAwait(continueOnCapturedContext: false);
    }

    /// <summary>
    /// Awaits a send that did not complete at once, and drops its failure when this link's close started
    /// meanwhile, for the reason <see cref="WriteAsync"/> gives. It uses the pooling builder, so a send that
    /// suspends allocates nothing in steady state.
    /// </summary>
    /// <param name="sending">
    /// The socket's send.
    /// </param>
    /// <returns>
    /// A task that completes when the send has ended: with success, or with the send's failure when this link had
    /// not started its close by then.
    /// </returns>
    [AsyncMethodBuilder(builderType: typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask AwaitSendAsync(ValueTask sending)
    {
        try
        {
            await sending.ConfigureAwait(continueOnCapturedContext: false);
        }
        catch (Exception) when (Volatile.Read(location: ref _closeStarted) != 0)
        {
            // The close frame went out first and the send after it failed: the write is dropped.
        }
    }

    /// <summary>
    /// Aborts the socket.
    /// </summary>
    public void Abort() => webSocket.Abort();
}
