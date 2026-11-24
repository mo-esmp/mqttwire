using System.Net.WebSockets;

namespace MqttWire.Tests.Link;

/// <summary>
/// A <see cref="WebSocket"/> whose sends wait until the test ends them, with success or a failure, and whose close
/// frame goes out at once. It lets a test start the close of a link while a send that passed the link's checks is
/// still pending, the order the socket's own send lock can produce but a real socket does not let a test hold.
/// </summary>
internal sealed class HeldSendWebSocket : WebSocket
{
    /// <summary>
    /// The pending send, which <see cref="FailSend"/> ends.
    /// </summary>
    private readonly TaskCompletionSource _send = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Gets the number of sends started.
    /// </summary>
    public int Sends { get; private set; }

    /// <summary>
    /// Gets the number of close frames sent.
    /// </summary>
    public int Closes { get; private set; }

    /// <summary>
    /// Gets no close status: the peer never closes this socket.
    /// </summary>
    public override WebSocketCloseStatus? CloseStatus => null;

    /// <summary>
    /// Gets no close description: the peer never closes this socket.
    /// </summary>
    public override string? CloseStatusDescription => null;

    /// <summary>
    /// Gets <see cref="WebSocketState.Open"/>, so the link sends its close frame.
    /// </summary>
    public override WebSocketState State => WebSocketState.Open;

    /// <summary>
    /// Gets the <c>mqtt</c> sub-protocol.
    /// </summary>
    public override string SubProtocol => "mqtt";

    /// <summary>
    /// Ends the pending send with the given failure.
    /// </summary>
    /// <param name="exception">
    /// The failure the send ends with.
    /// </param>
    public void FailSend(Exception exception) => _send.SetException(exception);

    /// <summary>
    /// Starts a send that waits until <see cref="FailSend"/> ends it.
    /// </summary>
    /// <param name="buffer">
    /// The bytes, which are not kept.
    /// </param>
    /// <param name="messageType">
    /// The message type, which is not checked.
    /// </param>
    /// <param name="endOfMessage">
    /// Whether the message ends, which is not checked.
    /// </param>
    /// <param name="cancellationToken">
    /// Not observed.
    /// </param>
    /// <returns>
    /// The pending send.
    /// </returns>
    public override ValueTask SendAsync(ReadOnlyMemory<byte> buffer, WebSocketMessageType messageType,
        bool endOfMessage, CancellationToken cancellationToken)
    {
        Sends++;
        return new ValueTask(_send.Task);
    }

    /// <summary>
    /// Starts a send that waits until <see cref="FailSend"/> ends it.
    /// </summary>
    /// <param name="buffer">
    /// The bytes, which are not kept.
    /// </param>
    /// <param name="messageType">
    /// The message type, which is not checked.
    /// </param>
    /// <param name="endOfMessage">
    /// Whether the message ends, which is not checked.
    /// </param>
    /// <param name="cancellationToken">
    /// Not observed.
    /// </param>
    /// <returns>
    /// The pending send.
    /// </returns>
    public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage,
        CancellationToken cancellationToken) => SendAsync(buffer.AsMemory(), messageType, endOfMessage, cancellationToken).AsTask();

    /// <summary>
    /// Sends the close frame at once.
    /// </summary>
    /// <param name="closeStatus">
    /// The close status, which is not kept.
    /// </param>
    /// <param name="statusDescription">
    /// The description, which is not kept.
    /// </param>
    /// <param name="cancellationToken">
    /// Not observed.
    /// </param>
    /// <returns>
    /// A completed task.
    /// </returns>
    public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription,
        CancellationToken cancellationToken)
    {
        Closes++;
        return Task.CompletedTask;
    }

    /// <summary>
    /// Not supported: the link never calls it.
    /// </summary>
    /// <param name="closeStatus">
    /// The close status.
    /// </param>
    /// <param name="statusDescription">
    /// The description.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the close.
    /// </param>
    /// <returns>
    /// Never returns.
    /// </returns>
    /// <exception cref="NotSupportedException">
    /// Always.
    /// </exception>
    public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription,
        CancellationToken cancellationToken) => throw new NotSupportedException();

    /// <summary>
    /// Not supported: the tests that use this socket do not read.
    /// </summary>
    /// <param name="buffer">
    /// Receives the bytes.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the receive.
    /// </param>
    /// <returns>
    /// Never returns.
    /// </returns>
    /// <exception cref="NotSupportedException">
    /// Always.
    /// </exception>
    public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer,
        CancellationToken cancellationToken) => throw new NotSupportedException();

    /// <summary>
    /// Does nothing: nothing runs under this socket.
    /// </summary>
    public override void Abort()
    {
    }

    /// <summary>
    /// Does nothing: nothing runs under this socket.
    /// </summary>
    public override void Dispose()
    {
    }
}
