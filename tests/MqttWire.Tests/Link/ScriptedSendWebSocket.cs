using System.Net.WebSockets;

namespace MqttWire.Tests.Link;

/// <summary>
/// A <see cref="WebSocket"/> whose sends do what the test scripts in <see cref="Send"/>, such as stay pending until the
/// test fails them or throw at once, and whose close frame goes out at once. It lets a test put the start of the
/// link's close and the failure of a send in an order of its choosing; it does not model what a real socket does.
/// </summary>
internal sealed class ScriptedSendWebSocket : WebSocket
{
    /// <summary>
    /// Gets or sets what a send does; by default it completes at once.
    /// </summary>
    public Func<ValueTask> Send { get; set; } = () => ValueTask.CompletedTask;

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
    /// Counts the send and does what <see cref="Send"/> scripts.
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
    /// The task <see cref="Send"/> returns.
    /// </returns>
    /// <exception cref="Exception">
    /// Whatever <see cref="Send"/> throws.
    /// </exception>
    public override ValueTask SendAsync(ReadOnlyMemory<byte> buffer, WebSocketMessageType messageType,
        bool endOfMessage, CancellationToken cancellationToken)
    {
        Sends++;
        return Send();
    }

    /// <summary>
    /// Counts the send and does what <see cref="Send"/> scripts.
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
    /// The task <see cref="Send"/> returns.
    /// </returns>
    /// <exception cref="Exception">
    /// Whatever <see cref="Send"/> throws.
    /// </exception>
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
