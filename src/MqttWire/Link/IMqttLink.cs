using System.Net.WebSockets;

namespace MqttWire.Link;

/// <summary>
/// The transport below the MQTT code: a stream of bytes in, and one binary message per write out, which holds one
/// MQTT packet or several.
/// </summary>
/// <remarks>
/// One reader and one writer at a time. Read and write failures surface as the exceptions the
/// underlying transport throws; the caller turns them into a reason.
/// </remarks>
internal interface IMqttLink
{
    /// <summary>
    /// Waits until a read would not wait, holding no buffer while it waits.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels the wait.
    /// </param>
    /// <returns>
    /// A task that completes when data has arrived or the link has ended; a following read then completes at once.
    /// </returns>
    ValueTask WaitForDataAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Reads the next bytes into the buffer.
    /// </summary>
    /// <param name="buffer">
    /// Receives the bytes.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the read.
    /// </param>
    /// <returns>
    /// The number of bytes read, or 0 when the link has ended: the peer closed, or a text message arrived and
    /// the link has already closed with <see cref="WebSocketCloseStatus.InvalidMessageType"/>.
    /// </returns>
    ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken);

    /// <summary>
    /// Writes the bytes as one binary message. One write can hold several MQTT packets, one after the other, as the
    /// answers the read loop writes together do; the peer reads them as one stream of bytes.
    /// </summary>
    /// <param name="packet">
    /// The encoded packet, or several encoded packets one after the other; it must stay valid until the returned
    /// task completes.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the write.
    /// </param>
    /// <returns>
    /// A task that completes when the bytes have been handed to the transport, or, once the link's own close has
    /// started, without sending them: nothing may follow a close frame, so such a link drops the write and reports
    /// success.
    /// </returns>
    ValueTask WriteAsync(ReadOnlyMemory<byte> packet, CancellationToken cancellationToken);

    /// <summary>
    /// Closes the sending side of the link with the given status.
    /// </summary>
    /// <param name="status">
    /// The close status the peer sees.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the close.
    /// </param>
    /// <returns>
    /// A task that completes when the close has been sent.
    /// </returns>
    ValueTask CloseAsync(WebSocketCloseStatus status, CancellationToken cancellationToken);

    /// <summary>
    /// Ends the link at once, without a close handshake; a pending read or wait ends with an exception.
    /// </summary>
    void Abort();
}
