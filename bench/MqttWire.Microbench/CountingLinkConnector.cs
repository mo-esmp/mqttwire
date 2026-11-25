using System.Net.WebSockets;
using MqttWire.Link;

namespace MqttWire.Microbench;

/// <summary>
/// Wraps another connector so that the links it opens count their writes, to report how many WebSocket messages a
/// benchmark makes the client send.
/// </summary>
/// <param name="inner">
/// The connector that opens the real links.
/// </param>
internal sealed class CountingLinkConnector(IMqttLinkConnector inner) : IMqttLinkConnector
{
    /// <summary>
    /// The number of writes, counted over every link this connector opened.
    /// </summary>
    private long _writes;

    /// <summary>
    /// Gets the number of writes the opened links made so far.
    /// </summary>
    public long Writes => Interlocked.Read(ref _writes);

    /// <summary>
    /// Opens a link with the inner connector and wraps it in a counting link.
    /// </summary>
    /// <param name="options">
    /// The client's options.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the attempt.
    /// </param>
    /// <returns>
    /// The inner connector's result, with its link, when there is one, wrapped.
    /// </returns>
    public async ValueTask<LinkConnectResult> ConnectAsync(MqttWireClientOptions options, CancellationToken cancellationToken)
    {
        LinkConnectResult result = await inner.ConnectAsync(options, cancellationToken).ConfigureAwait(false);
        return result.Link is null ? result : result with { Link = new CountingLink(result.Link, this) };
    }

    /// <summary>
    /// Counts one write.
    /// </summary>
    private void CountWrite() => Interlocked.Increment(ref _writes);

    /// <summary>
    /// A link that forwards everything to another link and counts the writes.
    /// </summary>
    /// <param name="inner">
    /// The link that does the work.
    /// </param>
    /// <param name="owner">
    /// The connector that holds the count.
    /// </param>
    private sealed class CountingLink(IMqttLink inner, CountingLinkConnector owner) : IMqttLink
    {
        /// <summary>
        /// Forwards the wait.
        /// </summary>
        /// <param name="cancellationToken">
        /// Cancels the wait.
        /// </param>
        /// <returns>
        /// The inner link's task.
        /// </returns>
        public ValueTask WaitForDataAsync(CancellationToken cancellationToken) => inner.WaitForDataAsync(cancellationToken);

        /// <summary>
        /// Forwards the read.
        /// </summary>
        /// <param name="buffer">
        /// Receives the bytes.
        /// </param>
        /// <param name="cancellationToken">
        /// Cancels the read.
        /// </param>
        /// <returns>
        /// The inner link's task.
        /// </returns>
        public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken) => inner.ReadAsync(buffer, cancellationToken);

        /// <summary>
        /// Counts the write and forwards it.
        /// </summary>
        /// <param name="packet">
        /// The encoded packet.
        /// </param>
        /// <param name="cancellationToken">
        /// Cancels the write.
        /// </param>
        /// <returns>
        /// The inner link's task.
        /// </returns>
        public ValueTask WriteAsync(ReadOnlyMemory<byte> packet, CancellationToken cancellationToken)
        {
            owner.CountWrite();
            return inner.WriteAsync(packet, cancellationToken);
        }

        /// <summary>
        /// Forwards the close.
        /// </summary>
        /// <param name="status">
        /// The close status.
        /// </param>
        /// <param name="cancellationToken">
        /// Cancels the close.
        /// </param>
        /// <returns>
        /// The inner link's task.
        /// </returns>
        public ValueTask CloseAsync(WebSocketCloseStatus status, CancellationToken cancellationToken) => inner.CloseAsync(status, cancellationToken);

        /// <summary>
        /// Forwards the abort.
        /// </summary>
        public void Abort() => inner.Abort();
    }
}
