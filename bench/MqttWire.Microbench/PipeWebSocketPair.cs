using System.Buffers;
using System.IO.Pipelines;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;

namespace MqttWire.Microbench;

/// <summary>
/// Builds two connected <see cref="WebSocket"/> instances over in-memory pipes, with no sockets, for the benchmarks
/// that run the client over a real <see cref="WebSocket"/>. The streams under them allocate nothing in steady
/// state, so what a benchmark allocates is the WebSocket's and the client's.
/// </summary>
internal static class PipeWebSocketPair
{
    /// <summary>
    /// Creates a connected client and server socket that speak the <c>mqtt</c> sub-protocol with keep-alive off.
    /// </summary>
    /// <returns>
    /// The client socket and the server socket; what one sends the other receives.
    /// </returns>
    public static (WebSocket Client, WebSocket Server) Create()
    {
        Pipe clientToServer = new();
        Pipe serverToClient = new();
        WebSocket client = WebSocket.CreateFromStream(
            new PipeStream(serverToClient.Reader, clientToServer.Writer),
            CreateOptions(isServer: false));
        WebSocket server = WebSocket.CreateFromStream(
            new PipeStream(clientToServer.Reader, serverToClient.Writer),
            CreateOptions(isServer: true));
        return (client, server);
    }

    /// <summary>
    /// Creates the options shared by both ends.
    /// </summary>
    /// <param name="isServer">
    /// Whether the socket is the server end.
    /// </param>
    /// <returns>
    /// The creation options.
    /// </returns>
    private static WebSocketCreationOptions CreateOptions(bool isServer) => new()
    {
        IsServer = isServer,
        SubProtocol = "mqtt",
        KeepAliveInterval = TimeSpan.Zero,
    };

    /// <summary>
    /// A duplex stream that reads from one pipe and writes to another. Its asynchronous methods use the pooling
    /// builder, so a read that waits allocates nothing in steady state.
    /// </summary>
    /// <param name="reader">
    /// The pipe the stream reads from.
    /// </param>
    /// <param name="writer">
    /// The pipe the stream writes to.
    /// </param>
    private sealed class PipeStream(PipeReader reader, PipeWriter writer) : Stream
    {
        /// <summary>
        /// Gets a value indicating that the stream can be read.
        /// </summary>
        public override bool CanRead => true;

        /// <summary>
        /// Gets a value indicating that the stream cannot seek.
        /// </summary>
        public override bool CanSeek => false;

        /// <summary>
        /// Gets a value indicating that the stream can be written.
        /// </summary>
        public override bool CanWrite => true;

        /// <summary>
        /// Not supported: the stream has no length.
        /// </summary>
        public override long Length => throw new NotSupportedException();

        /// <summary>
        /// Not supported: the stream cannot seek.
        /// </summary>
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        /// <summary>
        /// Reads from the incoming pipe; a zero-byte read only waits for data or the end.
        /// </summary>
        /// <param name="buffer">
        /// Receives the bytes.
        /// </param>
        /// <param name="cancellationToken">
        /// Cancels the read.
        /// </param>
        /// <returns>
        /// The number of bytes read, or 0 when the writer of the pipe completed or the read was zero bytes.
        /// </returns>
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            while (true)
            {
                ReadResult result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                ReadOnlySequence<byte> sequence = result.Buffer;
                if (buffer.IsEmpty)
                {
                    reader.AdvanceTo(sequence.Start, sequence.Start);
                    if (!sequence.IsEmpty || result.IsCompleted || result.IsCanceled)
                    {
                        return 0;
                    }

                    continue;
                }

                if (sequence.IsEmpty)
                {
                    reader.AdvanceTo(sequence.End);
                    if (result.IsCompleted || result.IsCanceled)
                    {
                        return 0;
                    }

                    continue;
                }

                int count = (int)Math.Min(buffer.Length, sequence.Length);
                sequence.Slice(0, count).CopyTo(buffer.Span);
                reader.AdvanceTo(sequence.GetPosition(count));
                return count;
            }
        }

        /// <summary>
        /// Writes to the outgoing pipe and flushes it.
        /// </summary>
        /// <param name="buffer">
        /// The bytes to write.
        /// </param>
        /// <param name="cancellationToken">
        /// Cancels the write.
        /// </param>
        /// <returns>
        /// A task that completes when the bytes have been flushed.
        /// </returns>
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => await writer.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);

        /// <summary>
        /// Flushes the outgoing pipe.
        /// </summary>
        /// <param name="cancellationToken">
        /// Cancels the flush.
        /// </param>
        /// <returns>
        /// A task that completes when the pipe has been flushed.
        /// </returns>
        public override async Task FlushAsync(CancellationToken cancellationToken)
            => await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

        /// <summary>
        /// Not supported: reads are asynchronous.
        /// </summary>
        /// <param name="buffer">
        /// Not used.
        /// </param>
        /// <param name="offset">
        /// Not used.
        /// </param>
        /// <param name="count">
        /// Not used.
        /// </param>
        /// <returns>
        /// Never returns.
        /// </returns>
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        /// <summary>
        /// Not supported: writes are asynchronous.
        /// </summary>
        /// <param name="buffer">
        /// Not used.
        /// </param>
        /// <param name="offset">
        /// Not used.
        /// </param>
        /// <param name="count">
        /// Not used.
        /// </param>
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        /// <summary>
        /// Does nothing: use <see cref="FlushAsync(CancellationToken)"/>.
        /// </summary>
        public override void Flush()
        {
        }

        /// <summary>
        /// Not supported: the stream cannot seek.
        /// </summary>
        /// <param name="offset">
        /// Not used.
        /// </param>
        /// <param name="origin">
        /// Not used.
        /// </param>
        /// <returns>
        /// Never returns.
        /// </returns>
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        /// <summary>
        /// Not supported: the stream has no length.
        /// </summary>
        /// <param name="value">
        /// Not used.
        /// </param>
        public override void SetLength(long value) => throw new NotSupportedException();

        /// <summary>
        /// Completes the outgoing pipe, so the peer's reads end, and cancels a read pending on the incoming one.
        /// </summary>
        /// <param name="disposing">
        /// Whether the stream is being disposed rather than finalized.
        /// </param>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                reader.CancelPendingRead();
                writer.Complete();
            }

            base.Dispose(disposing);
        }
    }
}
