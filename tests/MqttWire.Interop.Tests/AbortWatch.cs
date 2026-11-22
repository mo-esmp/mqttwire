using System.Net.WebSockets;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;

namespace MqttWire.Interop.Tests;

/// <summary>
/// Watches the aborts of the WebSockets an application accepts: it wraps each accepted WebSocket, counts the calls of
/// <see cref="WebSocket.Abort"/>, and marks the thread that makes one for exactly as long as the call lasts. So a test
/// can ask, from any code the server runs, whether that code runs inside an abort, on the thread that aborts,
/// without looking at a stack trace, whose frames depend on what the JIT inlines.
/// </summary>
internal sealed class AbortWatch
{
    /// <summary>
    /// The watch whose WebSocket the current thread is aborting, or <see langword="null"/> when it aborts none.
    /// </summary>
    [ThreadStatic]
    private static AbortWatch? t_aborting;

    /// <summary>
    /// The number of aborts so far.
    /// </summary>
    private int _aborts;

    /// <summary>
    /// Gets the number of calls of <see cref="WebSocket.Abort"/> on the WebSockets this watch wraps.
    /// </summary>
    public int Aborts => Volatile.Read(ref _aborts);

    /// <summary>
    /// Gets a value indicating whether the current thread is inside a call of <see cref="WebSocket.Abort"/> on one
    /// of the WebSockets this watch wraps.
    /// </summary>
    public bool IsCurrentThreadAborting => ReferenceEquals(t_aborting, this);

    /// <summary>
    /// Makes an application wrap every WebSocket it accepts from here on. Call it after <c>UseWebSockets()</c> and
    /// before the endpoint is reached.
    /// </summary>
    /// <param name="app">
    /// The application.
    /// </param>
    public void Use(WebApplication app) =>
        app.Use((HttpContext context, RequestDelegate next) =>
        {
            if (context.Features.Get<IHttpWebSocketFeature>() is { } feature)
            {
                context.Features.Set<IHttpWebSocketFeature>(new WatchedFeature(feature, this));
            }

            return next(context);
        });

    /// <summary>
    /// Aborts a WebSocket with the current thread marked as aborting for as long as the call lasts.
    /// </summary>
    /// <param name="webSocket">
    /// The WebSocket to abort.
    /// </param>
    private void Abort(WebSocket webSocket)
    {
        Interlocked.Increment(ref _aborts);
        AbortWatch? outer = t_aborting;
        t_aborting = this;
        try
        {
            webSocket.Abort();
        }
        finally
        {
            t_aborting = outer;
        }
    }

    /// <summary>
    /// The WebSocket feature of a request, accepting through the application's own and wrapping what it accepts.
    /// </summary>
    /// <param name="inner">
    /// The application's feature.
    /// </param>
    /// <param name="watch">
    /// The watch the accepted WebSocket reports to.
    /// </param>
    private sealed class WatchedFeature(IHttpWebSocketFeature inner, AbortWatch watch) : IHttpWebSocketFeature
    {
        /// <summary>
        /// Gets a value indicating whether the request is a WebSocket request, as the application's feature says.
        /// </summary>
        public bool IsWebSocketRequest => inner.IsWebSocketRequest;

        /// <summary>
        /// Accepts the WebSocket through the application's feature and wraps it.
        /// </summary>
        /// <param name="context">
        /// How to accept.
        /// </param>
        /// <returns>
        /// The wrapped WebSocket.
        /// </returns>
        public async Task<WebSocket> AcceptAsync(WebSocketAcceptContext context) =>
            new WatchedWebSocket(await inner.AcceptAsync(context).ConfigureAwait(false), watch);
    }

    /// <summary>
    /// A WebSocket that hands every call on to another, and its abort through the watch.
    /// </summary>
    /// <param name="inner">
    /// The WebSocket the application accepted.
    /// </param>
    /// <param name="watch">
    /// The watch.
    /// </param>
    private sealed class WatchedWebSocket(WebSocket inner, AbortWatch watch) : WebSocket
    {
        /// <summary>
        /// Gets the close status the peer sent, as the inner WebSocket has it.
        /// </summary>
        public override WebSocketCloseStatus? CloseStatus => inner.CloseStatus;

        /// <summary>
        /// Gets the close description the peer sent, as the inner WebSocket has it.
        /// </summary>
        public override string? CloseStatusDescription => inner.CloseStatusDescription;

        /// <summary>
        /// Gets the state of the inner WebSocket.
        /// </summary>
        public override WebSocketState State => inner.State;

        /// <summary>
        /// Gets the sub-protocol of the inner WebSocket.
        /// </summary>
        public override string? SubProtocol => inner.SubProtocol;

        /// <summary>
        /// Aborts the inner WebSocket, with the calling thread marked as aborting meanwhile.
        /// </summary>
        public override void Abort() => watch.Abort(inner);

        /// <summary>
        /// Closes the inner WebSocket.
        /// </summary>
        /// <param name="closeStatus">
        /// The close status.
        /// </param>
        /// <param name="statusDescription">
        /// The close description.
        /// </param>
        /// <param name="cancellationToken">
        /// Cancels the close.
        /// </param>
        /// <returns>
        /// The inner WebSocket's task.
        /// </returns>
        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription,
            CancellationToken cancellationToken) => inner.CloseAsync(closeStatus, statusDescription, cancellationToken);

        /// <summary>
        /// Closes the sending side of the inner WebSocket.
        /// </summary>
        /// <param name="closeStatus">
        /// The close status.
        /// </param>
        /// <param name="statusDescription">
        /// The close description.
        /// </param>
        /// <param name="cancellationToken">
        /// Cancels the close.
        /// </param>
        /// <returns>
        /// The inner WebSocket's task.
        /// </returns>
        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription,
            CancellationToken cancellationToken) =>
            inner.CloseOutputAsync(closeStatus, statusDescription, cancellationToken);

        /// <summary>
        /// Disposes the inner WebSocket.
        /// </summary>
        public override void Dispose() => inner.Dispose();

        /// <summary>
        /// Receives from the inner WebSocket.
        /// </summary>
        /// <param name="buffer">
        /// Receives the bytes.
        /// </param>
        /// <param name="cancellationToken">
        /// Cancels the receive.
        /// </param>
        /// <returns>
        /// The inner WebSocket's task.
        /// </returns>
        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer,
            CancellationToken cancellationToken) => inner.ReceiveAsync(buffer, cancellationToken);

        /// <summary>
        /// Receives from the inner WebSocket.
        /// </summary>
        /// <param name="buffer">
        /// Receives the bytes.
        /// </param>
        /// <param name="cancellationToken">
        /// Cancels the receive.
        /// </param>
        /// <returns>
        /// The inner WebSocket's task.
        /// </returns>
        public override ValueTask<ValueWebSocketReceiveResult> ReceiveAsync(Memory<byte> buffer,
            CancellationToken cancellationToken) => inner.ReceiveAsync(buffer, cancellationToken);

        /// <summary>
        /// Sends through the inner WebSocket.
        /// </summary>
        /// <param name="buffer">
        /// The bytes.
        /// </param>
        /// <param name="messageType">
        /// The message's type.
        /// </param>
        /// <param name="endOfMessage">
        /// Whether the message ends with these bytes.
        /// </param>
        /// <param name="cancellationToken">
        /// Cancels the send.
        /// </param>
        /// <returns>
        /// The inner WebSocket's task.
        /// </returns>
        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage,
            CancellationToken cancellationToken) => inner.SendAsync(buffer, messageType, endOfMessage, cancellationToken);

        /// <summary>
        /// Sends through the inner WebSocket.
        /// </summary>
        /// <param name="buffer">
        /// The bytes.
        /// </param>
        /// <param name="messageType">
        /// The message's type.
        /// </param>
        /// <param name="endOfMessage">
        /// Whether the message ends with these bytes.
        /// </param>
        /// <param name="cancellationToken">
        /// Cancels the send.
        /// </param>
        /// <returns>
        /// The inner WebSocket's task.
        /// </returns>
        public override ValueTask SendAsync(ReadOnlyMemory<byte> buffer, WebSocketMessageType messageType,
            bool endOfMessage, CancellationToken cancellationToken) =>
            inner.SendAsync(buffer, messageType, endOfMessage, cancellationToken);
    }
}
