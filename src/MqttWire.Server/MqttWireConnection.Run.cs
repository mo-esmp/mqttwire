using System.Diagnostics;
using System.Net.WebSockets;
using Microsoft.AspNetCore.Http;
using MqttWire.Link;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// The run of a connection: the accept, the connect phase, the loop and the end.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// The WebSocket sub-protocol of MQTT.
    /// </summary>
    internal const string SubProtocol = "mqtt";

    /// <summary>
    /// Gets the link, for the code that runs after <see cref="RunAsync"/> has set it.
    /// </summary>
    private IMqttLink RunningLink =>
        _link!; // RunAsync sets _link before the connection takes its slot, and only code that runs after reads this.

    /// <summary>
    /// Runs the connection from its accept until it is Closed. It is the only async method whose state lives as
    /// long as the connection (plan decision 3), so it keeps as few locals alive across its awaits as it can: it
    /// reads the link from <see cref="_link"/>, holds no buffer while it waits for data, and hands what arrives to
    /// <see cref="HandleAvailableAsync"/>, whose state goes back to a pool. The connect phase is a separate method
    /// that has completed before the loop starts.
    /// </summary>
    /// <param name="link">
    /// The link to run over, or <see langword="null"/> to accept the WebSocket of the request: sub-protocol
    /// <c>mqtt</c>, no WebSocket keep-alive and no compression.
    /// </param>
    /// <returns>
    /// A task that completes when the connection is Closed. It does not fail for anything the client does; it
    /// fails only when the WebSocket cannot be accepted, before the connection takes its slot.
    /// </returns>
    internal async Task RunAsync(IMqttLink? link)
    {
        _link = link ?? await AcceptWebSocketAsync().ConfigureAwait(false);
        Volatile.Write(ref _stateEnteredAt, _server.Time.GetTimestamp());
        if (_server.Registry.Add(this))
        {
            // The server's stop had begun when the slot was taken, so its walk may have missed this connection: it
            // ends itself, Accepted, with its link aborted, and the read below fails at once.
            BeginStop();
        }

        try
        {
            bool open = await ConnectPhaseAsync().ConfigureAwait(false);
            while (open)
            {
                // Bytes that came behind the CONNECT are in the buffer already; otherwise wait, holding nothing.
                if (!_readBuffer.IsHeld)
                {
                    await RunningLink.WaitForDataAsync(HttpContext.RequestAborted).ConfigureAwait(false);
                }

                open = await HandleAvailableAsync().ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
            // The wait or a read failed: the link was aborted or lost. A reason recorded first is kept.
            BeginClose(CloseCause.ConnectionLost, MqttReasonCode.Success, reasonString: null);
        }
        finally
        {
            await FinishAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Ends the connection because the server is stopping (design 6.6, "When the host stops"), as its state calls
    /// for: an Accepted one, which has sent nothing that could be answered, is aborted; an Authenticating one has its
    /// host's token cancelled through the thread pool, as at the authentication deadline, so no host code runs on the
    /// stopping thread, and its connect phase then answers CONNACK <c>0x8B</c> (see <see cref="RefuseUnanswered"/>);
    /// an Active one begins its close with DISCONNECT <c>0x8B</c>; a Closing or Closed one is left as it is. A
    /// connection that moves on while this looks at it is looked at again in its new state. An Authenticating
    /// connection whose host has answered already is refused by the admission step, which sees the server stopping.
    /// Called by <see cref="MqttWireServer.StopAsync"/>, after <see cref="MqttWireServer.IsStopping"/> was set, and by
    /// the run of a connection that took its slot after that; safe from any thread, and never throws.
    /// </summary>
    internal void BeginStop()
    {
        while (true)
        {
            ConnectionState state = _state;
            if (state == ConnectionState.Accepted)
            {
                if (AbortFromAccepted(CloseCause.ServerStopping))
                {
                    return;
                }

                // The CONNECT came meanwhile, or another close began: look again.
                continue;
            }

            if (state == ConnectionState.Authenticating)
            {
                // The source is in its field before the move to Authenticating; when it is gone, the host has
                // answered and the admission step refuses.
                CancelAuthentication();
            }
            else if (state == ConnectionState.Active)
            {
                BeginClose(CloseCause.ServerStopping, MqttReasonCode.ServerShuttingDown, reasonString: null);
            }

            return;
        }
    }

    /// <summary>
    /// Accepts the WebSocket of the request, with the sub-protocol <c>mqtt</c>, the WebSocket's own keep-alive
    /// turned off (MQTT has its own, and this saves a timer per connection) and no compression.
    /// </summary>
    /// <returns>
    /// The link over the accepted WebSocket.
    /// </returns>
    private async Task<IMqttLink> AcceptWebSocketAsync()
    {
        WebSocketAcceptContext accept = new()
        {
            SubProtocol = SubProtocol, KeepAliveInterval = TimeSpan.Zero, DangerousEnableCompression = false,
        };
        WebSocket webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync(accept).ConfigureAwait(false);
        return new WebSocketLink(webSocket);
    }

    /// <summary>
    /// Ends the run once the send path is idle (design 6.6, "Closing, bounded", step 5: the connection is Closed
    /// when the read loop has ended and no write is in progress). The read loop has ended and the close has begun,
    /// so the send path, once idle, stays idle: nothing is added to a Closing connection, and only the call that
    /// began the close could have taken the path. When nothing is in progress, which is the common case, this
    /// completes at once and allocates nothing. Otherwise it waits for whoever holds the send path to give it up,
    /// which that holder does when its write, or its close, has ended and its buffer is back. The wait is bounded by
    /// the sweep: the close deadline aborts the link, and an aborted link's write ends. That the close has begun is
    /// what makes the wait sound (only a give-up in Closing completes it), so it is asserted, and should it ever not
    /// hold, the close begins here, before the send path is looked at.
    /// </summary>
    /// <returns>
    /// A task that completes when the connection is Closed and its slot is free.
    /// </returns>
    private ValueTask FinishAsync()
    {
        Debug.Assert(_state >= ConnectionState.Closing, "Every way out of the run begins the close first.");
        if (_state < ConnectionState.Closing)
        {
            BeginClose(CloseCause.ConnectionLost, MqttReasonCode.Success, reasonString: null);
        }

        Task? idle = null;
        lock (_sendLock)
        {
            if (_writing)
            {
                // Completed, never inline, by the hold that gives the send path up: what follows calls the host.
                // Shared with a test that already waits for the same give-up (WhenSendPathIdleAsync).
                _sendIdle ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                idle = _sendIdle.Task;
            }
        }

        if (idle is null)
        {
            EndRun();
            return ValueTask.CompletedTask;
        }

        return new ValueTask(FinishWhenIdleAsync(idle));
    }

    /// <summary>
    /// Waits for the send path to be idle, then ends the run as <see cref="EndRun"/> does.
    /// </summary>
    /// <param name="idle">
    /// Completes when the send path has been given up.
    /// </param>
    /// <returns>
    /// A task that completes when the connection is Closed and its slot is free.
    /// </returns>
    private async Task FinishWhenIdleAsync(Task idle)
    {
        await idle.ConfigureAwait(false);
        EndRun();
    }

    /// <summary>
    /// Ends the run, in the order of design 6.6 ("Closing, bounded", step 5): removes the connection from the
    /// client-id table, only if it is still the one there; makes it Closed; then calls
    /// <see cref="IMqttWireHandler.OnClosed"/> once if the connection was admitted, after every other handler call
    /// for it has returned; and only then frees its slot, so that <see cref="ConnectionRegistry.WhenEmptyAsync"/>
    /// completes after every <see cref="IMqttWireHandler.OnClosed"/> has returned. During
    /// <see cref="IMqttWireHandler.OnClosed"/> the connection is Closed: a close changes nothing and the sweep
    /// ignores it. Every buffer the connection held is back in the pool before this runs: the read path returned
    /// its own, and the send path is idle.
    /// </summary>
    private void EndRun()
    {
        ConnectionRegistry registry = _server.Registry;
        registry.RemoveFromTable(this);
        _state = ConnectionState.Closed;
        if (_admitted)
        {
            ReportClosed();
        }

        registry.Release(this);
    }
}
