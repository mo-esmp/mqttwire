using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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
    /// Gets the link, for the code that runs after <see cref="RunAsync"/> has set it and before the run has ended:
    /// the run itself, and whoever holds the send path, which the end of the run waits for.
    /// </summary>
    private IMqttLink RunningLink =>
        _link!; // Set before the slot is taken, cleared when the run ends: only the run and a holder of the path read it.

    /// <summary>
    /// Gets the context of the WebSocket request, for the code of the run itself, which ends before the context is
    /// let go.
    /// </summary>
    private HttpContext RunningContext =>
        HttpContext!; // Set by the constructor and cleared only when the run has decided to end: only the run reads it.

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
    /// A task that completes when the connection is Closed. It does not fail for anything the client does, nor for
    /// an exception inside the run, which ends the connection as a lost one and is logged as an error (event 21)
    /// unless it is what a broken link gives; it fails only when the WebSocket cannot be accepted, before the
    /// connection takes its slot.
    /// </returns>
    internal Task RunAsync(IMqttLink? link)
    {
        // Kept in the field, not in a parameter of the async method: a parameter would live in the method's state
        // for as long as the connection does, 8 bytes for every idle connection.
        _link = link;
        return RunCoreAsync();
    }

    /// <summary>
    /// The run itself (see <see cref="RunAsync"/>): over the link already in <see cref="_link"/>, or,
    /// when there is none, over the WebSocket of the request, which it accepts first.
    /// </summary>
    /// <returns>
    /// A task that completes when the connection is Closed; it fails only when the WebSocket cannot be accepted.
    /// </returns>
    private async Task RunCoreAsync()
    {
        _link ??= await AcceptWebSocketAsync().ConfigureAwait(false);
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
                    await RunningLink.WaitForDataAsync(RunningContext.RequestAborted).ConfigureAwait(false);
                }

                open = await HandleAvailableAsync().ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            // The wait or a read failed: the link was aborted or lost. A reason recorded first is kept.
            BeginClose(CloseCause.ConnectionLost, MqttReasonCode.Success, reasonString: null);
            if (!IsLinkFailure(exception))
            {
                // Anything else is a bug, of MqttWire or of what it runs on: never hidden behind a lost connection.
                LogEnd(CloseCause.ConnectionLost, MqttReasonCode.Success, payloadLength: 0, method: null, exception);
            }
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
            switch (_state)
            {
                case ConnectionState.Accepted when AbortFromAccepted(CloseCause.ServerStopping):
                    return;
                // The CONNECT came meanwhile, or another close began: look again.
                case ConnectionState.Accepted:
                    continue;
                case ConnectionState.Authenticating:
                    // The source is in its field before the move to Authenticating; when it is gone, the host has
                    // answered and the admission step refuses.
                    CancelAuthentication();
                    break;
                case ConnectionState.Active:
                    BeginClose(CloseCause.ServerStopping, MqttReasonCode.ServerShuttingDown, reasonString: null);
                    break;
            }

            return;
        }
    }

    /// <summary>
    /// Tells whether an exception that ended the run is what a broken link gives: a WebSocket that failed or was
    /// aborted, an I/O error, a cancellation (the request was aborted) or a disposed socket. ASP.NET Core's own
    /// exceptions for an aborted or reset connection derive from these. Such an end is a lost connection and nothing
    /// more; any other exception is logged as an error (event 21).
    /// </summary>
    /// <param name="exception">
    /// What ended the run.
    /// </param>
    /// <returns>
    /// <see langword="true"/> for a failure of the link; <see langword="false"/> for anything else.
    /// </returns>
    private static bool IsLinkFailure(Exception exception) =>
        exception is WebSocketException or IOException or OperationCanceledException or ObjectDisposedException;

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
        WebSocket webSocket = await RunningContext.WebSockets.AcceptWebSocketAsync(accept).ConfigureAwait(false);
        return new WebSocketLink(webSocket, dropWritesAfterClose: false);
    }

    /// <summary>
    /// Ends the run once the send path is idle and no abort of the link is in flight (design 6.6, "Closing,
    /// bounded", step 5: the connection is Closed when the read loop has ended, no write is in progress and no abort
    /// of its link is being issued). The read loop has ended and the close has begun, so the send path, once idle,
    /// stays idle: nothing is added to a Closing connection, and only the call that began the close could have taken
    /// the path. When nothing is in progress, which is the common case, this completes at once and allocates nothing.
    /// Otherwise it waits for whoever holds the send path to give it up, which that holder does when its write, or
    /// its close, has ended and its buffer is back, and for whoever is aborting the link to have done so. The wait is
    /// bounded: by the sweep for a write (the close deadline aborts the link, and an aborted link's write ends), and
    /// by the abort's own return. That the close has begun is what makes the wait sound (only a give-up in Closing
    /// completes it for good), so it is asserted, and should it ever not hold, the close begins here, before the send
    /// path is looked at.
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

        if (TryEndRun(out Task? busy))
        {
            EndRun();
            return ValueTask.CompletedTask;
        }

        return new ValueTask(FinishWhenIdleAsync(busy));
    }

    /// <summary>
    /// The one hold of the send path's lock in which the run decides to end. With the send path taken or an abort of
    /// the link in flight, the run does not end: it is given what to wait on. With neither, the run ends in this
    /// hold: it lets go of the link, so that every later look at <see cref="_link"/> under the lock finds
    /// <see langword="null"/> and no abort is issued any more (<see cref="BeginAbort"/>), and of the request's
    /// context. The request can only end after this hold, and no abort can begin after it, so a link is never
    /// aborted once its request has ended, and a connection the host keeps holds nothing of the request.
    /// </summary>
    /// <param name="busy">
    /// When the result is <see langword="false"/>, a task that completes, never inline, when the send path has been
    /// given up or an abort has returned, after which the caller looks again: what follows calls the host, so it
    /// never runs inside the hold or the abort that completes the wait. Otherwise <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the run has ended and <see cref="EndRun"/> is to follow; <see langword="false"/>
    /// when it has to wait, and nothing changed.
    /// </returns>
    private bool TryEndRun([NotNullWhen(false)] out Task? busy)
    {
        lock (_sendLock)
        {
            if (_writing || _aborting)
            {
                // Shared with a test that already waits for the give-up of the send path (WhenSendPathIdleAsync).
                _sendIdle ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                busy = _sendIdle.Task;
                return false;
            }

            _link = null;
            HttpContext = null;
            busy = null;
            return true;
        }
    }

    /// <summary>
    /// Waits until the send path is idle and no abort of the link is in flight, then ends the run as
    /// <see cref="EndRun"/> does. It looks again after every wake-up: the give-up of the send path wakes it while an
    /// abort may still be in flight, and an abort may begin between the wake-up and the look.
    /// </summary>
    /// <param name="busy">
    /// What <see cref="TryEndRun"/> gave to wait on.
    /// </param>
    /// <returns>
    /// A task that completes when the connection is Closed and its slot is free.
    /// </returns>
    private async Task FinishWhenIdleAsync(Task busy)
    {
        Task? next = busy;
        while (next is not null)
        {
            await next.ConfigureAwait(false);
            TryEndRun(out next);
        }

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
    /// its own, and the send path is idle. The link was let go by the hold that decided the end
    /// (<see cref="TryEndRun"/>), so nothing here, and nothing the host does from <see cref="IMqttWireHandler.OnClosed"/>
    /// on, can reach it.
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
