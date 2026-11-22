using System.Net.WebSockets;
using Microsoft.Extensions.Logging;
using MqttWire.Codec;
using MqttWire.Link;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// Ending a connection: the move to Closing, the sweep's checks of the deadlines, the report to the host.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// Begins the close: moves the connection to Closing, once, records the reason, writes the packet that carries
    /// the reason code, and starts the WebSocket close with status 1000 without waiting for the peer; the sweep
    /// aborts the link at the close deadline. No MQTT packet precedes the CONNACK: a connection whose success CONNACK
    /// was handed to the link is sent DISCONNECT with the code, except when the client disconnected or the link was
    /// lost; one that was not admitted is sent CONNACK with the code when the code is a failure (plan decision 7);
    /// one closed during or after its admission but before its CONNACK is sent nothing. Safe from any thread; a call
    /// after the first changes nothing.
    /// </summary>
    /// <param name="reason">
    /// Why the connection ends.
    /// </param>
    /// <param name="code">
    /// The reason code the end carries; <see cref="MqttReasonCode.Success"/> when it carries none.
    /// </param>
    /// <param name="reasonString">
    /// The reason string, at most 256 bytes, or <see langword="null"/>.
    /// </param>
    /// <exception cref="ArgumentException">
    /// The reason string cannot be carried (the host's bug); the connection is Closing and its close has begun all
    /// the same, without the packet.
    /// </exception>
    internal void BeginClose(MqttCloseReason reason, MqttReasonCode code, string? reasonString)
    {
        if (!TryEnterClosing(reason, out ConnectionState left))
        {
            return;
        }

        IMqttLink? link = _link;
        if (link is null)
        {
            return;
        }

        try
        {
            // Read after the move to Closing, the order TryBeginHandingConnAck relies on.
            ConnAckProgress progress =
                Interlocked.CompareExchange(ref _connAck, ConnAckProgress.Withheld, ConnAckProgress.NotHanded);
            if (progress == ConnAckProgress.NotHanded)
            {
                // Not admitted: this close's CONNACK, if any, is the first packet.
                if ((byte)code >= 0x80)
                {
                    SendConnAck(new ConnAckFields { ReasonCode = code, ReasonString = reasonString });
                }
            }
            else if (progress == ConnAckProgress.Handed && left == ConnectionState.Active
                     && reason is not (MqttCloseReason.ClientDisconnected or MqttCloseReason.ConnectionLost))
            {
                SendDisconnect(code, reasonString);
            }

            // Admitting: closed during or right after its admission, before its CONNACK; no packet at all.
            // Task 8: Handing: the success CONNACK is being handed to the link on the connect phase's thread, so no
            // DISCONNECT is written; but the WebSocket close below may still reach the link before that CONNACK's
            // write. Task 8 closes this by reserving the send path for the CONNACK at admission.
        }
        finally
        {
            _ = CloseLinkAsync(link, WebSocketCloseStatus.NormalClosure);
        }
    }

    /// <summary>
    /// Tells the host that the admitted connection ended, with the reason of the first <see cref="BeginClose"/>.
    /// Called once, by the end of the run. An <see cref="IMqttWireHandler.OnClosed"/> that throws is logged and
    /// nothing else: the connection ends all the same.
    /// </summary>
    private void ReportClosed()
    {
        try
        {
            _server.Handler.OnClosed(this, CloseReason);
        }
        catch (Exception exception)
        {
            ServerLog.HandlerThrew(_server.Logger, ClientId, nameof(IMqttWireHandler.OnClosed), exception);
        }
    }

    /// <summary>
    /// Refuses a first packet that is not a CONNECT, or whose fixed header is malformed: no MQTT packet can answer
    /// it, so the connection moves to Closing and the WebSocket is closed with status 1002 (plan decision 7). Does
    /// nothing when the connection is Closing already.
    /// </summary>
    /// <param name="reason">
    /// Why the connection ends.
    /// </param>
    private void RefuseFirstPacket(MqttCloseReason reason)
    {
        if (TryEnterClosing(reason, out _) && _link is { } link)
        {
            _ = CloseLinkAsync(link, WebSocketCloseStatus.ProtocolError);
        }
    }

    /// <summary>
    /// Acts on the deadlines that have passed; called by the sweep, from the timer's thread. An Accepted
    /// connection is aborted at the connect deadline; an Authenticating one has its cancellation source taken at the
    /// authentication deadline and handed to the thread pool, which cancels it (see
    /// <see cref="CancelAuthentication"/>); a Closing one is aborted at the close deadline. Each check acts only on
    /// the state it read: the move out of Accepted is a compare-exchange that expects Accepted, the cancellation
    /// source exists only while the connection is Authenticating and is taken from its field by one exchange, and the
    /// close deadline is measured from the stamp that only the winner of the move to Closing writes. On the sweep's
    /// thread it runs no host code and no part of the connection's end beyond an abort, and never throws; it
    /// allocates only when it hands a source to the thread pool. An abort ends the link's pending reads, whose
    /// continuations may run on the thread that aborts, as with any abort.
    /// </summary>
    /// <param name="now">
    /// The sweep's timestamp, from the server's clock.
    /// </param>
    internal void CheckDeadlines(long now)
    {
        if (_state == ConnectionState.Accepted)
        {
            long accepted = Volatile.Read(ref _stateEnteredAt);
            if (accepted != 0 && now - accepted >= _server.ConnectDeadlineLength
                              && TryEnterClosingFrom(ConnectionState.Accepted, MqttCloseReason.ConnectionLost))
            {
                // The connection was never admitted, so no OnClosed reports the reason; the link failing is the end.
                _link?.Abort();
            }
        }
        else if (_state == ConnectionState.Authenticating)
        {
            // Stamped before the move to Authenticating, so this is that state's stamp.
            long entered = Volatile.Read(ref _stateEnteredAt);
            if (now - entered >= _server.AuthenticationDeadlineLength)
            {
                CancelAuthentication();
            }
        }
        else if (_state == ConnectionState.Closing)
        {
            // 0 until the winner of the move to Closing has stamped it: that sweep skips the connection.
            long closing = Volatile.Read(ref _closingAt);
            if (closing != 0 && now - closing >= _server.CloseDeadlineLength)
            {
                _link?.Abort();
            }
        }
    }

    /// <summary>
    /// Closes the link and observes the outcome, so that nobody waits on it. A close that completes at once
    /// allocates nothing.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <param name="status">
    /// The WebSocket status the peer sees.
    /// </param>
    /// <returns>
    /// A task that completes when the close was sent, or failed and the link was aborted; it never fails.
    /// </returns>
    private static async Task CloseLinkAsync(IMqttLink link, WebSocketCloseStatus status)
    {
        try
        {
            await link.CloseAsync(status, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The close failed: end the link now rather than at the close deadline. The run then sees it end.
            link.Abort();
        }
    }

    /// <summary>
    /// Ends the host's wait at the authentication deadline: takes the cancellation source out of its field, on the
    /// sweep's thread, and hands it to the thread pool, which cancels and disposes it. When the connect phase took it
    /// first, the host has answered and nothing is done. So the source is still cancelled and disposed exactly once,
    /// and the token's callbacks (the host's, and the connect phase's continuation with the CONNACK 0x88 and the end
    /// of the run) never run on the sweep's thread.
    /// </summary>
    private void CancelAuthentication()
    {
        CancellationTokenSource? source = Interlocked.Exchange(ref _authentication, null);
        if (source is null)
        {
            return;
        }

        ThreadPool.UnsafeQueueUserWorkItem(static state => CancelOnThreadPool(state.Source, state.Logger, state.ClientId),
            (Source: source, Logger: (ILogger)_server.Logger, ClientId), preferLocal: false);
    }

    /// <summary>
    /// Cancels and disposes a cancellation source of the Authenticating state, on a thread-pool thread. A token
    /// callback that throws is the host's bug: it is logged as an error of <see cref="IMqttWireHandler.OnConnectAsync"/>
    /// and the token is cancelled all the same.
    /// </summary>
    /// <param name="source">
    /// The source; nothing else cancels or disposes it.
    /// </param>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    private static void CancelOnThreadPool(CancellationTokenSource source, ILogger logger, string clientId)
    {
        try
        {
            source.Cancel();
        }
        catch (AggregateException exception)
        {
            ServerLog.HandlerThrew(logger, clientId, nameof(IMqttWireHandler.OnConnectAsync), exception);
        }
        finally
        {
            source.Dispose();
        }
    }

    /// <summary>
    /// Moves the connection to Closing unless it is there or beyond, records the reason and stamps the time.
    /// </summary>
    /// <param name="reason">
    /// Why the connection ends.
    /// </param>
    /// <param name="left">
    /// The state the connection left when the result is <see langword="true"/>; otherwise the state it was in.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when this call made the move; <see langword="false"/> when another had made it, and
    /// nothing changed.
    /// </returns>
    private bool TryEnterClosing(MqttCloseReason reason, out ConnectionState left)
    {
        left = _state;
        while (left < ConnectionState.Closing)
        {
            if (TryEnterClosingFrom(left, reason))
            {
                return true;
            }

            left = _state;
        }

        return false;
    }

    /// <summary>
    /// Moves the connection to Closing only from one state; the winner records the reason and writes
    /// <see cref="_closingAt"/>, which nothing else writes.
    /// </summary>
    /// <param name="expected">
    /// The state the move must start from; it is below Closing.
    /// </param>
    /// <param name="reason">
    /// Why the connection ends.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when this call made the move; <see langword="false"/> when the connection was in
    /// another state, and nothing changed.
    /// </returns>
    private bool TryEnterClosingFrom(ConnectionState expected, MqttCloseReason reason)
    {
        if (Interlocked.CompareExchange(ref _state, ConnectionState.Closing, expected) != expected)
        {
            return false;
        }

        CloseReason = reason;
        Volatile.Write(ref _closingAt, _server.Time.GetTimestamp());
        return true;
    }
}
