using System.Net.WebSockets;
using MqttWire.Codec;
using MqttWire.Link;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// Ending a connection: the move to Closing, the sweep's checks of the deadlines.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// Begins the close: moves the connection to Closing, once, records the reason, writes the packet that carries
    /// the reason code, and starts the WebSocket close with status 1000 without waiting for the peer; the sweep
    /// aborts the link at the close deadline. A connection that was Active is sent DISCONNECT with the code, except
    /// when the client disconnected or the link was lost; one that was not Active yet is sent CONNACK with the code
    /// when the code is a failure (plan decision 7). Safe from any thread; a call after the first changes nothing.
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
            if (left == ConnectionState.Active)
            {
                if (reason is not (MqttCloseReason.ClientDisconnected or MqttCloseReason.ConnectionLost))
                {
                    SendDisconnect(code, reasonString);
                }
            }
            else if ((byte)code >= 0x80)
            {
                SendConnAck(new ConnAckFields { ReasonCode = code, ReasonString = reasonString });
            }
        }
        finally
        {
            _ = CloseLinkAsync(link, WebSocketCloseStatus.NormalClosure);
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
    /// connection is aborted at the connect deadline; an Authenticating one has the token of the host's
    /// <see cref="IMqttWireHandler.OnConnectAsync"/> cancelled at the authentication deadline, and the connect phase
    /// then refuses it; a Closing one is aborted at the close deadline. Each check acts only on the state it read:
    /// the move out of Accepted is a compare-exchange that expects Accepted, the cancellation source exists only
    /// while the connection is Authenticating and is taken from its field by one exchange, and the close deadline is
    /// measured from the stamp that only the winner of the move to Closing writes. It allocates nothing and never
    /// throws.
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
    /// Cancels the token of the host's <see cref="IMqttWireHandler.OnConnectAsync"/> at the authentication deadline:
    /// takes the cancellation source out of its field, cancels it and disposes it. When the connect phase took it
    /// first, the host has answered and nothing is done. The cancellation runs the token's callbacks on this thread,
    /// the host's among them; one that throws is the host's bug and does not stop the sweep.
    /// </summary>
    private void CancelAuthentication()
    {
        CancellationTokenSource? source = Interlocked.Exchange(ref _authentication, null);
        if (source is null)
        {
            return;
        }

        try
        {
            source.Cancel();
        }
        catch (AggregateException)
        {
            // A callback of the host's threw; the token is cancelled all the same, and the sweep goes on.
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
