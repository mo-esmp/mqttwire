using System.Net.WebSockets;
using MqttWire.Link;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// Ending a connection: the move to Closing, the sweep's checks of the deadlines.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// Begins the close: moves the connection to Closing, once, records the reason and starts the WebSocket close
    /// without waiting for the peer; the sweep aborts the link at the close deadline. Before a CONNACK no MQTT packet
    /// can carry a reason, so a refusal in the Accepted state closes with WebSocket status 1002; any other close
    /// uses 1000. Safe from any thread.
    /// </summary>
    /// <param name="reason">
    /// Why the connection ends.
    /// </param>
    /// <param name="code">
    /// The reason code the end carries; <see cref="MqttReasonCode.Success"/> when it carries none.
    /// </param>
    /// <param name="reasonString">
    /// The reason string, or <see langword="null"/>; the DISCONNECT that carries it comes with a later step.
    /// </param>
    internal void BeginClose(MqttCloseReason reason, MqttReasonCode code, string? reasonString)
    {
        if (!TryEnterClosing(reason, out ConnectionState left))
        {
            return;
        }

        IMqttLink? link = _link;
        if (link is not null)
        {
            WebSocketCloseStatus status = left == ConnectionState.Accepted && (byte)code >= 0x80
                ? WebSocketCloseStatus.ProtocolError
                : WebSocketCloseStatus.NormalClosure;
            _ = CloseLinkAsync(link, status);
        }
    }

    /// <summary>
    /// Acts on the deadlines that have passed; called by the sweep, from the timer's thread. An Accepted
    /// connection is aborted at the connect deadline, and a Closing one at the close deadline. It allocates
    /// nothing and never throws.
    /// </summary>
    /// <param name="now">
    /// The sweep's timestamp, from the server's clock.
    /// </param>
    internal void CheckDeadlines(long now)
    {
        ConnectionState state = _state;
        long entered = Volatile.Read(ref _stateEnteredAt);
        if (entered == 0)
        {
            return;
        }

        long elapsed = now - entered;
        if (state == ConnectionState.Accepted && elapsed >= _server.ConnectDeadlineLength)
        {
            // The connection was never admitted, so no OnClosed reports the reason; the link failing is the end.
            if (TryEnterClosing(MqttCloseReason.ConnectionLost, out _))
            {
                _link?.Abort();
            }
        }
        else if (state == ConnectionState.Closing && elapsed >= _server.CloseDeadlineLength)
        {
            _link?.Abort();
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
            ConnectionState seen = Interlocked.CompareExchange(ref _state, ConnectionState.Closing, left);
            if (seen == left)
            {
                CloseReason = reason;
                Volatile.Write(ref _stateEnteredAt, _server.Time.GetTimestamp());
                return true;
            }

            left = seen;
        }

        return false;
    }
}
