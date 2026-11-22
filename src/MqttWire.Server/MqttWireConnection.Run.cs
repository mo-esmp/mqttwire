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
    private const string SubProtocol = "mqtt";

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
        _server.Registry.Add(this);
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
            BeginClose(MqttCloseReason.ConnectionLost, MqttReasonCode.Success, reasonString: null);
        }
        finally
        {
            await FinishAsync().ConfigureAwait(false);
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
    /// Ends the run, in the order of design 6.6 ("Closing, bounded", step 5): removes the connection from the
    /// client-id table, only if it is still the one there; makes it Closed; then calls
    /// <see cref="IMqttWireHandler.OnClosed"/> once if the connection was admitted, after every other handler call
    /// for it has returned; and only then frees its slot, so that <see cref="ConnectionRegistry.WhenEmptyAsync"/>
    /// completes after every <see cref="IMqttWireHandler.OnClosed"/> has returned. During
    /// <see cref="IMqttWireHandler.OnClosed"/> the connection is Closed: a close changes nothing and the sweep
    /// ignores it. Every buffer the read path held was returned before this runs.
    /// </summary>
    /// <returns>
    /// A task that completes when the connection is Closed and its slot is free.
    /// </returns>
    private ValueTask FinishAsync()
    {
        // Task 8: a write started by SendConnAck or SendDisconnect (fire-and-forget) can still be pending over a real
        // WebSocket when the read loop ends, holding its pool buffer. FinishAsync must wait for it before the state
        // becomes Closed, so that the slot is freed and WhenEmptyAsync completes only when its buffer is back.
        ConnectionRegistry registry = _server.Registry;
        registry.RemoveFromTable(this);
        _state = ConnectionState.Closed;
        if (_admitted)
        {
            ReportClosed();
        }

        registry.Release(this);
        return ValueTask.CompletedTask;
    }
}
