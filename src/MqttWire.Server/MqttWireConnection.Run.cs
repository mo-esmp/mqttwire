using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.AspNetCore.Http;
using MqttWire.Codec;
using MqttWire.Link;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// The run of a connection: the accept, the connect phase and the end.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// The WebSocket sub-protocol of MQTT.
    /// </summary>
    private const string SubProtocol = "mqtt";

    /// <summary>
    /// Runs the connection from its accept until it is Closed. It is the only async method whose state lives as
    /// long as the connection (plan decision 3), so it keeps as few locals alive across its awaits as it can; the
    /// connect phase is a separate method that has completed before anything else runs.
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
        link ??= await AcceptWebSocketAsync().ConfigureAwait(false);
        _link = link;
        Volatile.Write(ref _stateEnteredAt, _server.Time.GetTimestamp());
        _server.Registry.Add(this);
        try
        {
            await ConnectPhaseAsync(link).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The wait or a read failed: the link was aborted or lost. A reason recorded first is kept.
            BeginClose(MqttCloseReason.ConnectionLost, MqttReasonCode.Success, null);
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
            SubProtocol = SubProtocol,
            KeepAliveInterval = TimeSpan.Zero,
            DangerousEnableCompression = false,
        };
        WebSocket webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync(accept).ConfigureAwait(false);
        return new WebSocketLink(webSocket);
    }

    /// <summary>
    /// The connect phase: reads the first packet, which must be a complete CONNECT, decodes it and asks the host's
    /// <see cref="IMqttWireHandler.OnConnectAsync"/>. The read buffer is back in the pool on every way out, before
    /// the host is asked. Until the CONNACK is written by a later step, the connection then closes its WebSocket
    /// with 1000 and ends.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <returns>
    /// A task that completes when the phase is over; when it refused the first packet, the close has begun.
    /// </returns>
    /// <exception cref="Exception">
    /// The wait or a read failed, or the host's handler threw; the run records it.
    /// </exception>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ConnectPhaseAsync(IMqttLink link)
    {
        MqttConnectRequest? request;
        try
        {
            int length = await ReadConnectAsync(link).ConfigureAwait(false);
            request = length > 0 ? DecodeConnect(length) : null;
        }
        finally
        {
            // Every read has ended here, so the buffer can go back.
            _readBuffer.Return(_server.Pool);
        }

        if (request is null)
        {
            return;
        }

        Volatile.Write(ref _stateEnteredAt, _server.Time.GetTimestamp());
        if (Interlocked.CompareExchange(ref _state, ConnectionState.Authenticating, ConnectionState.Accepted)
            != ConnectionState.Accepted)
        {
            // The connect deadline passed while the CONNECT was decoded.
            return;
        }

        await _server.Handler.OnConnectAsync(request, HttpContext.RequestAborted).ConfigureAwait(false);

        // The CONNACK and the admission come with a later step; until then the connection ends here.
        await link.CloseAsync(WebSocketCloseStatus.NormalClosure, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Decodes the complete CONNECT at the start of the read buffer and takes the client id from it.
    /// </summary>
    /// <param name="length">
    /// The CONNECT's total length.
    /// </param>
    /// <returns>
    /// The request for the host; <see langword="null"/> when the CONNECT is malformed or not allowed, and the
    /// close has begun.
    /// </returns>
    private MqttConnectRequest? DecodeConnect(int length)
    {
        MqttReasonCode code = ConnectCodec.TryDecode(_readBuffer.GetData()[..length], out ConnectPacket connect);
        if (code != MqttReasonCode.Success)
        {
            MqttCloseReason reason = code == MqttReasonCode.MalformedPacket
                ? MqttCloseReason.MalformedPacket
                : MqttCloseReason.ProtocolError;
            BeginClose(reason, code, null);
            return null;
        }

        ClientId = Encoding.UTF8.GetString(connect.ClientId);
        return new MqttConnectRequest(ClientId, null, null, [], null, false, HttpContext);
    }

    /// <summary>
    /// Ends the run: frees the connection's slot and makes it Closed. Every buffer was returned by the code that
    /// held it before this runs.
    /// </summary>
    /// <returns>
    /// A task that completes when the connection is Closed.
    /// </returns>
    private ValueTask FinishAsync()
    {
        _server.Registry.Remove(this);
        _state = ConnectionState.Closed;
        return ValueTask.CompletedTask;
    }
}
