using System.Buffers;
using System.Net.WebSockets;
using Microsoft.Extensions.Time.Testing;
using MqttWire.Link;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests the client over a real <see cref="WebSocketLink"/> on an in-process WebSocket pair, with no sockets, for
/// what the in-memory link cannot show: how the WebSocket itself behaves under the client.
/// </summary>
public sealed class ClientOverWebSocketTests
{
    /// <summary>
    /// Proves the server closing the WebSocket after CONNACK, while the read loop waits for data, is reported as a
    /// lost connection with no exception, and the client answers the close rather than aborting.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_ServerClosesTheWebSocketAfterConnAck_ReportsConnectionLostAndAnswersTheClose()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        TaskCompletionSource<MqttClientCloseInfo> closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        MqttWireClientOptions options = new()
        {
            Uri = new Uri("ws://test/mqtt"),
            ClientId = "c1",
        };
        await using MqttWireClient mqtt = new(
            options,
            null,
            info => closed.TrySetResult(info),
            new PairConnector(clientSocket),
            new FakeTimeProvider(),
            ArrayPool<byte>.Shared);
        byte[] buffer = new byte[256];
        Task<MqttConnectAck> connect = mqtt.ConnectAsync();
        await serverSocket.ReceiveAsync(buffer, CancellationToken.None);
        await serverSocket.SendAsync(ClientPackets.ConnAckAccepted, WebSocketMessageType.Binary, true, CancellationToken.None);
        MqttConnectAck ack = await connect;

        // Act
        await serverSocket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        MqttClientCloseInfo info = await closed.Task;
        WebSocketReceiveResult answer = await serverSocket.ReceiveAsync(buffer, CancellationToken.None);

        // Assert
        ack.IsConnected.ShouldBeTrue();
        info.ShouldBe(new MqttClientCloseInfo(MqttClientCloseReason.ConnectionLost, MqttReasonCode.Success, null, null));
        answer.MessageType.ShouldBe(WebSocketMessageType.Close);
        answer.CloseStatus.ShouldBe(WebSocketCloseStatus.NormalClosure);
    }

    /// <summary>
    /// A connector that hands out one <see cref="WebSocketLink"/> over a socket the test made.
    /// </summary>
    /// <param name="socket">
    /// The client end of the pair.
    /// </param>
    private sealed class PairConnector(WebSocket socket) : IMqttLinkConnector
    {
        /// <summary>
        /// Hands out a new link over the socket, at once.
        /// </summary>
        /// <param name="options">
        /// Not used.
        /// </param>
        /// <param name="cancellationToken">
        /// Not used.
        /// </param>
        /// <returns>
        /// A completed task with the link.
        /// </returns>
        public ValueTask<LinkConnectResult> ConnectAsync(MqttWireClientOptions options, CancellationToken cancellationToken)
            => new(new LinkConnectResult(new WebSocketLink(socket, dropWritesAfterClose: true), null));
    }
}
