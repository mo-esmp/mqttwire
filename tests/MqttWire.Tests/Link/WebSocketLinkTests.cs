using System.Net.WebSockets;
using MqttWire.Link;

namespace MqttWire.Tests.Link;

/// <summary>
/// Tests <see cref="WebSocketLink"/> over an in-process WebSocket pair, with no sockets.
/// </summary>
public sealed class WebSocketLinkTests
{
    /// <summary>
    /// Sends one binary message from the given socket.
    /// </summary>
    /// <param name="socket">
    /// The sending socket.
    /// </param>
    /// <param name="bytes">
    /// The message payload.
    /// </param>
    /// <returns>
    /// A task that completes when the message has been sent.
    /// </returns>
    private static Task SendBinaryAsync(WebSocket socket, byte[] bytes)
        => socket.SendAsync(bytes, WebSocketMessageType.Binary, true, CancellationToken.None);

    /// <summary>
    /// Reads from the link until the given number of bytes has arrived or the link ends.
    /// </summary>
    /// <param name="link">
    /// The link to read from.
    /// </param>
    /// <param name="count">
    /// The number of bytes to read.
    /// </param>
    /// <returns>
    /// The bytes read, in order.
    /// </returns>
    private static async Task<byte[]> ReadExactlyAsync(WebSocketLink link, int count)
    {
        byte[] result = new byte[count];
        int total = 0;
        while (total < count)
        {
            int read = await link.ReadAsync(result.AsMemory(total), CancellationToken.None);
            if (read == 0)
            {
                break;
            }

            total += read;
        }

        return result.AsSpan(0, total).ToArray();
    }

    /// <summary>
    /// Proves a single binary message is read as its bytes.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadAsync_OneBinaryMessage_ReturnsItsBytes()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        await SendBinaryAsync(serverSocket, [0xC0, 0x00]);

        // Act
        byte[] bytes = await ReadExactlyAsync(link, 2);

        // Assert
        bytes.ShouldBe(new byte[] { 0xC0, 0x00 });
    }

    /// <summary>
    /// Proves two packets in one message are read as one continuous stream.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadAsync_TwoPacketsInOneMessage_ReturnsBothAsOneStream()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        await SendBinaryAsync(serverSocket, [0xD0, 0x00, 0xD0, 0x00]);

        // Act
        byte[] bytes = await ReadExactlyAsync(link, 4);

        // Assert
        bytes.ShouldBe(new byte[] { 0xD0, 0x00, 0xD0, 0x00 });
    }

    /// <summary>
    /// Proves a packet split across two messages is read as its bytes in order.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadAsync_PacketSplitAcrossMessages_ReturnsTheBytesInOrder()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        await SendBinaryAsync(serverSocket, [0x32, 0x0A, 0x00]);
        await SendBinaryAsync(serverSocket, [0x03, 0x61, 0x2F, 0x62, 0x00, 0x01, 0x00, 0x68, 0x69]);

        // Act
        byte[] bytes = await ReadExactlyAsync(link, 12);

        // Assert
        bytes.ShouldBe(new byte[] { 0x32, 0x0A, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x00, 0x01, 0x00, 0x68, 0x69 });
    }

    /// <summary>
    /// Proves a text message ends the link: the read returns zero and the peer sees close status 1003.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadAsync_TextMessage_ClosesWith1003AndReturnsZero()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        await serverSocket.SendAsync("x"u8.ToArray(), WebSocketMessageType.Text, true, CancellationToken.None);
        byte[] buffer = new byte[16];

        // Act
        int read = await link.ReadAsync(buffer, CancellationToken.None);
        WebSocketReceiveResult closeResult = await serverSocket.ReceiveAsync(buffer, CancellationToken.None);

        // Assert
        read.ShouldBe(0);
        closeResult.MessageType.ShouldBe(WebSocketMessageType.Close);
        closeResult.CloseStatus.ShouldBe(WebSocketCloseStatus.InvalidMessageType);
    }

    /// <summary>
    /// Proves a close from the peer ends the read with zero.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadAsync_PeerCloses_ReturnsZero()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        await serverSocket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        byte[] buffer = new byte[16];

        // Act
        int read = await link.ReadAsync(buffer, CancellationToken.None);

        // Assert
        read.ShouldBe(0);
    }

    /// <summary>
    /// Proves a close from the peer that ends a wait for data makes the next read return zero rather than throw,
    /// and that the link can then answer the close.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadAsync_PeerClosesDuringWait_ReturnsZeroAndTheCloseCanBeAnswered()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        byte[] buffer = new byte[16];
        ValueTask wait = link.WaitForDataAsync(CancellationToken.None);
        await serverSocket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        await wait;

        // Act
        int read = await link.ReadAsync(buffer, CancellationToken.None);
        await link.CloseAsync(WebSocketCloseStatus.NormalClosure, CancellationToken.None);
        WebSocketReceiveResult answer = await serverSocket.ReceiveAsync(buffer, CancellationToken.None);

        // Assert
        read.ShouldBe(0);
        answer.MessageType.ShouldBe(WebSocketMessageType.Close);
        answer.CloseStatus.ShouldBe(WebSocketCloseStatus.NormalClosure);
    }

    /// <summary>
    /// Proves waiting for data completes when data arrives and leaves the data to be read.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WaitForDataAsync_DataArrives_CompletesWithoutConsumingIt()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        ValueTask wait = link.WaitForDataAsync(CancellationToken.None);
        bool completedBeforeData = wait.IsCompleted;

        // Act
        await SendBinaryAsync(serverSocket, [0xC0, 0x00]);
        await wait;
        byte[] bytes = await ReadExactlyAsync(link, 2);

        // Assert
        completedBeforeData.ShouldBeFalse();
        bytes.ShouldBe(new byte[] { 0xC0, 0x00 });
    }

    /// <summary>
    /// Proves a written packet arrives as exactly one binary message.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WriteAsync_Packet_ArrivesAsOneBinaryMessage()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        byte[] buffer = new byte[16];

        // Act
        await link.WriteAsync(new byte[] { 0xC0, 0x00 }, CancellationToken.None);
        WebSocketReceiveResult result = await serverSocket.ReceiveAsync(buffer, CancellationToken.None);

        // Assert
        result.MessageType.ShouldBe(WebSocketMessageType.Binary);
        result.EndOfMessage.ShouldBeTrue();
        result.Count.ShouldBe(2);
        buffer.AsSpan(0, 2).ToArray().ShouldBe(new byte[] { 0xC0, 0x00 });
    }

    /// <summary>
    /// Proves a normal close is seen by the peer as status 1000.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task CloseAsync_NormalClosure_PeerSeesStatus1000()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        byte[] buffer = new byte[16];

        // Act
        await link.CloseAsync(WebSocketCloseStatus.NormalClosure, CancellationToken.None);
        WebSocketReceiveResult result = await serverSocket.ReceiveAsync(buffer, CancellationToken.None);

        // Assert
        result.MessageType.ShouldBe(WebSocketMessageType.Close);
        result.CloseStatus.ShouldBe(WebSocketCloseStatus.NormalClosure);
    }

    /// <summary>
    /// Proves aborting the link ends a pending read with an exception.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_ReadPending_ReadEndsWithAnException()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        byte[] buffer = new byte[16];
        ValueTask<int> read = link.ReadAsync(buffer, CancellationToken.None);

        // Act
        link.Abort();

        // Assert
        await Should.ThrowAsync<Exception>(async () => await read);
    }
}
