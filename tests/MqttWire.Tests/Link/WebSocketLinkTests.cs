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
    /// Sends one binary message over a pair created inline, with the receiver carrying on from the message on this
    /// thread before the send has returned. The test runner's synchronization context is taken off the thread
    /// meanwhile: with it, .NET would queue what follows the receive to the thread pool, and a test could not tell
    /// when it has run.
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
    private static Task SendBinaryInlineAsync(WebSocket socket, byte[] bytes)
    {
        SynchronizationContext? context = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(syncContext: null);
        try
        {
            return SendBinaryAsync(socket, bytes);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(context);
        }
    }

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
        WebSocketReceiveResult closeResult = await serverSocket.ReceiveAsync(buffer, CancellationToken.None).Within("The peer's receive");

        // Assert
        read.ShouldBe(0);
        closeResult.MessageType.ShouldBe(WebSocketMessageType.Close);
        closeResult.CloseStatus.ShouldBe(WebSocketCloseStatus.InvalidMessageType);
    }

    /// <summary>
    /// Proves the close a caller hands to the link after a text message ended it, as the server's close of that
    /// connection does, completes without sending a second close and without failing: the link's own close with 1003
    /// already went out, and a failed close would make the caller abort the link, which can reset the connection before
    /// the peer has read the 1003.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task CloseAsync_AfterATextMessageClosedTheLink_CompletesWithoutSendingAgain()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        await serverSocket.SendAsync("x"u8.ToArray(), WebSocketMessageType.Text, true, CancellationToken.None);
        byte[] buffer = new byte[16];
        (await link.ReadAsync(buffer, CancellationToken.None)).ShouldBe(0);

        // Act
        await link.CloseAsync(WebSocketCloseStatus.NormalClosure, CancellationToken.None);
        WebSocketReceiveResult closeResult = await serverSocket.ReceiveAsync(buffer, CancellationToken.None).Within("The peer's receive");

        // Assert
        closeResult.MessageType.ShouldBe(WebSocketMessageType.Close);
        closeResult.CloseStatus.ShouldBe(WebSocketCloseStatus.InvalidMessageType);
        clientSocket.State.ShouldBe(WebSocketState.CloseSent);
    }

    /// <summary>
    /// Proves the link starts one close frame when its own close for a text message is still waiting behind a write
    /// in progress and a caller closes it meanwhile: the caller's close completes at once without sending, and the
    /// peer reads the whole message and then the one close, with 1003.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task CloseAsync_WhileTheTextCloseWaitsBehindAWrite_SendsNoSecondClose()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        byte[] message = new byte[200_000];

        // More than the pipe takes before its reader reads, so the write stays pending.
        ValueTask write = link.WriteAsync(message, CancellationToken.None);
        write.IsCompleted.ShouldBeFalse();
        await serverSocket.SendAsync("x"u8.ToArray(), WebSocketMessageType.Text, true, CancellationToken.None);

        // The wait sees the text message, so the read starts the link's own close before it awaits anything.
        await link.WaitForDataAsync(CancellationToken.None);
        Task<int> read = link.ReadAsync(new byte[16], CancellationToken.None).AsTask();
        read.IsCompleted.ShouldBeFalse();

        // Act
        ValueTask close = link.CloseAsync(WebSocketCloseStatus.NormalClosure, CancellationToken.None);
        bool closedAtOnce = close.IsCompletedSuccessfully;
        byte[] buffer = new byte[256_000];
        int received = 0;
        WebSocketReceiveResult result;
        do
        {
            result = await serverSocket
                .ReceiveAsync(new ArraySegment<byte>(buffer, received, buffer.Length - received), CancellationToken.None)
                .Within("The peer's receive");
            received += result.Count;
        }
        while (result.MessageType == WebSocketMessageType.Binary);

        await write;
        await close;

        // Assert
        closedAtOnce.ShouldBeTrue();
        received.ShouldBe(message.Length);
        result.MessageType.ShouldBe(WebSocketMessageType.Close);
        result.CloseStatus.ShouldBe(WebSocketCloseStatus.InvalidMessageType);
        (await read).ShouldBe(0);
    }

    /// <summary>
    /// Proves the link starts one close frame when a caller closed it before a text message arrived: the read that
    /// meets the text message sends no close with 1003, returns zero without failing, and the peer sees the caller's
    /// close.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadAsync_TextMessageAfterTheCallerClosed_SendsNoSecondCloseAndReturnsZero()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        await link.CloseAsync(WebSocketCloseStatus.NormalClosure, CancellationToken.None);
        await serverSocket.SendAsync("x"u8.ToArray(), WebSocketMessageType.Text, true, CancellationToken.None);
        byte[] buffer = new byte[16];

        // Act
        int read = await link.ReadAsync(buffer, CancellationToken.None);
        WebSocketReceiveResult closeResult = await serverSocket.ReceiveAsync(buffer, CancellationToken.None).Within("The peer's receive");

        // Assert
        read.ShouldBe(0);
        closeResult.MessageType.ShouldBe(WebSocketMessageType.Close);
        closeResult.CloseStatus.ShouldBe(WebSocketCloseStatus.NormalClosure);
        clientSocket.State.ShouldBe(WebSocketState.CloseSent);
    }

    /// <summary>
    /// Proves an empty text message that the zero-byte wait takes whole still ends the link: the read after the
    /// wait returns zero and the peer sees close status 1003, though binary data follows the text message.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadAsync_EmptyTextMessageSeenByTheWait_ClosesWith1003AndReturnsZero()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        await serverSocket.SendAsync(Array.Empty<byte>(), WebSocketMessageType.Text, true, CancellationToken.None);
        await SendBinaryAsync(serverSocket, [0xC0, 0x00]);
        byte[] buffer = new byte[16];

        // Act
        await link.WaitForDataAsync(CancellationToken.None);
        int read = await link.ReadAsync(buffer, CancellationToken.None);
        WebSocketReceiveResult closeResult = await serverSocket.ReceiveAsync(buffer, CancellationToken.None).Within("The peer's receive");

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
        WebSocketReceiveResult answer = await serverSocket.ReceiveAsync(buffer, CancellationToken.None).Within("The peer's receive");

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
    /// Proves an empty binary message, which a zero-byte receive takes whole, does not complete the wait for data,
    /// and that the message with bytes that follows it does.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WaitForDataAsync_EmptyBinaryMessage_GoesOnWaitingUntilDataArrives()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create(inline: true);
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        ValueTask wait = link.WaitForDataAsync(CancellationToken.None);

        // Act: over the inline pair the link has seen a message by the time its send returns.
        await SendBinaryInlineAsync(serverSocket, []);
        bool completedByTheEmptyMessage = wait.IsCompleted;
        await SendBinaryInlineAsync(serverSocket, [0xC0, 0x00]);
        bool completedByTheData = wait.IsCompleted;
        await wait;
        byte[] bytes = await ReadExactlyAsync(link, 2);

        // Assert
        completedByTheEmptyMessage.ShouldBeFalse();
        completedByTheData.ShouldBeTrue();
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
        WebSocketReceiveResult result = await serverSocket.ReceiveAsync(buffer, CancellationToken.None).Within("The peer's receive");

        // Assert
        result.MessageType.ShouldBe(WebSocketMessageType.Binary);
        result.EndOfMessage.ShouldBeTrue();
        result.Count.ShouldBe(2);
        buffer.AsSpan(0, 2).ToArray().ShouldBe(new byte[] { 0xC0, 0x00 });
    }

    /// <summary>
    /// Proves a write after a text message made the link start its close with 1003 completes without failing and
    /// sends nothing: the peer receives the close frame and no binary message.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WriteAsync_AfterATextMessageClosedTheLink_SendsNothingAndCompletes()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        await serverSocket.SendAsync("x"u8.ToArray(), WebSocketMessageType.Text, true, CancellationToken.None);
        byte[] buffer = new byte[16];
        (await link.ReadAsync(buffer, CancellationToken.None)).ShouldBe(0);

        // Act
        ValueTask write = link.WriteAsync(new byte[] { 0x40, 0x02, 0x00, 0x01 }, CancellationToken.None);
        bool completedAtOnce = write.IsCompletedSuccessfully;
        await write;
        WebSocketReceiveResult first = await serverSocket.ReceiveAsync(buffer, CancellationToken.None).Within("The peer's receive");

        // Assert
        completedAtOnce.ShouldBeTrue();
        first.MessageType.ShouldBe(WebSocketMessageType.Close);
        first.CloseStatus.ShouldBe(WebSocketCloseStatus.InvalidMessageType);
    }

    /// <summary>
    /// Proves a write that starts while the link's own close with 1003 still waits behind a write in progress
    /// completes at once without failing, so it does not queue behind the close: the peer reads the first message
    /// whole and then the close. What would follow the close the peer cannot read, so the test does not look for it.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WriteAsync_WhileTheTextCloseWaitsBehindAWrite_CompletesAtOnceWithoutQueueing()
    {
        // Arrange
        (WebSocket client, WebSocket server) = WebSocketPair.Create();
        using WebSocket clientSocket = client;
        using WebSocket serverSocket = server;
        WebSocketLink link = new(clientSocket);
        byte[] message = new byte[200_000];

        // More than the pipe takes before its reader reads, so the write stays pending.
        ValueTask firstWrite = link.WriteAsync(message, CancellationToken.None);
        firstWrite.IsCompleted.ShouldBeFalse();
        await serverSocket.SendAsync("x"u8.ToArray(), WebSocketMessageType.Text, true, CancellationToken.None);

        // The wait sees the text message, so the read starts the link's own close, which waits behind the write.
        await link.WaitForDataAsync(CancellationToken.None);
        Task<int> read = link.ReadAsync(new byte[16], CancellationToken.None).AsTask();
        read.IsCompleted.ShouldBeFalse();

        // Act
        ValueTask secondWrite = link.WriteAsync(new byte[] { 0x40, 0x02, 0x00, 0x01 }, CancellationToken.None);
        bool completedAtOnce = secondWrite.IsCompletedSuccessfully;
        byte[] buffer = new byte[256_000];
        int received = 0;
        WebSocketReceiveResult result;
        do
        {
            result = await serverSocket
                .ReceiveAsync(new ArraySegment<byte>(buffer, received, buffer.Length - received), CancellationToken.None)
                .Within("The peer's receive");
            received += result.Count;
        }
        while (result.MessageType == WebSocketMessageType.Binary);

        await firstWrite;
        await secondWrite.AsTask().Within("The second write");

        // Assert
        completedAtOnce.ShouldBeTrue();
        received.ShouldBe(message.Length);
        result.MessageType.ShouldBe(WebSocketMessageType.Close);
        result.CloseStatus.ShouldBe(WebSocketCloseStatus.InvalidMessageType);
        (await read).ShouldBe(0);
    }

    /// <summary>
    /// Proves the link drops a pending send's failure once its close has started: the scripted socket fails the send
    /// after the link's close frame went out, and the write completes without failing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WriteAsync_SendFailsAfterTheCloseStarted_CompletesWithoutFailing()
    {
        // Arrange
        TaskCompletionSource send = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using ScriptedSendWebSocket socket = new() { Send = () => new ValueTask(send.Task) };
        WebSocketLink link = new(socket);
        ValueTask write = link.WriteAsync(new byte[] { 0x40, 0x02, 0x00, 0x01 }, CancellationToken.None);
        await link.CloseAsync(WebSocketCloseStatus.NormalClosure, CancellationToken.None);

        // Act
        send.SetException(new WebSocketException(WebSocketError.InvalidState));

        // Assert
        await Should.NotThrowAsync(write.AsTask().Within("The write"));
        socket.Sends.ShouldBe(1);
        socket.Closes.ShouldBe(1);
    }

    /// <summary>
    /// Proves a pending send's failure surfaces as the write's failure while the link has not started its close.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WriteAsync_SendFailsBeforeAnyClose_ThrowsTheFailure()
    {
        // Arrange
        TaskCompletionSource send = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using ScriptedSendWebSocket socket = new() { Send = () => new ValueTask(send.Task) };
        WebSocketLink link = new(socket);
        ValueTask write = link.WriteAsync(new byte[] { 0x40, 0x02, 0x00, 0x01 }, CancellationToken.None);

        // Act
        send.SetException(new WebSocketException(WebSocketError.ConnectionClosedPrematurely));

        // Assert
        await Should.ThrowAsync<WebSocketException>(write.AsTask().Within("The write"));
    }

    /// <summary>
    /// Proves the link drops a send that the socket refuses at once, by throwing, when the link's close started after
    /// the write's check: the write completes at once without failing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WriteAsync_SendThrowsAtOnceAfterTheCloseStarted_CompletesWithoutFailing()
    {
        // Arrange
        using ScriptedSendWebSocket socket = new();
        WebSocketLink link = new(socket);
        ValueTask close = default;
        socket.Send = () =>
        {
            // The close starts between the link's check and the socket's refusal.
            close = link.CloseAsync(WebSocketCloseStatus.NormalClosure, CancellationToken.None);
            throw new WebSocketException(WebSocketError.InvalidState);
        };

        // Act
        ValueTask write = link.WriteAsync(new byte[] { 0x40, 0x02, 0x00, 0x01 }, CancellationToken.None);

        // Assert
        write.IsCompletedSuccessfully.ShouldBeTrue();
        await close;
        socket.Sends.ShouldBe(1);
        socket.Closes.ShouldBe(1);
    }

    /// <summary>
    /// Proves a send that the socket refuses at once, by throwing, surfaces as the write's failure while the link has
    /// not started its close.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WriteAsync_SendThrowsAtOnceBeforeAnyClose_ThrowsTheFailure()
    {
        // Arrange
        using ScriptedSendWebSocket socket = new()
        {
            Send = () => throw new WebSocketException(WebSocketError.ConnectionClosedPrematurely),
        };
        WebSocketLink link = new(socket);

        // Act
        Func<Task> write = async () => await link.WriteAsync(new byte[] { 0x40, 0x02, 0x00, 0x01 }, CancellationToken.None);

        // Assert
        await Should.ThrowAsync<WebSocketException>(write);
        socket.Closes.ShouldBe(0);
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
        WebSocketReceiveResult result = await serverSocket.ReceiveAsync(buffer, CancellationToken.None).Within("The peer's receive");

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
