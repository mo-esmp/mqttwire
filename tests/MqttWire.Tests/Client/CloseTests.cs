using System.Net.WebSockets;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests how a connection ends: by the application, by the server, by the link, or by a packet the client cannot
/// accept; and that the end is reported once.
/// </summary>
public sealed class CloseTests
{
    /// <summary>
    /// The payload of the publishes these tests send, "hi".
    /// </summary>
    private static readonly byte[] s_hi = "hi"u8.ToArray();

    /// <summary>
    /// Packets a server never sends a client, as hex: CONNECT, SUBSCRIBE, UNSUBSCRIBE, PINGREQ, AUTH and a second
    /// CONNACK.
    /// </summary>
    public static TheoryData<string> PacketsAClientNeverReceives => new()
    {
        "10 18 00 04 4D 51 54 54 05 C2 00 0F 03 21 04 00 00 02 63 31 00 01 75 00 01 70",
        "82 09 00 01 00 00 03 61 2F 2B 01",
        "A2 08 00 02 00 00 03 61 2F 2B",
        "C0 00",
        "F0 00",
        "20 03 00 00 00",
    };

    /// <summary>
    /// Proves disposing a connected client sends DISCONNECT 0x00, closes the link with 1000 and reports
    /// <see cref="MqttClientCloseReason.Disposed"/> once the server has answered the close.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task DisposeAsync_Connected_SendsDisconnect00ThenClosesWith1000()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();

        // Act
        ValueTask dispose = harness.Client.DisposeAsync();
        byte[] disconnect = harness.NextPacket();
        WebSocketCloseStatus? closedWith = harness.Link.ClosedWith;
        bool connectedWhileClosing = harness.Client.IsConnected;
        harness.Link.EndFromPeer();
        await dispose;

        // Assert
        disconnect.ShouldBe(ClientPackets.Disconnect);
        closedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        connectedWhileClosing.ShouldBeFalse();
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.Disposed, MqttReasonCode.Success, null, null)]);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a close the peer never takes, here a DISCONNECT write that never completes, is ended by aborting the
    /// link after two seconds, not before.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task DisposeAsync_PeerNeverAcceptsTheClose_AbortsAfterTwoSeconds()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.Link.HoldWrites();
        ValueTask dispose = harness.Client.DisposeAsync();

        // Act
        harness.Time.Advance(TimeSpan.FromSeconds(2) - TimeSpan.FromTicks(1));
        bool abortedEarly = harness.Link.IsAborted;
        harness.Time.Advance(TimeSpan.FromTicks(1));
        bool aborted = harness.Link.IsAborted;
        harness.Link.CompleteHeldWrites(new OperationCanceledException());
        await dispose;

        // Assert
        abortedEarly.ShouldBeFalse();
        aborted.ShouldBeTrue();
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.Disposed, MqttReasonCode.Success, null, null)]);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a DISCONNECT from the server is reported once with its reason code, the client answers with a close
    /// and no DISCONNECT of its own, and nothing more is reported when the link then ends.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_ServerDisconnects_ReportsServerDisconnectedOnce()
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(ClientPackets.DisconnectSessionTakenOver);
        bool connected = harness.Client.IsConnected;
        WebSocketCloseStatus? closedWith = harness.Link.ClosedWith;
        harness.Link.EndFromPeer();
        harness.Time.Advance(TimeSpan.FromSeconds(10));

        // Assert
        connected.ShouldBeFalse();
        closedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Link.TakeWritten().ShouldBeEmpty();
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ServerDisconnected, MqttReasonCode.SessionTakenOver, null, null)]);
    }

    /// <summary>
    /// Proves the link ending from the server's side is reported as a lost connection, and the client answers the
    /// close.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_PeerEndsTheLink_ReportsConnectionLost()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);

        // Act
        harness.Link.EndFromPeer();

        // Assert
        harness.Client.IsConnected.ShouldBeFalse();
        harness.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ConnectionLost, MqttReasonCode.Success, null, null)]);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a failed read is reported as a lost connection carrying the read's exception.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_ReadFails_ReportsConnectionLostWithTheException()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        IOException error = new("read failed");

        // Act
        harness.Link.FailReads(error);

        // Assert
        harness.Client.IsConnected.ShouldBeFalse();
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ConnectionLost, MqttReasonCode.Success, null, error)]);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a packet whose header announces more than <c>MaxIncomingPacketBytes</c> is refused with DISCONNECT
    /// 0x95 as soon as its header is read, without renting a buffer of its size.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_PacketOverMaxIncomingPacketBytes_SendsDisconnect95BeforeBuffering()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool, maxIncomingPacketBytes: 4096);
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(Hex.Parse("30 FF FF 7F"));
        byte[] disconnect = harness.NextPacket();
        WebSocketCloseStatus? closedWith = harness.Link.ClosedWith;
        harness.Link.EndFromPeer();

        // Assert
        disconnect.ShouldBe(ClientPackets.DisconnectPacketTooLarge);
        closedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.PacketTooLarge, null, null)]);
        pool.LargestRent.ShouldBeLessThan(4097);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a remaining length that runs past four bytes is malformed and answered with DISCONNECT 0x81.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_InvalidRemainingLength_SendsDisconnect81()
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(Hex.Parse("30 FF FF FF FF"));
        byte[] disconnect = harness.NextPacket();
        harness.Link.EndFromPeer();

        // Assert
        disconnect.ShouldBe(ClientPackets.DisconnectMalformedPacket);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.MalformedPacket, null, null)]);
    }

    /// <summary>
    /// Proves a packet a server never sends a client is a protocol error answered with DISCONNECT 0x82.
    /// </summary>
    /// <param name="hex">
    /// The packet the server sends.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [MemberData(nameof(PacketsAClientNeverReceives))]
    public async Task Read_PacketAClientNeverReceives_SendsDisconnect82(string hex)
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(Hex.Parse(hex));
        byte[] disconnect = harness.NextPacket();
        harness.Link.EndFromPeer();

        // Assert
        disconnect.ShouldBe(ClientPackets.DisconnectProtocolError);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.ProtocolError, null, null)]);
    }

    /// <summary>
    /// Proves disposing while the link is still opening cancels the opening, waits for the connect to end, and
    /// makes the connect throw <see cref="ObjectDisposedException"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task DisposeAsync_WhileTheLinkOpens_CancelsItAndTheConnectThrowsObjectDisposed()
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();
        harness.Connector.HoldConnects();
        Task<MqttConnectAck> connect = harness.Client.ConnectAsync();
        int pendingBefore = harness.Connector.PendingConnects;

        // Act
        await harness.Client.DisposeAsync();
        bool connectEnded = connect.IsCompleted;
        Exception? exception = await Record.ExceptionAsync(() => connect);

        // Assert
        pendingBefore.ShouldBe(1);
        connectEnded.ShouldBeTrue();
        exception.ShouldBeOfType<ObjectDisposedException>();
        harness.Connector.PendingConnects.ShouldBe(0);
        harness.Connector.Links.ShouldBeEmpty();
        harness.Client.IsConnected.ShouldBeFalse();
        harness.Closed.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves disposing while the client waits for CONNACK aborts the link, sends nothing more, waits for the
    /// connect to end, and makes the connect throw <see cref="ObjectDisposedException"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task DisposeAsync_WhileWaitingForConnAck_AbortsAndTheConnectThrowsObjectDisposed()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);
        Task<MqttConnectAck> connect = harness.Client.ConnectAsync();
        harness.NextPacket();

        // Act
        await harness.Client.DisposeAsync();
        bool connectEnded = connect.IsCompleted;
        Exception? exception = await Record.ExceptionAsync(() => connect);

        // Assert
        connectEnded.ShouldBeTrue();
        exception.ShouldBeOfType<ObjectDisposedException>();
        harness.Link.IsAborted.ShouldBeTrue();
        harness.Link.TakeWritten().ShouldBeEmpty();
        harness.Client.IsConnected.ShouldBeFalse();
        harness.Closed.ShouldBeEmpty();
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a PINGRESP is accepted after connecting and its arrival time is noted, without ending anything.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_PingResp_NotesWhenItArrived()
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();
        harness.Time.Advance(TimeSpan.FromSeconds(3));

        // Act
        harness.Link.Deliver(ClientPackets.PingResp);

        // Assert
        harness.Client.LastPingResponseTimestamp.ShouldBe(harness.Time.GetTimestamp());
        harness.Client.IsConnected.ShouldBeTrue();
        harness.Link.TakeWritten().ShouldBeEmpty();
        harness.Closed.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves that when several ways out happen one after another, a DISCONNECT from the server, the link ending
    /// and the application disposing, only the first is reported.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_EveryWayOut_ReportsOnClosedExactlyOnce()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);

        // Act
        harness.Link.Deliver(ClientPackets.DisconnectSessionTakenOver);
        harness.Link.EndFromPeer();
        await harness.Client.DisposeAsync();
        harness.Time.Advance(TimeSpan.FromSeconds(10));

        // Assert
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ServerDisconnected, MqttReasonCode.SessionTakenOver, null, null)]);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves an exception from <c>OnClosed</c> is swallowed: the connection still ends cleanly and disposing
    /// does not throw.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task OnClosed_Throws_IsSwallowed()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool, onClosed: _ => throw new InvalidOperationException("handler failed"));
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);

        // Act
        harness.Link.EndFromPeer();
        Exception? exception = await Record.ExceptionAsync(async () => await harness.Client.DisposeAsync());

        // Assert
        exception.ShouldBeNull();
        harness.Closed.Count.ShouldBe(1);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves an <c>OnClosed</c> that starts <see cref="MqttWireClient.DisposeAsync"/> without waiting for it does
    /// not deadlock: the dispose completes once the connection is finished, and <c>OnClosed</c> is called once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task DisposeAsync_FromOnClosed_DoesNotDeadlock()
    {
        // Arrange
        MqttWireClient? client = null;
        Task? dispose = null;
        ClientHarness harness = ClientHarness.Create(onClosed: _ =>
        {
            if (client is not null)
            {
                dispose = client.DisposeAsync().AsTask();
            }
        });
        client = harness.Client;
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();

        // Act
        harness.Link.EndFromPeer();
        Task started = dispose.ShouldNotBeNull();
        await started;

        // Assert
        started.IsCompletedSuccessfully.ShouldBeTrue();
        harness.Link.TakeWritten().ShouldBeEmpty();
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ConnectionLost, MqttReasonCode.Success, null, null)]);
    }

    /// <summary>
    /// Proves an <c>OnMessage</c> that starts <see cref="MqttWireClient.DisposeAsync"/> without waiting for it ends
    /// the connection once, without deadlock: the second PUBLISH of the same read is not handed on, and
    /// <c>OnClosed</c> reports <see cref="MqttClientCloseReason.Disposed"/> once, after the last <c>OnMessage</c>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task OnMessage_CallsDisposeAsync_EndsOnceWithoutDeadlock()
    {
        // Arrange
        MqttWireClient? client = null;
        Task? dispose = null;
        List<string> events = [];
        ClientHarness harness = ClientHarness.Create(
            onMessage: (in _) =>
            {
                events.Add("message");
                if (client is not null && dispose is null)
                {
                    dispose = client.DisposeAsync().AsTask();
                }
            },
            onClosed: _ => events.Add("closed"));
        client = harness.Client;
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();

        // Act
        harness.Link.Deliver([.. ClientPackets.PublishQoS0, .. ClientPackets.PublishQoS0]);
        byte[] disconnect = harness.NextPacket();
        harness.Link.EndFromPeer();
        Task started = dispose.ShouldNotBeNull();
        await started;

        // Assert
        disconnect.ShouldBe(ClientPackets.Disconnect);
        harness.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        events.ShouldBe(["message", "closed"]);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.Disposed, MqttReasonCode.Success, null, null)]);
    }

    /// <summary>
    /// Proves a publish still queued behind a held write when the connection ends is never written and completes
    /// with <see cref="MqttRequestStatus.ConnectionLost"/>, and every buffer comes back once the held write ends.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_QueuedWhenTheConnectionEnds_IsNotWrittenAndCompletesWithConnectionLost()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();
        harness.Link.HoldWrites();
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        ValueTask<MqttRequestResult> queued = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);

        // Act
        harness.Link.EndFromPeer();
        MqttRequestResult result = await queued;
        harness.Link.CompleteHeldWrites();
        await pool.WhenNoneOutstandingAsync();

        // Assert
        result.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
        harness.Link.TakeWritten().ShouldBe(ClientPackets.PublishQoS0);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ConnectionLost, MqttReasonCode.Success, null, null)]);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that when the connection ends with 1,600 QoS 1 publishes made from eight threads, while the server
    /// acknowledges some and sends messages, every request completes exactly once: the 800 acknowledged with
    /// <see cref="MqttRequestStatus.Completed"/>, the rest with <see cref="MqttRequestStatus.ConnectionLost"/>; no
    /// identifier stays in use; <c>OnClosed</c> comes once, after the last <c>OnMessage</c>; and the pool
    /// balances.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task End_ManyThreadsPublishing_EveryRequestCompletesExactlyOnce()
    {
        // Arrange
        const int Threads = 8;
        const int PerThreadPerRound = 100;
        CountingPool pool = new();
        Lock gate = new();
        int messages = 0;
        int closedCount = 0;
        bool messageAfterClosed = false;
        ClientHarness harness = ClientHarness.Create(
            pool: pool,
            onMessage: (in _) =>
            {
                lock (gate)
                {
                    messageAfterClosed |= closedCount > 0;
                    messages++;
                }
            },
            onClosed: _ =>
            {
                lock (gate)
                {
                    closedCount++;
                }
            });
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();

        // Identifiers 1 to 800, of which the server acknowledges 1 to 600 at once. With at most 1,000 in flight,
        // no publish meets the limit of 1,024.
        Task<MqttRequestResult>[][] first = await PublishFromThreadsAsync(harness.Client, Threads, PerThreadPerRound);
        harness.Link.Deliver([.. PubAcks(1, 600), .. ClientPackets.PublishQoS0]);

        // Act
        // Identifiers 801 to 1,600 from eight threads, while the server acknowledges 601 to 800 and sends messages.
        Task server = Task.Factory.StartNew(
            () =>
            {
                for (int id = 601; id <= 800; id += 10)
                {
                    harness.Link.Deliver([.. PubAcks(id, 10), .. ClientPackets.PublishQoS0]);
                }
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        Task<MqttRequestResult>[][] second = await PublishFromThreadsAsync(harness.Client, Threads, PerThreadPerRound);
        await server;
        harness.Link.EndFromPeer();
        MqttRequestResult[] results = await Task.WhenAll(first.Concat(second).SelectMany(static tasks => tasks));
        await harness.Client.DisposeAsync();
        await pool.WhenNoneOutstandingAsync();

        // Assert
        results.Length.ShouldBe(2 * Threads * PerThreadPerRound);
        results.Count(static result => result.Status == MqttRequestStatus.Completed).ShouldBe(800);
        results.Count(static result => result.Status == MqttRequestStatus.ConnectionLost).ShouldBe(800);
        harness.Client.RequestsInFlight.ShouldBe(0);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ConnectionLost, MqttReasonCode.Success, null, null)]);
        lock (gate)
        {
            closedCount.ShouldBe(1);
            messages.ShouldBe(21);
            messageAfterClosed.ShouldBeFalse();
        }

        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Publishes at QoS 1 from several threads at once, each starting when all are ready.
    /// </summary>
    /// <param name="client">
    /// The connected client.
    /// </param>
    /// <param name="threads">
    /// How many threads publish.
    /// </param>
    /// <param name="perThread">
    /// How many publishes each thread makes.
    /// </param>
    /// <returns>
    /// Each thread's publishes, in its call order, once every call has returned.
    /// </returns>
    private static async Task<Task<MqttRequestResult>[][]> PublishFromThreadsAsync(MqttWireClient client, int threads, int perThread)
    {
        using Barrier start = new(threads);
        Task<Task<MqttRequestResult>[]>[] publishers = new Task<Task<MqttRequestResult>[]>[threads];
        for (int thread = 0; thread < threads; thread++)
        {
            publishers[thread] = Task.Factory.StartNew(
                () =>
                {
                    start.SignalAndWait();
                    Task<MqttRequestResult>[] publishes = new Task<MqttRequestResult>[perThread];
                    for (int i = 0; i < perThread; i++)
                    {
                        publishes[i] = client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce).AsTask();
                    }

                    return publishes;
                },
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        return await Task.WhenAll(publishers);
    }

    /// <summary>
    /// Encodes successful PUBACKs for consecutive packet identifiers, one after another.
    /// </summary>
    /// <param name="first">
    /// The first packet identifier.
    /// </param>
    /// <param name="count">
    /// How many PUBACKs.
    /// </param>
    /// <returns>
    /// The PUBACKs, four bytes each.
    /// </returns>
    private static byte[] PubAcks(int first, int count)
    {
        byte[] bytes = new byte[count * 4];
        for (int i = 0; i < count; i++)
        {
            int id = first + i;
            bytes[i * 4] = 0x40;
            bytes[(i * 4) + 1] = 0x02;
            bytes[(i * 4) + 2] = (byte)(id >> 8);
            bytes[(i * 4) + 3] = (byte)id;
        }

        return bytes;
    }
}
