using System.Net.WebSockets;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests the client's send path (the client's send-path design, section 5): writes handed over while another is in
/// progress wait in a queue and reach the link in the order they were handed over, one at a time; a caller writes at
/// most sixteen packets on its own thread before the thread pool takes the rest; and every way out, a failed write,
/// the end of the connection or a link that refuses the registration of a continuation, gives every buffer back
/// exactly once, and never while its write may still use it.
/// </summary>
public sealed class SendPathTests
{
    /// <summary>
    /// The payload of the publishes these tests send, "hi".
    /// </summary>
    private static readonly byte[] s_hi = "hi"u8.ToArray();

    /// <summary>
    /// Proves publishes handed over while a write is held complete at once, and reach the link afterwards in the
    /// order of the calls, one write at a time.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_WhileWriteHeld_LinkGetsPublishesInCallOrder()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        MemoryLink link = harness.Link;
        link.TakeWritten();
        link.HoldWrites();
        int writesBefore = link.WriteCount;

        // Act
        bool[] completedAtOnce = new bool[3];
        for (int i = 0; i < completedAtOnce.Length; i++)
        {
            ValueTask<MqttRequestResult> publish =
                harness.Client.PublishAsync("a/b", new[] { (byte)('1' + i) }, MqttQoS.AtMostOnce);
            completedAtOnce[i] = publish.IsCompleted;
        }

        link.CompleteHeldWrites();
        await link.WhenWrittenAsync(writesBefore + 3);

        // Assert
        completedAtOnce.ShouldAllBe(static completed => completed);
        link.TakeWritten().ShouldBe([.. PublishOf((byte)'1'), .. PublishOf((byte)'2'), .. PublishOf((byte)'3')]);
        link.OverlappingWrites.ShouldBe(0);
    }

    /// <summary>
    /// Proves a caller that takes the free path while sixteen other packets are handed over behind it writes
    /// sixteen packets on its own thread, its own and fifteen others, and the thread pool writes the last one, all
    /// in the order they were handed over.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_SixteenWaitingWhenCallerTakesPath_CallerWritesSixteenAndPoolWritesRest()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        MqttWireClient client = harness.Client;
        MemoryLink link = harness.Link;
        link.TakeWritten();
        int writesBefore = link.WriteCount;
        int callersThread = Environment.CurrentManagedThreadId;
        bool inFirstCall = false;
        bool othersHandedOver = false;
        int writesOnCallersThread = 0;
        link.Writing = _ =>
        {
            if (Volatile.Read(ref inFirstCall) && Environment.CurrentManagedThreadId == callersThread)
            {
                writesOnCallersThread++;
            }

            if (othersHandedOver)
            {
                return;
            }

            // Inside the first write, which holds the path: these sixteen wait behind it.
            othersHandedOver = true;
            PublishOthers(client);
        };

        // Act
        Volatile.Write(ref inFirstCall, true);
        _ = client.PublishAsync("a/b", new byte[] { 0 }, MqttQoS.AtMostOnce);
        Volatile.Write(ref inFirstCall, false);
        int writtenOnCallersThread = writesOnCallersThread;
        await link.WhenWrittenAsync(writesBefore + 17);

        // Assert
        writtenOnCallersThread.ShouldBe(16);
        byte[] expected = [];
        for (int i = 0; i <= 16; i++)
        {
            expected = [.. expected, .. PublishOf((byte)i)];
        }

        link.TakeWritten().ShouldBe(expected);
        link.OverlappingWrites.ShouldBe(0);
    }

    /// <summary>
    /// Proves a write that fails while others wait ends the connection with
    /// <see cref="MqttClientCloseReason.WriteFailed"/> and the failure, drops the waiting writes unwritten, and gives
    /// every buffer back exactly once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_WriteFailsWithWritesWaiting_EndsWithWriteFailedAndDropsThem()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool);
        MemoryLink link = harness.Link;
        link.TakeWritten();
        link.PendWrites();
        IOException error = new("write failed");
        for (int i = 0; i < 3; i++)
        {
            _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        }

        int outstandingWhileWaiting = pool.Outstanding;

        // Act
        link.CompletePendingWrite(error);

        // Assert
        outstandingWhileWaiting.ShouldBe(3);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.WriteFailed, MqttReasonCode.Success, null, error)]);
        link.TakeWritten().ShouldBeEmpty();
        link.IsAborted.ShouldBeTrue();
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a dispose begun while writes wait behind a held one writes, once that write ends, only the DISCONNECT
    /// after it: the waiting publishes are dropped, the link is closed with 1000 after the DISCONNECT, and the
    /// dispose completes with every buffer back exactly once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task DisposeAsync_WritesWaiting_WritesDisconnectAfterHeldWriteOnly()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool);
        MemoryLink link = harness.Link;
        link.TakeWritten();
        link.HoldWrites();
        int writesBefore = link.WriteCount;
        for (int i = 0; i < 3; i++)
        {
            _ = harness.Client.PublishAsync("a/b", new[] { (byte)('1' + i) }, MqttQoS.AtMostOnce);
        }

        // Act
        Task dispose = harness.Client.DisposeAsync().AsTask();
        bool disposedWhileHeld = dispose.IsCompleted;
        link.CompleteHeldWrites();

        // The held write's continuation may run on the thread pool: the peer ends the link once the DISCONNECT is in.
        await link.WhenWrittenAsync(writesBefore + 2);
        link.EndFromPeer();
        await dispose.Within("the dispose");

        // Assert
        disposedWhileHeld.ShouldBeFalse();
        link.TakeWritten().ShouldBe([.. PublishOf((byte)'1'), .. ClientPackets.Disconnect]);
        link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        link.OverlappingWrites.ShouldBe(0);
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a dispose after an end that aborted the link does not complete while a write is still held, and
    /// completes once that write has ended, with every buffer back.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task DisposeAsync_WriteHeld_ReturnsOnlyAfterWriteEnds()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool);
        harness.Link.HoldWrites();
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        harness.Link.FailReads(new IOException("read failed"));

        // Act
        Task dispose = harness.Client.DisposeAsync().AsTask();
        bool disposedWhileHeld = dispose.IsCompleted;
        int outstandingWhileHeld = pool.Outstanding;
        harness.Link.CompleteHeldWrites();
        await dispose.Within("the dispose");

        // Assert
        disposedWhileHeld.ShouldBeFalse();
        outstandingWhileHeld.ShouldBe(1);
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a link that refuses the registration of a write's continuation ends the connection with
    /// <see cref="MqttClientCloseReason.WriteFailed"/>, and the write's buffer, which the write may still use, goes
    /// back only when that write ends, exactly once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_LinkRefusesRegistration_EndsAndReturnsBufferOnlyWhenWriteEnds()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool);
        harness.Link.RefuseWriteRegistration();

        // Act
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        MqttClientCloseReason[] closed = [.. harness.Closed.Select(static info => info.Reason)];
        int outstandingBeforeTheWriteEnds = pool.Outstanding;
        bool ended = harness.Link.CompleteRefusedWrite();

        // Assert
        closed.ShouldBe([MqttClientCloseReason.WriteFailed]);
        harness.Link.IsAborted.ShouldBeTrue();
        outstandingBeforeTheWriteEnds.ShouldBe(1);
        ended.ShouldBeTrue();
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that once a link has refused the registration of a write's continuation, nothing more reaches it while
    /// that write may still be in progress: a dispose begun as the write starts records the end with a close and
    /// queues the DISCONNECT, yet the refusal aborts the link all the same, the DISCONNECT is dropped and the close is
    /// not attempted, no write overlaps the refused one, and every buffer but the refused write's is back when the
    /// dispose completes; that one comes back, once, when the write ends.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_RefusedWhileDisposeQueuesDisconnect_WritesNothingMoreOnTheLink()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool);
        MqttWireClient client = harness.Client;
        MemoryLink link = harness.Link;
        link.TakeWritten();
        Task? dispose = null;
        link.Writing = _ => dispose ??= client.DisposeAsync().AsTask();
        link.RefuseWriteRegistration();

        // Act
        _ = client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        await dispose.ShouldNotBeNull().Within("the dispose");
        int outstandingAfterTheDispose = pool.Outstanding;
        bool ended = link.CompleteRefusedWrite();

        // Assert
        link.IsAborted.ShouldBeTrue();
        link.OverlappingWrites.ShouldBe(0);
        link.WritesAfterAbort.ShouldBe(0);
        link.ClosedWith.ShouldBeNull();
        harness.Closed.Select(static info => info.Reason).ShouldBe([MqttClientCloseReason.Disposed]);
        outstandingAfterTheDispose.ShouldBe(1);
        ended.ShouldBeTrue();
        link.TakeWritten().ShouldBe(ClientPackets.PublishQoS0);
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a pool whose return throws once, at the end of a write that carries the send path on, gives the path up
    /// instead of leaving it held: the connection ends with <see cref="MqttClientCloseReason.WriteFailed"/> and what
    /// was thrown, the writes waiting are dropped unwritten, the dispose completes, and every buffer but the one whose
    /// return threw comes back exactly once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task CompletePendingWrite_PoolReturnThrowsWithWritesWaiting_EndsWithWriteFailedAndDisposeCompletes()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool);
        MemoryLink link = harness.Link;
        link.TakeWritten();
        link.PendWrites();
        for (int i = 0; i < 3; i++)
        {
            _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        }

        InvalidOperationException refused = new("return refused");
        int armed = 1;
        pool.Returning = _ =>
        {
            if (Interlocked.Exchange(ref armed, 0) == 1)
            {
                throw refused;
            }
        };

        // Act
        link.CompletePendingWrite();
        await harness.Client.DisposeAsync().AsTask().Within("the dispose");

        // Assert
        harness.Closed.ShouldBe(
            [new MqttClientCloseInfo(MqttClientCloseReason.WriteFailed, MqttReasonCode.Success, null, refused)]);
        link.TakeWritten().ShouldBe(ClientPackets.PublishQoS0);
        link.IsAborted.ShouldBeTrue();
        link.OverlappingWrites.ShouldBe(0);

        // The buffer whose return threw is never returned again: a second return could hand it out twice.
        pool.Outstanding.ShouldBe(1);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a link whose abort throws once, as the failure of a write that carries the send path on aborts it,
    /// leaves no write behind: the connection ends with <see cref="MqttClientCloseReason.WriteFailed"/> and the
    /// write's failure, the failed write and the writes waiting give their buffers back exactly once, the link is
    /// aborted all the same, and the dispose completes.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task CompletePendingWrite_AbortThrowsAfterFailedWrite_EndsWithWriteFailedAndReturnsEveryBuffer()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool);
        MemoryLink link = harness.Link;
        link.TakeWritten();
        link.PendWrites();
        for (int i = 0; i < 3; i++)
        {
            _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        }

        IOException error = new("write failed");
        link.FailNextAbort(new InvalidOperationException("abort refused"));

        // Act
        link.CompletePendingWrite(error);
        await harness.Client.DisposeAsync().AsTask().Within("the dispose");

        // Assert
        harness.Closed.ShouldBe(
            [new MqttClientCloseInfo(MqttClientCloseReason.WriteFailed, MqttReasonCode.Success, null, error)]);
        link.TakeWritten().ShouldBeEmpty();
        link.IsAborted.ShouldBeTrue();
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a write whose link refused the registration of its continuation, and whose end then meets a pool that
    /// throws, throws nothing into the thread that ends it: the connection stays ended as the refusal ended it.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task CompleteRefusedWrite_PoolReturnThrows_ThrowsNothing()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool);
        harness.Link.RefuseWriteRegistration();
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        pool.Returning = static _ => throw new InvalidOperationException("return refused");

        // Act
        bool ended = false;
        Exception? thrown = Record.Exception(() => ended = harness.Link.CompleteRefusedWrite());

        // Assert
        thrown.ShouldBeNull();
        ended.ShouldBeTrue();
        harness.Closed.Select(static info => info.Reason).ShouldBe([MqttClientCloseReason.WriteFailed]);
        harness.Link.IsAborted.ShouldBeTrue();
    }

    /// <summary>
    /// Proves an end the read loop records itself, for a PUBLISH with a Topic Alias, while writes wait behind a held
    /// one, writes once that write ends only the DISCONNECT 0x94 after it: the waiting publishes are dropped, the link
    /// is closed with 1000 after the DISCONNECT, and every buffer comes back exactly once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadLoop_ProtocolErrorWithWritesWaiting_WritesHeldWriteThenOnlyDisconnect()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool);
        MemoryLink link = harness.Link;
        link.TakeWritten();
        link.HoldWrites();
        int writesBefore = link.WriteCount;
        for (int i = 0; i < 3; i++)
        {
            _ = harness.Client.PublishAsync("a/b", new[] { (byte)('1' + i) }, MqttQoS.AtMostOnce);
        }

        // Act
        link.Deliver(ClientPackets.PublishWithTopicAlias);
        link.CompleteHeldWrites();

        // The held write's continuation may run on the thread pool: the peer ends the link once the DISCONNECT is in.
        await link.WhenWrittenAsync(writesBefore + 2);
        link.EndFromPeer();
        await harness.Client.DisposeAsync().AsTask().Within("the dispose");

        // Assert
        link.TakeWritten().ShouldBe([.. PublishOf((byte)'1'), .. ClientPackets.DisconnectTopicAliasInvalid]);
        link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Closed.ShouldBe(
            [new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.TopicAliasInvalid, null, null)]);
        link.OverlappingWrites.ShouldBe(0);
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a PINGREQ queued behind a pending write and dropped by the end of the connection is never written, and
    /// its buffer goes back exactly once, when the write ahead of it ends.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Tick_PingReqDroppedByEnd_ReturnsItsBufferUnwritten()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool);
        MemoryLink link = harness.Link;
        link.TakeWritten();
        link.PendWrites();
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);

        // Nothing was written for half the 15-second keep-alive: the tick queues a PINGREQ behind the publish.
        harness.Time.Advance(TimeSpan.FromSeconds(7.5));
        int outstandingWithPingQueued = pool.Outstanding;

        // Act
        link.FailReads(new IOException("read failed"));
        link.CompletePendingWrite();

        // Assert
        outstandingWithPingQueued.ShouldBe(2);
        link.TakeWritten().ShouldBe(ClientPackets.PublishQoS0);
        harness.Closed.Select(static info => info.Reason).ShouldBe([MqttClientCloseReason.ConnectionLost]);
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves the buffer of a CONNECT whose write fails, which carries the user name and password, is cleared before
    /// it goes back to the pool, and goes back exactly once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_ConnectWriteFails_ClearsConnectBytes()
    {
        // Arrange
        CountingPool pool = new();
        IOException error = new("write failed");
        ClientHarness? harness = null;
        List<(byte[] Array, byte[] Written)> returned = [];

        // The link exists once the client rents the CONNECT's buffer, and fails the write that follows.
        pool.Renting = _ =>
        {
            if (harness is { Connector.Links: [.., MemoryLink link] })
            {
                link.FailWrites(error);
            }
        };
        pool.Returning = array => returned.Add((array, array.AsSpan(0, ClientPackets.Connect.Length).ToArray()));
        harness = ClientHarness.Create(pool: pool);

        // Act
        MqttConnectAck ack = await harness.Client.ConnectAsync().Within("the connect");

        // Assert
        ack.Failure.ShouldBe(MqttClientCloseReason.Unreachable);
        ack.Exception.ShouldBe(error);
        returned.Count.ShouldBe(1);
        returned[0].Written.ShouldAllBe(static value => value == 0);
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Connects a client and returns its harness.
    /// </summary>
    /// <param name="pool">
    /// The pool the client rents from, or <see langword="null"/> for the shared pool.
    /// </param>
    /// <returns>
    /// The connected client's harness.
    /// </returns>
    private static async Task<ClientHarness> ConnectAsync(CountingPool? pool = null)
    {
        ClientHarness harness = ClientHarness.Create(pool: pool);
        MqttConnectAck ack = await harness.ConnectAsync(ClientPackets.ConnAckAccepted).Within("the connect");
        ack.IsConnected.ShouldBeTrue();
        return harness;
    }

    /// <summary>
    /// Publishes sixteen QoS 0 messages to "a/b", with the one-byte payloads 1 to 16 in that order.
    /// </summary>
    /// <param name="client">
    /// The connected client.
    /// </param>
    private static void PublishOthers(MqttWireClient client)
    {
        for (int i = 1; i <= 16; i++)
        {
            _ = client.PublishAsync("a/b", new[] { (byte)i }, MqttQoS.AtMostOnce);
        }
    }

    /// <summary>
    /// Encodes a QoS 0 PUBLISH to "a/b" with a one-byte payload.
    /// </summary>
    /// <param name="payload">
    /// The payload's one byte.
    /// </param>
    /// <returns>
    /// The packet.
    /// </returns>
    private static byte[] PublishOf(byte payload) => [0x30, 0x07, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x00, payload];
}
