using System.Buffers;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests how the read loop hands its answers to the send path (the client's send-path design, section 5, "The read
/// loop" and "How many answers may wait"): it does not wait for their write, so the packets behind a held write are
/// still handled and their answers go out after it, in order; and only once more than 4,096 answers wait to be written
/// does it stop handling packets, until enough of them have been written or the connection ends.
/// </summary>
public sealed class AnswerCapTests
{
    /// <summary>
    /// The number of answers that may wait in the send path before the read loop waits for them.
    /// </summary>
    private const int Cap = 4_096;

    /// <summary>
    /// The number of unknown PUBRELs a batch of the flood holds: as many as the read loop answers in one write.
    /// </summary>
    private const int Batch = 16;

    /// <summary>
    /// The number of batches of the flood: one past the cap.
    /// </summary>
    private const int Batches = (Cap / Batch) + 1;

    /// <summary>
    /// The payload of the publishes these tests send, "hi".
    /// </summary>
    private static readonly byte[] s_hi = "hi"u8.ToArray();

    /// <summary>
    /// Proves the messages the server sends while a caller's write is held all reach <c>OnMessage</c> before it is
    /// released, and their PUBACKs follow the caller's write, in order.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadLoop_CallerWriteHeld_DeliversMessagesAndWritesPubAcksAfter()
    {
        // Arrange
        int messages = 0;
        ClientHarness harness = await ConnectAsync((in _) => Interlocked.Increment(ref messages));
        MemoryLink link = harness.Link;
        link.HoldWrites();
        int writesBefore = link.WriteCount;
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);

        // Act
        link.Deliver([.. PublishesQoS1(3), .. ClientPackets.PublishQoS0]);
        int messagesWhileHeld = Volatile.Read(ref messages);
        link.CompleteHeldWrites();
        await link.WhenWrittenAsync(writesBefore + 2);

        // Assert
        messagesWhileHeld.ShouldBe(4);
        link.TakeWritten().ShouldBe([.. ClientPackets.PublishQoS0, .. PubAcks(1, 3)]);
        link.OverlappingWrites.ShouldBe(0);
    }

    /// <summary>
    /// Proves the read loop goes on handling packets while the write of its own answers is held, and every answer
    /// goes out after it, in order.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadLoop_OwnAnswersWriteHeld_HandlesNextPackets()
    {
        // Arrange
        int messages = 0;
        ClientHarness harness = await ConnectAsync((in _) => Interlocked.Increment(ref messages));
        MemoryLink link = harness.Link;
        link.HoldWrites();
        int writesBefore = link.WriteCount;

        // Act
        link.Deliver(PublishQoS1(1));
        bool ownAnswersHeld = link.HeldWriteBuffer.Span.SequenceEqual(PubAck(1));
        link.Deliver([.. PublishQoS1(2), .. PublishQoS1(3)]);
        int messagesWhileHeld = Volatile.Read(ref messages);
        link.CompleteHeldWrites();
        await link.WhenWrittenAsync(writesBefore + 2);

        // Assert
        ownAnswersHeld.ShouldBeTrue();
        messagesWhileHeld.ShouldBe(3);
        link.TakeWritten().ShouldBe(PubAcks(1, 3));
        link.OverlappingWrites.ShouldBe(0);
    }

    /// <summary>
    /// Proves a flood of unknown PUBRELs behind a held write stops the read loop once more than 4,096 answers wait,
    /// so a message behind them is not handled, and that once the write is released the answers go out in order, the
    /// message arrives and the client stays connected.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadLoop_FloodOfUnknownPubRelWhileWriteHeld_StopsAtCapUntilReleased()
    {
        // Arrange
        TaskCompletionSource qoS0Arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        ClientHarness harness = await ConnectAsync((in message) =>
        {
            if (message.QoS == MqttQoS.AtMostOnce)
            {
                qoS0Arrived.TrySetResult();
            }
        });
        MemoryLink link = harness.Link;
        link.HoldWrites();
        int writesBefore = link.WriteCount;
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        DeliverFlood(link);

        // Act
        link.Deliver(ClientPackets.PublishQoS0);
        bool arrivedAtCap = qoS0Arrived.Task.IsCompleted;
        link.CompleteHeldWrites();
        await qoS0Arrived.Task.Within("the QoS 0 message");
        await link.WhenWrittenAsync(writesBefore + 1 + Batches);

        // Assert
        arrivedAtCap.ShouldBeFalse();
        link.TakeWritten()
            .ShouldBe([.. ClientPackets.PublishQoS0, .. Repeat(ClientPackets.PubCompNotFound7, Batches * Batch)]);
        link.OverlappingWrites.ShouldBe(0);
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves exactly 4,096 answers waiting in the send path do not stop the read loop: with 256 batches of 16 behind
    /// a held write, a message behind them is still handled while the write is held. It is the cap's boundary: the
    /// read loop waits only once the count is past the cap, not when it reaches it.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadLoop_ExactlyCapAnswersWaitingWhileWriteHeld_StillHandlesNextPacket()
    {
        // Arrange
        TaskCompletionSource qoS0Arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        ClientHarness harness = await ConnectAsync((in message) =>
        {
            if (message.QoS == MqttQoS.AtMostOnce)
            {
                qoS0Arrived.TrySetResult();
            }
        });
        MemoryLink link = harness.Link;
        link.HoldWrites();
        int writesBefore = link.WriteCount;
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        DeliverFlood(link, Cap / Batch);

        // Act
        link.Deliver(ClientPackets.PublishQoS0);
        bool arrivedWhileHeld = qoS0Arrived.Task.IsCompleted;
        link.CompleteHeldWrites();
        await link.WhenWrittenAsync(writesBefore + 1 + (Cap / Batch));

        // Assert: one write of 16 answers per batch, so exactly the cap's worth waited while the write was held.
        arrivedWhileHeld.ShouldBeTrue();
        link.TakeWritten()
            .ShouldBe([.. ClientPackets.PublishQoS0, .. Repeat(ClientPackets.PubCompNotFound7, Cap)]);
        link.WriteCount.ShouldBe(writesBefore + 1 + (Cap / Batch));
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a link that refuses the registration of a write's continuation while the read loop waits at the cap
    /// ends that wait: the connection ends with <see cref="MqttClientCloseReason.WriteFailed"/>, the read loop
    /// finishes, and every buffer comes back exactly once, the refused write's only when that write ends.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadLoop_RegistrationRefusedWhileWaitingAtCap_FinishesAndReturnsEveryBuffer()
    {
        // Arrange
        CountingPool pool = new();
        TaskCompletionSource closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        ClientHarness harness = await ConnectAsync(pool: pool, onClosed: _ => closed.TrySetResult());
        MemoryLink link = harness.Link;
        link.HoldWrites();
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        DeliverFlood(link);

        // The first answers' write, taken once the held publish ends, refuses the registration of its continuation.
        link.RefuseWriteRegistration();

        // Act
        link.CompleteHeldWrites();
        await closed.Task.Within("the end of the connection");
        await harness.Client.DisposeAsync().AsTask().Within("the dispose");
        int outstandingBeforeTheWriteEnds = pool.Outstanding;
        bool ended = link.CompleteRefusedWrite();

        // Assert
        harness.Closed.Select(static info => info.Reason).ShouldBe([MqttClientCloseReason.WriteFailed]);
        link.IsAborted.ShouldBeTrue();
        link.WritesAfterAbort.ShouldBe(0);
        link.OverlappingWrites.ShouldBe(0);
        outstandingBeforeTheWriteEnds.ShouldBe(1);
        ended.ShouldBeTrue();
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a dispose begun while the read loop waits at the cap ends that wait: the answers waiting are dropped,
    /// only the DISCONNECT follows the held write, and the dispose completes with every buffer back exactly once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task DisposeAsync_ReadLoopWaitingAtCap_Returns()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(pool: pool);
        MemoryLink link = harness.Link;
        link.HoldWrites();
        int writesBefore = link.WriteCount;
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        DeliverFlood(link);

        // Act
        Task dispose = harness.Client.DisposeAsync().AsTask();
        link.CompleteHeldWrites();

        // The held write's continuation may run on the thread pool: the peer ends the link once the DISCONNECT is in.
        await link.WhenWrittenAsync(writesBefore + 2);
        link.EndFromPeer();
        await dispose.Within("the dispose");

        // Assert
        link.TakeWritten().ShouldBe([.. ClientPackets.PublishQoS0, .. ClientPackets.Disconnect]);
        harness.Closed.Select(static info => info.Reason).ShouldBe([MqttClientCloseReason.Disposed]);
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a held write that fails while the read loop waits at the cap ends the connection with
    /// <see cref="MqttClientCloseReason.WriteFailed"/>, ends the wait, lets the read loop finish, and gives every
    /// buffer back exactly once, the dropped answers' included.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadLoop_WriteFailsWhileWaitingAtCap_EndsAndReturnsEveryBuffer()
    {
        // Arrange
        CountingPool pool = new();
        TaskCompletionSource closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        ClientHarness harness = await ConnectAsync(pool: pool, onClosed: _ => closed.TrySetResult());
        MemoryLink link = harness.Link;
        link.HoldWrites();
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        DeliverFlood(link);
        int outstandingAtCap = pool.Outstanding;
        IOException error = new("write failed");

        // Act
        link.CompleteHeldWrites(error);
        await closed.Task.Within("the end of the connection");
        await harness.Client.DisposeAsync().AsTask().Within("the dispose");

        // Assert
        outstandingAtCap.ShouldBeGreaterThan(Batches);
        harness.Closed.ShouldBe(
            [new MqttClientCloseInfo(MqttClientCloseReason.WriteFailed, MqttReasonCode.Success, null, error)]);
        link.TakeWritten().ShouldBeEmpty();
        link.IsAborted.ShouldBeTrue();
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a dispose begun while the read loop waits at the cap ends that wait by itself, before any write has
    /// ended: the read loop finishes the connection once the peer ends the link, while the write ahead of the answers
    /// is still held, and the dispose completes once that write ends, with every buffer back exactly once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task DisposeAsync_ReadLoopWaitingAtCapWhileWriteStaysHeld_ReadLoopFinishes()
    {
        // Arrange
        CountingPool pool = new();
        TaskCompletionSource closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        ClientHarness harness = await ConnectAsync(pool: pool, onClosed: _ => closed.TrySetResult());
        MemoryLink link = harness.Link;
        link.HoldWrites();
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        DeliverFlood(link);

        // Act
        Task dispose = harness.Client.DisposeAsync().AsTask();
        link.EndFromPeer();
        await closed.Task.Within("the end of the connection");
        bool disposedWhileHeld = dispose.IsCompleted;
        link.CompleteHeldWrites();
        await dispose.Within("the dispose");

        // Assert
        disposedWhileHeld.ShouldBeFalse();
        harness.Closed.Select(static info => info.Reason).ShouldBe([MqttClientCloseReason.Disposed]);
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves an end recorded by the keep-alive while the read loop waits at the cap ends that wait: the held QoS 1
    /// publish goes unacknowledged for twice the keep-alive, the connection ends with
    /// <see cref="MqttClientCloseReason.AcknowledgementTimedOut"/>, and the read loop finishes while that write is
    /// still held; once it ends, every buffer is back exactly once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadLoop_AcknowledgementTimesOutWhileWaitingAtCap_Finishes()
    {
        // Arrange
        CountingPool pool = new();
        TaskCompletionSource closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        ClientHarness harness = await ConnectAsync(pool: pool, onClosed: _ => closed.TrySetResult());
        MemoryLink link = harness.Link;
        link.HoldWrites();
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        DeliverFlood(link);

        // Act: four ticks of half the 15-second keep-alive, the last of them twice the keep-alive after the publish.
        for (int tick = 0; tick < 4; tick++)
        {
            harness.Time.Advance(TimeSpan.FromSeconds(7.5));
        }

        await closed.Task.Within("the end of the connection");
        link.CompleteHeldWrites();
        await harness.Client.DisposeAsync().AsTask().Within("the dispose");

        // Assert
        harness.Closed.Select(static info => info.Reason).ShouldBe([MqttClientCloseReason.AcknowledgementTimedOut]);
        link.IsAborted.ShouldBeTrue();
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a server that keeps to its window of 1,024 unanswered QoS 1 messages never makes the read loop wait:
    /// with a caller's write held, all of them and a QoS 0 message behind them reach <c>OnMessage</c>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadLoop_ServerWithinWindowWhileWriteHeld_NeverWaits()
    {
        // Arrange
        int messages = 0;
        ClientHarness harness = await ConnectAsync((in _) => Interlocked.Increment(ref messages));
        MemoryLink link = harness.Link;
        link.HoldWrites();
        int writesBefore = link.WriteCount;
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);

        // Act
        link.Deliver([.. PublishesQoS1(1_024), .. ClientPackets.PublishQoS0]);
        int messagesWhileHeld = Volatile.Read(ref messages);
        link.CompleteHeldWrites();
        await link.WhenWrittenAsync(writesBefore + 1 + (1_024 / Batch));

        // Assert
        messagesWhileHeld.ShouldBe(1_025);
        link.TakeWritten().ShouldBe([.. ClientPackets.PublishQoS0, .. PubAcks(1, 1_024)]);
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a QoS 1 publish an <c>OnMessage</c> makes while the PUBACK of an earlier message is being written goes
    /// out between that PUBACK and the PUBACK of its own message, which were handed to the send path before and after
    /// it, and completes on the server's PUBACK.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task OnMessage_PublishesBehindHeldAnswers_GoesOutBetweenThemAndCompletes()
    {
        // Arrange
        MqttWireClient? client = null;
        Task<MqttRequestResult>? publish = null;
        int messages = 0;
        ClientHarness harness = await ConnectAsync((in _) =>
        {
            messages++;
            if (messages == 2 && client is not null)
            {
                publish = client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce).AsTask();
            }
        });
        client = harness.Client;
        MemoryLink link = harness.Link;
        link.HoldWrites();
        int writesBefore = link.WriteCount;

        // Act
        link.Deliver(PublishQoS1(1));
        link.Deliver(PublishQoS1(2));
        link.CompleteHeldWrites();
        await link.WhenWrittenAsync(writesBefore + 3);
        byte[] written = link.TakeWritten();
        link.Deliver(ClientPackets.PubAck1);
        MqttRequestResult result = await publish.ShouldNotBeNull().Within("the publish");

        // Assert
        written.ShouldBe([.. PubAck(1), .. ClientPackets.PublishQoS1, .. PubAck(2)]);
        result.Status.ShouldBe(MqttRequestStatus.Completed);
        link.OverlappingWrites.ShouldBe(0);
    }

    /// <summary>
    /// Delivers the flood of these tests: reads of <see cref="Batch"/> PUBRELs for the unknown identifier 7 each,
    /// which the read loop answers in one write per read.
    /// </summary>
    /// <param name="link">
    /// The connected client's link.
    /// </param>
    /// <param name="batches">
    /// How many reads; by default <see cref="Batches"/>, one past the cap's worth of answers.
    /// </param>
    private static void DeliverFlood(MemoryLink link, int batches = Batches)
    {
        byte[] batch = Repeat(ClientPackets.PubRel7, Batch);
        for (int i = 0; i < batches; i++)
        {
            link.Deliver(batch);
        }
    }

    /// <summary>
    /// Builds a QoS 1 PUBLISH of "hi" to "a/b" with the given packet identifier.
    /// </summary>
    /// <param name="id">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// <c>32 0A 00 03 61 2F 62 00 &lt;id&gt; 00 68 69</c>.
    /// </returns>
    private static byte[] PublishQoS1(ushort id)
        => [0x32, 0x0A, 0x00, 0x03, 0x61, 0x2F, 0x62, (byte)(id >> 8), (byte)id, 0x00, 0x68, 0x69];

    /// <summary>
    /// Builds a PUBACK with success for the given packet identifier.
    /// </summary>
    /// <param name="id">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// <c>40 02 &lt;id&gt;</c>.
    /// </returns>
    private static byte[] PubAck(ushort id) => [0x40, 0x02, (byte)(id >> 8), (byte)id];

    /// <summary>
    /// Builds QoS 1 PUBLISHes with identifiers 1 to <paramref name="count"/>, one after the other.
    /// </summary>
    /// <param name="count">
    /// How many.
    /// </param>
    /// <returns>
    /// The packets, concatenated.
    /// </returns>
    private static byte[] PublishesQoS1(int count)
    {
        List<byte> bytes = [];
        for (int id = 1; id <= count; id++)
        {
            bytes.AddRange(PublishQoS1((ushort)id));
        }

        return [.. bytes];
    }

    /// <summary>
    /// Builds the PUBACKs for identifiers <paramref name="first"/> to <paramref name="last"/>, one after the other.
    /// </summary>
    /// <param name="first">
    /// The first identifier.
    /// </param>
    /// <param name="last">
    /// The last identifier.
    /// </param>
    /// <returns>
    /// The packets, concatenated.
    /// </returns>
    private static byte[] PubAcks(int first, int last)
    {
        List<byte> bytes = [];
        for (int id = first; id <= last; id++)
        {
            bytes.AddRange(PubAck((ushort)id));
        }

        return [.. bytes];
    }

    /// <summary>
    /// Repeats a packet.
    /// </summary>
    /// <param name="packet">
    /// The packet.
    /// </param>
    /// <param name="count">
    /// How many times.
    /// </param>
    /// <returns>
    /// The packet <paramref name="count"/> times over.
    /// </returns>
    private static byte[] Repeat(byte[] packet, int count)
    {
        byte[] bytes = new byte[packet.Length * count];
        for (int i = 0; i < count; i++)
        {
            packet.CopyTo(bytes, i * packet.Length);
        }

        return bytes;
    }

    /// <summary>
    /// Creates a client and connects it, dropping the CONNECT it wrote from the written bytes.
    /// </summary>
    /// <param name="onMessage">
    /// The message handler, or <see langword="null"/> for none.
    /// </param>
    /// <param name="pool">
    /// The pool the client rents from, or <see langword="null"/> for the shared pool.
    /// </param>
    /// <param name="onClosed">
    /// A callback the client's <c>OnClosed</c> calls, or <see langword="null"/>.
    /// </param>
    /// <returns>
    /// The harness around the connected client.
    /// </returns>
    private static async Task<ClientHarness> ConnectAsync(
        MqttMessageHandler? onMessage = null,
        ArrayPool<byte>? pool = null,
        Action<MqttClientCloseInfo>? onClosed = null)
    {
        ClientHarness harness = ClientHarness.Create(pool: pool, onMessage: onMessage, onClosed: onClosed);
        MqttConnectAck ack = await harness.ConnectAsync(ClientPackets.ConnAckAccepted).Within("the connect");
        ack.IsConnected.ShouldBeTrue();
        harness.Link.TakeWritten();
        return harness;
    }
}
