using System.Buffers;
using System.Buffers.Binary;
using MqttWire.Codec;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Server;

/// <summary>
/// The stress tests of the send path (design 6.13 item 3, "Stress"): many threads call <c>TrySend</c> on one
/// connection, and a close races them, while the in-memory link completes writes sometimes at once and sometimes
/// later. Real threads start together at a gate; nothing waits in real time for correctness, and spins only vary the
/// interleaving. Every outcome asserted is exact.
/// </summary>
/// <remarks>
/// The bounds of a connection (design 6.13 item 5) are looked at while the threads run, not only once they have
/// stopped: after the queue has drained every count is 0, so a bound passed for a moment would not show. Each
/// thread looks after each of its steps (<see cref="BoundsWatch"/>), and the pool counts the most buffers that were
/// out at one moment at every rent.
/// </remarks>
public sealed class ServerStressTests
{
    /// <summary>
    /// The number of threads that send in the tests of eight senders.
    /// </summary>
    private const int Senders = 8;

    /// <summary>
    /// The number of messages each of them sends.
    /// </summary>
    private const int MessagesPerSender = 5_000;

    /// <summary>
    /// The number of messages the eight senders send together.
    /// </summary>
    private const int Total = Senders * MessagesPerSender;

    /// <summary>
    /// The length of the payload of each stress message: the sender's number and the message's sequence number.
    /// </summary>
    private const int PayloadLength = 8;

    /// <summary>
    /// The length of the topic the stress messages are sent to, <see cref="ServerHarness.Topic"/>, in bytes, which
    /// is counted against <see cref="MqttWireServerOptions.MaxOutgoingBytes"/> with each payload.
    /// </summary>
    private const int TopicLength = 3;

    /// <summary>
    /// The number of rounds of <see cref="TrySendAndClose_Racing_NeverWriteAfterTheCloseAndReturnEveryBuffer"/>.
    /// </summary>
    private const int CloseRounds = 200;

    /// <summary>
    /// The number of threads that send in each of those rounds.
    /// </summary>
    private const int RacingSenders = 4;

    /// <summary>
    /// The most messages each of them sends in a round.
    /// </summary>
    private const int RacingMessages = 50;

    /// <summary>
    /// Proves that eight threads calling <c>TrySend</c> at once on one connection, 5,000 QoS 0 messages each, while a
    /// ninth thread holds the link's writes and releases them again and again, get every message written exactly once
    /// and, for each thread, in the order of its calls: 40,000 PUBLISH packets, whose payloads, the sender's number and
    /// a sequence number, give each sender's 0 to 4,999 in order. Afterwards the send path is idle, nothing waits, no
    /// byte is counted and every buffer is back. The limits are raised so that nothing is evicted: every message
    /// may wait at once, each counted with its topic and its payload. A tenth thread runs the sweep without pause
    /// meanwhile, with the clock standing still, so that the sweep reads the connection while its writes begin and
    /// end and no deadline passes. While they run, every thread looks at the connection's bounds after each of its
    /// steps, and no look finds one passed.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_EightThreadsOnOneConnection_WritesEverythingOnceAndInOrder()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options =>
        {
            options.MaxOutgoingMessages = Total;
            options.MaxOutgoingBytes = Total * (TopicLength + PayloadLength);
        });
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        MemoryLink link = connection.Link;

        // Act
        EightSenders run = RunEightSenders(harness, connection, stall: false);
        await connection.WhenSendPathIdleAsync();
        ConnectionCounts counts = server.GetCounts();
        harness.AssertBounds();
        List<int>[] sequences = ReadSequences(link.TakeWritten(), out int packets, out bool disconnected);

        // Assert
        run.Taken.Sum().ShouldBe(Total);
        run.Evicted.ShouldBe(0);
        run.NotOpen.ShouldBe(0);
        packets.ShouldBe(Total);
        disconnected.ShouldBeFalse();
        for (int sender = 0; sender < Senders; sender++)
        {
            sequences[sender].ShouldBe(Enumerable.Range(0, MessagesPerSender), $"sender {sender}");
        }

        counts.IsWriting.ShouldBeFalse();
        counts.WaitingDeliveries.ShouldBe(0);
        counts.WaitingControlPackets.ShouldBe(0);
        counts.OutgoingBytes.ShouldBe(0);
        connection.Run.IsCompleted.ShouldBeFalse();
        link.OverlappingWrites.ShouldBe(0);
        link.WritesAfterAbort.ShouldBe(0);
        run.Sweeps.ShouldBeGreaterThan(0);
        run.Watch.AssertNoBoundWasPassed(looksAtLeast: Total);
        harness.Pool.PeakOutstanding.ShouldBeLessThanOrEqualTo(ServerHarness.MaxBuffersPerConnection);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the outgoing limits hold at every moment while eight threads send against a writer that stalls:
    /// with limits small enough to be reached, the senders are evicted, the connection ends with
    /// <see cref="MqttCloseReason.OutgoingLimitPassed"/> (0x97), and no look at the connection, taken by every thread
    /// after each of its steps, ever finds more deliveries waiting than
    /// <see cref="MqttWireServerOptions.MaxOutgoingMessages"/>, more bytes counted than
    /// <see cref="MqttWireServerOptions.MaxOutgoingBytes"/>, or more buffers out than a connection may hold. Nothing
    /// is written after the close; what was written is, for each sender, the first of its messages in order, no more
    /// than were taken; every buffer is back once and nothing is counted any more.
    /// </summary>
    /// <param name="maxOutgoingMessages">
    /// The limit of waiting deliveries.
    /// </param>
    /// <param name="maxOutgoingBytes">
    /// The limit of bytes on the way out.
    /// </param>
    /// <param name="reached">
    /// The limit the stalled writer makes the senders reach: <c>"messages"</c> or <c>"bytes"</c>.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(8, 1_048_576, "messages")]
    [InlineData(Total, 65_536, "bytes")]
    public async Task TrySend_EightThreadsAgainstAStalledWriterWithSmallLimits_AreEvictedWithinTheBounds(
        int maxOutgoingMessages, int maxOutgoingBytes, string reached)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(options =>
        {
            options.MaxOutgoingMessages = maxOutgoingMessages;
            options.MaxOutgoingBytes = maxOutgoingBytes;
        });
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        MemoryLink link = connection.Link;

        // Act
        EightSenders run = RunEightSenders(harness, connection, stall: true);
        link.EndFromPeer();
        await connection.RunEnded();
        List<int>[] sequences = ReadSequences(link.TakeWritten(), out int packets, out _);

        // Assert
        run.Evicted.ShouldBeGreaterThan(0);
        run.Evicted.ShouldBeLessThanOrEqualTo(Senders);
        (run.Taken.Sum() + run.Evicted + run.NotOpen).ShouldBe(Total);
        harness.ClosedCalls().ShouldBe(["OnClosed c1 OutgoingLimitPassed"]);
        run.WritesAfterTheClose.ShouldBe(0);
        packets.ShouldBeLessThanOrEqualTo(run.Taken.Sum());
        for (int sender = 0; sender < Senders; sender++)
        {
            sequences[sender].ShouldBe(Enumerable.Range(0, sequences[sender].Count), $"sender {sender}");
            sequences[sender].Count.ShouldBeLessThanOrEqualTo(run.Taken[sender], $"sender {sender}");
        }

        // What waited when the connection was evicted waits until the stalled write ends, after every sender has
        // finished, so the looks that followed the eviction saw the limit reached exactly, and none saw it passed.
        run.Watch.AssertNoBoundWasPassed(looksAtLeast: Total);
        if (reached == "messages")
        {
            run.Watch.PeakWaitingDeliveries.ShouldBe(maxOutgoingMessages);
        }
        else
        {
            // As many messages of 11 bytes as fit the limit: one more would have passed it.
            const int MessageBytes = TopicLength + PayloadLength;
            run.Watch.PeakOutgoingBytes.ShouldBe(maxOutgoingBytes / MessageBytes * MessageBytes);
        }

        link.OverlappingWrites.ShouldBe(0);
        link.WritesAfterAbort.ShouldBeLessThanOrEqualTo(1);
        link.AbortsAfterEnd.ShouldBe(0);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Pool.PeakOutstanding.ShouldBeLessThanOrEqualTo(ServerHarness.MaxBuffersPerConnection);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a close racing four sending threads and the sweep never lets anything be written after the close,
    /// and returns every buffer once: in each of 200 rounds four threads send at once while a fifth closes the
    /// connection after a spin that changes from round to round, with the link holding its writes in every other
    /// round, and a sixth runs the sweep without pause while it moves the clock on, by a step that changes from
    /// round to round, so that in some rounds the write deadline of a held write, the keep-alive deadline and the
    /// close deadline pass while the others race. In every round: no write reaches the link after its WebSocket
    /// close; a DISCONNECT, when one is written, is the last packet; no more messages are written than were taken; a
    /// send after the close has returned says "not open"; the link saw one write at a time, at most the one write
    /// that was decided on before its abort, and no abort after the run had ended; no look at the connection, taken
    /// by every thread after each of its steps, found a bound passed; and once the run has ended every buffer is
    /// back and none was returned twice.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySendAndClose_Racing_NeverWriteAfterTheCloseAndReturnEveryBuffer()
    {
        // Arrange
        List<string> failures = [];

        for (int round = 0; round < CloseRounds; round++)
        {
            ServerHarness harness = ServerHarness.Create();
            TestConnection connection = harness.Connect();
            MqttWireConnection server = connection.Connection.ShouldNotBeNull();
            MemoryLink link = connection.Link;
            BoundsWatch watch = new(harness, server);
            bool held = round % 2 == 1;
            if (held)
            {
                link.HoldWrites();
            }

            int writesAfterTheClose = 0;
            link.Writing = _ =>
            {
                if (link.ClosedWith is not null)
                {
                    Interlocked.Increment(ref writesAfterTheClose);
                }
            };
            int ready = 0;
            int taken = 0;
            MqttSendResult afterTheClose = MqttSendResult.Taken;
            int closerDelay = round % 20 * 100;
            int senderPause = round / 2 % 3 * 200;

            // The clock's step per sweep: none, so that only the racing threads end the connection; or one that lets
            // the write deadline (15 s), the keep-alive deadline (45 s) and the close deadline (2 s) pass in the round.
            TimeSpan clockStep = TimeSpan.FromMilliseconds((round / 4 % 4) switch { 0 => 0, 1 => 50, 2 => 500, _ => 3_000 });
            if (held && clockStep > TimeSpan.Zero)
            {
                // A write the link has held for just under the write deadline when the race begins: the sweeper's
                // first steps take it past the deadline while the others send and close.
                server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
                taken = 1;
                harness.Time.Advance(ServerLimits.WriteDeadline - TimeSpan.FromMilliseconds(500));
            }
            int raceOver = 0;
            Thread sweeper = new(() =>
            {
                Gate.Wait(ref ready, RacingSenders + 2);
                while (Volatile.Read(ref raceOver) == 0)
                {
                    if (clockStep > TimeSpan.Zero)
                    {
                        // The fake clock is safe to move from here while the server reads it; its timer runs the
                        // sweep on this thread too, whenever a step crosses the sweep's interval.
                        harness.Time.Advance(clockStep);
                    }

                    harness.Server.Sweep();
                    watch.Look();
                }
            });
            Thread[] threads = new Thread[RacingSenders + 1];
            for (int sender = 0; sender < RacingSenders; sender++)
            {
                threads[sender] = new Thread(() =>
                {
                    Gate.Wait(ref ready, RacingSenders + 2);
                    for (int i = 0; i < RacingMessages; i++)
                    {
                        MqttSendResult result = server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce);
                        watch.Look();
                        if (result != MqttSendResult.Taken)
                        {
                            break;
                        }

                        Interlocked.Increment(ref taken);

                        // In some rounds the senders pause, so that the close also finds the send path idle.
                        Thread.SpinWait(senderPause);
                    }
                });
            }

            threads[RacingSenders] = new Thread(() =>
            {
                Gate.Wait(ref ready, RacingSenders + 2);
                Thread.SpinWait(closerDelay);
                server.Close(MqttReasonCode.AdministrativeAction);
                watch.Look();
                afterTheClose = server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce);
            });

            // Act
            sweeper.Start();
            foreach (Thread thread in threads)
            {
                thread.Start();
            }

            foreach (Thread thread in threads)
            {
                thread.JoinWithin("A sending or closing thread");
            }

            Volatile.Write(ref raceOver, 1);
            sweeper.JoinWithin("The sweeping thread");
            if (held)
            {
                link.CompleteHeldWrites();
            }

            link.EndFromPeer();
            await connection.RunEnded();

            // Assert, collected so that one report names every round that failed.
            List<byte> types = PacketTypes(link.TakeWritten());
            int published = types.Count(static type => type == (byte)PacketType.Publish);
            int disconnect = types.IndexOf((byte)PacketType.Disconnect);
            if (writesAfterTheClose != 0 || (disconnect >= 0 && disconnect != types.Count - 1) || published > taken
                || afterTheClose != MqttSendResult.NotOpen || harness.Pool.Outstanding != 0
                || harness.Pool.FaultyReturns != 0 || link.OverlappingWrites != 0 || link.WritesAfterAbort > 1
                || link.AbortsAfterEnd != 0 || watch.BoundsPassed != 0
                || harness.Pool.PeakOutstanding > ServerHarness.MaxBuffersPerConnection)
            {
                failures.Add($"round {round} (held {held}): writes after the close {writesAfterTheClose}, packets "
                             + $"[{string.Join(' ', types)}], taken {taken}, after the close {afterTheClose}, "
                             + $"outstanding {harness.Pool.Outstanding}, faulty returns {harness.Pool.FaultyReturns}, "
                             + $"overlapping writes {link.OverlappingWrites}, writes after the abort "
                             + $"{link.WritesAfterAbort}, aborts after the end {link.AbortsAfterEnd}, "
                             + $"{watch.Describe()}, most buffers out {harness.Pool.PeakOutstanding}");
            }
        }

        failures.ShouldBeEmpty();
    }

    /// <summary>
    /// Runs the eight senders against one connection, with a ninth thread that holds and releases the link's writes
    /// and a tenth that runs the sweep without pause, the clock standing still, and waits for all of them. Each
    /// sender sends 5,000 QoS 0 messages whose payload is its number and a sequence number, whatever the answers: a
    /// message that is not taken is counted by its result. Every thread looks at the connection's bounds after each
    /// of its steps.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <param name="connection">
    /// The connection, Active.
    /// </param>
    /// <param name="stall">
    /// Whether the writer stalls: the toggling thread then holds the link's writes and never releases them until
    /// every sender has finished, so that what waits can only grow and stays as it was when the connection was
    /// evicted, and then ends the write it holds with an error, as an aborted socket does. Otherwise it holds and
    /// releases until every sender has finished, and releases what is held at the end.
    /// </param>
    /// <returns>
    /// What the threads counted.
    /// </returns>
    private static EightSenders RunEightSenders(ServerHarness harness, TestConnection connection, bool stall)
    {
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        MemoryLink link = connection.Link;
        BoundsWatch watch = new(harness, server);
        int ready = 0;
        int sendersDone = 0;
        int evicted = 0;
        int notOpen = 0;
        int[] taken = new int[Senders];
        Thread[] senders = new Thread[Senders];
        for (int sender = 0; sender < Senders; sender++)
        {
            int number = sender;
            senders[sender] = new Thread(() =>
            {
                Gate.Wait(ref ready, Senders + 1);
                int mine = 0;
                for (int sequence = 0; sequence < MessagesPerSender; sequence++)
                {
                    byte[] payload = new byte[PayloadLength];
                    BinaryPrimitives.WriteInt32LittleEndian(payload, number);
                    BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(4), sequence);
                    MqttSendResult result = server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtMostOnce);
                    watch.Look();
                    switch (result)
                    {
                        case MqttSendResult.Taken:
                            mine++;
                            break;
                        case MqttSendResult.Evicted:
                            Interlocked.Increment(ref evicted);
                            break;
                        default:
                            Interlocked.Increment(ref notOpen);
                            break;
                    }
                }

                // Each sender writes its own slot, once, before it counts itself done.
                taken[number] = mine;
                Interlocked.Increment(ref sendersDone);
            });
        }

        // A write the toggler released carries the send path on, on the toggler's thread, for as long as the writes
        // after it complete at once; every 61st write is held as it begins, so that such a drain also stops halfway.
        int writes = 0;
        int writesAfterTheClose = 0;
        link.Writing = _ =>
        {
            if (link.ClosedWith is not null)
            {
                Interlocked.Increment(ref writesAfterTheClose);
            }

            if (Interlocked.Increment(ref writes) % 61 == 0)
            {
                link.HoldWrites();
            }
        };
        if (stall)
        {
            // Held before any thread starts: the first eviction then meets a held write whichever thread runs
            // first, so what waited at that moment is still there for every look that follows.
            link.HoldWrites();
        }

        Thread toggler = new(() =>
        {
            Gate.Wait(ref ready, Senders + 1);
            for (int turn = 0; Volatile.Read(ref sendersDone) < Senders; turn++)
            {
                // Held for a varying while, then released: the writes held meanwhile complete on this thread. A
                // writer that stalls is never released.
                link.HoldWrites();
                watch.Look();
                Thread.SpinWait(turn % 16 * 50);
                if (!stall)
                {
                    link.CompleteHeldWrites();
                    watch.Look();
                }

                Thread.SpinWait(turn % 7 * 50);
            }

            link.CompleteHeldWrites(stall ? new IOException("The socket was aborted.") : null);
            watch.Look();
        });

        int sweeps = 0;
        Thread sweeper = new(() =>
        {
            while (Volatile.Read(ref sendersDone) < Senders)
            {
                harness.Server.Sweep();
                watch.Look();
                Interlocked.Increment(ref sweeps);
            }
        });

        foreach (Thread sender in senders)
        {
            sender.Start();
        }

        toggler.Start();
        sweeper.Start();
        foreach (Thread sender in senders)
        {
            sender.JoinWithin("A sending thread");
        }

        toggler.JoinWithin("The toggling thread");
        sweeper.JoinWithin("The sweeping thread");
        return new EightSenders(watch, taken, Volatile.Read(ref evicted), Volatile.Read(ref notOpen),
            Volatile.Read(ref sweeps), Volatile.Read(ref writesAfterTheClose));
    }

    /// <summary>
    /// Reads the stress messages out of what was written: each a QoS 0 PUBLISH to "a/b" whose payload is the sender's
    /// number and the message's sequence number, with at most one DISCONNECT, which must be the last packet.
    /// </summary>
    /// <param name="written">
    /// The written bytes.
    /// </param>
    /// <param name="packets">
    /// The number of PUBLISH packets read.
    /// </param>
    /// <param name="disconnected">
    /// Whether a DISCONNECT follows the last PUBLISH.
    /// </param>
    /// <returns>
    /// For each sender, the sequence numbers in the order they were written.
    /// </returns>
    private static List<int>[] ReadSequences(byte[] written, out int packets, out bool disconnected)
    {
        List<int>[] sequences = new List<int>[Senders];
        for (int sender = 0; sender < Senders; sender++)
        {
            sequences[sender] = [];
        }

        packets = 0;
        disconnected = false;
        ReadOnlySpan<byte> rest = written;
        while (!rest.IsEmpty)
        {
            PacketHeader.TryRead(rest, out PacketHeader header).ShouldBe(OperationStatus.Done);
            if (header.Type == PacketType.Disconnect)
            {
                // Nothing follows a DISCONNECT.
                rest.Length.ShouldBe(header.TotalLength);
                disconnected = true;
                break;
            }

            header.Type.ShouldBe(PacketType.Publish);
            ReadOnlySpan<byte> payload = rest.Slice(header.TotalLength - PayloadLength, PayloadLength);
            sequences[BinaryPrimitives.ReadInt32LittleEndian(payload)].Add(BinaryPrimitives.ReadInt32LittleEndian(payload[4..]));
            rest = rest[header.TotalLength..];
            packets++;
        }

        return sequences;
    }

    /// <summary>
    /// Lists the types of the packets in what was written, in order.
    /// </summary>
    /// <param name="written">
    /// The written bytes, whole packets.
    /// </param>
    /// <returns>
    /// The type of each packet, as its number.
    /// </returns>
    private static List<byte> PacketTypes(byte[] written)
    {
        List<byte> types = [];
        ReadOnlySpan<byte> rest = written;
        while (!rest.IsEmpty)
        {
            PacketHeader.TryRead(rest, out PacketHeader header).ShouldBe(OperationStatus.Done);
            types.Add((byte)header.Type);
            rest = rest[header.TotalLength..];
        }

        return types;
    }

    /// <summary>
    /// What the threads of <see cref="RunEightSenders"/> counted.
    /// </summary>
    /// <param name="Watch">
    /// What the looks at the connection's bounds found while the threads ran.
    /// </param>
    /// <param name="Taken">
    /// How many messages of each sender were taken.
    /// </param>
    /// <param name="Evicted">
    /// How many sends were answered "evicted".
    /// </param>
    /// <param name="NotOpen">
    /// How many sends were answered "not open".
    /// </param>
    /// <param name="Sweeps">
    /// How many times the sweep ran.
    /// </param>
    /// <param name="WritesAfterTheClose">
    /// How many writes were begun after the link's WebSocket close.
    /// </param>
    private sealed record EightSenders(BoundsWatch Watch, int[] Taken, int Evicted, int NotOpen, int Sweeps,
        int WritesAfterTheClose);

    /// <summary>
    /// Looks at one connection's bounds from any thread, while a test's threads run, and keeps what it saw: the
    /// highest value of each count, and how many looks found a bound passed. A look takes the connection's counts in
    /// one hold of its lock, as the harness does between the steps of a single-threaded test, and adds to nothing but
    /// a few interlocked numbers, so it allocates nothing and may be called from every thread after every step.
    /// </summary>
    /// <param name="harness">
    /// The harness, whose server's options and pool give the bounds.
    /// </param>
    /// <param name="server">
    /// The connection.
    /// </param>
    private sealed class BoundsWatch(ServerHarness harness, MqttWireConnection server)
    {
        /// <summary>
        /// The most deliveries that may wait.
        /// </summary>
        private readonly int _maxOutgoingMessages = harness.Server.Options.MaxOutgoingMessages;

        /// <summary>
        /// The most bytes that may be counted on the way out.
        /// </summary>
        private readonly int _maxOutgoingBytes = harness.Server.Options.MaxOutgoingBytes;

        /// <summary>
        /// The largest buffer the server may ask the pool for: an encoding buffer, or a whole PUBLISH of the first
        /// band, as the harness's own bound has it.
        /// </summary>
        private readonly int _largestRent = Math.Max(ServerLimits.MaxEncodingBufferBytes,
            harness.Server.Options.MaxPublishPayloadBytes + ServerLimits.PublishOverheadBytes + 5);

        /// <summary>
        /// The pool the server rents from.
        /// </summary>
        private readonly CountingPool _pool = harness.Pool;

        /// <summary>
        /// The most deliveries any look found waiting.
        /// </summary>
        private int _peakWaitingDeliveries;

        /// <summary>
        /// The most bytes any look found counted.
        /// </summary>
        private long _peakOutgoingBytes;

        /// <summary>
        /// The most buffers any look found out of the pool.
        /// </summary>
        private int _peakBuffers;

        /// <summary>
        /// How many looks found a bound passed.
        /// </summary>
        private int _boundsPassed;

        /// <summary>
        /// How many looks were taken.
        /// </summary>
        private int _looks;

        /// <summary>
        /// Gets the most deliveries any look found waiting.
        /// </summary>
        public int PeakWaitingDeliveries => Volatile.Read(ref _peakWaitingDeliveries);

        /// <summary>
        /// Gets the most bytes any look found counted against
        /// <see cref="MqttWireServerOptions.MaxOutgoingBytes"/>.
        /// </summary>
        public long PeakOutgoingBytes => Volatile.Read(ref _peakOutgoingBytes);

        /// <summary>
        /// Gets how many looks found a bound passed.
        /// </summary>
        public int BoundsPassed => Volatile.Read(ref _boundsPassed);

        /// <summary>
        /// Looks at the connection now: its counts, taken in one hold of its lock, and the pool's. Safe from any
        /// thread, and allocates nothing.
        /// </summary>
        public void Look()
        {
            ConnectionCounts counts = server.GetCounts();
            int buffers = _pool.Outstanding;
            int largestRent = _pool.LargestRent;
            Interlocked.Increment(ref _looks);
            RaiseTo(ref _peakWaitingDeliveries, counts.WaitingDeliveries);
            RaiseTo(ref _peakBuffers, buffers);
            long seenBytes = Volatile.Read(ref _peakOutgoingBytes);
            while (counts.OutgoingBytes > seenBytes)
            {
                long before = Interlocked.CompareExchange(ref _peakOutgoingBytes, counts.OutgoingBytes, seenBytes);
                if (before == seenBytes)
                {
                    break;
                }

                seenBytes = before;
            }

            bool passed = counts.WaitingDeliveries > _maxOutgoingMessages
                          || counts.OutgoingBytes > _maxOutgoingBytes
                          || counts.OutgoingBytes < 0
                          || counts.WaitingControlPackets > ServerLimits.MaxWaitingControlPackets
                          || buffers > ServerHarness.MaxBuffersPerConnection
                          || largestRent > _largestRent;
            if (passed)
            {
                Interlocked.Increment(ref _boundsPassed);
            }
        }

        /// <summary>
        /// Asserts that enough looks were taken to mean something, and that none of them found a bound passed.
        /// </summary>
        /// <param name="looksAtLeast">
        /// The fewest looks the test's threads must have taken.
        /// </param>
        public void AssertNoBoundWasPassed(int looksAtLeast)
        {
            Volatile.Read(ref _looks).ShouldBeGreaterThanOrEqualTo(looksAtLeast);
            BoundsPassed.ShouldBe(0, Describe());
            PeakWaitingDeliveries.ShouldBeLessThanOrEqualTo(_maxOutgoingMessages);
            PeakOutgoingBytes.ShouldBeLessThanOrEqualTo(_maxOutgoingBytes);
            Volatile.Read(ref _peakBuffers).ShouldBeLessThanOrEqualTo(ServerHarness.MaxBuffersPerConnection);
        }

        /// <summary>
        /// Describes what the looks saw, for the message of a failure.
        /// </summary>
        /// <returns>
        /// The counts as text.
        /// </returns>
        public string Describe() =>
            $"{Volatile.Read(ref _looks)} looks, {BoundsPassed} found a bound passed, most waiting deliveries "
            + $"{PeakWaitingDeliveries} of {_maxOutgoingMessages}, most outgoing bytes {PeakOutgoingBytes} of "
            + $"{_maxOutgoingBytes}, most buffers seen out {Volatile.Read(ref _peakBuffers)}";

        /// <summary>
        /// Raises a highest value to one seen, unless a higher one is there already.
        /// </summary>
        /// <param name="peak">
        /// The highest value so far.
        /// </param>
        /// <param name="seen">
        /// The value seen.
        /// </param>
        private static void RaiseTo(ref int peak, int seen)
        {
            int current = Volatile.Read(ref peak);
            while (seen > current)
            {
                int before = Interlocked.CompareExchange(ref peak, seen, current);
                if (before == current)
                {
                    return;
                }

                current = before;
            }
        }
    }
}
