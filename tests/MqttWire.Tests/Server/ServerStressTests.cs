using System.Buffers;
using System.Buffers.Binary;
using MqttWire.Codec;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Server;

/// <summary>
/// The stress tests of the send path (design 6.13 item 3, "Stress"): many threads call <c>TrySend</c> on one
/// connection, and a close races them, while the in-memory link completes writes sometimes at once and sometimes
/// later. Real threads start together at a gate; nothing waits in real time for correctness, and spins only vary the
/// interleaving. Every outcome asserted is exact.
/// </summary>
public sealed class ServerStressTests
{
    /// <summary>
    /// The number of threads that send in <see cref="TrySend_EightThreadsOnOneConnection_WritesEverythingOnceAndInOrder"/>.
    /// </summary>
    private const int Senders = 8;

    /// <summary>
    /// The number of messages each of them sends.
    /// </summary>
    private const int MessagesPerSender = 5_000;

    /// <summary>
    /// The length of the payload of each stress message: the sender's number and the message's sequence number.
    /// </summary>
    private const int PayloadLength = 8;

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
    /// byte is counted and every buffer is back. The limits are raised so that nothing is evicted. A tenth thread runs
    /// the sweep without pause meanwhile, with the clock standing still, so that the sweep reads the connection while
    /// its writes begin and end and no deadline passes.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_EightThreadsOnOneConnection_WritesEverythingOnceAndInOrder()
    {
        // Arrange
        const int Total = Senders * MessagesPerSender;
        ServerHarness harness = ServerHarness.Create(static options =>
        {
            options.MaxOutgoingMessages = Total;
            options.MaxOutgoingBytes = Total * PayloadLength;
        });
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        MemoryLink link = connection.Link;
        int ready = 0;
        int sendersDone = 0;
        int notTaken = 0;
        Thread[] senders = new Thread[Senders];
        for (int sender = 0; sender < Senders; sender++)
        {
            int number = sender;
            senders[sender] = new Thread(() =>
            {
                Gate.Wait(ref ready, Senders + 1);
                for (int sequence = 0; sequence < MessagesPerSender; sequence++)
                {
                    byte[] payload = new byte[PayloadLength];
                    BinaryPrimitives.WriteInt32LittleEndian(payload, number);
                    BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(4), sequence);
                    if (server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtMostOnce) != MqttSendResult.Taken)
                    {
                        Interlocked.Increment(ref notTaken);
                    }
                }

                Interlocked.Increment(ref sendersDone);
            });
        }

        // A write the toggler released carries the send path on, on the toggler's thread, for as long as the writes
        // after it complete at once; every 61st write is held as it begins, so that such a drain also stops halfway.
        int writes = 0;
        link.Writing = _ =>
        {
            if (Interlocked.Increment(ref writes) % 61 == 0)
            {
                link.HoldWrites();
            }
        };
        Thread toggler = new(() =>
        {
            Gate.Wait(ref ready, Senders + 1);
            for (int turn = 0; Volatile.Read(ref sendersDone) < Senders; turn++)
            {
                // Held for a varying while, then released: the writes held meanwhile complete on this thread.
                link.HoldWrites();
                Thread.SpinWait(turn % 16 * 50);
                link.CompleteHeldWrites();
                Thread.SpinWait(turn % 7 * 50);
            }

            link.CompleteHeldWrites();
        });

        int sweeps = 0;
        Thread sweeper = new(() =>
        {
            while (Volatile.Read(ref sendersDone) < Senders)
            {
                harness.Server.Sweep();
                Interlocked.Increment(ref sweeps);
            }
        });

        // Act
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
        await connection.WhenSendPathIdleAsync();
        ConnectionCounts counts = server.GetCounts();
        harness.AssertBounds();
        List<int>[] sequences = ReadSequences(link.TakeWritten(), out int packets);

        // Assert
        notTaken.ShouldBe(0);
        packets.ShouldBe(Total);
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
        Volatile.Read(ref sweeps).ShouldBeGreaterThan(0);
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
    /// that was decided on before its abort, and no abort after the run had ended; and once the run has ended every
    /// buffer is back and none was returned twice.
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
                        if (server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce) != MqttSendResult.Taken)
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
                || link.AbortsAfterEnd != 0)
            {
                failures.Add($"round {round} (held {held}): writes after the close {writesAfterTheClose}, packets "
                             + $"[{string.Join(' ', types)}], taken {taken}, after the close {afterTheClose}, "
                             + $"outstanding {harness.Pool.Outstanding}, faulty returns {harness.Pool.FaultyReturns}, "
                             + $"overlapping writes {link.OverlappingWrites}, writes after the abort "
                             + $"{link.WritesAfterAbort}, aborts after the end {link.AbortsAfterEnd}");
            }
        }

        failures.ShouldBeEmpty();
    }

    /// <summary>
    /// Reads the stress messages out of what was written: each a QoS 0 PUBLISH to "a/b" whose payload is the sender's
    /// number and the message's sequence number.
    /// </summary>
    /// <param name="written">
    /// The written bytes.
    /// </param>
    /// <param name="packets">
    /// The number of packets read.
    /// </param>
    /// <returns>
    /// For each sender, the sequence numbers in the order they were written.
    /// </returns>
    private static List<int>[] ReadSequences(byte[] written, out int packets)
    {
        List<int>[] sequences = new List<int>[Senders];
        for (int sender = 0; sender < Senders; sender++)
        {
            sequences[sender] = [];
        }

        packets = 0;
        ReadOnlySpan<byte> rest = written;
        while (!rest.IsEmpty)
        {
            PacketHeader.TryRead(rest, out PacketHeader header).ShouldBe(OperationStatus.Done);
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
}
