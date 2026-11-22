using System.Net.WebSockets;

namespace MqttWire.Tests.Link;

/// <summary>
/// Tests the contract of <see cref="MemoryLink"/>, the in-memory link every client test relies on.
/// </summary>
public sealed class MemoryLinkTests
{
    /// <summary>
    /// Proves delivering bytes completes a pending read at once, on the calling thread.
    /// </summary>
    [Fact]
    public async Task Deliver_ReadPending_CompletesTheReadInline()
    {
        // Arrange
        MemoryLink link = new();
        byte[] buffer = new byte[16];
        ValueTask<int> read = link.ReadAsync(buffer, CancellationToken.None);
        bool pendingBefore = link.IsReadPending;

        // Act
        link.Deliver([1, 2, 3]);

        // Assert
        pendingBefore.ShouldBeTrue();
        read.IsCompleted.ShouldBeTrue();
        (await read).ShouldBe(3);
        buffer.AsSpan(0, 3).ToArray().ShouldBe(new byte[] { 1, 2, 3 });
        link.IsReadPending.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a held write stays pending until the held writes are completed.
    /// </summary>
    [Fact]
    public void HoldWrites_WriteIssued_StaysPendingUntilCompleted()
    {
        // Arrange
        MemoryLink link = new();
        link.HoldWrites();
        byte[] packet = [0xC0, 0x00];

        // Act
        ValueTask write = link.WriteAsync(packet, CancellationToken.None);
        bool pending = !write.IsCompleted;
        ReadOnlyMemory<byte> held = link.HeldWriteBuffer;
        link.CompleteHeldWrites();

        // Assert
        pending.ShouldBeTrue();
        held.ToArray().ShouldBe(packet);
        write.IsCompleted.ShouldBeTrue();
        link.TakeWritten().ShouldBe(packet);
        link.WriteCount.ShouldBe(1);
    }

    /// <summary>
    /// Proves aborting does not complete a held write: it stays pending until the test completes it.
    /// </summary>
    [Fact]
    public void Abort_WriteHeld_WriteStaysPendingUntilCompleted()
    {
        // Arrange
        MemoryLink link = new();
        link.HoldWrites();
        ValueTask write = link.WriteAsync(new byte[] { 0xC0, 0x00 }, CancellationToken.None);

        // Act
        link.Abort();
        bool pendingAfterAbort = !write.IsCompleted;
        link.CompleteHeldWrites();

        // Assert
        link.IsAborted.ShouldBeTrue();
        pendingAfterAbort.ShouldBeTrue();
        write.IsCompleted.ShouldBeTrue();
    }

    /// <summary>
    /// Proves seven bytes delivered in chunks of three are read as three, three and one.
    /// </summary>
    [Fact]
    public async Task DeliverInChunks_SevenBytesChunksOfThree_ReadsThreeThreeOne()
    {
        // Arrange
        MemoryLink link = new();
        byte[] buffer = new byte[16];
        link.DeliverInChunks([1, 2, 3, 4, 5, 6, 7], 3);

        // Act
        int first = await link.ReadAsync(buffer, CancellationToken.None);
        int second = await link.ReadAsync(buffer, CancellationToken.None);
        int third = await link.ReadAsync(buffer, CancellationToken.None);

        // Assert
        first.ShouldBe(3);
        second.ShouldBe(3);
        third.ShouldBe(1);
    }

    /// <summary>
    /// Proves a wait that finds data already queued completes at once and leaves the data to be read.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WaitForDataAsync_DataQueued_CompletesAtOnceWithoutConsumingIt()
    {
        // Arrange
        MemoryLink link = new();
        link.Deliver([9, 8]);
        byte[] buffer = new byte[4];

        // Act
        ValueTask wait = link.WaitForDataAsync(CancellationToken.None);
        bool completed = wait.IsCompleted;
        int read = await link.ReadAsync(buffer, CancellationToken.None);

        // Assert
        completed.ShouldBeTrue();
        read.ShouldBe(2);
        buffer.AsSpan(0, 2).ToArray().ShouldBe(new byte[] { 9, 8 });
    }

    /// <summary>
    /// Proves a pending wait completes inline when bytes are delivered, without consuming them.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WaitForDataAsync_DataDelivered_CompletesInlineAndLeavesTheBytes()
    {
        // Arrange
        MemoryLink link = new();
        ValueTask wait = link.WaitForDataAsync(CancellationToken.None);
        bool pendingBefore = link.IsReadPending;
        byte[] buffer = new byte[4];

        // Act
        link.Deliver([5]);
        bool completed = wait.IsCompleted;
        await wait;
        int read = await link.ReadAsync(buffer, CancellationToken.None);

        // Assert
        pendingBefore.ShouldBeTrue();
        completed.ShouldBeTrue();
        read.ShouldBe(1);
        buffer[0].ShouldBe((byte)5);
    }

    /// <summary>
    /// Proves ending from the peer completes a pending read with zero.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task EndFromPeer_ReadPending_CompletesTheReadWithZero()
    {
        // Arrange
        MemoryLink link = new();
        ValueTask<int> read = link.ReadAsync(new byte[4], CancellationToken.None);

        // Act
        link.EndFromPeer();

        // Assert
        read.IsCompleted.ShouldBeTrue();
        (await read).ShouldBe(0);
        link.IsReadPending.ShouldBeFalse();
    }

    /// <summary>
    /// Proves bytes delivered before the end are still read, and only then does a read return zero.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task EndFromPeer_DataStillQueued_ReadsTheDataThenZero()
    {
        // Arrange
        MemoryLink link = new();
        byte[] buffer = new byte[8];
        link.Deliver([1, 2, 3]);

        // Act
        link.EndFromPeer();
        int first = await link.ReadAsync(buffer, CancellationToken.None);
        int second = await link.ReadAsync(buffer, CancellationToken.None);

        // Assert
        first.ShouldBe(3);
        second.ShouldBe(0);
    }

    /// <summary>
    /// Proves failing reads ends a pending read with the exception, and later reads and waits throw it too.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task FailReads_ReadPending_FailsThePendingAndLaterOperations()
    {
        // Arrange
        MemoryLink link = new();
        InvalidOperationException failure = new("boom");
        ValueTask<int> pending = link.ReadAsync(new byte[4], CancellationToken.None);

        // Act
        link.FailReads(failure);

        // Assert
        InvalidOperationException thrown = await Should.ThrowAsync<InvalidOperationException>(async () => await pending);
        thrown.ShouldBeSameAs(failure);
        await Should.ThrowAsync<InvalidOperationException>(
            async () => await link.ReadAsync(new byte[4], CancellationToken.None));
        await Should.ThrowAsync<InvalidOperationException>(
            async () => await link.WaitForDataAsync(CancellationToken.None));
    }

    /// <summary>
    /// Proves failing writes makes later writes throw the exception and records nothing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task FailWrites_WriteIssued_ThrowsAndWritesNothing()
    {
        // Arrange
        MemoryLink link = new();
        InvalidOperationException failure = new("boom");
        link.FailWrites(failure);

        // Act
        InvalidOperationException thrown = await Should.ThrowAsync<InvalidOperationException>(
            async () => await link.WriteAsync(new byte[] { 1 }, CancellationToken.None));

        // Assert
        thrown.ShouldBeSameAs(failure);
        link.WriteCount.ShouldBe(0);
        link.TakeWritten().ShouldBeEmpty();
    }

    /// <summary>
    /// Proves completing the held writes with an exception fails them and records nothing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task CompleteHeldWrites_WithException_FailsTheHeldWrite()
    {
        // Arrange
        MemoryLink link = new();
        link.HoldWrites();
        ValueTask write = link.WriteAsync(new byte[] { 1 }, CancellationToken.None);

        // Act
        link.CompleteHeldWrites(new InvalidOperationException("boom"));

        // Assert
        await Should.ThrowAsync<InvalidOperationException>(async () => await write);
        link.WriteCount.ShouldBe(0);
    }

    /// <summary>
    /// Proves aborting ends a pending read with an OperationCanceledException, and later reads and waits throw it too.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_ReadPending_EndsItAndLaterReadsThrow()
    {
        // Arrange
        MemoryLink link = new();
        ValueTask<int> pending = link.ReadAsync(new byte[4], CancellationToken.None);

        // Act
        link.Abort();

        // Assert
        await Should.ThrowAsync<OperationCanceledException>(async () => await pending);
        await Should.ThrowAsync<OperationCanceledException>(
            async () => await link.ReadAsync(new byte[4], CancellationToken.None));
        await Should.ThrowAsync<OperationCanceledException>(
            async () => await link.WaitForDataAsync(CancellationToken.None));
        link.IsReadPending.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a read issued after an abort throws even though bytes are queued.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_ThenRead_Throws()
    {
        // Arrange
        MemoryLink link = new();
        link.Deliver([1]);
        link.Abort();

        // Act
        Func<Task> act = async () => await link.ReadAsync(new byte[4], CancellationToken.None);

        // Assert
        await Should.ThrowAsync<OperationCanceledException>(act);
    }

    /// <summary>
    /// Proves closing records the status.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task CloseAsync_NormalClosure_RecordsTheStatus()
    {
        // Arrange
        MemoryLink link = new();

        // Act
        await link.CloseAsync(WebSocketCloseStatus.NormalClosure, CancellationToken.None);

        // Assert
        link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
    }

    /// <summary>
    /// Proves cancelling the token of a pending read ends it with an OperationCanceledException.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadAsync_TokenCancelledWhilePending_EndsWithOperationCanceled()
    {
        // Arrange
        MemoryLink link = new();
        using CancellationTokenSource cts = new();
        ValueTask<int> pending = link.ReadAsync(new byte[4], cts.Token);

        // Act
        await cts.CancelAsync();

        // Assert
        pending.IsCompleted.ShouldBeTrue();
        await Should.ThrowAsync<OperationCanceledException>(async () => await pending);
        link.IsReadPending.ShouldBeFalse();
    }

    /// <summary>
    /// Proves cancelling the token of a pending wait ends it with an OperationCanceledException.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WaitForDataAsync_TokenCancelledWhilePending_EndsWithOperationCanceled()
    {
        // Arrange
        MemoryLink link = new();
        using CancellationTokenSource cts = new();
        ValueTask pending = link.WaitForDataAsync(cts.Token);

        // Act
        await cts.CancelAsync();

        // Assert
        pending.IsCompleted.ShouldBeTrue();
        await Should.ThrowAsync<OperationCanceledException>(async () => await pending);
        link.IsReadPending.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a token that is already cancelled makes a read throw at once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadAsync_TokenAlreadyCancelled_Throws()
    {
        // Arrange
        MemoryLink link = new();
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        // Act
        Action act = () => link.ReadAsync(new byte[4], cts.Token);

        // Assert
        Should.Throw<OperationCanceledException>(act);
    }

    /// <summary>
    /// Proves a read that completed normally is not disturbed by its token firing later: the next read stays pending.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadAsync_CompletedThenTokenCancelled_NextReadIsUnaffected()
    {
        // Arrange
        MemoryLink link = new();
        using CancellationTokenSource cts = new();
        ValueTask<int> first = link.ReadAsync(new byte[4], cts.Token);
        link.Deliver([1]);
        await first;
        ValueTask<int> second = link.ReadAsync(new byte[4], CancellationToken.None);

        // Act
        await cts.CancelAsync();

        // Assert
        second.IsCompleted.ShouldBeFalse();
        link.IsReadPending.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a second read issued while one is pending throws.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadAsync_AnotherReadPending_Throws()
    {
        // Arrange
        MemoryLink link = new();
        ValueTask<int> pending = link.ReadAsync(new byte[4], CancellationToken.None);

        // Act
        Func<Task> act = async () => await link.ReadAsync(new byte[4], CancellationToken.None);

        // Assert
        await Should.ThrowAsync<InvalidOperationException>(act);
        pending.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a delivery that wraps around the end of the ring is read back in order.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Deliver_WrapsAroundTheRing_ReadsBytesInOrder()
    {
        // Arrange
        MemoryLink link = new();
        byte[] first = Pattern(0, 3000);
        byte[] second = Pattern(3000, 3000);
        byte[] buffer = new byte[3000];
        link.Deliver(first);
        await link.ReadAsync(buffer, CancellationToken.None);

        // Act
        link.Deliver(second);
        int read = await link.ReadAsync(buffer, CancellationToken.None);

        // Assert
        read.ShouldBe(3000);
        buffer.ShouldBe(second);
    }

    /// <summary>
    /// Proves the ring grows when a delivery does not fit, keeping the unread bytes in order even when they had wrapped.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Deliver_MoreThanTheRingHolds_GrowsAndKeepsEveryByteInOrder()
    {
        // Arrange
        MemoryLink link = new();
        byte[] all = Pattern(0, 9000);
        byte[] expected = all[1000..];
        byte[] scratch = new byte[1000];
        link.Deliver(all.AsSpan(0, 3000));
        await link.ReadAsync(scratch, CancellationToken.None);
        link.Deliver(all.AsSpan(3000, 3000));

        // Act
        link.Deliver(all.AsSpan(6000, 3000));
        byte[] read = new byte[8000];
        int total = 0;
        while (total < read.Length)
        {
            total += await link.ReadAsync(read.AsMemory(total), CancellationToken.None);
        }

        // Assert
        total.ShouldBe(8000);
        read.ShouldBe(expected);
    }

    /// <summary>
    /// Proves more chunks than the chunk ring starts with are each read back as their own chunk.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task DeliverInChunks_MoreChunksThanTheInitialRing_ReadsEachChunkInOrder()
    {
        // Arrange
        MemoryLink link = new();
        byte[] all = Pattern(0, 200);
        byte[] buffer = new byte[16];
        link.DeliverInChunks(all, 1);

        // Act
        byte[] read = new byte[200];
        int[] counts = new int[200];
        for (int i = 0; i < 200; i++)
        {
            counts[i] = await link.ReadAsync(buffer, CancellationToken.None);
            read[i] = buffer[0];
        }

        // Assert
        counts.ShouldAllBe(c => c == 1);
        read.ShouldBe(all);
    }

    /// <summary>
    /// Proves a read with a buffer smaller than the chunk takes part of it and leaves the rest for the next read.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ReadAsync_BufferSmallerThanChunk_ReturnsTheRestOnTheNextRead()
    {
        // Arrange
        MemoryLink link = new();
        link.Deliver([1, 2, 3, 4, 5]);
        byte[] buffer = new byte[2];

        // Act
        int first = await link.ReadAsync(buffer, CancellationToken.None);
        int second = await link.ReadAsync(buffer, CancellationToken.None);
        int third = await link.ReadAsync(buffer, CancellationToken.None);

        // Assert
        first.ShouldBe(2);
        second.ShouldBe(2);
        third.ShouldBe(1);
        buffer[0].ShouldBe((byte)5);
    }

    /// <summary>
    /// Proves writes are concatenated in order, counted, and taken once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WriteAsync_SeveralWrites_TakeWrittenReturnsThemConcatenatedOnce()
    {
        // Arrange
        MemoryLink link = new();

        // Act
        await link.WriteAsync(new byte[] { 1, 2 }, CancellationToken.None);
        await link.WriteAsync(new byte[] { 3 }, CancellationToken.None);
        byte[] taken = link.TakeWritten();
        byte[] again = link.TakeWritten();

        // Assert
        taken.ShouldBe(new byte[] { 1, 2, 3 });
        again.ShouldBeEmpty();
        link.WriteCount.ShouldBe(2);
    }

    /// <summary>
    /// Proves writes from several threads at once are all present and no packet is interleaved inside another.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WriteAsync_SeveralThreadsAtOnce_EveryPacketIsWholeAndNothingIsLost()
    {
        // Arrange
        const int Threads = 8;
        const int PacketsPerThread = 500;
        const int PacketLength = 16;
        MemoryLink link = new();
        using ManualResetEventSlim start = new();
        Task[] tasks = new Task[Threads];
        for (int t = 0; t < Threads; t++)
        {
            byte marker = (byte)(t + 1);
            tasks[t] = Task.Run(async () =>
            {
                byte[] packet = new byte[PacketLength];
                Array.Fill(packet, marker);
                start.Wait();
                for (int i = 0; i < PacketsPerThread; i++)
                {
                    await link.WriteAsync(packet, CancellationToken.None);
                }
            });
        }

        // Act
        start.Set();
        await Task.WhenAll(tasks);
        byte[] written = link.TakeWritten();

        // Assert
        written.Length.ShouldBe(Threads * PacketsPerThread * PacketLength);
        link.WriteCount.ShouldBe(Threads * PacketsPerThread);
        int[] perMarker = new int[Threads + 1];
        for (int offset = 0; offset < written.Length; offset += PacketLength)
        {
            byte marker = written[offset];
            marker.ShouldBeInRange((byte)1, (byte)Threads);
            for (int j = 1; j < PacketLength; j++)
            {
                written[offset + j].ShouldBe(marker);
            }

            perMarker[marker]++;
        }

        for (int t = 1; t <= Threads; t++)
        {
            perMarker[t].ShouldBe(PacketsPerThread);
        }
    }

    /// <summary>
    /// Proves the link counts a write, and a close, begun while a write it holds has not ended, on itself and in the
    /// checks it shares, and counts none for writes that follow each other.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WriteAsync_BegunWhileAnotherIsHeld_IsCountedAsOverlapping()
    {
        // Arrange
        MemoryLinkChecks checks = new();
        MemoryLink link = new() { Checks = checks };
        await link.WriteAsync(new byte[] { 1 }, CancellationToken.None);
        await link.WriteAsync(new byte[] { 2 }, CancellationToken.None);
        int afterWritesInTurn = link.OverlappingWrites;
        link.HoldWrites();
        ValueTask held = link.WriteAsync(new byte[] { 3 }, CancellationToken.None);

        // Act
        ValueTask overlapping = link.WriteAsync(new byte[] { 4 }, CancellationToken.None);
        await link.CloseAsync(WebSocketCloseStatus.NormalClosure, CancellationToken.None);
        link.CompleteHeldWrites();
        await held;
        await overlapping;

        // Assert
        afterWritesInTurn.ShouldBe(0);
        link.OverlappingWrites.ShouldBe(2);
        checks.OverlappingWrites.ShouldBe(2);
        link.WritesAfterAbort.ShouldBe(0);
    }

    /// <summary>
    /// Proves a write begun after the abort fails at once, also while writes are held, and is counted; that the
    /// shared checks count only what follows the first such write on a link; and that a write held before the abort
    /// is still the test's to complete.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task WriteAsync_BegunAfterTheAbort_FailsAndIsCounted()
    {
        // Arrange
        MemoryLinkChecks checks = new();
        MemoryLink link = new() { Checks = checks };
        link.HoldWrites();
        ValueTask heldBeforeTheAbort = link.WriteAsync(new byte[] { 1 }, CancellationToken.None);
        link.Abort();
        link.CompleteHeldWrites();
        await heldBeforeTheAbort;
        link.HoldWrites();

        // Act
        ValueTask first = link.WriteAsync(new byte[] { 2 }, CancellationToken.None);
        int sharedAfterTheFirst = checks.FurtherWritesAfterAbort;
        ValueTask second = link.WriteAsync(new byte[] { 3 }, CancellationToken.None);

        // Assert
        first.IsFaulted.ShouldBeTrue();
        second.IsFaulted.ShouldBeTrue();
        await Should.ThrowAsync<OperationCanceledException>(async () => await first);
        await Should.ThrowAsync<OperationCanceledException>(async () => await second);
        link.WritesAfterAbort.ShouldBe(2);
        sharedAfterTheFirst.ShouldBe(0);
        checks.FurtherWritesAfterAbort.ShouldBe(1);
        link.TakeWritten().ShouldBe(new byte[] { 1 });
    }

    /// <summary>
    /// Creates a deterministic byte pattern.
    /// </summary>
    /// <param name="start">
    /// The index the pattern starts at.
    /// </param>
    /// <param name="length">
    /// The number of bytes.
    /// </param>
    /// <returns>
    /// The bytes, each a function of its index so a misplaced byte is detected.
    /// </returns>
    private static byte[] Pattern(int start, int length)
    {
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++)
        {
            int index = start + i;
            result[i] = (byte)((index * 7) + (index >> 8));
        }

        return result;
    }
}
