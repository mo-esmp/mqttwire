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
}
