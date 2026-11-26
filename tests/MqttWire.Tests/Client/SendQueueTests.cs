using MqttWire.Client;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests the queue of the writes that wait for their turn on the link.
/// </summary>
public sealed class SendQueueTests
{
    /// <summary>
    /// Builds a write whose length tells which one it is.
    /// </summary>
    /// <param name="length">
    /// The length, used as the write's identity.
    /// </param>
    /// <returns>
    /// The write.
    /// </returns>
    private static QueuedWrite WriteOf(int length) => new(buffer: null, length: length, kind: SendKind.WritesEnded, answers: 0);

    /// <summary>
    /// Dequeues every write and returns their lengths.
    /// </summary>
    /// <param name="queue">
    /// The queue to empty.
    /// </param>
    /// <returns>
    /// The lengths in the order they came out.
    /// </returns>
    private static List<int> Drain(ref SendQueue queue)
    {
        List<int> lengths = [];
        while (queue.TryDequeue(out QueuedWrite write))
        {
            lengths.Add(write.Length);
        }

        return lengths;
    }

    /// <summary>
    /// Proves an empty queue gives nothing and has a count of zero.
    /// </summary>
    [Fact]
    public void TryDequeue_Empty_ReturnsFalse()
    {
        // Arrange
        SendQueue queue = new();

        // Act
        bool dequeued = queue.TryDequeue(out _);

        // Assert
        dequeued.ShouldBeFalse();
        queue.Count.ShouldBe(0);
    }

    /// <summary>
    /// Proves writes come out in the order they went in.
    /// </summary>
    [Fact]
    public void TryDequeue_AfterEnqueues_ReturnsThemInOrder()
    {
        // Arrange
        SendQueue queue = new();
        queue.Enqueue(WriteOf(1));
        queue.Enqueue(WriteOf(2));
        queue.Enqueue(WriteOf(3));

        // Act
        List<int> lengths = Drain(ref queue);

        // Assert
        lengths.ShouldBe([1, 2, 3]);
    }

    /// <summary>
    /// Proves a queue that outgrows its first capacity keeps every write in order and counts them down to zero.
    /// </summary>
    [Fact]
    public void Enqueue_PastFirstCapacity_GrowsAndKeepsOrder()
    {
        // Arrange
        SendQueue queue = new();

        // Act
        for (int i = 0; i < 20; i++)
        {
            queue.Enqueue(WriteOf(i));
        }

        int countBefore = queue.Count;
        List<int> lengths = Drain(ref queue);

        // Assert
        countBefore.ShouldBe(20);
        lengths.ShouldBe(Enumerable.Range(0, 20).ToList());
        queue.Count.ShouldBe(0);
    }

    /// <summary>
    /// Proves the order holds when the ring wraps around, and when it then has to grow from the wrapped state.
    /// </summary>
    [Fact]
    public void Enqueue_AfterWrapAround_KeepsOrder()
    {
        // Arrange
        SendQueue queue = new();
        List<int> afterWrap = [];
        for (int i = 0; i < 6; i++)
        {
            queue.Enqueue(WriteOf(i));
        }

        // Act
        for (int i = 0; i < 4; i++)
        {
            queue.TryDequeue(out QueuedWrite write);
            afterWrap.Add(write.Length);
        }

        for (int i = 6; i < 12; i++)
        {
            queue.Enqueue(WriteOf(i));
        }

        afterWrap.AddRange(Drain(ref queue));

        // The ring is empty with its head at index 6 of 8: nine writes wrap around the end and then force growth.
        for (int i = 100; i < 109; i++)
        {
            queue.Enqueue(WriteOf(i));
        }

        List<int> afterGrowth = Drain(ref queue);

        // Assert
        afterWrap.ShouldBe(Enumerable.Range(0, 12).ToList());
        afterGrowth.ShouldBe(Enumerable.Range(100, 9).ToList());
    }
}
