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

        // The ring is empty with its head at index 4 of 8: nine writes wrap around the end and then force growth.
        for (int i = 100; i < 109; i++)
        {
            queue.Enqueue(WriteOf(i));
        }

        List<int> afterGrowth = Drain(ref queue);

        // Assert
        afterWrap.ShouldBe(Enumerable.Range(0, 12).ToList());
        afterGrowth.ShouldBe(Enumerable.Range(100, 9).ToList());
    }

    /// <summary>
    /// Proves a ring grown past the retained size by a burst is released once the burst has drained, and the next
    /// write to wait creates a small one again.
    /// </summary>
    [Fact]
    public void TryDequeue_LastWriteOfBurst_ReleasesOversizedRing()
    {
        // Arrange
        SendQueue queue = new();
        for (int i = 0; i < 1_000; i++)
        {
            queue.Enqueue(WriteOf(i));
        }

        int capacityDuringBurst = queue.Capacity;

        // Act
        List<int> lengths = Drain(ref queue);
        int capacityAfterBurst = queue.Capacity;
        queue.Enqueue(WriteOf(5));

        // Assert
        capacityDuringBurst.ShouldBeGreaterThan(ClientLimits.SendQueueRetainedCapacity);
        lengths.ShouldBe(Enumerable.Range(0, 1_000).ToList());
        capacityAfterBurst.ShouldBe(0);
        queue.Capacity.ShouldBe(ClientLimits.SendQueueFirstCapacity);
        Drain(ref queue).ShouldBe([5]);
    }

    /// <summary>
    /// Proves a ring within the retained size is kept when it empties, so that routine contention, the same number of
    /// writes waiting again and again, allocates nothing once the ring has grown.
    /// </summary>
    [Fact]
    public void Enqueue_RepeatedRoutineContention_KeepsRingAndAllocatesNothing()
    {
        // Arrange
        SendQueue queue = new();
        Fill(ref queue, ClientLimits.SendQueueRetainedCapacity);
        int capacity = queue.Capacity;

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int round = 0; round < 100; round++)
        {
            Fill(ref queue, ClientLimits.SendQueueRetainedCapacity);
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        // Assert
        capacity.ShouldBe(ClientLimits.SendQueueRetainedCapacity);
        queue.Capacity.ShouldBe(capacity);
        allocated.ShouldBe(0);
    }

    /// <summary>
    /// Proves answers are appended to the newest write when it is an answers write with room in its buffer: the bytes
    /// follow its own, it counts both sets of answers, and no write is added.
    /// </summary>
    [Fact]
    public void TryAppendAnswers_NewestIsAnswersWithRoom_AppendsBytesAndAnswers()
    {
        // Arrange
        SendQueue queue = new();
        queue.Enqueue(new QueuedWrite(new byte[8], length: 0, SendKind.Publish, answers: 0));
        byte[] buffer = new byte[8];
        buffer[0] = 1;
        buffer[1] = 2;
        queue.Enqueue(new QueuedWrite(buffer, length: 2, SendKind.Answers, answers: 1));

        // Act
        bool appended = queue.TryAppendAnswers(new QueuedWrite([3, 4, 5], length: 3, SendKind.Answers, answers: 2));

        // Assert
        appended.ShouldBeTrue();
        queue.Count.ShouldBe(2);
        queue.TryDequeue(out _);
        queue.TryDequeue(out QueuedWrite newest);
        newest.Buffer.ShouldBeSameAs(buffer);
        newest.Length.ShouldBe(5);
        newest.Answers.ShouldBe(3);
        buffer.AsSpan(0, 5).ToArray().ShouldBe([1, 2, 3, 4, 5]);
    }

    /// <summary>
    /// Proves answers are not appended when the newest write is not answers, or its buffer has no room for them, or
    /// nothing waits: the queue is left as it was.
    /// </summary>
    [Fact]
    public void TryAppendAnswers_NewestNotAnswersOrFull_ReturnsFalse()
    {
        // Arrange
        QueuedWrite answers = new([9, 9], length: 2, SendKind.Answers, answers: 1);
        SendQueue empty = new();
        SendQueue behindPublish = new();
        behindPublish.Enqueue(new QueuedWrite(new byte[8], length: 2, SendKind.Answers, answers: 1));
        behindPublish.Enqueue(new QueuedWrite(new byte[8], length: 2, SendKind.Publish, answers: 0));
        SendQueue full = new();
        full.Enqueue(new QueuedWrite(new byte[4], length: 3, SendKind.Answers, answers: 1));

        // Act
        bool intoEmpty = empty.TryAppendAnswers(answers);
        bool behindAPublish = behindPublish.TryAppendAnswers(answers);
        bool intoFull = full.TryAppendAnswers(answers);

        // Assert
        intoEmpty.ShouldBeFalse();
        behindAPublish.ShouldBeFalse();
        intoFull.ShouldBeFalse();
        full.TryDequeue(out QueuedWrite unchanged);
        unchanged.Length.ShouldBe(3);
        unchanged.Answers.ShouldBe(1);
    }

    /// <summary>
    /// Queues a number of writes and then takes them all again.
    /// </summary>
    /// <param name="queue">
    /// The queue, empty before and after.
    /// </param>
    /// <param name="count">
    /// How many writes wait at once.
    /// </param>
    private static void Fill(ref SendQueue queue, int count)
    {
        for (int i = 0; i < count; i++)
        {
            queue.Enqueue(WriteOf(i));
        }

        while (queue.TryDequeue(out _))
        {
        }
    }
}
