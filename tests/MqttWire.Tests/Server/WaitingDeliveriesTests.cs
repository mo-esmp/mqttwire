using System.Runtime.CompilerServices;
using MqttWire.Server;
using MqttWire.Server.Internal;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of <see cref="WaitingDeliveries"/>, the ring of one connection's waiting deliveries, and of the policy of
/// the server's pool of them (plan decision 12): the ring keeps the order across a wrap and a growth, drops every
/// reference it held when cleared, and the pool keeps only a ring that stayed small.
/// </summary>
public sealed class WaitingDeliveriesTests
{
    /// <summary>
    /// The topic every delivery carries.
    /// </summary>
    private static readonly MqttTopic s_topic = MqttTopic.Create("a/b");

    /// <summary>
    /// Proves that deliveries come out in the order they went in when the ring's positions wrap past its last slot.
    /// </summary>
    [Fact]
    public void Dequeue_AcrossAWrapOfTheRing_KeepsTheOrder()
    {
        // Arrange
        WaitingDeliveries queue = new();
        Enqueue(queue, first: 0, count: 12);
        Dequeue(queue, count: 10);

        // Act
        // The oldest is in slot 10 of 16: these go into slots 12 to 15 and then 0 to 7.
        Enqueue(queue, first: 12, count: 12);
        List<int> order = DrainOrder(queue);

        // Assert
        queue.Capacity.ShouldBe(16);
        order.ShouldBe(Enumerable.Range(10, 14).ToList());
        queue.Count.ShouldBe(0);
        queue.PayloadBytes.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a ring that grows while its deliveries wrap past its last slot keeps their order.
    /// </summary>
    [Fact]
    public void Enqueue_GrowthWhileWrapped_KeepsTheOrder()
    {
        // Arrange
        WaitingDeliveries queue = new();
        Enqueue(queue, first: 0, count: 16);
        Dequeue(queue, count: 10);

        // The ring is full, wrapped: slots 10 to 15 hold 10 to 15, slots 0 to 9 hold 16 to 25.
        Enqueue(queue, first: 16, count: 10);

        // Act
        Enqueue(queue, first: 26, count: 1);
        List<int> order = DrainOrder(queue);

        // Assert
        queue.Capacity.ShouldBe(32);
        order.ShouldBe(Enumerable.Range(10, 17).ToList());
    }

    /// <summary>
    /// Proves that clearing a wrapped ring drops every topic and payload reference it held, in both of its parts, and
    /// keeps its storage for the next use.
    /// </summary>
    [Fact]
    public void Clear_WrappedRing_DropsEveryReference()
    {
        // Arrange
        WaitingDeliveries queue = new();
        Enqueue(queue, first: 0, count: 16);
        Dequeue(queue, count: 10);
        Enqueue(queue, first: 16, count: 6);

        // Act
        queue.Clear();

        // Assert
        queue.Count.ShouldBe(0);
        queue.PayloadBytes.ShouldBe(0);
        queue.Capacity.ShouldBe(16);
        Items(queue).ShouldAllBe(static slot => slot.Equals(default(PendingDelivery)));
    }

    /// <summary>
    /// Proves that the payload bytes the ring reports are exact through every change: each enqueue adds its payload's
    /// length, each dequeue takes it away, and a clear makes them 0.
    /// </summary>
    [Fact]
    public void PayloadBytes_EnqueueDequeueAndClear_StayExact()
    {
        // Arrange
        WaitingDeliveries queue = new();

        // Act
        // Payloads of 1 to 20 bytes: 210 bytes in all.
        for (int length = 1; length <= 20; length++)
        {
            queue.Enqueue(new PendingDelivery(s_topic, new byte[length], MqttQoS.AtMostOnce));
        }

        long afterEnqueue = queue.PayloadBytes;

        // The first five take away 1 + 2 + 3 + 4 + 5 = 15 bytes.
        Dequeue(queue, count: 5);
        long afterDequeue = queue.PayloadBytes;
        queue.Clear();

        // Assert
        afterEnqueue.ShouldBe(210);
        afterDequeue.ShouldBe(195);
        queue.PayloadBytes.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the pool's policy clears every queue that comes back, keeps one whose storage is at most 64 slots,
    /// and refuses one that grew past them, so a burst's storage is left to the garbage collector.
    /// </summary>
    [Fact]
    public void Return_QueueGrownPast64_IsClearedAndRefused()
    {
        // Arrange
        ServerPools.WaitingDeliveriesPolicy policy = new();
        WaitingDeliveries small = policy.Create();
        WaitingDeliveries atTheLimit = policy.Create();
        WaitingDeliveries grown = policy.Create();
        Enqueue(small, first: 0, count: 3);
        Enqueue(atTheLimit, first: 0, count: 64);
        Enqueue(grown, first: 0, count: 65);

        // Act
        bool keptSmall = policy.Return(small);
        bool keptAtTheLimit = policy.Return(atTheLimit);
        bool keptGrown = policy.Return(grown);

        // Assert
        keptSmall.ShouldBeTrue();
        keptAtTheLimit.ShouldBeTrue();
        atTheLimit.Capacity.ShouldBe(ServerLimits.MaxPooledQueueCapacity);
        keptGrown.ShouldBeFalse();
        grown.Capacity.ShouldBe(128);
        small.Count.ShouldBe(0);
        atTheLimit.Count.ShouldBe(0);
        grown.Count.ShouldBe(0);
        Items(grown).ShouldAllBe(static slot => slot.Equals(default(PendingDelivery)));
    }

    /// <summary>
    /// Proves that a connection that ends with deliveries waiting gives its queue back to the server's pool, empty:
    /// the queue the pool hands out afterwards is the same object, holding nothing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_WithWaitingDeliveries_ReturnsTheQueueToThePoolEmpty()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        WaitingDeliveries pooled = harness.Server.Pools.Waiting.Get();
        harness.Server.Pools.Waiting.Return(pooled);
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, "m3"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        int waiting = server.GetCounts().WaitingDeliveries;

        // Act
        // The close finds the write in progress and aborts; the write then ends and drops what waits.
        server.Close(MqttReasonCode.AdministrativeAction);
        connection.CompleteHeldWrites(new IOException("The socket was aborted."));
        await connection.RunEnded();
        WaitingDeliveries afterwards = harness.Server.Pools.Waiting.Get();

        // Assert
        waiting.ShouldBe(2);
        afterwards.ShouldBeSameAs(pooled);
        afterwards.Count.ShouldBe(0);
        afterwards.PayloadBytes.ShouldBe(0);
        Items(afterwards).ShouldAllBe(static slot => slot.Equals(default(PendingDelivery)));
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Adds deliveries whose one-byte payloads number them.
    /// </summary>
    /// <param name="queue">
    /// The queue.
    /// </param>
    /// <param name="first">
    /// The number of the first delivery.
    /// </param>
    /// <param name="count">
    /// How many to add, numbered on from <paramref name="first"/>.
    /// </param>
    private static void Enqueue(WaitingDeliveries queue, int first, int count)
    {
        for (int i = first; i < first + count; i++)
        {
            queue.Enqueue(new PendingDelivery(s_topic, new[] { (byte)i }, MqttQoS.AtMostOnce));
        }
    }

    /// <summary>
    /// Removes the oldest deliveries.
    /// </summary>
    /// <param name="queue">
    /// The queue.
    /// </param>
    /// <param name="count">
    /// How many to remove; at most as many as wait.
    /// </param>
    private static void Dequeue(WaitingDeliveries queue, int count)
    {
        for (int i = 0; i < count; i++)
        {
            queue.Dequeue();
        }
    }

    /// <summary>
    /// Removes every delivery, oldest first, and reads the number each one's payload carries.
    /// </summary>
    /// <param name="queue">
    /// The queue.
    /// </param>
    /// <returns>
    /// The numbers, in the order the deliveries came out.
    /// </returns>
    private static List<int> DrainOrder(WaitingDeliveries queue)
    {
        List<int> order = [];
        while (queue.Count > 0)
        {
            order.Add(queue.Peek().Payload.Span[0]);
            queue.Dequeue();
        }

        return order;
    }

    /// <summary>
    /// Reads the ring's slots, to see what references it still holds.
    /// </summary>
    /// <param name="queue">
    /// The queue.
    /// </param>
    /// <returns>
    /// The slots themselves.
    /// </returns>
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_items")]
    private static extern ref PendingDelivery[] Items(WaitingDeliveries queue);
}
