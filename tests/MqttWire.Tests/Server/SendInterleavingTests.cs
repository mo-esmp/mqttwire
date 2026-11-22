using System.Net.WebSockets;
using System.Threading.Tasks.Sources;
using Microsoft.AspNetCore.Http;
using MqttWire.Link;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of "only while there is work" on the send path (design 6.13 item 3, the deterministic kind): each forces
/// one interleaving of the write's completion with a caller that adds a message, a control packet or a close, by
/// holding the link's write open and releasing it at a chosen point, or by acting from the pool's return hook, which
/// runs after a write has ended and before the send path has looked for what is next. Nothing waits in real time.
/// </summary>
public sealed class SendInterleavingTests
{
    /// <summary>
    /// The topic every test sends to.
    /// </summary>
    private static readonly MqttTopic s_topic = MqttTopic.Create("a/b");

    /// <summary>
    /// How many messages the tests of the hand-over to the thread pool add behind the first one.
    /// </summary>
    private const int Added = 40;

    /// <summary>
    /// Proves that a message added while a write is in progress is written by whoever completes that write: once,
    /// after it, and the send path ends idle.
    /// </summary>
    [Fact]
    public void Drain_MessageAddedBeforeTheWriteIsReleased_IsWrittenByTheDrain()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);

        // Act
        MqttSendResult added = server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce);
        int writesWhenAdded = connection.Link.WriteCount;
        connection.CompleteHeldWrites();

        // Assert
        added.ShouldBe(MqttSendResult.Taken);
        writesWhenAdded.ShouldBe(1);
        connection.TakeWritten().ShouldBe([.. ServerPackets.Delivery("m1"u8), .. ServerPackets.Delivery("m2"u8)]);
        AssertIdle(harness, server);
    }

    /// <summary>
    /// Proves that a message added after a write has ended but before the send path has looked for what is next is
    /// not lost and not written by its own caller: it waits, and the send path, which has not gone idle, writes it,
    /// once and in order.
    /// </summary>
    [Fact]
    public void Drain_MessageAddedWhileTheBufferIsReturned_IsWrittenOnceAndInOrder()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        MqttSendResult? added = null;
        ConnectionCounts whenAdded = default;
        int writesWhenAdded = 0;
        harness.Pool.Returning = _ =>
        {
            // The first message's write has ended and its buffer is going back: the drain has not looked yet.
            harness.Pool.Returning = null;
            added = server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce);
            whenAdded = server.GetCounts();
            writesWhenAdded = connection.Link.WriteCount;
        };

        // Act
        MqttSendResult first = server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);

        // Assert
        first.ShouldBe(MqttSendResult.Taken);
        added.ShouldBe(MqttSendResult.Taken);

        // The CONNACK and the first message were written; the second waited instead of being written by its caller.
        writesWhenAdded.ShouldBe(2);
        whenAdded.IsWriting.ShouldBeTrue();
        whenAdded.WaitingDeliveries.ShouldBe(1);
        connection.TakeWritten().ShouldBe([.. ServerPackets.Delivery("m1"u8), .. ServerPackets.Delivery("m2"u8)]);
        connection.Link.WriteCount.ShouldBe(3);
        AssertIdle(harness, server);
    }

    /// <summary>
    /// Proves that a message added after the send path found nothing waiting and went idle is written by its own
    /// caller, at once.
    /// </summary>
    [Fact]
    public void Drain_MessageAddedAfterTheDrainWentIdle_IsWrittenByItsCaller()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        connection.CompleteHeldWrites();
        bool writingAfterTheDrain = server.GetCounts().IsWriting;

        // Act
        MqttSendResult added = server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce);
        int writesWhenItReturned = connection.Link.WriteCount;

        // Assert
        writingAfterTheDrain.ShouldBeFalse();
        added.ShouldBe(MqttSendResult.Taken);
        writesWhenItReturned.ShouldBe(3);
        connection.TakeWritten().ShouldBe([.. ServerPackets.Delivery("m1"u8), .. ServerPackets.Delivery("m2"u8)]);
        AssertIdle(harness, server);
    }

    /// <summary>
    /// Proves that a control packet added after a write has ended but before the send path has looked for what is
    /// next goes out before the deliveries that already wait.
    /// </summary>
    [Fact]
    public void Drain_ControlPacketAddedWhileTheBufferIsReturned_GoesBeforeWaitingDeliveries()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        ConnectionCounts whenAdded = default;
        harness.Pool.Returning = _ =>
        {
            // The first message's write has ended and its buffer is going back: the PINGREQ is read and answered now.
            harness.Pool.Returning = null;
            connection.Deliver(ClientPackets.PingReq);
            whenAdded = server.GetCounts();
        };

        // Act
        connection.CompleteHeldWrites();

        // Assert
        whenAdded.IsWriting.ShouldBeTrue();
        whenAdded.WaitingControlPackets.ShouldBe(1);
        whenAdded.WaitingDeliveries.ShouldBe(1);
        connection.TakeWritten().ShouldBe(
            [.. ServerPackets.Delivery("m1"u8), .. ClientPackets.PingResp, .. ServerPackets.Delivery("m2"u8)]);
        AssertIdle(harness, server);
    }

    /// <summary>
    /// Proves that a message added from inside a write that completes at once, while its caller is still in the
    /// link, waits for that caller's drain and is written once, after the write it was added during.
    /// </summary>
    [Fact]
    public void Drain_MessageAddedDuringAWriteThatCompletesAtOnce_IsWrittenByThatCaller()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        MqttSendResult? added = null;
        connection.Link.Writing = _ =>
        {
            connection.Link.Writing = null;
            added = server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce);
        };

        // Act
        MqttSendResult first = server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);

        // Assert
        first.ShouldBe(MqttSendResult.Taken);
        added.ShouldBe(MqttSendResult.Taken);
        connection.TakeWritten().ShouldBe([.. ServerPackets.Delivery("m1"u8), .. ServerPackets.Delivery("m2"u8)]);
        AssertIdle(harness, server);
    }

    /// <summary>
    /// Proves that a caller whose own write keeps finding more to write does not write for ever: after 16 writes
    /// that completed at once inside its call it hands the send path to the thread pool and returns, and everything
    /// is still written once and in order. Each write adds one more message behind itself here, 40 in all.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Drain_ManyWritesCompleteAtOnceOnTheCallersThread_HandsTheRestToTheThreadPool()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Set in the test's own call and nowhere else: the thread pool runs what is handed to it without the caller's
        // execution context, whichever thread it uses, so there the value is false.
        AsyncLocal<bool> insideTheCall = new() { Value = true };
        int writes = 0;
        int writesInsideTheCall = 0;
        connection.Link.Writing = _ =>
        {
            // Runs under the send path, one write at a time: each write adds the next message behind itself.
            int written = ++writes;
            if (insideTheCall.Value)
            {
                writesInsideTheCall++;
            }

            if (written <= Added)
            {
                server.TrySend(s_topic, new[] { (byte)written }, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
            }
        };
        List<byte> expected = [.. ServerPackets.Delivery([0])];
        for (int i = 1; i <= Added; i++)
        {
            expected.AddRange(ServerPackets.Delivery([(byte)i]));
        }

        // Act
        MqttSendResult first = server.TrySend(s_topic, new byte[1], MqttQoS.AtMostOnce);

        // The CONNACK, the first message and the 40 added.
        await connection.Link.WhenWrittenAsync(count: Added + 2);
        await harness.Pool.WhenNoneOutstandingAsync();
        await connection.WhenSendPathIdleAsync();

        // Assert
        first.ShouldBe(MqttSendResult.Taken);
        writes.ShouldBe(Added + 1);
        writesInsideTheCall.ShouldBe(16);
        connection.TakeWritten().ShouldBe(expected.ToArray());
        AssertIdle(harness, server);
    }

    /// <summary>
    /// Proves that a write that looked pending when it was started, but has completed by the time it is awaited,
    /// counts as one of its caller's writes: the send path carries on on the caller's stack only 16 times, then goes
    /// to the thread pool, so such writes cannot nest without a bound, and everything is still written once and in
    /// order. The link here reports every write as pending at the first look and as completed at the second.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Drain_WriteCompletedByTheTimeItIsAwaited_CountsAsOneOfTheCallersWrites()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        MemoryLink inner = new();
        LateLookingLink link = new(inner);
        using CancellationTokenSource requestAborted = new();
        DefaultHttpContext httpContext = new() { RequestAborted = requestAborted.Token };
        Task run = harness.Server.RunConnectionAsync(httpContext, link);
        TestConnection connection = new(harness, inner, run, requestAborted, httpContext);
        connection.Deliver(ServerPackets.Connect);
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAck);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        AsyncLocal<bool> insideTheCall = new() { Value = true };
        int writes = 0;
        int writesInsideTheCall = 0;
        List<byte> expected = [.. ServerPackets.Delivery([0])];
        for (int i = 1; i <= Added; i++)
        {
            expected.AddRange(ServerPackets.Delivery([(byte)i]));
        }

        inner.Writing = _ =>
        {
            // Runs under the send path, one write at a time. The first write adds the 40 messages behind itself.
            if (++writes == 1)
            {
                for (int i = 1; i <= Added; i++)
                {
                    server.TrySend(s_topic, new[] { (byte)i }, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
                }
            }

            if (insideTheCall.Value)
            {
                writesInsideTheCall++;
            }
        };

        // Act
        MqttSendResult first = server.TrySend(s_topic, new byte[1], MqttQoS.AtMostOnce);

        // The CONNACK, the first message and the 40 added.
        await inner.WhenWrittenAsync(count: Added + 2);
        await harness.Pool.WhenNoneOutstandingAsync();
        await connection.WhenSendPathIdleAsync();

        // Assert
        first.ShouldBe(MqttSendResult.Taken);
        writes.ShouldBe(Added + 1);
        writesInsideTheCall.ShouldBe(16);
        connection.TakeWritten().ShouldBe(expected.ToArray());
        AssertIdle(harness, server);
    }

    /// <summary>
    /// Asserts that a connection's send path is idle: no write in progress, nothing waiting, no bytes counted, and
    /// every buffer back in the pool.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <param name="server">
    /// The connection.
    /// </param>
    private static void AssertIdle(ServerHarness harness, MqttWireConnection server)
    {
        ConnectionCounts counts = server.GetCounts();
        counts.IsWriting.ShouldBeFalse();
        counts.WaitingDeliveries.ShouldBe(0);
        counts.WaitingControlPackets.ShouldBe(0);
        counts.OutgoingBytes.ShouldBe(0);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.AssertBounds();
    }

    /// <summary>
    /// A link over a <see cref="MemoryLink"/> whose writes are taken at once but look pending the first time their
    /// task is looked at, as a write does that completes on another thread between two looks.
    /// </summary>
    /// <param name="inner">
    /// The link every operation goes to.
    /// </param>
    private sealed class LateLookingLink(MemoryLink inner) : IMqttLink
    {
        /// <inheritdoc/>
        public ValueTask WaitForDataAsync(CancellationToken cancellationToken) =>
            inner.WaitForDataAsync(cancellationToken);

        /// <inheritdoc/>
        public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken) =>
            inner.ReadAsync(buffer, cancellationToken);

        /// <summary>
        /// Hands the packet to the inner link, which takes it at once, and returns a task that reports itself
        /// pending the first time it is looked at and completed from then on.
        /// </summary>
        /// <param name="packet">
        /// The packet to write.
        /// </param>
        /// <param name="cancellationToken">
        /// Passed to the inner link.
        /// </param>
        /// <returns>
        /// The task.
        /// </returns>
        public ValueTask WriteAsync(ReadOnlyMemory<byte> packet, CancellationToken cancellationToken)
        {
            ValueTask taken = inner.WriteAsync(packet, cancellationToken);
            taken.IsCompletedSuccessfully.ShouldBeTrue();
            return new ValueTask(new LateLookingWrite(), token: 0);
        }

        /// <inheritdoc/>
        public ValueTask CloseAsync(WebSocketCloseStatus status, CancellationToken cancellationToken) =>
            inner.CloseAsync(status, cancellationToken);

        /// <inheritdoc/>
        public void Abort() => inner.Abort();
    }

    /// <summary>
    /// The source of a write that has succeeded but reports itself pending the first time its status is asked for.
    /// </summary>
    private sealed class LateLookingWrite : IValueTaskSource
    {
        /// <summary>
        /// How many times the status was asked for.
        /// </summary>
        private int _looks;

        /// <summary>
        /// Tells the status: pending at the first look, succeeded afterwards.
        /// </summary>
        /// <param name="token">
        /// Unused.
        /// </param>
        /// <returns>
        /// The status.
        /// </returns>
        public ValueTaskSourceStatus GetStatus(short token) =>
            _looks++ == 0 ? ValueTaskSourceStatus.Pending : ValueTaskSourceStatus.Succeeded;

        /// <summary>
        /// Takes the result: nothing, the write succeeded.
        /// </summary>
        /// <param name="token">
        /// Unused.
        /// </param>
        public void GetResult(short token)
        {
            // The write succeeded and has no result.
        }

        /// <summary>
        /// Never called: the write is completed by the time it is awaited.
        /// </summary>
        /// <param name="continuation">
        /// Unused.
        /// </param>
        /// <param name="state">
        /// Unused.
        /// </param>
        /// <param name="token">
        /// Unused.
        /// </param>
        /// <param name="flags">
        /// Unused.
        /// </param>
        /// <exception cref="InvalidOperationException">
        /// Always: nothing waits for a write that has completed.
        /// </exception>
        public void OnCompleted(Action<object?> continuation, object? state, short token,
            ValueTaskSourceOnCompletedFlags flags) =>
            throw new InvalidOperationException("The write has completed: nothing waits for it.");
    }
}
