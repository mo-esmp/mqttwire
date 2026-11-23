using System.Net.WebSockets;
using System.Threading.Tasks.Sources;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using MqttWire.Link;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the pooled object that waits for a write that did not complete at once (design 6.6, "Sending"): it is
/// taken only while a write is pending, it is back in the server's pool on every way out, never twice, and the next
/// pending write reuses it; a pool that cannot hand one out costs that write an allocation and nothing else; a write
/// that completes while its continuation is being registered counts as one of its caller's writes; and a link that
/// refuses the registration ends the connection as a fault instead of throwing on the thread that completes writes.
/// </summary>
public sealed class PendingWriteTests
{
    /// <summary>
    /// The event id of <c>SendPathFaulted</c>.
    /// </summary>
    private const int SendPathFaultedEvent = 20;

    /// <summary>
    /// The number of messages the first write adds behind itself in the test of the writes that nest.
    /// </summary>
    private const int Added = 40;

    /// <summary>
    /// Proves that the pooled object is out of the pool exactly while a write is pending and back when the write has
    /// succeeded, and that one object serves every pending write of a drain: three messages over a link that leaves
    /// every write pending, each write ended in its turn.
    /// </summary>
    [Fact]
    public void Write_PendingThenSucceeds_ReturnsTheObjectAndTheNextPendingWriteReusesIt()
    {
        // Arrange
        (ServerHarness harness, CountingObjectPool<PendingWrite> pending) = CreateHarness();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        int outBeforeAnyWrite = pending.Outstanding;
        int writes = 0;
        connection.Link.Writing = _ => writes++;
        connection.Link.PendWrites();
        List<int> outWhilePending = [];

        // Act
        server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(ServerHarness.Topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(ServerHarness.Topic, "m3"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        for (int i = 0; i < 3; i++)
        {
            outWhilePending.Add(pending.Outstanding);
            connection.Link.CompletePendingWrite().ShouldBeTrue();
        }

        // Assert
        outBeforeAnyWrite.ShouldBe(0);
        writes.ShouldBe(3);
        outWhilePending.ShouldBe([1, 1, 1]);
        pending.Outstanding.ShouldBe(0);
        pending.Distinct.ShouldBe(1);
        pending.LastHandedOut.ShouldNotBeNull().IsIdle.ShouldBeTrue();
        connection.TakeWritten().ShouldBe(
            [.. ServerPackets.Delivery("m1"u8), .. ServerPackets.Delivery("m2"u8), .. ServerPackets.Delivery("m3"u8)]);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Log.Entries.ShouldNotContain(static entry => entry.Id.Id == SendPathFaultedEvent);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a write that completes at once takes no object from the pool at all.
    /// </summary>
    [Fact]
    public void Write_CompletesAtOnce_TakesNothingFromThePool()
    {
        // Arrange
        (ServerHarness harness, CountingObjectPool<PendingWrite> pending) = CreateHarness();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        for (int i = 0; i < 100; i++)
        {
            server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        }

        // Assert
        pending.Distinct.ShouldBe(0);
        pending.Outstanding.ShouldBe(0);
        connection.Link.WriteCount.ShouldBe(101);
    }

    /// <summary>
    /// Proves that the pooled object is back, once, when the pending write fails: the connection ends as a lost one,
    /// its buffer is returned once, and nothing is logged as a fault.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Write_PendingThenFails_ReturnsTheObjectOnce()
    {
        // Arrange
        (ServerHarness harness, CountingObjectPool<PendingWrite> pending) = CreateHarness();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(ServerHarness.Topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        int outWhilePending = pending.Outstanding;

        // Act
        connection.CompleteHeldWrites(new IOException("The socket failed."));
        await connection.RunEnded();

        // Assert
        outWhilePending.ShouldBe(1);
        pending.Outstanding.ShouldBe(0);
        pending.Distinct.ShouldBe(1);
        connection.TakeWritten().ShouldBeEmpty();
        harness.ClosedCalls().ShouldBe(["OnClosed c1 ConnectionLost"]);
        harness.Log.Entries.ShouldNotContain(static entry => entry.Id.Id == SendPathFaultedEvent);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that aborting a connection while its write is pending releases nothing by itself: the pooled object,
    /// like the write's buffer, stays out until the write has ended, and is then back, once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Write_PendingWhenTheHostClosesTheConnection_ReturnsTheObjectWhenTheWriteHasEnded()
    {
        // Arrange
        (ServerHarness harness, CountingObjectPool<PendingWrite> pending) = CreateHarness();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);

        // Act
        server.Close(MqttReasonCode.AdministrativeAction);
        bool aborted = connection.Link.IsAborted;
        int outAfterTheAbort = pending.Outstanding;
        int buffersAfterTheAbort = harness.Pool.Outstanding;
        bool endedBeforeTheWrite = connection.Run.IsCompleted;
        connection.CompleteHeldWrites(new OperationCanceledException());
        await connection.RunEnded();

        // Assert
        aborted.ShouldBeTrue();
        outAfterTheAbort.ShouldBe(1);
        buffersAfterTheAbort.ShouldBe(1);
        endedBeforeTheWrite.ShouldBeFalse();
        pending.Outstanding.ShouldBe(0);
        pending.Distinct.ShouldBe(1);
        harness.ClosedCalls().ShouldBe(["OnClosed c1 ClosedByHost"]);
        harness.Log.Entries.ShouldNotContain(static entry => entry.Id.Id == SendPathFaultedEvent);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the server's stop, with a write pending on each of three connections, ends with every pooled
    /// object back, once: each is out until its connection's write has ended, and the stop waits for that.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Write_PendingWhenTheServerStops_ReturnsEveryObjectBeforeTheStopCompletes()
    {
        // Arrange
        (ServerHarness harness, CountingObjectPool<PendingWrite> pending) = CreateHarness();
        TestConnection[] connections = [harness.Connect("c1"), harness.Connect("c2"), harness.Connect("c3")];
        foreach (TestConnection connection in connections)
        {
            connection.Link.HoldWrites();
            connection.Connection.ShouldNotBeNull().TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtLeastOnce)
                .ShouldBe(MqttSendResult.Taken);
        }

        int outWhilePending = pending.Outstanding;

        // Act
        Task stop = harness.Server.StopAsync();
        int outAfterTheStopBegan = pending.Outstanding;
        bool stoppedBeforeTheWrites = stop.IsCompleted;
        foreach (TestConnection connection in connections)
        {
            connection.CompleteHeldWrites(new OperationCanceledException());
        }

        await stop.Within("The server's stop");

        // Assert
        outWhilePending.ShouldBe(3);
        outAfterTheStopBegan.ShouldBe(3);
        stoppedBeforeTheWrites.ShouldBeFalse();
        pending.Outstanding.ShouldBe(0);
        pending.Distinct.ShouldBe(3);
        connections.ShouldAllBe(static connection => connection.Link.IsAborted && connection.Run.IsCompleted);
        harness.ClosedCalls().Count.ShouldBe(3);
        harness.Log.Entries.ShouldNotContain(static entry => entry.Id.Id == SendPathFaultedEvent);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves the fall-back: when the pool cannot hand out an object for a write that is already with the link, that
    /// write is awaited all the same, its buffer goes back exactly once after it has ended, what waited behind it is
    /// written, and the connection goes on; once the pool works again, the next pending write uses it.
    /// </summary>
    [Fact]
    public void Write_PendingAndThePoolThrowsOnGet_IsStillAwaitedAndItsBufferReturnedOnce()
    {
        // Arrange
        ThrowingPendingWritePool throwing = new() { ThrowOnGet = true };
        ServerPools defaults = new();
        ServerHarness harness =
            ServerHarness.Create(pools: new ServerPools(defaults.Waiting, defaults.Records, throwing));
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();

        // Act
        MqttSendResult first = server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
        MqttSendResult second = server.TrySend(ServerHarness.Topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce);
        int buffersWhilePending = harness.Pool.Outstanding;
        ConnectionCounts whilePending = server.GetCounts();
        connection.CompleteHeldWrites();
        byte[] written = connection.TakeWritten();
        int buffersAfterwards = harness.Pool.Outstanding;
        int getsThatThrew = throwing.Throws;
        throwing.ThrowOnGet = false;
        connection.Link.HoldWrites();
        MqttSendResult third = server.TrySend(ServerHarness.Topic, "m3"u8.ToArray(), MqttQoS.AtMostOnce);
        int handedOutForTheThird = throwing.HandedOut;
        connection.CompleteHeldWrites();

        // Assert
        first.ShouldBe(MqttSendResult.Taken);
        second.ShouldBe(MqttSendResult.Taken);
        buffersWhilePending.ShouldBe(1);
        whilePending.IsWriting.ShouldBeTrue();
        whilePending.WaitingDeliveries.ShouldBe(1);
        written.ShouldBe([.. ServerPackets.Delivery("m1"u8), .. ServerPackets.Delivery("m2"u8)]);
        buffersAfterwards.ShouldBe(0);
        getsThatThrew.ShouldBe(1);
        third.ShouldBe(MqttSendResult.Taken);
        handedOutForTheThird.ShouldBe(1);
        throwing.Returned.ShouldBe(1);
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery("m3"u8));
        connection.Link.IsAborted.ShouldBeFalse();
        server.StateNow.ShouldBe(ConnectionState.Active);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Log.Entries.ShouldNotContain(static entry => entry.Id.Id == SendPathFaultedEvent);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a pool that refuses the object when the write has ended does not throw on the thread that
    /// completed the write and leaves nothing taken: the connection ends as a fault, logged with event 20, the
    /// write's buffer goes back once, the run ends and the slot is freed.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Write_PendingAndThePoolThrowsOnReturn_EndsTheConnectionAsAFaultAndGivesThePathUp()
    {
        // Arrange
        ThrowingPendingWritePool throwing = new() { ThrowOnReturn = true };
        ServerPools defaults = new();
        ServerHarness harness =
            ServerHarness.Create(pools: new ServerPools(defaults.Waiting, defaults.Records, throwing));
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(ServerHarness.Topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(ServerHarness.Topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);

        // Act
        Exception? thrownAtTheCompleter = Record.Exception(() => connection.CompleteHeldWrites());
        bool aborted = connection.Link.IsAborted;
        await connection.RunEnded();

        // Assert
        thrownAtTheCompleter.ShouldBeNull();
        aborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery("m1"u8));
        harness.Log.Entries.ShouldContain(entry => entry.Id.Id == SendPathFaultedEvent
                                                   && entry.Level == LogLevel.Error
                                                   && entry.Exception == throwing.Thrown);
        harness.ClosedCalls().ShouldBe(["OnClosed c1 ConnectionLost"]);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a write that looks pending both times it is looked at, and completes while its continuation is
    /// being registered, so that the continuation runs inside the registration on the caller's stack, counts as one
    /// of its caller's writes: the send path carries on on that stack only 16 times, then goes to the thread pool, so
    /// such writes cannot nest without a bound, and everything is still written once and in order with every pooled
    /// object back.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Write_CompletesWhileItsContinuationIsRegistered_CountsAsOneOfTheCallersWrites()
    {
        // Arrange
        (ServerHarness harness, CountingObjectPool<PendingWrite> pending) = CreateHarness();
        MemoryLink inner = new();
        SourceLink link = new(inner, static () => new CompletingInsideTheRegistration());
        using CancellationTokenSource requestAborted = new();
        DefaultHttpContext httpContext = new() { RequestAborted = requestAborted.Token };
        Task run = harness.Server.RunConnectionAsync(httpContext, link);
        TestConnection connection = new(harness, inner, run, requestAborted, httpContext);
        connection.Deliver(ServerPackets.Connect);
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAck);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Set in the test's own call and nowhere else: the thread pool runs what is handed to it without the caller's
        // execution context, so there the value is false.
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
                    server.TrySend(ServerHarness.Topic, new[] { (byte)i }, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
                }
            }

            if (insideTheCall.Value)
            {
                writesInsideTheCall++;
            }
        };

        // Act
        MqttSendResult first = server.TrySend(ServerHarness.Topic, new byte[1], MqttQoS.AtMostOnce);

        // The CONNACK, the first message and the 40 added.
        await inner.WhenWrittenAsync(count: Added + 2);
        await harness.Pool.WhenNoneOutstandingAsync();
        await connection.WhenSendPathIdleAsync();

        // Assert
        first.ShouldBe(MqttSendResult.Taken);
        writes.ShouldBe(Added + 1);
        writesInsideTheCall.ShouldBe(16);
        connection.TakeWritten().ShouldBe(expected.ToArray());
        pending.Outstanding.ShouldBe(0);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Log.Entries.ShouldNotContain(static entry => entry.Id.Id == SendPathFaultedEvent);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a link whose write refuses the registration of a continuation does not crash the thread that
    /// writes: the connection ends as a fault, logged with event 20, the send path is given up, so the run ends and
    /// the slot is freed, and the write's buffer is returned by nobody on the write's behalf; when the write ends
    /// after all, its own completion returns the buffer and the pooled object, once each.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Write_RefusesTheRegistration_EndsTheConnectionAsAFaultAndReturnsTheBufferOnlyWhenTheWriteEnds()
    {
        // Arrange
        (ServerHarness harness, CountingObjectPool<PendingWrite> pending) = CreateHarness();
        MemoryLink inner = new();
        RefusingTheRegistration? refusing = null;
        SourceLink link = new(inner, () => refusing = new RefusingTheRegistration());
        using CancellationTokenSource requestAborted = new();
        DefaultHttpContext httpContext = new() { RequestAborted = requestAborted.Token };
        Task run = harness.Server.RunConnectionAsync(httpContext, link);
        TestConnection connection = new(harness, inner, run, requestAborted, httpContext);
        inner.Deliver(ServerPackets.Connect);

        // The CONNACK is the first write: it is the one that refuses.
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        await connection.RunEnded();
        bool aborted = inner.IsAborted;
        int buffersAfterTheFault = harness.Pool.Outstanding;
        int objectsAfterTheFault = pending.Outstanding;
        Exception? thrownAtTheCompleter = Record.Exception(() => refusing.ShouldNotBeNull().Complete());

        // Assert
        aborted.ShouldBeTrue();
        harness.Log.Entries.ShouldContain(entry => entry.Id.Id == SendPathFaultedEvent
                                                   && entry.Level == LogLevel.Error
                                                   && entry.Exception == refusing.ShouldNotBeNull().Thrown);
        harness.ClosedCalls().ShouldBe(["OnClosed c1 ConnectionLost"]);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        buffersAfterTheFault.ShouldBe(1);
        objectsAfterTheFault.ShouldBe(1);
        thrownAtTheCompleter.ShouldBeNull();
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
        pending.Outstanding.ShouldBe(0);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
    }

    /// <summary>
    /// Creates a harness whose server takes its pending-write objects from a counting pool over the server's own.
    /// </summary>
    /// <returns>
    /// The harness and the counting pool.
    /// </returns>
    private static (ServerHarness Harness, CountingObjectPool<PendingWrite> Pending) CreateHarness()
    {
        ServerPools defaults = new();
        CountingObjectPool<PendingWrite> pending = new(defaults.PendingWrites);
        ServerHarness harness =
            ServerHarness.Create(pools: new ServerPools(defaults.Waiting, defaults.Records, pending));
        return (harness, pending);
    }

    /// <summary>
    /// A pool of pending-write objects that throws on <see cref="Get"/> or on <see cref="Return"/> when told to, as a
    /// pool may that cannot allocate or that refuses an object, and counts what it does.
    /// </summary>
    private sealed class ThrowingPendingWritePool : ObjectPool<PendingWrite>
    {
        /// <summary>
        /// Gets the exception <see cref="Get"/> and <see cref="Return"/> throw.
        /// </summary>
        public InvalidOperationException Thrown { get; } = new("The pool of pending writes failed.");

        /// <summary>
        /// Gets or sets a value indicating whether <see cref="Get"/> throws.
        /// </summary>
        public bool ThrowOnGet { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether <see cref="Return"/> throws.
        /// </summary>
        public bool ThrowOnReturn { get; set; }

        /// <summary>
        /// Gets the number of calls of <see cref="Get"/> that threw.
        /// </summary>
        public int Throws { get; private set; }

        /// <summary>
        /// Gets the number of objects handed out.
        /// </summary>
        public int HandedOut { get; private set; }

        /// <summary>
        /// Gets the number of objects taken back.
        /// </summary>
        public int Returned { get; private set; }

        /// <summary>
        /// Hands out a new object, unless told to throw.
        /// </summary>
        /// <returns>
        /// An object that awaits nothing.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// <see cref="ThrowOnGet"/> is set.
        /// </exception>
        public override PendingWrite Get()
        {
            if (ThrowOnGet)
            {
                Throws++;
                throw Thrown;
            }

            HandedOut++;
            return new PendingWrite();
        }

        /// <summary>
        /// Takes an object back and drops it, unless told to throw.
        /// </summary>
        /// <param name="obj">
        /// The object.
        /// </param>
        /// <exception cref="InvalidOperationException">
        /// <see cref="ThrowOnReturn"/> is set; the object is not taken back.
        /// </exception>
        public override void Return(PendingWrite obj)
        {
            if (ThrowOnReturn)
            {
                throw Thrown;
            }

            Returned++;
        }
    }

    /// <summary>
    /// A link over a <see cref="MemoryLink"/> whose writes are taken at once by the inner link and reported through a
    /// source of the test's own, one for each write.
    /// </summary>
    /// <param name="inner">
    /// The link every operation goes to.
    /// </param>
    /// <param name="createSource">
    /// Creates the source of one write.
    /// </param>
    private sealed class SourceLink(MemoryLink inner, Func<IValueTaskSource> createSource) : IMqttLink
    {
        /// <inheritdoc/>
        public ValueTask WaitForDataAsync(CancellationToken cancellationToken) =>
            inner.WaitForDataAsync(cancellationToken);

        /// <inheritdoc/>
        public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken) =>
            inner.ReadAsync(buffer, cancellationToken);

        /// <summary>
        /// Hands the packet to the inner link, which takes it at once, and returns a task over a new source.
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
            return taken.IsCompletedSuccessfully ? new ValueTask(createSource(), token: 0) : taken;
        }

        /// <inheritdoc/>
        public ValueTask CloseAsync(WebSocketCloseStatus status, CancellationToken cancellationToken) =>
            inner.CloseAsync(status, cancellationToken);

        /// <inheritdoc/>
        public void Abort() => inner.Abort();
    }

    /// <summary>
    /// The source of a write that has succeeded but reports itself pending until a continuation is registered, and
    /// runs that continuation inside the registration: a write that completes at the very moment it is awaited, on a
    /// source that does not defer.
    /// </summary>
    private sealed class CompletingInsideTheRegistration : IValueTaskSource
    {
        /// <summary>
        /// Whether a continuation was registered, from when on the write reports itself completed.
        /// </summary>
        private bool _registered;

        /// <summary>
        /// Tells the status: pending until a continuation is registered, succeeded afterwards.
        /// </summary>
        /// <param name="token">
        /// Unused.
        /// </param>
        /// <returns>
        /// The status.
        /// </returns>
        public ValueTaskSourceStatus GetStatus(short token) =>
            _registered ? ValueTaskSourceStatus.Succeeded : ValueTaskSourceStatus.Pending;

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
        /// Completes the write and runs the continuation at once, on the registering thread.
        /// </summary>
        /// <param name="continuation">
        /// What awaits the write.
        /// </param>
        /// <param name="state">
        /// Its state.
        /// </param>
        /// <param name="token">
        /// Unused.
        /// </param>
        /// <param name="flags">
        /// Unused.
        /// </param>
        public void OnCompleted(Action<object?> continuation, object? state, short token,
            ValueTaskSourceOnCompletedFlags flags)
        {
            _registered = true;
            continuation(state);
        }
    }

    /// <summary>
    /// The source of a write that stays pending, keeps the continuation it is given and then throws out of the
    /// registration, as a link may that is broken; the test ends the write later.
    /// </summary>
    private sealed class RefusingTheRegistration : IValueTaskSource
    {
        /// <summary>
        /// What awaits the write, kept although the registration threw.
        /// </summary>
        private Action<object?>? _continuation;

        /// <summary>
        /// The state of <see cref="_continuation"/>.
        /// </summary>
        private object? _state;

        /// <summary>
        /// Whether the test has ended the write.
        /// </summary>
        private bool _completed;

        /// <summary>
        /// Gets the exception the registration throws.
        /// </summary>
        public InvalidOperationException Thrown { get; } = new("The write takes no continuation.");

        /// <summary>
        /// Ends the write successfully and runs what was registered, on the calling thread.
        /// </summary>
        public void Complete()
        {
            _completed = true;
            _continuation?.Invoke(_state);
        }

        /// <summary>
        /// Tells the status: pending until the test ends the write.
        /// </summary>
        /// <param name="token">
        /// Unused.
        /// </param>
        /// <returns>
        /// The status.
        /// </returns>
        public ValueTaskSourceStatus GetStatus(short token) =>
            _completed ? ValueTaskSourceStatus.Succeeded : ValueTaskSourceStatus.Pending;

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
        /// Keeps the continuation, then throws.
        /// </summary>
        /// <param name="continuation">
        /// What awaits the write.
        /// </param>
        /// <param name="state">
        /// Its state.
        /// </param>
        /// <param name="token">
        /// Unused.
        /// </param>
        /// <param name="flags">
        /// Unused.
        /// </param>
        /// <exception cref="InvalidOperationException">
        /// Always.
        /// </exception>
        public void OnCompleted(Action<object?> continuation, object? state, short token,
            ValueTaskSourceOnCompletedFlags flags)
        {
            _continuation = continuation;
            _state = state;
            throw Thrown;
        }
    }
}
