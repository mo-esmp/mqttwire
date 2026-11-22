using System.Buffers;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the send path against a fault inside MqttWire (design 6.6, 6.8): whatever throws while the send path is
/// held (an encoding, the pool, the link) ends the connection with <see cref="MqttCloseReason.ConnectionLost"/>,
/// aborts the link, drops what waits, returns every buffer once and gives the send path up, so that the run ends, the
/// slot is freed and nothing is left unobserved; the fault is logged with event 20.
/// </summary>
public sealed class SendHardeningTests
{
    /// <summary>
    /// The event id of <c>SendPathFaulted</c>.
    /// </summary>
    private const int SendPathFaultedEvent = 20;

    /// <summary>
    /// The topic every test sends to.
    /// </summary>
    private static readonly MqttTopic s_topic = MqttTopic.Create("a/b");

    /// <summary>
    /// What <c>GetCounts</c> reports for a connection that holds nothing and writes nothing.
    /// </summary>
    private static readonly ConnectionCounts s_idle = new(IncomingRecords: 0, DeliveryRecords: 0, IncomingInFlight: 0,
        HoldsRecords: false, WaitingDeliveries: 0,
        WaitingControlPackets: 0, OutgoingBytes: 0, IsWriting: false, HoldsReadBuffer: false);

    /// <summary>
    /// Proves that a message whose payload memory throws while it is encoded, by the caller that took the send path,
    /// is not taken (the host gets "not open", not the exception), and that the connection ends: the link is aborted,
    /// the run completes, <c>OnClosed</c> reports <see cref="MqttCloseReason.ConnectionLost"/> once, and the fault is
    /// logged as an error with event 20.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_EncodingThrows_ReturnsNotOpenAndEndsTheConnection()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        ThrowingMemory payload = new(length: 10);

        // Act
        MqttSendResult result = server.TrySend(s_topic, payload.Memory, MqttQoS.AtMostOnce);
        bool aborted = connection.Link.IsAborted;
        await connection.Run;

        // Assert
        result.ShouldBe(MqttSendResult.NotOpen);
        aborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        harness.Log.Entries.ShouldContain(entry => entry.Id.Id == SendPathFaultedEvent
                                                   && entry.Level == LogLevel.Error && entry.Exception == payload.Thrown);
        server.GetCounts().ShouldBe(s_idle);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a pool that throws when the buffer of a message is rented, after the caller took the send path,
    /// ends the connection and gives everything up: the run completes, the registry's slot is free, and the connection
    /// holds nothing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_PoolThrowsOnRent_EndsTheConnectionAndFreesItsSlot()
    {
        // Arrange
        ThrowingPool pool = new();
        ServerHarness harness = ServerHarness.Create(pool: pool);
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        pool.ThrowOnRent(which: 1);

        // Act
        MqttSendResult result = server.TrySend(s_topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce);
        await connection.Run;

        // Assert
        result.ShouldBe(MqttSendResult.NotOpen);
        pool.HasThrown.ShouldBeTrue();
        connection.Link.IsAborted.ShouldBeTrue();
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        server.GetCounts().ShouldBe(s_idle);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        pool.Inner.Outstanding.ShouldBe(0);
        pool.Inner.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a PINGREQ whose PINGRESP cannot be rented ends the connection: the run completes, <c>OnClosed</c>
    /// reports <see cref="MqttCloseReason.ConnectionLost"/>, and the fault is logged with event 20.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Answer_PoolThrowsOnRent_EndsTheConnection()
    {
        // Arrange
        ThrowingPool pool = new();
        ServerHarness harness = ServerHarness.Create(pool: pool);
        TestConnection connection = harness.Connect();

        // The first rent is the read buffer the PINGREQ is read into, the second the PINGRESP's.
        pool.ThrowOnRent(which: 2);

        // Act
        connection.Deliver(ClientPackets.PingReq);
        await connection.Run;

        // Assert
        pool.HasThrown.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        harness.Log.Entries.ShouldContain(entry => entry.Id.Id == SendPathFaultedEvent && entry.Exception == pool.Thrown);
        pool.Inner.Outstanding.ShouldBe(0);
        pool.Inner.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a delivery whose payload throws when the drain, carrying on after the write before it, encodes it,
    /// releases everything that waits: m1 is held, m2 (whose payload throws) and m3 wait; when m1's write ends the
    /// link is aborted, nothing waits, the run completes, and m3's payload is no longer referenced.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Drain_EncodingTheNextWaitingDeliveryThrows_ReleasesWhatWaits()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, new ThrowingMemory(length: 10).Memory, MqttQoS.AtMostOnce)
            .ShouldBe(MqttSendResult.Taken);
        WeakReference third = SendHeld(server);
        int waiting = server.GetCounts().WaitingDeliveries;

        // Act
        connection.CompleteHeldWrites();
        bool aborted = connection.Link.IsAborted;
        ConnectionCounts afterTheFault = server.GetCounts();
        await connection.Run;
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Assert
        waiting.ShouldBe(2);
        aborted.ShouldBeTrue();
        afterTheFault.ShouldBe(s_idle);
        third.IsAlive.ShouldBeFalse();
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery("m1"u8));
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a fault in the send path carried on by the completion of a later write is caught and logged, not
    /// left in a task nobody observes: m1 is held, m2 is held in its turn, and when m2's write completes the encoding
    /// of m3 throws; the fault is logged with event 20 and no unobserved task exception carries it.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Drain_ThrowsOnTheCompletionOfALaterWrite_IsObservedAndLogged()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        ThrowingMemory payload = new(length: 10);
        bool unobserved = false;
        EventHandler<UnobservedTaskExceptionEventArgs> watch = (_, e) =>
            unobserved |= e.Exception.Flatten().InnerExceptions.Contains(payload.Thrown);
        int writes = 0;
        connection.Link.Writing = _ =>
        {
            // The second message's write is held in its turn, so that the third is taken by its completion.
            if (++writes == 2)
            {
                connection.Link.HoldWrites();
            }
        };
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, payload.Memory, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        TaskScheduler.UnobservedTaskException += watch;
        try
        {
            // Act
            connection.CompleteHeldWrites();
            await connection.Run;
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= watch;
        }

        // Assert
        writes.ShouldBe(2);
        unobserved.ShouldBeFalse();
        harness.Log.Entries.ShouldContain(entry => entry.Id.Id == SendPathFaultedEvent
                                                   && entry.Level == LogLevel.Error && entry.Exception == payload.Thrown);
        connection.TakeWritten().ShouldBe([.. ServerPackets.Delivery("m1"u8), .. ServerPackets.Delivery("m2"u8)]);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a fault does not keep a host's stop waiting: once a connection that faulted has ended, the
    /// registry's wait for every connection to end has completed.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopLike_AfterAFault_WhenEmptyAsyncCompletes()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.TrySend(s_topic, new ThrowingMemory(length: 10).Memory, MqttQoS.AtMostOnce)
            .ShouldBe(MqttSendResult.NotOpen);

        // Act
        await connection.Run;
        Task empty = harness.Server.Registry.WhenEmptyAsync();

        // Assert
        empty.IsCompletedSuccessfully.ShouldBeTrue();
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a logging provider that throws while the fault is logged does not defeat the fault path: no
    /// exception reaches the host's <c>TrySend</c>, the link is aborted, the run completes, the slot is freed and
    /// nothing is held.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Fault_LoggerThrows_StillGivesThePathUpAndEndsTheConnection()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Log.ThrowForEventId = SendPathFaultedEvent;
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        MqttSendResult result = server.TrySend(s_topic, new ThrowingMemory(length: 10).Memory, MqttQoS.AtMostOnce);
        bool aborted = connection.Link.IsAborted;
        await connection.Run;

        // Assert
        result.ShouldBe(MqttSendResult.NotOpen);
        aborted.ShouldBeTrue();
        harness.Log.Entries.ShouldContain(entry => entry.Id.Id == SendPathFaultedEvent);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        server.GetCounts().ShouldBe(s_idle);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a logging provider that throws does not let the fault path take an open step: behind a held write
    /// a control packet and three deliveries wait, the second of which throws when it is encoded; when the held write
    /// ends, the PINGRESP and the first are written, the fault ends the connection with the third still waiting, and
    /// the path is given up through the Closing step, so nothing more is written and nothing is held.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Fault_LoggerThrowsWithSomethingWaiting_StillGivesThePathUpThroughTheClosingStep()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Log.ThrowForEventId = SendPathFaultedEvent;
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, new ThrowingMemory(length: 10).Memory, MqttQoS.AtMostOnce)
            .ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, "m4"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        connection.Deliver(ClientPackets.PingReq);
        ConnectionCounts before = server.GetCounts();

        // Act
        connection.CompleteHeldWrites();
        bool aborted = connection.Link.IsAborted;
        ConnectionCounts afterTheFault = server.GetCounts();
        await connection.Run;

        // Assert
        before.WaitingDeliveries.ShouldBe(3);
        before.WaitingControlPackets.ShouldBe(1);
        aborted.ShouldBeTrue();
        afterTheFault.ShouldBe(s_idle);
        connection.TakeWritten().ShouldBe(
            [.. ServerPackets.Delivery("m1"u8), .. ClientPackets.PingResp, .. ServerPackets.Delivery("m2"u8)]);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a QoS 1 message whose exchange record cannot be had, the records' pool throwing in the hold that
    /// would take the send path, is not taken (no exception reaches the host) and ends the connection: the path is not
    /// left taken, the run completes and the slot is freed.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_RecordsPoolThrowsOnGet_EndsTheConnectionAndFreesItsSlot()
    {
        // Arrange
        ThrowingRecordsPool records = new();
        ServerPools pools = new(
            new DefaultObjectPool<WaitingDeliveries>(new ServerPools.WaitingDeliveriesPolicy()), records);
        ServerHarness harness = ServerHarness.Create(pools: pools);
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        records.ThrowOnGet = true;

        // Act
        MqttSendResult result = server.TrySend(s_topic, "hi"u8.ToArray(), MqttQoS.AtLeastOnce);
        await connection.Run;

        // Assert
        result.ShouldBe(MqttSendResult.NotOpen);
        connection.Link.IsAborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        harness.Log.Entries.ShouldContain(entry => entry.Id.Id == SendPathFaultedEvent && entry.Exception == records.Thrown);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        server.GetCounts().ShouldBe(s_idle);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the records' pool throwing when the drain takes a record ends the connection like any fault of the
    /// send path: a QoS 1 delivery waits behind a held QoS 0 write while the connection holds no records object; when
    /// the write ends, the drain's attempt to take a record throws; the connection ends with
    /// <see cref="MqttCloseReason.ConnectionLost"/> and event 20, the waiting delivery's queue goes back to its pool
    /// once, the run completes, the slot is freed and nothing is held.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Drain_RecordsPoolThrowsOnGet_EndsTheConnectionAndDropsTheWaitingDeliveryOnce()
    {
        // Arrange
        ThrowingRecordsPool records = new();
        CountingObjectPool<WaitingDeliveries> waiting = new(new ServerPools().Waiting);
        ServerHarness harness = ServerHarness.Create(pools: new ServerPools(waiting, records));
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        harness.AssertBounds();
        server.TrySend(s_topic, "m2"u8.ToArray(), MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
        harness.AssertBounds();
        ConnectionCounts before = server.GetCounts();
        records.ThrowOnGet = true;

        // Act
        connection.CompleteHeldWrites();
        bool aborted = connection.Link.IsAborted;
        await connection.Run;

        // Assert
        before.WaitingDeliveries.ShouldBe(1);
        before.HoldsRecords.ShouldBeFalse();
        aborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery("m1"u8));
        harness.Log.Entries.ShouldContain(entry => entry.Id.Id == SendPathFaultedEvent && entry.Exception == records.Thrown);
        waiting.Outstanding.ShouldBe(0);
        waiting.Distinct.ShouldBe(1);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        server.GetCounts().ShouldBe(s_idle);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a pool that throws when the DISCONNECT of a host's close is rented, while the close holds the send
    /// path, throws nothing at the host and ends the connection: the link is aborted, the run completes, the reason
    /// the host gave is kept, and the fault is logged with event 20.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_PoolThrowsOnTheDisconnectRent_EndsTheConnection()
    {
        // Arrange
        ThrowingPool pool = new();
        ServerHarness harness = ServerHarness.Create(pool: pool);
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        pool.ThrowOnRent(which: 1);

        // Act
        server.Close(MqttReasonCode.AdministrativeAction);
        bool aborted = connection.Link.IsAborted;
        await connection.Run;

        // Assert
        pool.HasThrown.ShouldBeTrue();
        aborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.ClosedWith.ShouldBeNull();
        harness.Log.Entries.ShouldContain(entry => entry.Id.Id == SendPathFaultedEvent && entry.Exception == pool.Thrown);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
        server.GetCounts().ShouldBe(s_idle);
        pool.Inner.Outstanding.ShouldBe(0);
        pool.Inner.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a pool that throws when the CONNACK of an admitted connection is rented, while the admission's
    /// reservation holds the send path, ends the connection before any packet is written, and that the host is told
    /// once: one <c>OnClosed</c>, with <see cref="MqttCloseReason.ConnectionLost"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_PoolThrowsOnTheConnAckRent_EndsTheConnectionWithoutOnClosedTwice()
    {
        // Arrange
        ThrowingPool pool = new();
        ServerHarness harness = ServerHarness.Create(pool: pool);
        TestConnection connection = harness.Accept();

        // The first rent is the read buffer the CONNECT is read into, the second the CONNACK's.
        pool.ThrowOnRent(which: 2);

        // Act
        connection.Deliver(ServerPackets.Connect);
        await connection.Run;

        // Assert
        pool.HasThrown.ShouldBeTrue();
        connection.Link.IsAborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        harness.Handler.Calls.ShouldContain(static call => call.StartsWith("OnAdmit", StringComparison.Ordinal));
        harness.Log.Entries.ShouldContain(entry => entry.Id.Id == SendPathFaultedEvent && entry.Exception == pool.Thrown);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ConnectionLost"]);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        pool.Inner.Outstanding.ShouldBe(0);
        pool.Inner.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Sends a message of the test's own while a write is held, in a frame of its own so that no local of the test
    /// keeps its payload alive.
    /// </summary>
    /// <param name="server">
    /// The connection to send to.
    /// </param>
    /// <returns>
    /// A weak reference to the payload.
    /// </returns>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference SendHeld(MqttWireConnection server)
    {
        byte[] payload = "m3"u8.ToArray();
        server.TrySend(s_topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        return new WeakReference(payload);
    }

    /// <summary>
    /// Gets the calls of <c>OnClosed</c> the handler recorded, oldest first.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <returns>
    /// The calls as text.
    /// </returns>
    private static List<string> ClosedCalls(ServerHarness harness) =>
        [.. harness.Handler.Calls.Where(static call => call.StartsWith("OnClosed", StringComparison.Ordinal))];

    /// <summary>
    /// The memory of a payload whose bytes cannot be read: its length is known, but every look at its span throws,
    /// as host memory may whose owner has gone.
    /// </summary>
    /// <param name="length">
    /// The payload's length.
    /// </param>
    private sealed class ThrowingMemory(int length) : MemoryManager<byte>
    {
        /// <summary>
        /// Gets the exception every look at the span throws, so a test can find it where it was logged.
        /// </summary>
        public InvalidOperationException Thrown { get; } = new("The payload's memory is gone.");

        /// <summary>
        /// Gets the payload's memory, without looking at its span.
        /// </summary>
        public override Memory<byte> Memory => CreateMemory(length);

        /// <summary>
        /// Throws: the bytes cannot be read.
        /// </summary>
        /// <returns>
        /// Never returns.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Always.
        /// </exception>
        public override Span<byte> GetSpan() => throw Thrown;

        /// <summary>
        /// Throws: the bytes cannot be pinned.
        /// </summary>
        /// <param name="elementIndex">
        /// Unused.
        /// </param>
        /// <returns>
        /// Never returns.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Always.
        /// </exception>
        public override MemoryHandle Pin(int elementIndex = 0) => throw Thrown;

        /// <summary>
        /// Does nothing: nothing is ever pinned.
        /// </summary>
        public override void Unpin()
        {
            // Nothing is ever pinned.
        }

        /// <summary>
        /// Does nothing: the manager holds nothing to release.
        /// </summary>
        /// <param name="disposing">
        /// Unused.
        /// </param>
        protected override void Dispose(bool disposing)
        {
            // Nothing to release.
        }
    }

    /// <summary>
    /// A pool of exchange records that throws on <see cref="Get"/> once told to, as a pool may that cannot allocate.
    /// </summary>
    private sealed class ThrowingRecordsPool : ObjectPool<ExchangeRecords>
    {
        /// <summary>
        /// Gets the exception <see cref="Get"/> throws.
        /// </summary>
        public InvalidOperationException Thrown { get; } = new("The records cannot be had.");

        /// <summary>
        /// Gets or sets a value indicating whether <see cref="Get"/> throws.
        /// </summary>
        public bool ThrowOnGet { get; set; }

        /// <summary>
        /// Hands out new records, unless told to throw.
        /// </summary>
        /// <returns>
        /// Records that hold none.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// <see cref="ThrowOnGet"/> is set.
        /// </exception>
        public override ExchangeRecords Get() => ThrowOnGet ? throw Thrown : new ExchangeRecords();

        /// <summary>
        /// Takes records back and drops them, cleared.
        /// </summary>
        /// <param name="obj">
        /// The records.
        /// </param>
        public override void Return(ExchangeRecords obj) => obj.Clear();
    }
}
