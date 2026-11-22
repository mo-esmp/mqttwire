using System.Buffers;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
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
    /// A pool over a <see cref="CountingPool"/> that throws on one chosen rent, as a pool may that cannot allocate.
    /// </summary>
    private sealed class ThrowingPool : ArrayPool<byte>
    {
        /// <summary>
        /// How many rents are left before the one that throws; 0 when none is to throw.
        /// </summary>
        private int _rentsLeft;

        /// <summary>
        /// Gets the pool every rent and return goes to, which counts what is out.
        /// </summary>
        public CountingPool Inner { get; } = new();

        /// <summary>
        /// Gets the exception the chosen rent throws.
        /// </summary>
        public InvalidOperationException Thrown { get; } = new("The pool cannot rent.");

        /// <summary>
        /// Gets a value indicating whether the chosen rent has thrown.
        /// </summary>
        public bool HasThrown { get; private set; }

        /// <summary>
        /// Chooses the rent that throws, counted from the next one; only that one throws.
        /// </summary>
        /// <param name="which">
        /// 1 for the next rent, 2 for the one after it, and so on.
        /// </param>
        public void ThrowOnRent(int which) => Volatile.Write(ref _rentsLeft, which);

        /// <summary>
        /// Rents from <see cref="Inner"/>, unless this is the chosen rent.
        /// </summary>
        /// <param name="minimumLength">
        /// The smallest length the buffer must have.
        /// </param>
        /// <returns>
        /// A buffer at least <paramref name="minimumLength"/> long.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// This is the chosen rent; nothing is rented.
        /// </exception>
        public override byte[] Rent(int minimumLength)
        {
            if (Volatile.Read(ref _rentsLeft) > 0 && Interlocked.Decrement(ref _rentsLeft) == 0)
            {
                HasThrown = true;
                throw Thrown;
            }

            return Inner.Rent(minimumLength);
        }

        /// <summary>
        /// Returns a buffer to <see cref="Inner"/>.
        /// </summary>
        /// <param name="array">
        /// The buffer.
        /// </param>
        /// <param name="clearArray">
        /// Whether the buffer is cleared.
        /// </param>
        public override void Return(byte[] array, bool clearArray = false) => Inner.Return(array, clearArray);
    }
}
