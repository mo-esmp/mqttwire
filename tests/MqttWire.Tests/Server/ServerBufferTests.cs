using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Extensions.ObjectPool;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of who owns the server's buffers (design 6.8, 6.13 items 4 and 5): every way out of a connection returns
/// every buffer exactly once; a buffer handed to a read or a write stays the operation's until it has ended, also
/// when the link is aborted meanwhile and the memory a pool overwrites and hands out again at once is rented by
/// another connection; an abort at the very moment a write completes returns nothing twice; a closed connection keeps
/// no reference to a host's payload; and connect, work and disconnect cycles leave nothing behind. The bounds are
/// asserted after every step, not only at the end.
/// </summary>
/// <remarks>
/// The class is in a collection whose definition disables parallelization, because
/// <see cref="Cycles_ThousandConnectWorkDisconnect_AccumulateNothing"/> reads the process's managed heap: xUnit runs it
/// only once every parallel collection of the assembly has finished, so no other test allocates meanwhile.
/// </remarks>
[Collection(nameof(ServerBufferTests))]
[CollectionDefinition(nameof(ServerBufferTests), DisableParallelization = true)]
public sealed class ServerBufferTests
{
    /// <summary>
    /// How many times the race of an abort and a write's completion is run on two threads.
    /// </summary>
    private const int RaceRounds = 500;

    /// <summary>
    /// How many different delays of the close the race cycles through.
    /// </summary>
    private const int RaceDelaySteps = 25;

    /// <summary>
    /// The spin iterations one step of the close's delay adds.
    /// </summary>
    private const int RaceDelayStep = 8;

    /// <summary>
    /// How many connect, work and disconnect cycles the accumulation test runs.
    /// </summary>
    private const int Cycles = 1_000;

    /// <summary>
    /// The most the managed heap may differ after the cycles from before them, in bytes.
    /// </summary>
    private const long MaxHeapDrift = 65_536;

    /// <summary>
    /// The byte the tests fill the arrays they rent themselves with, so that a write into them shows.
    /// </summary>
    private const byte Mine = 0x11;

    /// <summary>
    /// The topic every test sends to.
    /// </summary>
    private static readonly MqttTopic s_topic = MqttTopic.Create("a/b");

    /// <summary>
    /// The payload of the messages the tests send, "hi".
    /// </summary>
    private static readonly byte[] s_hi = "hi"u8.ToArray();

    /// <summary>
    /// The first four bytes of a QoS 0 PUBLISH: its fixed header and the start of its topic, which the read loop keeps
    /// in its buffer, with a read pending, until the rest comes.
    /// </summary>
    private static readonly byte[] s_publishStart = ClientPackets.PublishQoS0[..4];

    /// <summary>
    /// What <c>GetCounts</c> reports for a connection that holds nothing and writes nothing.
    /// </summary>
    private static readonly ConnectionCounts s_idle = new(IncomingRecords: 0, DeliveryRecords: 0, IncomingInFlight: 0,
        HoldsRecords: false, WaitingDeliveries: 0, WaitingControlPackets: 0, OutgoingBytes: 0, IsWriting: false,
        HoldsReadBuffer: false);

    /// <summary>
    /// A way a connection ends (design 6.8 rule 5: normal completion, a refusal, an overflow, a deadline, an
    /// exception, an abort).
    /// </summary>
    public enum WayOut
    {
        /// <summary>
        /// The client sends DISCONNECT.
        /// </summary>
        NormalDisconnect,

        /// <summary>
        /// MqttWire refuses the CONNECT itself (protocol version 4).
        /// </summary>
        RefusedConnect,

        /// <summary>
        /// The host's <c>OnConnectAsync</c> refuses the connection.
        /// </summary>
        HostRefusal,

        /// <summary>
        /// The host's <c>OnConnectAsync</c> does not answer within the authentication deadline.
        /// </summary>
        AuthenticationTimeout,

        /// <summary>
        /// Half a CONNECT arrives, and the rest never does within the connect deadline.
        /// </summary>
        ConnectTimeout,

        /// <summary>
        /// Half a PUBLISH arrives, and the rest never does within the partial-packet deadline.
        /// </summary>
        PartialPacketTimeout,

        /// <summary>
        /// No packet comes for one and a half keep-alive intervals.
        /// </summary>
        KeepAliveTimeout,

        /// <summary>
        /// A malformed PINGREQ arrives.
        /// </summary>
        MalformedPacket,

        /// <summary>
        /// A PUBLISH in the skip band is refused, then one above it ends the connection with 0x95.
        /// </summary>
        PublishAboveTheSkipBand,

        /// <summary>
        /// With a write held, a control packet and two deliveries waiting, one more delivery evicts the connection.
        /// </summary>
        EvictionWithAHeldWrite,

        /// <summary>
        /// The host sends a message larger than <see cref="MqttWireServerOptions.MaxOutgoingBytes"/>.
        /// </summary>
        TooLarge,

        /// <summary>
        /// A second connection takes the client id over.
        /// </summary>
        Takeover,

        /// <summary>
        /// The host's <c>OnPublish</c> throws.
        /// </summary>
        HandlerThrows,

        /// <summary>
        /// A write fails.
        /// </summary>
        WriteFails,

        /// <summary>
        /// A read fails with half a packet in the read buffer.
        /// </summary>
        ReadFails,

        /// <summary>
        /// A write stays with the link past the write deadline.
        /// </summary>
        WriteDeadline,

        /// <summary>
        /// The host stops the server, with half a packet in the read buffer.
        /// </summary>
        ServerStop,

        /// <summary>
        /// The pool throws on the rent of a message's buffer, a fault of the send path.
        /// </summary>
        SendPathFault,

        /// <summary>
        /// The success CONNACK cannot be encoded: the host changed its user properties after accepting.
        /// </summary>
        ConnAckCannotBeEncoded,

        /// <summary>
        /// A connection takes its slot after the server's stop began, and aborts itself.
        /// </summary>
        RegisteredAfterTheStopBegan,
    }

    /// <summary>
    /// Proves that each way out of a connection returns every buffer it rented exactly once (design 6.8 rule 5): once
    /// every run has ended no buffer is out, none was returned twice or returned unrented, every slot is free, and the
    /// host was told of each admitted connection's end once, with its reason. Before a way out of an Active connection,
    /// publishes at QoS 1 and 2 from the client and a QoS 1 delivery have gone through.
    /// </summary>
    /// <param name="wayOut">
    /// How the connection ends.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(WayOut.NormalDisconnect)]
    [InlineData(WayOut.RefusedConnect)]
    [InlineData(WayOut.HostRefusal)]
    [InlineData(WayOut.AuthenticationTimeout)]
    [InlineData(WayOut.ConnectTimeout)]
    [InlineData(WayOut.PartialPacketTimeout)]
    [InlineData(WayOut.KeepAliveTimeout)]
    [InlineData(WayOut.MalformedPacket)]
    [InlineData(WayOut.PublishAboveTheSkipBand)]
    [InlineData(WayOut.EvictionWithAHeldWrite)]
    [InlineData(WayOut.TooLarge)]
    [InlineData(WayOut.Takeover)]
    [InlineData(WayOut.HandlerThrows)]
    [InlineData(WayOut.WriteFails)]
    [InlineData(WayOut.ReadFails)]
    [InlineData(WayOut.WriteDeadline)]
    [InlineData(WayOut.ServerStop)]
    [InlineData(WayOut.SendPathFault)]
    [InlineData(WayOut.ConnAckCannotBeEncoded)]
    [InlineData(WayOut.RegisteredAfterTheStopBegan)]
    public async Task WayOut_Each_ReturnsEveryBufferExactlyOnce(WayOut wayOut)
    {
        // Arrange
        // It counts through its inner pool, which is the harness's Pool, and throws only when a row tells it to.
        ThrowingPool pool = new();
        ServerHarness harness = ServerHarness.Create(options => Configure(options, wayOut), pool);

        // Act
        TestConnection[] ended = await TakeWayOutAsync(harness, pool, wayOut);
        foreach (TestConnection connection in ended)
        {
            await connection.RunEnded();
        }

        // Assert
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        ClosedCalls(harness).ShouldBe(ExpectedClosed(wayOut));
        AssertTheWayOutWasTaken(wayOut, ended);
    }

    /// <summary>
    /// Proves that the buffer of a read still in flight is never handed to anyone else while the link is aborted under
    /// it: with a pool that overwrites every returned array and hands it out again at once, a read pending when the
    /// close deadline aborts the link keeps its buffer out through the abort and through the rents of another
    /// connection that connects and works meanwhile; when data then completes the read, it lands in the read's own
    /// buffer and changes no array anyone else holds; and only then is the read's buffer returned, once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_ReadHeldOpen_TheReadNeverSeesReusedMemory()
    {
        // Arrange
        OverwritingPool pool = new();
        ServerHarness harness = ServerHarness.Create(pool: pool);
        TestConnection first = harness.Connect("c1");
        MqttWireConnection server = first.Connection.ShouldNotBeNull();
        first.Link.SkipWaits();
        first.Link.HoldReadsOnAbort();

        // The PINGREQ is handled, then the loop's next read stays pending with a buffer of its own.
        first.Deliver(ClientPackets.PingReq);
        first.TakeWritten().ShouldBe(ClientPackets.PingResp);
        Memory<byte> pending = first.Link.PendingReadBuffer;

        // Act
        server.Close(MqttReasonCode.AdministrativeAction);
        harness.Advance(ServerLimits.CloseDeadline);
        bool aborted = first.Link.IsAborted;
        bool outAfterTheAbort = pool.IsRented(pending);
        TestConnection second = harness.Connect("c2");
        second.Deliver(ClientPackets.PingReq);
        byte[] secondsAnswer = second.TakeWritten();
        byte[] mine = pool.Rent(pending.Length);
        harness.RentedByTheTest = 1;
        mine.AsSpan().Fill(Mine);
        bool outAfterOtherRents = pool.IsRented(pending);
        bool readStillPending = first.Link.IsReadPending;
        first.Link.Deliver(ClientPackets.PublishQoS0);
        await first.RunEnded();
        second.Deliver(ClientPackets.PingReq);
        byte[] secondsLaterAnswer = second.TakeWritten();

        // Assert
        pending.Length.ShouldBeGreaterThan(0);
        aborted.ShouldBeTrue();
        outAfterTheAbort.ShouldBeTrue();
        outAfterOtherRents.ShouldBeTrue();
        readStillPending.ShouldBeTrue();
        SameArray(mine, pending).ShouldBeFalse();
        mine.ShouldAllBe(static b => b == Mine);
        secondsAnswer.ShouldBe(ClientPackets.PingResp);
        secondsLaterAnswer.ShouldBe(ClientPackets.PingResp);
        pool.IsRented(pending).ShouldBeFalse();
        first.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.AdministrativeAction));
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
        pool.Outstanding.ShouldBe(1);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the buffer of a write still in flight is never handed to anyone else while the link is aborted
    /// under it: with a pool that overwrites every returned array and hands it out again at once, the held delivery's
    /// buffer still holds its PUBLISH after the abort and after another connection has connected and worked and the
    /// test has rented; when the write then completes successfully, as a socket may at the moment of its abort, the
    /// link takes the PUBLISH, not overwritten bytes; and only then is the buffer returned, once.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_WriteHeldOpen_TheWriteNeverSeesReusedMemory()
    {
        // Arrange
        OverwritingPool pool = new();
        ServerHarness harness = ServerHarness.Create(pool: pool);
        TestConnection first = harness.Connect("c1");
        MqttWireConnection server = first.Connection.ShouldNotBeNull();
        first.Link.HoldWrites();
        server.TrySend(s_topic, s_hi, MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
        harness.AssertBounds();
        ReadOnlyMemory<byte> held = first.Link.HeldWriteBuffer;

        // Act
        server.Close(MqttReasonCode.AdministrativeAction);
        bool abortedAtOnce = first.Link.IsAborted;
        byte[] afterTheAbort = held.ToArray();
        TestConnection second = harness.Connect("c2");
        second.Deliver(ClientPackets.PingReq);
        byte[] secondsAnswer = second.TakeWritten();
        byte[] mine = pool.Rent(held.Length);
        harness.RentedByTheTest = 1;
        mine.AsSpan().Fill(Mine);
        byte[] afterOtherRents = held.ToArray();
        bool outBeforeTheWriteEnds = pool.IsRented(held);
        first.CompleteHeldWrites();
        byte[] written = first.TakeWritten();
        await first.RunEnded();

        // Assert
        abortedAtOnce.ShouldBeTrue();
        afterTheAbort.ShouldBe(ClientPackets.PublishQoS1);
        afterOtherRents.ShouldBe(ClientPackets.PublishQoS1);
        outBeforeTheWriteEnds.ShouldBeTrue();
        SameArray(mine, held).ShouldBeFalse();
        secondsAnswer.ShouldBe(ClientPackets.PingResp);
        written.ShouldBe(ClientPackets.PublishQoS1);
        pool.IsRented(held).ShouldBeFalse();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
        pool.Outstanding.ShouldBe(1);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that an abort at the moment a held write completes returns nothing twice, in both orders forced
    /// deterministically: the host's close aborts the link just before the write completes, and the host's close runs
    /// from inside the pool as the completed write's buffer goes back. In each, the write's buffer is returned once,
    /// by the code that awaited the write; no DISCONNECT follows the PUBLISH; and every buffer is back once the run
    /// has ended.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_AtTheSameMomentAsTheWritesCompletion_ReturnsOnce()
    {
        // Arrange
        ServerHarness before = ServerHarness.Create();
        TestConnection abortedBefore = before.Connect();
        MqttWireConnection beforeServer = abortedBefore.Connection.ShouldNotBeNull();
        abortedBefore.Link.HoldWrites();
        beforeServer.TrySend(s_topic, s_hi, MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
        before.AssertBounds();

        ServerHarness inside = ServerHarness.Create();
        TestConnection abortedInside = inside.Connect();
        MqttWireConnection insideServer = abortedInside.Connection.ShouldNotBeNull();
        abortedInside.Link.HoldWrites();
        insideServer.TrySend(s_topic, s_hi, MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
        inside.AssertBounds();
        MemoryMarshal.TryGetArray(abortedInside.Link.HeldWriteBuffer, out ArraySegment<byte> heldSegment)
            .ShouldBeTrue();
        byte[] heldArray = heldSegment.Array.ShouldNotBeNull();
        bool closedInsideTheReturn = false;
        inside.Pool.Returning = array =>
        {
            if (ReferenceEquals(array, heldArray) && !closedInsideTheReturn)
            {
                closedInsideTheReturn = true;
                insideServer.Close(MqttReasonCode.AdministrativeAction);
            }
        };

        // Act
        beforeServer.Close(MqttReasonCode.AdministrativeAction);
        bool beforeAborted = abortedBefore.Link.IsAborted;
        int beforeOutAfterTheAbort = before.Pool.Outstanding;
        abortedBefore.CompleteHeldWrites();
        await abortedBefore.RunEnded();

        abortedInside.CompleteHeldWrites();
        bool insideAborted = abortedInside.Link.IsAborted;

        // Should the close inside the return never come, the peer ends the link, so the test fails on what it asserts
        // instead of waiting.
        abortedInside.Link.EndFromPeer();
        await abortedInside.RunEnded();

        // Assert
        beforeAborted.ShouldBeTrue();
        beforeOutAfterTheAbort.ShouldBe(1);
        abortedBefore.TakeWritten().ShouldBe(ClientPackets.PublishQoS1);
        before.Pool.Outstanding.ShouldBe(0);
        before.Pool.FaultyReturns.ShouldBe(0);
        closedInsideTheReturn.ShouldBeTrue();
        insideAborted.ShouldBeTrue();
        abortedInside.TakeWritten().ShouldBe(ClientPackets.PublishQoS1);
        inside.Pool.Outstanding.ShouldBe(0);
        inside.Pool.FaultyReturns.ShouldBe(0);
        ClosedCalls(before).ShouldBe(["OnClosed c1 ClosedByHost"]);
        ClosedCalls(inside).ShouldBe(["OnClosed c1 ClosedByHost"]);
    }

    /// <summary>
    /// Proves that an abort racing a held write's completion on two threads returns nothing twice, whichever wins: in
    /// each of 500 rounds one thread completes the write while the other closes the connection, released by one gate;
    /// afterwards no buffer was returned twice, every buffer is back once the run has ended, and the write's bytes, if
    /// the link took them, are the PUBLISH followed at most by a DISCONNECT.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_RacingTheWritesCompletionOnTwoThreads_ReturnsOnce()
    {
        // Arrange
        List<string> failures = [];
        byte[] publishThenDisconnect =
            [.. ClientPackets.PublishQoS1, .. ServerPackets.Disconnect(MqttReasonCode.AdministrativeAction)];

        for (int round = 0; round < RaceRounds; round++)
        {
            ServerHarness harness = ServerHarness.Create();
            TestConnection connection = harness.Connect();
            MqttWireConnection server = connection.Connection.ShouldNotBeNull();
            connection.Link.HoldWrites();
            server.TrySend(s_topic, s_hi, MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
            int ready = 0;

            // The close is otherwise far quicker to reach the send path's lock than the completion: delaying it by a
            // little more in each round moves the moment of the abort across the whole of the completion.
            int closerDelay = round % RaceDelaySteps * RaceDelayStep;
            Thread completer = new(() =>
                RunAtTheGate(ref ready, threads: 2, delay: 0, () => connection.Link.CompleteHeldWrites()));
            Thread closer = new(() => RunAtTheGate(ref ready, threads: 2, closerDelay,
                () => server.Close(MqttReasonCode.AdministrativeAction)));

            // Act
            completer.Start();
            closer.Start();
            completer.JoinWithin("The completing thread");
            closer.JoinWithin("The closing thread");
            connection.Link.EndFromPeer();
            await connection.RunEnded();

            // Assert, collected so that one report names every round that failed.
            byte[] written = connection.TakeWritten();
            if (harness.Pool.FaultyReturns != 0 || harness.Pool.Outstanding != 0
                || !(written.SequenceEqual(ClientPackets.PublishQoS1) || written.SequenceEqual(publishThenDisconnect)))
            {
                failures.Add($"round {round}: faulty returns {harness.Pool.FaultyReturns}, outstanding "
                             + $"{harness.Pool.Outstanding}, written {Convert.ToHexString(written)}");
            }
        }

        failures.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves, with a pool that overwrites every returned array and hands it out again at once, that a write aborted at
    /// the very moment its completion gives its buffer back never had that buffer taken from it early: the host's
    /// close runs from inside the pool as the completed write's buffer comes back, and another connection connects and
    /// works there too; at that moment the link has taken the PUBLISH, the buffer is still out and still holds the
    /// PUBLISH, and the other connection is answered correctly. The other order, the abort just before the
    /// completion, is <see cref="Abort_WriteHeldOpen_TheWriteNeverSeesReusedMemory"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_InsideTheReturnOfTheCompletedWritesBuffer_TheWriteNeverSeesReusedMemory()
    {
        // Arrange
        OverwritingPool pool = new();
        ServerHarness harness = ServerHarness.Create(pool: pool);
        TestConnection first = harness.Connect("c1");
        MqttWireConnection server = first.Connection.ShouldNotBeNull();
        first.Link.HoldWrites();
        server.TrySend(s_topic, s_hi, MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
        harness.AssertBounds();
        ReadOnlyMemory<byte> held = first.Link.HeldWriteBuffer;
        MemoryMarshal.TryGetArray(held, out ArraySegment<byte> heldSegment).ShouldBeTrue();
        byte[] heldArray = heldSegment.Array.ShouldNotBeNull();
        int writesBefore = first.Link.WriteCount;
        byte[]? bytesAtTheReturn = null;
        int writesAtTheReturn = 0;
        bool abortedAtTheReturn = false;
        bool outAfterTheOtherConnection = false;
        byte[]? secondsAnswer = null;
        TestConnection? second = null;
        pool.Returning = array =>
        {
            if (!ReferenceEquals(array, heldArray) || second is not null)
            {
                return;
            }

            bytesAtTheReturn = held.ToArray();
            writesAtTheReturn = first.Link.WriteCount;
            server.Close(MqttReasonCode.AdministrativeAction);
            abortedAtTheReturn = first.Link.IsAborted;
            second = harness.Connect("c2");
            second.Deliver(ClientPackets.PingReq);
            secondsAnswer = second.TakeWritten();
            outAfterTheOtherConnection = pool.IsRented(held);
        };

        // Act
        first.CompleteHeldWrites();

        // Should the close inside the return never come, the peer ends the link, so the test fails on what it asserts
        // instead of waiting.
        first.Link.EndFromPeer();
        await first.RunEnded();

        // Assert
        bytesAtTheReturn.ShouldBe(ClientPackets.PublishQoS1);
        writesAtTheReturn.ShouldBe(writesBefore + 1);
        abortedAtTheReturn.ShouldBeTrue();
        outAfterTheOtherConnection.ShouldBeTrue();
        secondsAnswer.ShouldBe(ClientPackets.PingResp);
        first.TakeWritten().ShouldBe(ClientPackets.PublishQoS1);
        pool.IsRented(held).ShouldBeFalse();
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves, with a pool that overwrites every returned array and hands it out again at once, that a held write whose
    /// completion races an abort and other rents never sees reused memory: in each of 500 rounds one thread completes
    /// the write, one closes the connection and one rents arrays and fills them, all released by one gate with delays
    /// that change from round to round. In every round the link took exactly the PUBLISH (followed at most by a
    /// DISCONNECT), not overwritten or foreign bytes; the arrays rented meanwhile were not changed
    /// by it; no buffer was returned twice; and once the run has ended and the test's arrays are back, none
    /// is out.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_RacingTheWritesCompletion_TheWriteNeverSeesReusedMemory()
    {
        // Arrange
        List<string> failures = [];
        byte[] publishThenDisconnect =
            [.. ClientPackets.PublishQoS1, .. ServerPackets.Disconnect(MqttReasonCode.AdministrativeAction)];

        for (int round = 0; round < RaceRounds; round++)
        {
            OverwritingPool pool = new();
            ServerHarness harness = ServerHarness.Create(pool: pool);
            TestConnection connection = harness.Connect();
            MqttWireConnection server = connection.Connection.ShouldNotBeNull();
            connection.Link.HoldWrites();
            server.TrySend(s_topic, s_hi, MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
            ReadOnlyMemory<byte> held = connection.Link.HeldWriteBuffer;
            byte[][] mine = new byte[3][];
            int ready = 0;
            int closerDelay = round % RaceDelaySteps * RaceDelayStep;
            int renterDelay = round * 7 % RaceDelaySteps * RaceDelayStep;
            Thread completer = new(() =>
                RunAtTheGate(ref ready, threads: 3, delay: 0, () => connection.Link.CompleteHeldWrites()));
            Thread closer = new(() => RunAtTheGate(ref ready, threads: 3, closerDelay,
                () => server.Close(MqttReasonCode.AdministrativeAction)));
            Thread renter = new(() => RunAtTheGate(ref ready, threads: 3, renterDelay, () => RentAndFill(pool, mine,
                held.Length)));
            harness.RentedByTheTest = mine.Length;

            // Act
            completer.Start();
            closer.Start();
            renter.Start();
            completer.JoinWithin("The completing thread");
            closer.JoinWithin("The closing thread");
            renter.JoinWithin("The renting thread");
            connection.Link.EndFromPeer();
            await connection.RunEnded();

            // Assert, collected so that one report names every round that failed.
            byte[] written = connection.TakeWritten();
            bool mineIntact = mine.All(static array => array.All(static b => b == Mine));
            foreach (byte[] array in mine)
            {
                pool.Return(array);
            }

            if (!(written.SequenceEqual(ClientPackets.PublishQoS1) || written.SequenceEqual(publishThenDisconnect))
                || !mineIntact || pool.FaultyReturns != 0 || pool.Outstanding != 0)
            {
                failures.Add($"round {round}: written {Convert.ToHexString(written)}, the test's arrays intact "
                             + $"{mineIntact}, faulty returns {pool.FaultyReturns}, outstanding {pool.Outstanding}");
            }
        }

        failures.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves, with a pool that overwrites every returned array and hands it out again at once, that a read aborted at
    /// the moment its data has arrived never has its buffer taken from it: a PINGREQ and a PUBLISH arrive in the
    /// pending read's buffer, and while the PINGREQ is being handled, as its PINGRESP's buffer goes back, the host's
    /// close aborts the link and another connection connects and works, and the test rents. At that moment the read's
    /// buffer is still out and still holds the bytes that arrived, the arrays rented meanwhile are not it and stay as
    /// they were filled, the PUBLISH is not handed on, and the read's buffer goes back once the read loop has ended.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_WhileTheReadsDataIsHandled_TheReadNeverSeesReusedMemory()
    {
        // Arrange
        OverwritingPool pool = new();
        ServerHarness harness = ServerHarness.Create(pool: pool);
        TestConnection first = harness.Connect("c1");
        MqttWireConnection server = first.Connection.ShouldNotBeNull();
        first.Link.SkipWaits();
        first.Deliver(ClientPackets.PingReq);
        first.TakeWritten().ShouldBe(ClientPackets.PingResp);
        Memory<byte> pending = first.Link.PendingReadBuffer;
        byte[] arriving = [.. ClientPackets.PingReq, .. ClientPackets.PublishQoS0];
        bool armed = true;
        byte[]? bytesAtTheAbort = null;
        bool abortedThere = false;
        bool outAfterOtherRents = false;
        byte[]? secondsAnswer = null;
        byte[] mine = [];
        pool.Returning = array =>
        {
            if (!armed || SameArray(array, pending))
            {
                return;
            }

            // The first buffer back is the PINGRESP's, while the read's buffer still holds what arrived.
            armed = false;
            server.Close(MqttReasonCode.AdministrativeAction);
            abortedThere = first.Link.IsAborted;
            TestConnection second = harness.Connect("c2");
            second.Deliver(ClientPackets.PingReq);
            secondsAnswer = second.TakeWritten();
            mine = pool.Rent(pending.Length);
            harness.RentedByTheTest = 1;
            mine.AsSpan().Fill(Mine);
            bytesAtTheAbort = pending[..arriving.Length].ToArray();
            outAfterOtherRents = pool.IsRented(pending);
        };

        // Act
        first.Link.Deliver(arriving);
        await first.RunEnded();

        // Assert
        abortedThere.ShouldBeTrue();
        bytesAtTheAbort.ShouldBe(arriving);
        outAfterOtherRents.ShouldBeTrue();
        SameArray(mine, pending).ShouldBeFalse();
        mine.ShouldAllBe(static b => b == Mine);
        secondsAnswer.ShouldBe(ClientPackets.PingResp);
        harness.Handler.Calls.ShouldNotContain(static call => call.StartsWith("OnPublish c1", StringComparison.Ordinal));
        first.TakeWritten().ShouldBe(ClientPackets.PingResp);
        pool.IsRented(pending).ShouldBeFalse();
        ClosedCalls(harness).ShouldContain("OnClosed c1 ClosedByHost");
        pool.Outstanding.ShouldBe(1);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves, with a pool that overwrites every returned array and hands it out again at once, that a pending read
    /// whose data races an abort of the link and other rents never sees reused memory: in each of 500 rounds one
    /// thread delivers a PINGREQ and a PUBLISH to the pending read, one aborts the link and one rents arrays and fills
    /// them, all released by one gate with delays that change from round to round. In every round the arrays rented
    /// meanwhile were not changed by the read, what was written is at most the PINGRESP, no buffer was returned twice,
    /// and once the run has ended only the test's arrays are out.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_RacingTheReadsData_TheReadNeverSeesReusedMemory()
    {
        // Arrange
        List<string> failures = [];
        byte[] arriving = [.. ClientPackets.PingReq, .. ClientPackets.PublishQoS0];

        for (int round = 0; round < RaceRounds; round++)
        {
            OverwritingPool pool = new();
            ServerHarness harness = ServerHarness.Create(pool: pool);
            TestConnection connection = harness.Connect();
            connection.Link.SkipWaits();
            connection.Deliver(ClientPackets.PingReq);
            connection.TakeWritten();
            Memory<byte> pending = connection.Link.PendingReadBuffer;
            byte[][] mine = new byte[3][];
            int ready = 0;
            int aborterDelay = round % RaceDelaySteps * RaceDelayStep;
            int renterDelay = round * 7 % RaceDelaySteps * RaceDelayStep;
            Thread deliverer = new(() =>
                RunAtTheGate(ref ready, threads: 3, delay: 0, () => connection.Link.Deliver(arriving)));
            Thread aborter = new(() => RunAtTheGate(ref ready, threads: 3, aborterDelay, connection.Link.Abort));
            Thread renter = new(() =>
                RunAtTheGate(ref ready, threads: 3, renterDelay, () => RentAndFill(pool, mine, pending.Length)));
            harness.RentedByTheTest = mine.Length;

            // Act
            deliverer.Start();
            aborter.Start();
            renter.Start();
            deliverer.JoinWithin("The delivering thread");
            aborter.JoinWithin("The aborting thread");
            renter.JoinWithin("The renting thread");
            await connection.RunEnded();

            // Assert, collected so that one report names every round that failed.
            byte[] written = connection.TakeWritten();
            bool mineIntact = mine.All(static array => array.All(static b => b == Mine));
            int outstanding = pool.Outstanding;
            if (!(written.Length == 0 || written.SequenceEqual(ClientPackets.PingResp)) || !mineIntact
                || pool.FaultyReturns != 0 || outstanding != mine.Length)
            {
                failures.Add($"round {round}: written {Convert.ToHexString(written)}, the test's arrays intact "
                             + $"{mineIntact}, faulty returns {pool.FaultyReturns}, outstanding {outstanding}");
            }
        }

        failures.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves that the control buffer of the packets that wait behind a held write is returned once when the
    /// connection is aborted: a PINGRESP and a PUBACK wait behind a held delivery; the host's close aborts the link and
    /// returns nothing; when the held write then fails, the Closing step returns the delivery's buffer and the control
    /// buffer, each once, and nothing is written.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_ControlBufferStillWaiting_IsReturnedOnce()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, s_hi, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        harness.AssertBounds();
        connection.Deliver(ClientPackets.PingReq);
        connection.Deliver(ClientPackets.PublishQoS1);
        ConnectionCounts waiting = server.GetCounts();
        int outWhileWaiting = harness.Pool.Outstanding;

        // Act
        server.Close(MqttReasonCode.AdministrativeAction);
        bool aborted = connection.Link.IsAborted;
        int outAfterTheAbort = harness.Pool.Outstanding;
        connection.CompleteHeldWrites(new OperationCanceledException());
        await connection.RunEnded();

        // Assert
        waiting.WaitingControlPackets.ShouldBe(2);
        outWhileWaiting.ShouldBe(2);
        aborted.ShouldBeTrue();
        outAfterTheAbort.ShouldBe(2);
        connection.TakeWritten().ShouldBeEmpty();
        server.GetCounts().ShouldBe(s_idle);
        ClosedCalls(harness).ShouldBe(["OnClosed c1 ClosedByHost"]);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a closed connection keeps no reference to a host's payload (design 6.8 rules 2 and 6): a large
    /// delivery whose header is held by the link, with the rest of its payload still to write from the host's memory,
    /// and a small and a large delivery waiting behind it, are all unreachable once the connection has been closed,
    /// its run has ended and the garbage collector has run.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Close_WaitingDeliveries_DropEveryPayloadReference()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        WeakReference[] payloads = SendAndForget(harness, server);
        ConnectionCounts waiting = server.GetCounts();

        // Act
        server.Close(MqttReasonCode.AdministrativeAction);
        connection.CompleteHeldWrites(new OperationCanceledException());
        await connection.RunEnded();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Assert
        waiting.WaitingDeliveries.ShouldBe(2);
        payloads.ShouldAllBe(static payload => !payload.IsAlive);
        server.GetCounts().ShouldBe(s_idle);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a thousand connect, work and disconnect cycles on one server accumulate nothing: after each cycle
    /// no buffer is out; afterwards none was returned twice, every slot is free, every queue and records object taken
    /// from the server's pools is back and the pools hold at most 1,024 of each, and the managed heap is within 64 KiB
    /// of what it was after the first cycles. Each cycle publishes at QoS 1 and 2, delivers at QoS 0 and 1 behind a
    /// held write with a PINGRESP waiting, and ends with the client's DISCONNECT.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Cycles_ThousandConnectWorkDisconnect_AccumulateNothing()
    {
        // Arrange
        ServerPools defaults = new();
        CountingObjectPool<WaitingDeliveries> waiting = new(defaults.Waiting);
        CountingObjectPool<ExchangeRecords> records = new(defaults.Records);
        ServerHarness harness = ServerHarness.Create(pools: new ServerPools(waiting, records));
        List<int> outstanding = [];

        // The first cycles create what is created once: the pools' objects and buffers, the JIT's work.
        for (int cycle = 0; cycle < 10; cycle++)
        {
            outstanding.Add(await RunCycleAsync(harness));
        }

        long heapBefore = ReadHeap(harness);

        // Act
        for (int cycle = 0; cycle < Cycles; cycle++)
        {
            outstanding.Add(await RunCycleAsync(harness));
        }

        // The last run ended on this thread; yielding leaves its frames before the heap is read.
        await Task.Yield();
        long heapAfter = ReadHeap(harness);

        // Assert
        outstanding.ShouldAllBe(static count => count == 0);
        harness.Pool.FaultyReturns.ShouldBe(0);
        harness.Server.Registry.Slots.ShouldAllBe(static slot => slot == null);
        waiting.Outstanding.ShouldBe(0);
        records.Outstanding.ShouldBe(0);
        waiting.Distinct.ShouldBeLessThanOrEqualTo(ServerLimits.MaxPooledObjects);
        records.Distinct.ShouldBeLessThanOrEqualTo(ServerLimits.MaxPooledObjects);
        Math.Abs(heapAfter - heapBefore).ShouldBeLessThanOrEqualTo(MaxHeapDrift,
            $"The heap was {heapBefore} bytes before the cycles and {heapAfter} after.");
    }

    /// <summary>
    /// Sets the options a way out needs: a limit it passes.
    /// </summary>
    /// <param name="options">
    /// The options to change.
    /// </param>
    /// <param name="wayOut">
    /// The way out.
    /// </param>
    private static void Configure(MqttWireServerOptions options, WayOut wayOut)
    {
        switch (wayOut)
        {
            case WayOut.PublishAboveTheSkipBand:
                options.MaxPublishPayloadBytes = 1_024;
                break;
            case WayOut.EvictionWithAHeldWrite:
                options.MaxOutgoingMessages = 2;
                break;
            case WayOut.TooLarge:
                options.MaxOutgoingBytes = 65_536;
                break;
            default:
                // The defaults.
                break;
        }
    }

    /// <summary>
    /// Opens the connections of a way out and ends them that way.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <param name="pool">
    /// The pool the server rents from.
    /// </param>
    /// <param name="wayOut">
    /// The way out.
    /// </param>
    /// <returns>
    /// The connections whose runs end by this way out.
    /// </returns>
    private static async Task<TestConnection[]> TakeWayOutAsync(ServerHarness harness, ThrowingPool pool,
        WayOut wayOut)
    {
        switch (wayOut)
        {
            case WayOut.RefusedConnect:
                return [ConnectRefused(harness, ServerPackets.ConnectVersion4, MqttReasonCode.UnsupportedProtocolVersion)];

            case WayOut.HostRefusal:
                harness.Handler.Connect = static (_, _) =>
                    ValueTask.FromResult(MqttConnectResult.Refuse(MqttReasonCode.NotAuthorized));
                return [ConnectRefused(harness, ServerPackets.Connect, MqttReasonCode.NotAuthorized)];

            case WayOut.AuthenticationTimeout:
                return [TimeOutAuthentication(harness)];

            case WayOut.ConnectTimeout:
                return [TimeOutConnect(harness)];

            case WayOut.ConnAckCannotBeEncoded:
                return [RefuseUnencodableConnAck(harness)];

            case WayOut.Takeover:
                return TakeOver(harness);

            case WayOut.ServerStop:
                return [await StopAsync(harness)];

            case WayOut.RegisteredAfterTheStopBegan:
                return await RegisterAfterTheStopBeganAsync(harness);

            default:
                TestConnection connection = harness.Connect();
                Work(harness, connection);
                EndActive(harness, pool, connection, wayOut);
                return [connection];
        }
    }

    /// <summary>
    /// Ends an Active connection, which has worked, the given way.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <param name="pool">
    /// The pool the server rents from.
    /// </param>
    /// <param name="connection">
    /// The connection.
    /// </param>
    /// <param name="wayOut">
    /// The way out; one of an Active connection.
    /// </param>
    private static void EndActive(ServerHarness harness, ThrowingPool pool, TestConnection connection, WayOut wayOut)
    {
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        switch (wayOut)
        {
            case WayOut.NormalDisconnect:
                connection.Deliver(ClientPackets.Disconnect);
                break;

            case WayOut.PartialPacketTimeout:
                connection.Deliver(s_publishStart);
                harness.Advance(ServerLimits.PartialPacketDeadline + ServerLimits.CloseDeadline);
                break;

            case WayOut.KeepAliveTimeout:
                // Keep-alive 30 s: closed at the first sweep from 45 s on, aborted at the close deadline after it.
                harness.Advance(TimeSpan.FromSeconds(45) + ServerLimits.SweepInterval + ServerLimits.CloseDeadline);
                break;

            case WayOut.MalformedPacket:
                connection.Deliver(Hex.Parse("C0 01 00"));
                break;

            case WayOut.PublishAboveTheSkipBand:
                connection.Deliver(ServerPackets.BuildPublishOfLength(MqttQoS.AtLeastOnce, 16 * 1_024));
                connection.TakeWritten().ShouldBe(ServerPackets.TooLargePubAck1);
                connection.Deliver(ServerPackets.BuildPublishOfLength(MqttQoS.AtLeastOnce, (16 * 1_024) + 1));
                break;

            case WayOut.EvictionWithAHeldWrite:
                connection.Link.HoldWrites();
                SendAndAssertBounds(harness, server, MqttSendResult.Taken);
                connection.Deliver(ClientPackets.PingReq);
                SendAndAssertBounds(harness, server, MqttSendResult.Taken);
                SendAndAssertBounds(harness, server, MqttSendResult.Taken);
                SendAndAssertBounds(harness, server, MqttSendResult.Evicted);
                connection.CompleteHeldWrites(new OperationCanceledException());
                break;

            case WayOut.TooLarge:
                server.TrySend(s_topic, new byte[65_537], MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.TooLarge);
                harness.AssertBounds();
                connection.Link.EndFromPeer();
                break;

            case WayOut.HandlerThrows:
                harness.Handler.Publish = static (_, in _) => throw new InvalidOperationException("The host's bug.");
                connection.Deliver(ClientPackets.PublishQoS1);
                break;

            case WayOut.WriteFails:
                connection.Link.FailWrites(new IOException("The socket is gone."));
                SendAndAssertBounds(harness, server, MqttSendResult.Taken);
                break;

            case WayOut.ReadFails:
                connection.Deliver(s_publishStart);
                connection.Link.FailReads(new IOException("The socket is gone."));
                harness.AssertBounds();
                break;

            case WayOut.WriteDeadline:
                connection.Link.HoldWrites();
                SendAndAssertBounds(harness, server, MqttSendResult.Taken);
                harness.Advance(ServerLimits.WriteDeadline + ServerLimits.SweepInterval);
                connection.CompleteHeldWrites(new OperationCanceledException());
                break;

            case WayOut.SendPathFault:
                pool.ThrowOnRent(which: 1);
                SendAndAssertBounds(harness, server, MqttSendResult.NotOpen);
                pool.HasThrown.ShouldBeTrue();
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(wayOut), wayOut, "Not a way out of an Active connection.");
        }
    }

    /// <summary>
    /// Opens a connection whose CONNECT is refused with a CONNACK.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <param name="connect">
    /// The CONNECT.
    /// </param>
    /// <param name="code">
    /// The reason code the CONNACK refuses it with.
    /// </param>
    /// <returns>
    /// The connection.
    /// </returns>
    private static TestConnection ConnectRefused(ServerHarness harness, byte[] connect, MqttReasonCode code)
    {
        TestConnection connection = harness.Accept();
        connection.Deliver(connect);
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(code));
        return connection;
    }

    /// <summary>
    /// Opens a connection whose host never answers, and moves the clock to the authentication deadline, at which it is
    /// refused with CONNACK 0x88 on the thread pool.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <returns>
    /// The connection.
    /// </returns>
    private static TestConnection TimeOutAuthentication(ServerHarness harness)
    {
        TaskCompletionSource<MqttConnectResult> never = new();
        harness.Handler.Connect = (_, _) => new ValueTask<MqttConnectResult>(never.Task);
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect);
        harness.Advance(ServerLimits.AuthenticationDeadline);
        return connection;
    }

    /// <summary>
    /// Opens a connection that sends half a CONNECT, which waits in its read buffer with a read pending, and moves the
    /// clock to the connect deadline, which aborts the link.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <returns>
    /// The connection.
    /// </returns>
    private static TestConnection TimeOutConnect(ServerHarness harness)
    {
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect.AsSpan(0, ServerPackets.Connect.Length / 2));
        harness.Advance(ServerLimits.ConnectDeadline);
        return connection;
    }

    /// <summary>
    /// Opens a connection whose host accepts it and then changes the user properties it returned, so that the success
    /// CONNACK cannot be encoded and CONNACK 0x83 is written in its place.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <returns>
    /// The connection.
    /// </returns>
    private static TestConnection RefuseUnencodableConnAck(ServerHarness harness)
    {
        MqttWireUserProperty[] properties = [new MqttWireUserProperty("k", "v")];
        harness.Handler.Connect = (_, _) =>
        {
            MqttConnectResult accepted = MqttConnectResult.Accept(userProperties: properties);

            // The host's bug: the array the server keeps until the CONNACK is written is changed after the accept.
            properties[0] = new MqttWireUserProperty("k", "bad\0value");
            return ValueTask.FromResult(accepted);
        };
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect);
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.ImplementationSpecificError));
        return connection;
    }

    /// <summary>
    /// Connects a client id, has it work, and takes it over with a second connection, which then disconnects.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <returns>
    /// The connection taken over and the one that took it over.
    /// </returns>
    private static TestConnection[] TakeOver(ServerHarness harness)
    {
        TestConnection first = harness.Connect();
        Work(harness, first);
        TestConnection second = harness.Connect();
        first.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.SessionTakenOver));
        first.Link.EndFromPeer();
        second.Deliver(ClientPackets.Disconnect);
        return [first, second];
    }

    /// <summary>
    /// Connects, works, leaves half a packet in the read buffer, and stops the server, whose DISCONNECT 0x8B the
    /// client answers by ending the link.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <returns>
    /// The connection, once the stop has completed.
    /// </returns>
    private static async Task<TestConnection> StopAsync(ServerHarness harness)
    {
        TestConnection connection = harness.Connect();
        Work(harness, connection);
        connection.Deliver(s_publishStart);
        Task stop = harness.Server.StopAsync();
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ServerShuttingDown));
        connection.Link.EndFromPeer();
        await stop.Within("The server's stop");
        return connection;
    }

    /// <summary>
    /// Connects and works, begins the server's stop, and opens a second connection, which takes its slot after the
    /// stop began and aborts itself; the first answers its DISCONNECT 0x8B by ending the link.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <returns>
    /// Both connections, once the stop has completed.
    /// </returns>
    private static async Task<TestConnection[]> RegisterAfterTheStopBeganAsync(ServerHarness harness)
    {
        TestConnection first = harness.Connect();
        Work(harness, first);
        Task stop = harness.Server.StopAsync();
        TestConnection late = harness.Accept();
        late.Link.IsAborted.ShouldBeTrue();
        first.Link.EndFromPeer();
        await stop.Within("The server's stop");
        return [first, late];
    }

    /// <summary>
    /// The work an Active connection does before its way out: a QoS 1 publish from the client and its PUBACK, a whole
    /// QoS 2 exchange from the client, and a QoS 1 delivery and its PUBACK.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <param name="connection">
    /// The connection, Active.
    /// </param>
    private static void Work(ServerHarness harness, TestConnection connection)
    {
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Deliver(ClientPackets.PublishQoS1);
        connection.Deliver(ClientPackets.PublishQoS2);
        connection.Deliver(ClientPackets.PubRel1);
        SendAndAssertBounds(harness, server, MqttSendResult.Taken, MqttQoS.AtLeastOnce);
        connection.Deliver(ClientPackets.PubAck1);
        connection.TakeWritten().ShouldBe([
            .. ClientPackets.PubAck1, .. ClientPackets.PubRec1, .. ClientPackets.PubComp1,
            .. ClientPackets.PublishQoS1,
        ]);
        server.GetCounts().ShouldBe(s_idle);
    }

    /// <summary>
    /// Sends "hi" to the connection, asserts what the send returned, and asserts the bounds.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <param name="server">
    /// The connection.
    /// </param>
    /// <param name="expected">
    /// What the send must return.
    /// </param>
    /// <param name="qos">
    /// The QoS to send with.
    /// </param>
    private static void SendAndAssertBounds(ServerHarness harness, MqttWireConnection server, MqttSendResult expected,
        MqttQoS qos = MqttQoS.AtMostOnce)
    {
        server.TrySend(s_topic, s_hi, qos).ShouldBe(expected);
        harness.AssertBounds();
    }

    /// <summary>
    /// Asserts what shows that a way out without an <c>OnClosed</c> call was the one taken, once its run has ended: the
    /// authentication deadline answered with CONNACK 0x88, and the connect deadline aborted the link without a write.
    /// The other ways out are told apart by their <c>OnClosed</c> reason, or by the CONNACK their own steps assert.
    /// </summary>
    /// <param name="wayOut">
    /// The way out.
    /// </param>
    /// <param name="ended">
    /// The connections whose runs it ended.
    /// </param>
    private static void AssertTheWayOutWasTaken(WayOut wayOut, TestConnection[] ended)
    {
        if (wayOut == WayOut.AuthenticationTimeout)
        {
            ended[0].TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.ServerUnavailable));
        }
        else if (wayOut == WayOut.ConnectTimeout)
        {
            ended[0].Link.IsAborted.ShouldBeTrue();
            ended[0].TakeWritten().ShouldBeEmpty();
        }
    }

    /// <summary>
    /// Gives the <c>OnClosed</c> calls a way out makes.
    /// </summary>
    /// <param name="wayOut">
    /// The way out.
    /// </param>
    /// <returns>
    /// The calls as text, in order; none for a connection that was never admitted.
    /// </returns>
    private static string[] ExpectedClosed(WayOut wayOut) => wayOut switch
    {
        WayOut.RefusedConnect or WayOut.HostRefusal or WayOut.AuthenticationTimeout or WayOut.ConnectTimeout => [],
        WayOut.NormalDisconnect => ["OnClosed c1 ClientDisconnected"],
        WayOut.PartialPacketTimeout => ["OnClosed c1 PartialPacketTimedOut"],
        WayOut.KeepAliveTimeout => ["OnClosed c1 KeepAliveTimedOut"],
        WayOut.MalformedPacket => ["OnClosed c1 MalformedPacket"],
        WayOut.PublishAboveTheSkipBand => ["OnClosed c1 ProtocolError"],
        WayOut.EvictionWithAHeldWrite => ["OnClosed c1 OutgoingLimitPassed"],
        WayOut.TooLarge => ["OnClosed c1 MessageTooLargeToSend"],
        WayOut.Takeover => ["OnClosed c1 TakenOver", "OnClosed c1 ClientDisconnected"],
        WayOut.HandlerThrows or WayOut.ConnAckCannotBeEncoded => ["OnClosed c1 HandlerThrew"],
        WayOut.WriteFails or WayOut.ReadFails or WayOut.SendPathFault => ["OnClosed c1 ConnectionLost"],
        WayOut.WriteDeadline => ["OnClosed c1 WriteTimedOut"],
        _ => ["OnClosed c1 ServerStopping"],
    };

    /// <summary>
    /// Sends, while the link holds writes, a large delivery whose header the link holds and two deliveries behind it,
    /// a small and a large one, in a frame of its own, so that no local of the test keeps a payload alive.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <param name="server">
    /// The connection, whose link holds writes.
    /// </param>
    /// <returns>
    /// Weak references to the three payloads.
    /// </returns>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference[] SendAndForget(ServerHarness harness, MqttWireConnection server)
    {
        byte[][] payloads = [new byte[20_000], "small"u8.ToArray(), new byte[30_000]];
        foreach (byte[] payload in payloads)
        {
            server.TrySend(s_topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
            harness.AssertBounds();
        }

        return [.. payloads.Select(static payload => new WeakReference(payload))];
    }

    /// <summary>
    /// Runs one connect, work and disconnect cycle on the harness: publishes at QoS 1 and 2 from the client, a QoS 0
    /// and a QoS 1 delivery behind a held write with a PINGRESP waiting, the PUBACK of the delivery, and the client's
    /// DISCONNECT.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <returns>
    /// The buffers out of the pool once the cycle's run has ended.
    /// </returns>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static async Task<int> RunCycleAsync(ServerHarness harness)
    {
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Deliver(ClientPackets.PublishQoS1);
        connection.Deliver(ClientPackets.PublishQoS2);
        connection.Deliver(ClientPackets.PubRel1);
        connection.Link.HoldWrites();
        SendAndAssertBounds(harness, server, MqttSendResult.Taken);
        SendAndAssertBounds(harness, server, MqttSendResult.Taken, MqttQoS.AtLeastOnce);
        connection.Deliver(ClientPackets.PingReq);
        connection.CompleteHeldWrites();
        connection.Deliver(ClientPackets.PubAck1);
        connection.Deliver(ClientPackets.Disconnect);
        await connection.RunEnded();
        connection.TakeWritten().ShouldBe([
            .. ClientPackets.PubAck1, .. ClientPackets.PubRec1, .. ClientPackets.PubComp1,
            .. ClientPackets.PublishQoS0, .. ClientPackets.PingResp, .. ClientPackets.PublishQoS1,
        ]);
        return harness.Pool.Outstanding;
    }

    /// <summary>
    /// Empties the handler's and the logger's records, the test's own, and reads the size of the managed heap after
    /// full, blocking, compacting collections with the pending finalizers run in between.
    /// </summary>
    /// <param name="harness">
    /// The harness whose records are emptied.
    /// </param>
    /// <returns>
    /// The bytes the heap holds.
    /// </returns>
    private static long ReadHeap(ServerHarness harness)
    {
        harness.Handler.Forget();
        harness.Log.Entries.Clear();
        harness.Log.Entries.TrimExcess();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        return GC.GetTotalMemory(forceFullCollection: true);
    }

    /// <summary>
    /// Runs an action on the calling thread once every thread of a race is at the gate, so that they start as close
    /// together as the scheduler allows, after a spin of its own that shifts where in the others' work it lands.
    /// </summary>
    /// <param name="ready">
    /// The number of threads at the gate, shared by all of them.
    /// </param>
    /// <param name="threads">
    /// The number of threads the gate waits for.
    /// </param>
    /// <param name="delay">
    /// How many iterations to spin once the gate is open; 0 for none.
    /// </param>
    /// <param name="action">
    /// What the thread does.
    /// </param>
    private static void RunAtTheGate(ref int ready, int threads, int delay, Action action)
    {
        Interlocked.Increment(ref ready);
        SpinWait spin = default;
        while (Volatile.Read(ref ready) < threads)
        {
            spin.SpinOnce(sleep1Threshold: -1);
        }

        Thread.SpinWait(delay);
        action();
    }

    /// <summary>
    /// Rents three arrays, as another connection would, one of the length of the buffer an operation in flight holds
    /// and two of other sizes, and fills each with <see cref="Mine"/>, so that a write into them by that operation
    /// shows.
    /// </summary>
    /// <param name="pool">
    /// The pool to rent from.
    /// </param>
    /// <param name="mine">
    /// Receives the three arrays.
    /// </param>
    /// <param name="length">
    /// The length of the buffer the operation in flight holds.
    /// </param>
    private static void RentAndFill(OverwritingPool pool, byte[][] mine, int length)
    {
        mine[0] = pool.Rent(length);
        mine[1] = pool.Rent(minimumLength: 16);
        mine[2] = pool.Rent(minimumLength: 4_096);
        foreach (byte[] array in mine)
        {
            array.AsSpan().Fill(Mine);
        }
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
    /// Tells whether memory lies in a given array.
    /// </summary>
    /// <param name="array">
    /// The array.
    /// </param>
    /// <param name="memory">
    /// The memory.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the memory is over that very array.
    /// </returns>
    private static bool SameArray(byte[] array, ReadOnlyMemory<byte> memory) =>
        MemoryMarshal.TryGetArray(memory, out ArraySegment<byte> segment) && ReferenceEquals(segment.Array, array);
}
