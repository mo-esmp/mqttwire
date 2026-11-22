using System.Buffers;
using System.Diagnostics;
using System.Reflection;
using MqttWire.Server;
using MqttWire.Tests.Client;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Server;

/// <summary>
/// Proves the server's hot paths allocate nothing in steady state (AGENTS "Hot Paths", design 6.6): the read loop
/// receiving PINGREQ and PUBLISH at QoS 0, 1 and 2 with their answers, <c>TrySend</c> at QoS 0, a QoS 1 exchange, a
/// large payload written in pieces, deliveries and answers that wait behind a held write, a delivery that waits for
/// the client's window, and the sweep over a thousand connections. Each test runs its work
/// <see cref="Iterations"/> times as a warm-up, so that every buffer, pool and queue has reached its size and the JIT
/// has done its work, then measures <see cref="Iterations"/> more as the difference of two readings of the allocated
/// bytes of the current thread.
/// </summary>
/// <remarks>
/// All measured work runs on the test thread and completes synchronously: the in-memory link runs the read loop
/// inline and its writes complete at once, and a held write is released with the test runner's synchronization
/// context taken off the thread (<see cref="TestConnection.CompleteHeldWrites"/>), so that what follows it runs here.
/// A write that does not complete at once allocates the state machine that awaits it, by design; the tests that hold
/// a write start that write outside the measurement, and measure only what is added behind it. The server rents from
/// <see cref="ArrayPool{T}.Shared"/>, the handler records no calls, and the delivered packets, the expected bytes and
/// the link are all taken before the first reading. The tests run alone, not in parallel with others. In a Debug
/// build, which compiles every async state machine as a class, they check only what was sent and handled.
/// </remarks>
[Collection(nameof(ServerAllocationTests))]
[CollectionDefinition(nameof(ServerAllocationTests), DisableParallelization = true)]
public sealed class ServerAllocationTests
{
    /// <summary>
    /// The number of measured iterations, and of warm-up iterations before them.
    /// </summary>
    private const int Iterations = 1_000;

    /// <summary>
    /// How many deliveries wait behind one held write: at most the queue capacity the server's pool keeps, so that
    /// the queue goes back to the pool and is reused.
    /// </summary>
    private const int DeliveriesPerHold = 50;

    /// <summary>
    /// How many answers wait behind one held write: fewer than the receive maximum the server announced, which the
    /// waiting PUBACKs count against.
    /// </summary>
    private const int AnswersPerHold = 200;

    /// <summary>
    /// The number of connections the sweep walks.
    /// </summary>
    private const int Connections = 1_000;

    /// <summary>
    /// The payload of the large delivery, written in pieces from the host's memory.
    /// </summary>
    private const int LargePayloadLength = 200_000;

    /// <summary>
    /// The writes of one large delivery: its header, then its payload in pieces of at most 64 KiB.
    /// </summary>
    private const int LargeDeliveryWrites = 5;

    /// <summary>
    /// The payload of the messages the tests send, "hi".
    /// </summary>
    private static readonly byte[] s_hi = "hi"u8.ToArray();

    /// <summary>
    /// Whether the server's assembly was built with optimizations, as it ships. A Debug build compiles every async
    /// method's state machine as a class, which the read loop then allocates, so there these tests check only what
    /// was sent and handled; CI runs them in Release.
    /// </summary>
    private static readonly bool s_serverIsOptimized =
        typeof(MqttWireServer).Assembly.GetCustomAttribute<DebuggableAttribute>()?.IsJITOptimizerDisabled != true;

    /// <summary>
    /// Proves receiving a PINGREQ and writing its PINGRESP allocates nothing in an optimized build: the read buffer is
    /// rented and returned, and the answer goes over the idle send path from a pooled buffer.
    /// </summary>
    [Fact]
    public void Receive_PingReq_AllocatesNothing()
    {
        // Arrange
        (_, TestConnection connection, _) = Connect(connect: null);
        MemoryLink link = connection.Link;
        DeliverEach(link, ClientPackets.PingReq);
        link.TakeWritten();
        byte[] expected = Repeat(ClientPackets.PingResp, Iterations);

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        DeliverEach(link, ClientPackets.PingReq);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        link.TakeWritten().ShouldBe(expected);
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves receiving a QoS 0 PUBLISH and handing it to the host's <c>OnPublish</c> allocates nothing in an
    /// optimized build: the packet is decoded in place in the pooled read buffer, and nothing is written back.
    /// </summary>
    [Fact]
    public void Receive_PublishQoS0ToTheHost_AllocatesNothing()
    {
        // Arrange
        (ServerHarness harness, TestConnection connection, _) = Connect(connect: null);
        int messages = 0;
        harness.Handler.Publish = (_, in _) =>
        {
            messages++;
            return MqttPublishResult.Accepted;
        };
        MemoryLink link = connection.Link;
        DeliverEach(link, ClientPackets.PublishQoS0);

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        DeliverEach(link, ClientPackets.PublishQoS0);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        messages.ShouldBe(2 * Iterations);
        link.TakeWritten().ShouldBeEmpty();
    }

    /// <summary>
    /// Proves receiving a QoS 1 PUBLISH, handing it on and writing its PUBACK allocates nothing in an optimized build,
    /// over the idle send path.
    /// </summary>
    [Fact]
    public void Receive_PublishQoS1AndPubAck_AllocatesNothing()
    {
        // Arrange
        (ServerHarness harness, TestConnection connection, _) = Connect(connect: null);
        int messages = 0;
        harness.Handler.Publish = (_, in _) =>
        {
            messages++;
            return MqttPublishResult.Accepted;
        };
        MemoryLink link = connection.Link;
        DeliverEach(link, ClientPackets.PublishQoS1);
        link.TakeWritten();
        byte[] expected = Repeat(ClientPackets.PubAck1, Iterations);

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        DeliverEach(link, ClientPackets.PublishQoS1);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        messages.ShouldBe(2 * Iterations);
        link.TakeWritten().ShouldBe(expected);
    }

    /// <summary>
    /// Proves the whole QoS 2 exchange from the client, PUBLISH answered with PUBREC and PUBREL answered with PUBCOMP,
    /// allocates nothing in an optimized build: the records object comes from the server's pool with the PUBLISH and
    /// goes back to it with the PUBREL, and both answers go over the idle send path.
    /// </summary>
    [Fact]
    public void Receive_PublishQoS2Exchange_AllocatesNothing()
    {
        // Arrange
        (ServerHarness harness, TestConnection connection, MqttWireConnection server) = Connect(connect: null);
        int messages = 0;
        harness.Handler.Publish = (_, in _) =>
        {
            messages++;
            return MqttPublishResult.Accepted;
        };
        MemoryLink link = connection.Link;
        ExchangeQoS2(link);
        link.TakeWritten();
        byte[] expected = Repeat([.. ClientPackets.PubRec7, .. ClientPackets.PubComp7], Iterations);

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        ExchangeQoS2(link);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        messages.ShouldBe(2 * Iterations);
        link.TakeWritten().ShouldBe(expected);
        server.GetCounts().HoldsRecords.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a QoS 0 <c>TrySend</c> on a socket that takes every write at once allocates nothing in an optimized
    /// build: the caller encodes the PUBLISH into a pooled buffer, writes it, and returns the buffer before it returns.
    /// </summary>
    [Fact]
    public void TrySend_QoS0OnAnOpenSocket_AllocatesNothing()
    {
        // Arrange
        (_, TestConnection connection, MqttWireConnection server) = Connect(connect: null);
        MemoryLink link = connection.Link;
        SendEach(server, s_hi, MqttQoS.AtMostOnce);
        link.TakeWritten();
        byte[] expected = Repeat(ClientPackets.PublishQoS0, Iterations);

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        SendEach(server, s_hi, MqttQoS.AtMostOnce);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        link.TakeWritten().ShouldBe(expected);
    }

    /// <summary>
    /// Proves a QoS 1 exchange of a delivery, <c>TrySend</c> and the client's PUBACK, allocates nothing in an optimized
    /// build: the records object comes from the server's pool with the delivery and goes back with its PUBACK.
    /// </summary>
    [Fact]
    public void TrySend_QoS1Exchange_AllocatesNothing()
    {
        // Arrange
        (_, TestConnection connection, MqttWireConnection server) = Connect(connect: null);
        MemoryLink link = connection.Link;
        byte[][] pubAcks = BuildPubAcks(2 * Iterations);
        ExchangeQoS1(server, link, pubAcks, first: 0);
        link.TakeWritten();
        byte[] expected = [.. Enumerable.Range(Iterations + 1, Iterations)
            .SelectMany(static identifier => ServerPackets.Delivery(MqttQoS.AtLeastOnce, (ushort)identifier, "hi"u8))];

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        ExchangeQoS1(server, link, pubAcks, first: Iterations);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        link.TakeWritten().ShouldBe(expected);
        server.GetCounts().HoldsRecords.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a delivery of 200,000 bytes, written as a header and then in pieces of at most 64 KiB straight from the
    /// host's memory, with every write completing at once, allocates nothing in an optimized build: the header's
    /// buffer is pooled and no copy of the payload is made. The first delivery's bytes are checked whole; the link
    /// then only counts the writes, five for each delivery.
    /// </summary>
    [Fact]
    public void TrySend_LargePayloadInPieces_AllocatesNothing()
    {
        // Arrange
        (_, TestConnection connection, MqttWireConnection server) = Connect(connect: null);
        MemoryLink link = connection.Link;
        byte[] payload = ServerPackets.Letters('x', LargePayloadLength);
        server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        link.TakeWritten().ShouldBe(ServerPackets.Delivery(payload));
        link.DiscardsWritten = true;
        SendEach(server, payload, MqttQoS.AtMostOnce);
        int writesBefore = link.WriteCount;

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        SendEach(server, payload, MqttQoS.AtMostOnce);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        (link.WriteCount - writesBefore).ShouldBe(LargeDeliveryWrites * Iterations);
        server.GetCounts().IsWriting.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a <c>TrySend</c> that waits behind a held write allocates nothing in an optimized build once the queue
    /// exists: the queue of waiting deliveries comes from the server's pool and goes back to it when it has been
    /// drained, so adding to it allocates nothing. Each hold of 50 deliveries is started and released outside the
    /// measurement, since the write that is held allocates its awaiting state by design; the 1,000 measured sends are
    /// the sum of the measurements of each hold.
    /// </summary>
    [Fact]
    public void TrySend_BehindAHeldWrite_AllocatesNothingOnceTheQueueExists()
    {
        // Arrange
        (_, TestConnection connection, MqttWireConnection server) = Connect(connect: null);
        MemoryLink link = connection.Link;
        byte[] expected = Repeat(ClientPackets.PublishQoS0, (DeliveriesPerHold + 1) * (Iterations / DeliveriesPerHold));
        SendBehindHeldWrites(connection, server);
        link.TakeWritten();

        // Act
        long allocated = SendBehindHeldWrites(connection, server);

        // Assert
        AssertNothingAllocated(allocated);
        link.TakeWritten().ShouldBe(expected);
        server.GetCounts().WaitingDeliveries.ShouldBe(0);
    }

    /// <summary>
    /// Proves that an answer that waits behind a held write allocates nothing in an optimized build once the control
    /// buffer has been rented: a QoS 1 PUBLISH is read, handed to the host, and its PUBACK appended behind the PUBACKs
    /// that wait. Each hold of 200 publishes is started and released outside the measurement, since the write that is
    /// held allocates its awaiting state by design; the 1,000 measured publishes are the sum of the measurements of
    /// each hold.
    /// </summary>
    [Fact]
    public void Answer_BehindAHeldWrite_AllocatesNothingOnceTheControlBufferIsRented()
    {
        // Arrange
        (ServerHarness harness, TestConnection connection, MqttWireConnection server) = Connect(connect: null);
        int messages = 0;
        harness.Handler.Publish = (_, in _) =>
        {
            messages++;
            return MqttPublishResult.Accepted;
        };
        MemoryLink link = connection.Link;
        byte[] expected = Repeat(
            [.. ClientPackets.PublishQoS0, .. Repeat(ClientPackets.PubAck1, AnswersPerHold)], Iterations / AnswersPerHold);
        AnswerBehindHeldWrites(connection, server);
        link.TakeWritten();

        // Act
        long allocated = AnswerBehindHeldWrites(connection, server);

        // Assert
        AssertNothingAllocated(allocated);
        messages.ShouldBe(2 * Iterations);
        link.TakeWritten().ShouldBe(expected);
        server.GetCounts().WaitingControlPackets.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a QoS 1 delivery that waits for room in the client's window, and is released by the PUBACK of the
    /// delivery before it, allocates nothing in an optimized build: with the client's Receive Maximum 1, A is written,
    /// B waits in a queue from the server's pool, A's PUBACK frees the place and the reading thread writes B, and B's
    /// PUBACK ends its exchange.
    /// </summary>
    [Fact]
    public void TrySend_QoS1WaitingForTheWindow_AllocatesNothingWhenAnAcknowledgementReleasesIt()
    {
        // Arrange
        (_, TestConnection connection, MqttWireConnection server) =
            Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MemoryLink link = connection.Link;
        byte[][] pubAcks = BuildPubAcks(4 * Iterations);
        ReleaseByTheWindowEach(server, link, pubAcks, first: 0);
        link.TakeWritten();
        byte[] expected = [.. Enumerable.Range((2 * Iterations) + 1, 2 * Iterations)
            .SelectMany(static identifier => ServerPackets.Delivery(MqttQoS.AtLeastOnce, (ushort)identifier, "hi"u8))];

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        ReleaseByTheWindowEach(server, link, pubAcks, first: 2 * Iterations);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        link.TakeWritten().ShouldBe(expected);
        server.GetCounts().HoldsRecords.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that the sweep over a thousand idle connections allocates nothing in an optimized build: it walks the
    /// slots without a lock and finds no deadline passed.
    /// </summary>
    [Fact]
    public void Sweep_ThousandIdleConnections_AllocatesNothing()
    {
        // Arrange
        ServerHarness harness = ConnectMany(holdRecords: false);
        SweepEach(harness);

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        SweepEach(harness);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        harness.Server.ConnectionCount.ShouldBe(Connections);
    }

    /// <summary>
    /// Proves that the sweep over a thousand connections that each hold an exchange record, a QoS 1 delivery whose
    /// PUBACK has not come, allocates nothing in an optimized build: it reads the oldest record of each under the
    /// connection's lock and finds no deadline passed.
    /// </summary>
    [Fact]
    public void Sweep_ThousandConnectionsHoldingExchangeRecords_AllocatesNothing()
    {
        // Arrange
        ServerHarness harness = ConnectMany(holdRecords: true);
        SweepEach(harness);

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        SweepEach(harness);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        harness.Server.ConnectionCount.ShouldBe(Connections);
        harness.Handler.Admitted.ShouldAllBe(static connection => connection.GetCounts().DeliveryRecords == 1);
    }

    /// <summary>
    /// Connects a client to a server that rents from the shared pool and whose handler records no calls, and drops
    /// the CONNACK from the written bytes.
    /// </summary>
    /// <param name="connect">
    /// The CONNECT, or <see langword="null"/> for the harness's own.
    /// </param>
    /// <returns>
    /// The harness, the connection, and the server's side of it.
    /// </returns>
    private static (ServerHarness Harness, TestConnection Connection, MqttWireConnection Server) Connect(byte[]? connect)
    {
        ServerHarness harness = ServerHarness.Create(pool: ArrayPool<byte>.Shared);
        harness.Handler.RecordsCalls = false;
        TestConnection connection = harness.Connect(connect: connect);
        return (harness, connection, connection.Connection.ShouldNotBeNull());
    }

    /// <summary>
    /// Connects <see cref="Connections"/> clients to one server, each with a client id of its own and, when asked,
    /// a QoS 1 delivery whose PUBACK never comes.
    /// </summary>
    /// <param name="holdRecords">
    /// Whether each connection is left holding an exchange record.
    /// </param>
    /// <returns>
    /// The harness.
    /// </returns>
    private static ServerHarness ConnectMany(bool holdRecords)
    {
        ServerHarness harness = ServerHarness.Create(pool: ArrayPool<byte>.Shared);
        harness.Handler.RecordsCalls = false;
        for (int i = 0; i < Connections; i++)
        {
            TestConnection connection = harness.Connect($"c{i}");
            if (holdRecords)
            {
                connection.Connection.ShouldNotBeNull().TrySend(ServerHarness.Topic, s_hi, MqttQoS.AtLeastOnce)
                    .ShouldBe(MqttSendResult.Taken);
            }
        }

        return harness;
    }

    /// <summary>
    /// Delivers a packet <see cref="Iterations"/> times, each in a read of its own.
    /// </summary>
    /// <param name="link">
    /// The connection's link, taken from the harness beforehand.
    /// </param>
    /// <param name="packet">
    /// The packet.
    /// </param>
    private static void DeliverEach(MemoryLink link, byte[] packet)
    {
        for (int i = 0; i < Iterations; i++)
        {
            link.Deliver(packet);
        }
    }

    /// <summary>
    /// Runs <see cref="Iterations"/> QoS 2 exchanges from the client with identifier 7: PUBLISH, then PUBREL.
    /// </summary>
    /// <param name="link">
    /// The connection's link, taken from the harness beforehand.
    /// </param>
    private static void ExchangeQoS2(MemoryLink link)
    {
        for (int i = 0; i < Iterations; i++)
        {
            link.Deliver(ClientPackets.PublishQoS2Id7);
            link.Deliver(ClientPackets.PubRel7);
        }
    }

    /// <summary>
    /// Sends a payload to "a/b" <see cref="Iterations"/> times.
    /// </summary>
    /// <param name="server">
    /// The connection.
    /// </param>
    /// <param name="payload">
    /// The payload.
    /// </param>
    /// <param name="qos">
    /// The QoS.
    /// </param>
    private static void SendEach(MqttWireConnection server, byte[] payload, MqttQoS qos)
    {
        for (int i = 0; i < Iterations; i++)
        {
            server.TrySend(ServerHarness.Topic, payload, qos);
        }
    }

    /// <summary>
    /// Runs <see cref="Iterations"/> QoS 1 exchanges of a delivery: <c>TrySend</c>, then the client's PUBACK.
    /// </summary>
    /// <param name="server">
    /// The connection.
    /// </param>
    /// <param name="link">
    /// The connection's link, taken from the harness beforehand.
    /// </param>
    /// <param name="pubAcks">
    /// The PUBACK of each identifier, built beforehand; identifier <c>n</c> at index <c>n - 1</c>.
    /// </param>
    /// <param name="first">
    /// The index of the PUBACK of the first exchange.
    /// </param>
    private static void ExchangeQoS1(MqttWireConnection server, MemoryLink link, byte[][] pubAcks, int first)
    {
        for (int i = 0; i < Iterations; i++)
        {
            server.TrySend(ServerHarness.Topic, s_hi, MqttQoS.AtLeastOnce);
            link.Deliver(pubAcks[first + i]);
        }
    }

    /// <summary>
    /// Runs <see cref="Iterations"/> rounds of two QoS 1 deliveries against a window of 1: A is written, B waits, A's
    /// PUBACK lets B out, and B's PUBACK ends its exchange.
    /// </summary>
    /// <param name="server">
    /// The connection, whose client announced Receive Maximum 1.
    /// </param>
    /// <param name="link">
    /// The connection's link, taken from the harness beforehand.
    /// </param>
    /// <param name="pubAcks">
    /// The PUBACK of each identifier, built beforehand; identifier <c>n</c> at index <c>n - 1</c>.
    /// </param>
    /// <param name="first">
    /// The index of the PUBACK of the first delivery.
    /// </param>
    private static void ReleaseByTheWindowEach(MqttWireConnection server, MemoryLink link, byte[][] pubAcks, int first)
    {
        for (int i = 0; i < Iterations; i++)
        {
            server.TrySend(ServerHarness.Topic, s_hi, MqttQoS.AtLeastOnce);
            server.TrySend(ServerHarness.Topic, s_hi, MqttQoS.AtLeastOnce);
            link.Deliver(pubAcks[first + (2 * i)]);
            link.Deliver(pubAcks[first + (2 * i) + 1]);
        }
    }

    /// <summary>
    /// Sends <see cref="Iterations"/> QoS 0 deliveries behind held writes, <see cref="DeliveriesPerHold"/> behind
    /// each, and measures only the sends that wait: each hold's write is started, and released, outside the
    /// measurement.
    /// </summary>
    /// <param name="connection">
    /// The connection.
    /// </param>
    /// <param name="server">
    /// The server's side of it.
    /// </param>
    /// <returns>
    /// The bytes the measured sends allocated, together.
    /// </returns>
    private static long SendBehindHeldWrites(TestConnection connection, MqttWireConnection server)
    {
        long allocated = 0;
        for (int hold = 0; hold < Iterations / DeliveriesPerHold; hold++)
        {
            connection.Link.HoldWrites();
            server.TrySend(ServerHarness.Topic, s_hi, MqttQoS.AtMostOnce);
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < DeliveriesPerHold; i++)
            {
                server.TrySend(ServerHarness.Topic, s_hi, MqttQoS.AtMostOnce);
            }

            allocated += GC.GetAllocatedBytesForCurrentThread() - before;
            connection.CompleteHeldWrites();
        }

        return allocated;
    }

    /// <summary>
    /// Delivers <see cref="Iterations"/> QoS 1 publishes behind held writes, <see cref="AnswersPerHold"/> behind each,
    /// and measures only the publishes, whose PUBACKs wait: each hold's write is started, and released, outside the
    /// measurement.
    /// </summary>
    /// <param name="connection">
    /// The connection.
    /// </param>
    /// <param name="server">
    /// The server's side of it.
    /// </param>
    /// <returns>
    /// The bytes the measured publishes allocated, together.
    /// </returns>
    private static long AnswerBehindHeldWrites(TestConnection connection, MqttWireConnection server)
    {
        MemoryLink link = connection.Link;
        long allocated = 0;
        for (int hold = 0; hold < Iterations / AnswersPerHold; hold++)
        {
            link.HoldWrites();
            server.TrySend(ServerHarness.Topic, s_hi, MqttQoS.AtMostOnce);
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < AnswersPerHold; i++)
            {
                link.Deliver(ClientPackets.PublishQoS1);
            }

            allocated += GC.GetAllocatedBytesForCurrentThread() - before;
            connection.CompleteHeldWrites();
        }

        return allocated;
    }

    /// <summary>
    /// Runs the sweep <see cref="Iterations"/> times, with the clock where it is, so that no deadline has passed.
    /// </summary>
    /// <param name="harness">
    /// The harness whose server sweeps.
    /// </param>
    private static void SweepEach(ServerHarness harness)
    {
        MqttWireServer server = harness.Server;
        for (int i = 0; i < Iterations; i++)
        {
            server.Sweep();
        }
    }

    /// <summary>
    /// Builds the PUBACK with success of every identifier from 1 on.
    /// </summary>
    /// <param name="count">
    /// How many identifiers.
    /// </param>
    /// <returns>
    /// The PUBACK of identifier <c>n</c> at index <c>n - 1</c>.
    /// </returns>
    private static byte[][] BuildPubAcks(int count) =>
        [.. Enumerable.Range(1, count).Select(static identifier => ServerPackets.PubAck((ushort)identifier))];

    /// <summary>
    /// Asserts a measurement found no allocation, in an optimized build of the server.
    /// </summary>
    /// <param name="allocated">
    /// The bytes the measured work allocated.
    /// </param>
    private static void AssertNothingAllocated(long allocated)
    {
        if (s_serverIsOptimized)
        {
            allocated.ShouldBe(0);
        }
    }

    /// <summary>
    /// Repeats a packet.
    /// </summary>
    /// <param name="packet">
    /// The packet.
    /// </param>
    /// <param name="count">
    /// How many times.
    /// </param>
    /// <returns>
    /// The packet <paramref name="count"/> times over.
    /// </returns>
    private static byte[] Repeat(byte[] packet, int count)
    {
        byte[] bytes = new byte[packet.Length * count];
        for (int i = 0; i < count; i++)
        {
            packet.CopyTo(bytes, i * packet.Length);
        }

        return bytes;
    }
}
