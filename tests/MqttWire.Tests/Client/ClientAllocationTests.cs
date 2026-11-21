using System.Diagnostics;
using System.Reflection;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Client;

/// <summary>
/// Proves the client's hot paths allocate nothing in steady state (AGENTS "Hot Paths"): the read loop receiving at
/// QoS 0, 1 and 2 and sending its acknowledgements, a QoS 0 publish, and the keep-alive's PINGREQ and PINGRESP.
/// Each test runs its work once as a warm-up, so that every buffer and pool has reached its size, then measures
/// <see cref="Iterations"/> more as the difference of two readings of the allocated bytes of the current thread.
/// All measured work runs on the test thread and completes synchronously: the in-memory link runs the read loop
/// inline, its writes complete at once, so the send lock is never contended (a contended
/// <see cref="SemaphoreSlim.WaitAsync()"/> allocates by design), and no request completion is awaited. Everything
/// the measured work touches is taken before the first reading, the link too (<see cref="ClientHarness.Link"/>
/// copies the connector's list on every call). The tests run alone, not in parallel with others.
/// </summary>
[Collection(nameof(ClientAllocationTests))]
[CollectionDefinition(nameof(ClientAllocationTests), DisableParallelization = true)]
public sealed class ClientAllocationTests
{
    /// <summary>
    /// The number of measured iterations, and of warm-up iterations before them.
    /// </summary>
    private const int Iterations = 1_000;

    /// <summary>
    /// The payload of the publishes these tests send, "hi".
    /// </summary>
    private static readonly byte[] s_hi = "hi"u8.ToArray();

    /// <summary>
    /// Whether the client's assembly was built with optimizations, as it ships. A Debug build compiles every async
    /// method's state machine as a class, which the read loop and the send path then allocate, so there these tests
    /// check only what was sent and handled; CI runs them in Release.
    /// </summary>
    private static readonly bool s_clientIsOptimized =
        typeof(MqttWireClient).Assembly.GetCustomAttribute<DebuggableAttribute>()?.IsJITOptimizerDisabled != true;

    /// <summary>
    /// Proves receiving a QoS 0 PUBLISH and handing it to <c>OnMessage</c> allocates nothing in an optimized build:
    /// the read buffer is rented and returned, the packet decoded in place.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Receive_PublishQoS0_AllocatesNothing()
    {
        // Arrange
        int messages = 0;
        ClientHarness harness = await ConnectAsync((in _) => messages++);
        MemoryLink link = harness.Link;
        DeliverEach(link, ClientPackets.PublishQoS0);

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        DeliverEach(link, ClientPackets.PublishQoS0);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        messages.ShouldBe(2 * Iterations);
        link.TakeWritten().ShouldBeEmpty();
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves receiving a QoS 1 PUBLISH, handing it on and writing its PUBACK allocates nothing in an optimized
    /// build, over the uncontended send path.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Receive_PublishQoS1AndSendPubAck_AllocatesNothing()
    {
        // Arrange
        int messages = 0;
        ClientHarness harness = await ConnectAsync((in _) => messages++);
        MemoryLink link = harness.Link;
        DeliverEach(link, ClientPackets.PublishQoS1);
        link.TakeWritten();

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        DeliverEach(link, ClientPackets.PublishQoS1);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        messages.ShouldBe(2 * Iterations);
        link.TakeWritten().ShouldBe(Repeat(ClientPackets.PubAck1, Iterations));
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves the whole QoS 2 exchange from the server, PUBLISH answered with PUBREC and PUBREL answered with
    /// PUBCOMP, allocates nothing in an optimized build: the identifier is remembered and forgotten in a table
    /// sized once, and both answers go over the uncontended send path.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Receive_PublishQoS2Exchange_AllocatesNothing()
    {
        // Arrange
        int messages = 0;
        ClientHarness harness = await ConnectAsync((in _) => messages++);
        MemoryLink link = harness.Link;
        ExchangeQoS2(link);
        link.TakeWritten();

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        ExchangeQoS2(link);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        messages.ShouldBe(2 * Iterations);
        link.TakeWritten().ShouldBe(Repeat([.. ClientPackets.PubRec7, .. ClientPackets.PubComp7], Iterations));
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a QoS 0 publish allocates nothing in an optimized build: the topic is encoded on the stack, the packet
    /// into a pooled buffer, and the uncontended send path writes it and returns the buffer before the call returns.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_QoS0_AllocatesNothing()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(null);
        MemoryLink link = harness.Link;
        PublishEach(harness.Client);
        link.TakeWritten();

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        PublishEach(harness.Client);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        link.TakeWritten().ShouldBe(Repeat(ClientPackets.PublishQoS0, Iterations));
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves keeping an idle connection alive allocates nothing in an optimized build: each tick of the fake clock
    /// sends PINGREQ from a pooled buffer over the uncontended send path, and each PINGRESP is read and noted.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task KeepAlive_PingReqAndPingResp_AllocatesNothing()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(null);
        MemoryLink link = harness.Link;
        PingEach(harness, link);
        link.TakeWritten();

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        PingEach(harness, link);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        AssertNothingAllocated(after - before);
        link.TakeWritten().ShouldBe(Repeat(ClientPackets.PingReq, Iterations));
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Connects a client over the shared pool and drops its CONNECT from the written bytes.
    /// </summary>
    /// <param name="onMessage">
    /// The message handler, or <see langword="null"/>.
    /// </param>
    /// <returns>
    /// The connected client's harness.
    /// </returns>
    private static async Task<ClientHarness> ConnectAsync(MqttMessageHandler? onMessage)
    {
        ClientHarness harness = ClientHarness.Create(onMessage: onMessage);
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.Link.TakeWritten();
        return harness;
    }

    /// <summary>
    /// Delivers a packet <see cref="Iterations"/> times, each in a read of its own.
    /// </summary>
    /// <param name="link">
    /// The connected client's link, taken from the harness beforehand: <see cref="ClientHarness.Link"/> copies the
    /// connector's list on every call.
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
    /// Runs <see cref="Iterations"/> QoS 2 exchanges with identifier 7: PUBLISH, then PUBREL.
    /// </summary>
    /// <param name="link">
    /// The connected client's link, taken from the harness beforehand.
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
    /// Publishes "hi" to "a/b" at QoS 0 <see cref="Iterations"/> times.
    /// </summary>
    /// <param name="client">
    /// The connected client.
    /// </param>
    private static void PublishEach(MqttWireClient client)
    {
        for (int i = 0; i < Iterations; i++)
        {
            _ = client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        }
    }

    /// <summary>
    /// Advances the clock by half the 15-second keep-alive, which sends PINGREQ, and answers with PINGRESP,
    /// <see cref="Iterations"/> times.
    /// </summary>
    /// <param name="harness">
    /// The connected client's harness, whose fake clock is advanced.
    /// </param>
    /// <param name="link">
    /// The connected client's link, taken from the harness beforehand.
    /// </param>
    private static void PingEach(ClientHarness harness, MemoryLink link)
    {
        for (int i = 0; i < Iterations; i++)
        {
            harness.Time.Advance(TimeSpan.FromSeconds(7.5));
            link.Deliver(ClientPackets.PingResp);
        }
    }

    /// <summary>
    /// Asserts a measurement found no allocation, in an optimized build of the client.
    /// </summary>
    /// <param name="allocated">
    /// The bytes the measured work allocated.
    /// </param>
    private static void AssertNothingAllocated(long allocated)
    {
        if (s_clientIsOptimized)
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
