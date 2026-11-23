using Microsoft.AspNetCore.Http;
using MqttWire.Server;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Server;

/// <summary>
/// Measures what an idle, connected connection costs over the in-memory link (plan decision 18): at most 768 bytes,
/// which leaves 256 of the 1 KB target for the <c>WebSocketLink</c> and its parked wait. The cost is the difference
/// between two readings of the managed heap: one with 2,000 sets of the test's own scaffolding alone (a
/// <see cref="MemoryLink"/>, the HTTP context and its cancellation source, the <see cref="TestConnection"/>), and one
/// with 2,000 more sets that carry a connected, idle connection each.
/// </summary>
/// <remarks>
/// The heap is the process's, so anything else that allocates between two readings is charged to the connections.
/// Two things keep that out. First, the class is in a collection whose definition disables parallelization: xUnit 2.9
/// runs every parallel collection of the assembly first, and only when they have all finished runs the collections
/// that disable it, one at a time, so no other test of <c>MqttWire.Tests</c> runs while this one measures. Work a
/// finished test left behind (a continuation, a finalizer) can still allocate, so, second, each reading follows full
/// blocking, compacting collections and <see cref="GC.WaitForPendingFinalizers"/>, and the whole measurement is
/// taken up to <see cref="MaxAttempts"/> times, the smallest figure counting. Noise mostly adds bytes (something
/// else allocated between two readings), so the smallest figure is the closest to the true cost. It can also take
/// bytes away, when something transient was alive at the baseline reading and gone at the last one; spread over
/// 2,000 connections that is a few bytes per connection at most, far less than the room between the figure
/// measured and the bound, so the bound is not loosened by it.
/// </remarks>
[Collection(nameof(IdleCostTests))]
[CollectionDefinition(nameof(IdleCostTests), DisableParallelization = true)]
public sealed class IdleCostTests
{
    /// <summary>
    /// The number of connections measured, and of scaffolding sets in the baseline.
    /// </summary>
    private const int Count = 2_000;

    /// <summary>
    /// The most an idle, connected connection may cost, in bytes.
    /// </summary>
    private const int MaxBytesPerConnection = 768;

    /// <summary>
    /// The most times the measurement is taken; it stops at the first figure within the bound.
    /// </summary>
    private const int MaxAttempts = 3;

    /// <summary>
    /// The payload of the burst's messages, shared by all of them: 256 bytes, as in the benchmark.
    /// </summary>
    private static readonly byte[] s_burstPayload = new byte[256];

    /// <summary>
    /// Proves that 2,000 connected, idle connections whose host keeps no state cost at most 768 bytes each beyond the
    /// test's own scaffolding.
    /// </summary>
    [Fact]
    public void Connect_TwoThousandIdleConnections_KeepAtMost768BytesEach()
    {
        // Arrange
        List<long> figures = [];
        long smallest = long.MaxValue;

        // Act
        for (int attempt = 0; attempt < MaxAttempts && smallest > MaxBytesPerConnection; attempt++)
        {
            long perConnection = MeasureBytesPerConnection(afterActivity: false);
            figures.Add(perConnection);
            smallest = Math.Min(smallest, perConnection);
        }

        // Assert
        smallest.ShouldBeLessThanOrEqualTo(MaxBytesPerConnection,
            $"An idle connection cost {smallest} bytes (the smallest of {figures.Count} measurements: "
            + $"{string.Join(", ", figures)} bytes per connection); the bound is {MaxBytesPerConnection}.");
    }

    /// <summary>
    /// Proves that a connection costs MqttWire no more after activity than the bound for one that has done nothing
    /// (design goal 2, "before and after activity alike"): 2,000 connections each publish at QoS 0 and at QoS 2 and
    /// receive and acknowledge a burst of ten messages at QoS 1, and then keep at most 768 bytes each.
    /// </summary>
    [Fact]
    public void Activity_TwoThousandConnectionsThenQuiet_KeepAtMost768BytesEach()
    {
        // Arrange
        List<long> figures = [];
        long smallest = long.MaxValue;

        // Act
        for (int attempt = 0; attempt < MaxAttempts && smallest > MaxBytesPerConnection; attempt++)
        {
            long perConnection = MeasureBytesPerConnection(afterActivity: true);
            figures.Add(perConnection);
            smallest = Math.Min(smallest, perConnection);
        }

        // Assert
        smallest.ShouldBeLessThanOrEqualTo(MaxBytesPerConnection,
            $"A connection that is quiet after activity cost {smallest} bytes (the smallest of {figures.Count} "
            + $"measurements: {string.Join(", ", figures)} bytes per connection); the bound is {MaxBytesPerConnection}.");
    }

    /// <summary>
    /// Takes the measurement once, on a server of its own: a warm-up connection, then the heap with nothing more,
    /// with 2,000 sets of scaffolding, and with 2,000 connected, idle connections besides.
    /// </summary>
    /// <param name="afterActivity">
    /// Whether every connection, the warm-up one first, goes through <see cref="RunActivity"/> before the last
    /// reading, so that the figure is that of a connection that is quiet after activity.
    /// </param>
    /// <returns>
    /// The bytes per connection beyond the scaffolding; at least the true cost, more when something else allocated
    /// between the readings.
    /// </returns>
    private static long MeasureBytesPerConnection(bool afterActivity)
    {
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Connect = static (_, _) => ValueTask.FromResult(MqttConnectResult.Accept(state: null));

        // One connection first, so that what is created once (the JIT's work, the pool's buffers, the statics) is in
        // every reading.
        TestConnection warmUp = harness.Connect("warm-up");
        if (afterActivity)
        {
            RunActivity(harness, warmUp, "warm-up");
        }

        ForgetCalls(harness);
        long empty = ReadHeap();
        TestConnection[] baseline = new TestConnection[Count];
        for (int i = 0; i < Count; i++)
        {
            baseline[i] = CreateScaffolding(harness);
        }

        long scaffoldingAlone = ReadHeap();
        TestConnection[] connected = new TestConnection[Count];
        for (int i = 0; i < Count; i++)
        {
            connected[i] = harness.Connect($"c{i}");
        }

        if (afterActivity)
        {
            for (int i = 0; i < Count; i++)
            {
                RunActivity(harness, connected[i], $"c{i}");
            }
        }

        ForgetCalls(harness);
        long withConnected = ReadHeap();
        harness.Server.ConnectionCount.ShouldBe(Count + 1);
        GC.KeepAlive(warmUp);
        GC.KeepAlive(baseline);
        GC.KeepAlive(connected);
        // The scaffolding is subtracted once, not twice: the 2,000 connections do not use the 2,000 sets created
        // above, each brings a set of its own (harness.Connect creates the link, the context and the test
        // connection). So the last step added 2,000 sets and 2,000 connections, and the first measured 2,000 sets.
        long scaffoldingBytes = scaffoldingAlone - empty;
        return (withConnected - scaffoldingAlone - scaffoldingBytes) / Count;
    }

    /// <summary>
    /// The activity of the benchmark (design 6.14 item 3) on one connection: it publishes one message at QoS 0 and
    /// one at QoS 2, with the full exchange, and receives ten messages of 256 bytes at QoS 1, which it acknowledges.
    /// The link is emptied after every step, so the test's own link never grows, and the connection is asserted to
    /// hold nothing at the end.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <param name="connection">
    /// The connected test connection.
    /// </param>
    /// <param name="clientId">
    /// Its client id, by which the server finds it: the handler's own records may have been forgotten.
    /// </param>
    private static void RunActivity(ServerHarness harness, TestConnection connection, string clientId)
    {
        harness.Server.TryGetConnection(clientId, out MqttWireConnection? found).ShouldBeTrue();
        MqttWireConnection server = found.ShouldNotBeNull();
        connection.Deliver(ServerPackets.BuildPublish(MqttQoS.AtMostOnce, "a/b"u8, packetIdentifier: 0, properties: [], "hi"u8));
        connection.Deliver(ServerPackets.BuildPublish(MqttQoS.ExactlyOnce, "a/b"u8, packetIdentifier: 1, properties: [], "hi"u8));
        connection.TakeWritten().ShouldBe(ServerPackets.PubRec(1));
        connection.Deliver(ServerPackets.PubRel(1));
        connection.TakeWritten().ShouldBe(ServerPackets.PubComp(1));
        for (int message = 0; message < 10; message++)
        {
            server.TrySend(ServerHarness.Topic, s_burstPayload, MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
        }

        connection.TakeWritten().Length.ShouldBeGreaterThan(10 * s_burstPayload.Length);
        for (ushort identifier = 1; identifier <= 10; identifier++)
        {
            connection.Deliver(ServerPackets.PubAck(identifier));
        }

        connection.TakeWritten().ShouldBeEmpty();
        server.GetCounts().ShouldBe(ServerHarness.Idle);
    }

    /// <summary>
    /// Reads the size of the managed heap after full, blocking, compacting collections with the pending finalizers
    /// run in between, so that neither garbage nor objects waiting for their finalizer are counted.
    /// </summary>
    /// <returns>
    /// The bytes the heap holds.
    /// </returns>
    private static long ReadHeap()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        return GC.GetTotalMemory(forceFullCollection: true);
    }

    /// <summary>
    /// Creates what the harness creates around a connection, without the connection: a link, an HTTP context with
    /// its cancellation source, with the members the connect phase reads already created, and a test connection.
    /// </summary>
    /// <param name="harness">
    /// The harness the test connection belongs to.
    /// </param>
    /// <returns>
    /// The test connection, whose run has completed and which no server knows.
    /// </returns>
    private static TestConnection CreateScaffolding(ServerHarness harness)
    {
        MemoryLink link = new();
        CancellationTokenSource requestAborted = new();
        DefaultHttpContext httpContext = new() { RequestAborted = requestAborted.Token };
        _ = httpContext.Connection.RemoteIpAddress;
        _ = httpContext.Connection.RemotePort;
        _ = httpContext.Request.IsHttps;
        return new TestConnection(harness, link, Task.CompletedTask, requestAborted, httpContext);
    }

    /// <summary>
    /// Empties the handler's records, the test's own, so that they are not counted as the connections' cost.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    private static void ForgetCalls(ServerHarness harness) => harness.Handler.Forget();
}
