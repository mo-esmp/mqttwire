using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests who owns the client's buffers (design 6.8, 6.13 items 4 and 5): every way out returns every buffer exactly
/// once, an idle connection holds none, a buffer handed to a read or a write stays the operation's until it has
/// ended, also when the link is aborted meanwhile, and connect, work and disconnect cycles leave nothing behind.
/// The pool's count is taken after every step, not only at the end.
/// </summary>
public sealed class ClientBufferTests
{
    /// <summary>
    /// The payload of the publishes these tests send, "hi".
    /// </summary>
    private static readonly byte[] s_hi = "hi"u8.ToArray();

    /// <summary>
    /// The first four bytes of <see cref="ClientPackets.PublishQoS0"/>: its whole fixed header and the start of its
    /// topic, which the read loop keeps in its buffer until the rest comes.
    /// </summary>
    private static readonly byte[] s_publishQoS0Start = ClientPackets.PublishQoS0[..4];

    /// <summary>
    /// The rest of <see cref="ClientPackets.PublishQoS0"/> after <see cref="s_publishQoS0Start"/>.
    /// </summary>
    private static readonly byte[] s_publishQoS0Rest = ClientPackets.PublishQoS0[4..];

    /// <summary>
    /// A way a connection, or a connect, ends.
    /// </summary>
    public enum WayOut
    {
        /// <summary>
        /// The server sends DISCONNECT.
        /// </summary>
        ServerDisconnects,

        /// <summary>
        /// The server ends the link.
        /// </summary>
        PeerEndsTheLink,

        /// <summary>
        /// A read fails.
        /// </summary>
        ReadFails,

        /// <summary>
        /// A write fails.
        /// </summary>
        WriteFails,

        /// <summary>
        /// A PINGREQ goes unanswered for a full interval.
        /// </summary>
        KeepAliveTimesOut,

        /// <summary>
        /// A QoS 1 publish goes unacknowledged for twice the interval.
        /// </summary>
        AcknowledgementTimesOut,

        /// <summary>
        /// The server sends a packet a client never receives, PINGREQ.
        /// </summary>
        ProtocolError,

        /// <summary>
        /// <c>OnMessage</c> throws.
        /// </summary>
        OnMessageThrows,

        /// <summary>
        /// The application disposes the client.
        /// </summary>
        DisposeAsync,

        /// <summary>
        /// No CONNACK comes within ten seconds.
        /// </summary>
        ConnectTimesOut,

        /// <summary>
        /// The CONNACK refuses the connection.
        /// </summary>
        ConnAckRefuses,
    }

    /// <summary>
    /// Proves that each way out of a connection, or of a connect, returns every buffer exactly once: the pool is
    /// back to 0 after every step but the one where the read loop legitimately holds a packet cut short, and no
    /// buffer is returned twice. Before a connected way out, a publish, a received message and its PUBACK have
    /// gone through, and half a packet waits in the read buffer.
    /// </summary>
    /// <param name="wayOut">
    /// How the connection ends.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(WayOut.ServerDisconnects)]
    [InlineData(WayOut.PeerEndsTheLink)]
    [InlineData(WayOut.ReadFails)]
    [InlineData(WayOut.WriteFails)]
    [InlineData(WayOut.KeepAliveTimesOut)]
    [InlineData(WayOut.AcknowledgementTimesOut)]
    [InlineData(WayOut.ProtocolError)]
    [InlineData(WayOut.OnMessageThrows)]
    [InlineData(WayOut.DisposeAsync)]
    [InlineData(WayOut.ConnectTimesOut)]
    [InlineData(WayOut.ConnAckRefuses)]
    public async Task EveryWayOut_ReturnsEveryBufferOnce(WayOut wayOut)
    {
        // Arrange
        CountingPool pool = new();
        bool handlerThrows = false;
        ClientHarness harness = ClientHarness.Create(
            pool: pool,
            onMessage: (in _) =>
            {
                if (handlerThrows)
                {
                    throw new InvalidOperationException("handler failed");
                }
            });
        List<string> steps = [];
        bool connects = wayOut is not (WayOut.ConnectTimesOut or WayOut.ConnAckRefuses);

        // Act
        Task<MqttConnectAck> connect = harness.Client.ConnectAsync();
        steps.Add($"connecting: {pool.Outstanding}");
        ValueTask dispose = default;
        if (connects)
        {
            harness.Link.Deliver(wayOut == WayOut.AcknowledgementTimesOut ? ClientPackets.ConnAckServerKeepAlive0 : ClientPackets.ConnAckAccepted);
            await connect;
            steps.Add($"connected: {pool.Outstanding}");
            await WorkAsync(harness);
            steps.Add($"worked: {pool.Outstanding}");
            harness.Link.Deliver(s_publishQoS0Start);
            steps.Add($"packet cut short: {pool.Outstanding}");
            handlerThrows = wayOut == WayOut.OnMessageThrows;
            dispose = TakeWayOut(harness, wayOut);
        }
        else if (wayOut == WayOut.ConnectTimesOut)
        {
            harness.Time.Advance(TimeSpan.FromSeconds(10));
        }
        else
        {
            harness.Link.Deliver(ClientPackets.ConnAckBadUserNameOrPassword);
        }

        steps.Add($"way out: {pool.Outstanding}");
        harness.Link.EndFromPeer();
        await dispose;
        await harness.Client.DisposeAsync();
        MqttConnectAck ack = await connect;
        steps.Add($"ended: {pool.Outstanding}");

        // Assert
        // Only a dispose leaves the read loop holding the cut-short packet until the server ends the link.
        string[] expectedSteps = connects
            ? ["connecting: 0", "connected: 0", "worked: 0", "packet cut short: 1", $"way out: {(wayOut == WayOut.DisposeAsync ? 1 : 0)}", "ended: 0"]
            : ["connecting: 0", "way out: 0", "ended: 0"];
        MqttClientCloseReason[] expectedClosed = connects ? [ExpectedReason(wayOut)] : [];
        steps.ShouldBe(expectedSteps);
        pool.FaultyReturns.ShouldBe(0);
        harness.Closed.Select(static info => info.Reason).ShouldBe(expectedClosed);
        ack.Failure.ShouldBe(connects ? null : (MqttClientCloseReason?)ExpectedReason(wayOut));
    }

    /// <summary>
    /// Proves the read loop holds no buffer between packets: once a PUBLISH has been handed to <c>OnMessage</c>
    /// (which sees it in a rented buffer) and its PUBACK written, and once a packet that came in two reads is
    /// whole and handled, nothing is out of the pool.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Idle_AfterAPacket_HoldsNoReadBuffer()
    {
        // Arrange
        CountingPool pool = new();
        List<int> duringOnMessage = [];
        ClientHarness harness = ClientHarness.Create(pool: pool, onMessage: (in _) => duringOnMessage.Add(pool.Outstanding));
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        List<string> steps = [$"connected: {pool.Outstanding}"];

        // Act
        harness.Link.Deliver(ClientPackets.PublishQoS0);
        steps.Add($"QoS 0 handled: {pool.Outstanding}");
        harness.Link.Deliver(ClientPackets.PublishQoS1);
        steps.Add($"QoS 1 handled and acknowledged: {pool.Outstanding}");
        harness.Link.Deliver(s_publishQoS0Start);
        steps.Add($"packet cut short: {pool.Outstanding}");
        harness.Link.Deliver(s_publishQoS0Rest);
        steps.Add($"rest handled: {pool.Outstanding}");

        // Assert
        steps.ShouldBe(["connected: 0", "QoS 0 handled: 0", "QoS 1 handled and acknowledged: 0", "packet cut short: 1", "rest handled: 0"]);
        duringOnMessage.ShouldBe([1, 1, 1]);
        harness.Client.IsConnected.ShouldBeTrue();
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves a write still in flight keeps its buffer when the link is aborted under it: disposing sends
    /// DISCONNECT behind a held QoS 1 publish, the close deadline aborts the link and the read loop finishes, yet
    /// the publish's buffer stays out, and the dispose waits, until the write ends with an exception; then every
    /// buffer is back. The DISCONNECT's own buffer, queued behind it, is the second one out.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_WriteHeld_BufferNotReturnedUntilTheWriteEnds()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.Link.HoldWrites();
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        List<string> steps = [$"publish held: {pool.Outstanding}"];

        // Act
        ValueTask dispose = harness.Client.DisposeAsync();
        steps.Add($"disposing: {pool.Outstanding}");
        harness.Time.Advance(TimeSpan.FromSeconds(2));
        bool aborted = harness.Link.IsAborted;
        int closedReports = harness.Closed.Count;
        bool heldBufferOut = pool.IsViewIntoRentedBuffer(harness.Link.HeldWriteBuffer.Span);
        bool disposedBeforeTheWriteEnded = dispose.IsCompleted;
        steps.Add($"aborted: {pool.Outstanding}");
        harness.Link.CompleteHeldWrites(new OperationCanceledException());
        await dispose;
        steps.Add($"write ended: {pool.Outstanding}");
        MqttRequestResult result = await publish;

        // Assert
        steps.ShouldBe(["publish held: 1", "disposing: 2", "aborted: 2", "write ended: 0"]);
        aborted.ShouldBeTrue();
        closedReports.ShouldBe(1);
        heldBufferOut.ShouldBeTrue();
        disposedBeforeTheWriteEnded.ShouldBeFalse();
        result.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves <see cref="MqttWireClient.DisposeAsync"/> keeps its promise that every buffer is back when it
    /// completes, also after an end that aborted the link while a write was in flight: here a failed read ends the
    /// connection, the read loop finishes and <c>OnClosed</c> runs while the held publish still owns its buffer;
    /// the dispose then waits until that write has ended.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task DisposeAsync_ReadFailedWhileAWriteIsHeld_CompletesOnlyOnceTheWriteEnds()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.Link.HoldWrites();
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        List<string> steps = [$"publish held: {pool.Outstanding}"];

        // Act
        harness.Link.FailReads(new IOException("read failed"));
        steps.Add($"read failed: {pool.Outstanding}");
        ValueTask dispose = harness.Client.DisposeAsync();
        bool disposedBeforeTheWriteEnded = dispose.IsCompleted;
        steps.Add($"disposing: {pool.Outstanding}");
        harness.Link.CompleteHeldWrites(new OperationCanceledException());
        await dispose;
        steps.Add($"disposed: {pool.Outstanding}");

        // Assert
        steps.ShouldBe(["publish held: 1", "read failed: 1", "disposing: 1", "disposed: 0"]);
        disposedBeforeTheWriteEnded.ShouldBeFalse();
        harness.Closed.Select(static info => info.Reason).ShouldBe([MqttClientCloseReason.ConnectionLost]);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves the buffer of a write still in flight is never handed to anyone else while the link is aborted
    /// under it: with a pool that overwrites every returned array and hands it out again at once, the held
    /// publish's buffer still holds its PUBLISH after the abort and after other rents, and when the write then
    /// completes successfully, at the moment of the abort, the link takes the PUBLISH, not overwritten bytes.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_WriteHeld_InFlightBufferIsNeverOverwritten()
    {
        // Arrange
        OverwritingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();
        harness.Link.HoldWrites();
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        ReadOnlyMemory<byte> held = harness.Link.HeldWriteBuffer;

        // Act
        ValueTask dispose = harness.Client.DisposeAsync();
        harness.Time.Advance(TimeSpan.FromSeconds(2));
        byte[] afterTheAbort = held.ToArray();
        byte[][] others = [pool.Rent(held.Length), pool.Rent(held.Length), pool.Rent(4096)];
        foreach (byte[] other in others)
        {
            other.AsSpan().Fill(0x11);
        }

        byte[] afterOtherRents = held.ToArray();
        bool heldStillOut = pool.IsRented(held);
        harness.Link.CompleteHeldWrites();
        await dispose;
        byte[] written = harness.Link.TakeWritten();

        // Assert
        afterTheAbort.ShouldBe(ClientPackets.PublishQoS1);
        afterOtherRents.ShouldBe(ClientPackets.PublishQoS1);
        heldStillOut.ShouldBeTrue();
        others.ShouldAllBe(other => !SameArray(other, held));
        written.ShouldBe(ClientPackets.PublishQoS1);
        pool.IsRented(held).ShouldBeFalse();
        pool.Outstanding.ShouldBe(others.Length);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves the buffer of a read still in flight is never handed to anyone else while the link is aborted under
    /// it: with a pool that overwrites and reuses every returned array, a read pending when the close deadline
    /// aborts the link keeps its buffer out through the abort and other rents; when data then completes the read,
    /// at the moment of the abort, it lands in the read's own buffer and changes no array rented by anyone else;
    /// and only then is the read buffer returned.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Abort_ReadPending_ReadBufferNotReusedUntilTheReadEnds()
    {
        // Arrange
        OverwritingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.Link.SkipWaits();
        harness.Link.HoldReadsOnAbort();
        harness.Link.Deliver(ClientPackets.PingResp);
        Memory<byte> pending = harness.Link.PendingReadBuffer;
        List<string> steps = [$"read pending: {pool.Outstanding}"];

        // Act
        ValueTask dispose = harness.Client.DisposeAsync();
        harness.Time.Advance(TimeSpan.FromSeconds(2));
        bool aborted = harness.Link.IsAborted;
        steps.Add($"aborted: {pool.Outstanding}");
        byte[][] others = [pool.Rent(pending.Length), pool.Rent(16), pool.Rent(4096)];
        foreach (byte[] other in others)
        {
            other.AsSpan().Fill(0x11);
        }

        bool readBufferOut = pool.IsRented(pending);
        steps.Add($"other rents: {pool.Outstanding}");
        harness.Link.Deliver(ClientPackets.PublishQoS0);
        await dispose;
        steps.Add($"read ended: {pool.Outstanding}");

        // Assert
        aborted.ShouldBeTrue();
        steps.ShouldBe(["read pending: 1", "aborted: 1", "other rents: 4", "read ended: 3"]);
        readBufferOut.ShouldBeTrue();
        others.ShouldAllBe(other => !SameArray(other, pending) && other.All(static b => b == 0x11));
        pool.IsRented(pending).ShouldBeFalse();
        harness.Closed.Select(static info => info.Reason).ShouldBe([MqttClientCloseReason.Disposed]);
        pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a hundred connect, work and disconnect cycles, each with a new client on one pool, leave
    /// nothing behind: after each cycle no buffer is out and none was returned twice, and once collected, no
    /// client is still reachable.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectWorkDisconnect_HundredCycles_NothingAccumulates()
    {
        // Arrange
        const int Cycles = 100;
        CountingPool pool = new();
        List<WeakReference> clients = [];
        List<int> outstanding = [];

        // Act
        for (int cycle = 0; cycle < Cycles; cycle++)
        {
            clients.Add(await RunCycleAsync(pool));
            outstanding.Add(pool.Outstanding);
        }

        // The last dispose completed on the thread pool, and this method runs on inside that continuation, whose
        // frames still reference the last client; yielding leaves them before the collection.
        await Task.Yield();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        int alive = clients.Count(static client => client.IsAlive);

        // Assert
        outstanding.ShouldAllBe(count => count == 0);
        pool.FaultyReturns.ShouldBe(0);
        alive.ShouldBe(0);
    }

    /// <summary>
    /// Connects a new client, publishes at QoS 1, receives at QoS 2, and disposes it.
    /// </summary>
    /// <param name="pool">
    /// The pool the client rents from.
    /// </param>
    /// <returns>
    /// A weak reference to the client, which this method no longer holds once it has returned.
    /// </returns>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static async Task<WeakReference> RunCycleAsync(CountingPool pool)
    {
        ClientHarness harness = ClientHarness.Create(pool: pool, onMessage: static (in _) => { });
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        harness.Link.Deliver(ClientPackets.PubAck1);
        await publish;
        harness.Link.Deliver(ClientPackets.PublishQoS2Id7);
        harness.Link.Deliver(ClientPackets.PubRel7);
        ValueTask dispose = harness.Client.DisposeAsync();
        harness.Link.EndFromPeer();
        await dispose;
        return new WeakReference(harness.Client);
    }

    /// <summary>
    /// Publishes at QoS 1 and has it acknowledged, and receives a QoS 1 PUBLISH, which the client acknowledges.
    /// </summary>
    /// <param name="harness">
    /// A connected client's harness.
    /// </param>
    /// <returns>
    /// A task that completes when the publish is acknowledged.
    /// </returns>
    private static async Task WorkAsync(ClientHarness harness)
    {
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        harness.Link.Deliver(ClientPackets.PubAck1);
        (await publish).Status.ShouldBe(MqttRequestStatus.Completed);
        harness.Link.Deliver(ClientPackets.PublishQoS1);
    }

    /// <summary>
    /// Ends a connected client's connection the given way, with the rest of a cut-short PUBLISH still to come.
    /// </summary>
    /// <param name="harness">
    /// A connected client's harness.
    /// </param>
    /// <param name="wayOut">
    /// How the connection ends; not a connect's way out.
    /// </param>
    /// <returns>
    /// The dispose under way for <see cref="WayOut.DisposeAsync"/>; otherwise a completed task.
    /// </returns>
    private static ValueTask TakeWayOut(ClientHarness harness, WayOut wayOut)
    {
        switch (wayOut)
        {
            case WayOut.ServerDisconnects:
                harness.Link.Deliver([.. s_publishQoS0Rest, .. ClientPackets.DisconnectSessionTakenOver]);
                break;
            case WayOut.PeerEndsTheLink:
                harness.Link.EndFromPeer();
                break;
            case WayOut.ReadFails:
                harness.Link.FailReads(new IOException("read failed"));
                break;
            case WayOut.WriteFails:
                harness.Link.FailWrites(new IOException("write failed"));
                _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
                break;
            case WayOut.KeepAliveTimesOut:
                // PINGREQ at 7.5 s; it has waited the full 15 s at the tick of 22.5 s.
                AdvanceInHalfIntervals(harness, 4);
                break;
            case WayOut.AcknowledgementTimesOut:
                // The server turned keep-alive off; the publish has waited twice 15 s at the tick of 30 s.
                _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
                AdvanceInHalfIntervals(harness, 5);
                break;
            case WayOut.ProtocolError:
                harness.Link.Deliver([.. s_publishQoS0Rest, .. ClientPackets.PingReq]);
                break;
            case WayOut.OnMessageThrows:
                harness.Link.Deliver(s_publishQoS0Rest);
                break;
            case WayOut.DisposeAsync:
                return harness.Client.DisposeAsync();
            default:
                throw new ArgumentOutOfRangeException(nameof(wayOut), wayOut, "Not a way out of a connected client.");
        }

        return default;
    }

    /// <summary>
    /// Advances the fake clock by half the harness's 15-second keep-alive interval, tick by tick.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <param name="ticks">
    /// How many keep-alive ticks to run.
    /// </param>
    private static void AdvanceInHalfIntervals(ClientHarness harness, int ticks)
    {
        for (int tick = 0; tick < ticks; tick++)
        {
            harness.Time.Advance(TimeSpan.FromSeconds(7.5));
        }
    }

    /// <summary>
    /// Gives the reason a way out is reported with: by <c>OnClosed</c>, or as the connect's failure.
    /// </summary>
    /// <param name="wayOut">
    /// The way out.
    /// </param>
    /// <returns>
    /// The reason.
    /// </returns>
    private static MqttClientCloseReason ExpectedReason(WayOut wayOut) => wayOut switch
    {
        WayOut.ServerDisconnects => MqttClientCloseReason.ServerDisconnected,
        WayOut.PeerEndsTheLink or WayOut.ReadFails => MqttClientCloseReason.ConnectionLost,
        WayOut.WriteFails => MqttClientCloseReason.WriteFailed,
        WayOut.KeepAliveTimesOut => MqttClientCloseReason.KeepAliveTimedOut,
        WayOut.AcknowledgementTimesOut => MqttClientCloseReason.AcknowledgementTimedOut,
        WayOut.ProtocolError => MqttClientCloseReason.ProtocolError,
        WayOut.OnMessageThrows => MqttClientCloseReason.MessageHandlerThrew,
        WayOut.DisposeAsync => MqttClientCloseReason.Disposed,
        WayOut.ConnectTimesOut => MqttClientCloseReason.ConnectTimedOut,
        _ => MqttClientCloseReason.ConnectRefused,
    };

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
    private static bool SameArray(byte[] array, ReadOnlyMemory<byte> memory)
        => MemoryMarshal.TryGetArray(memory, out ArraySegment<byte> segment) && ReferenceEquals(segment.Array, array);
}
