using System.Diagnostics;
using System.Reflection;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests the client's keep-alive timer, which ticks every half interval: the PINGREQ it sends when nothing was
/// sent, the end when no PINGRESP comes back, the end when a request goes unacknowledged for twice the interval,
/// the server's interval, and the timer's disposal at the end of the connection.
/// </summary>
public sealed class KeepAliveTests
{
    /// <summary>
    /// The payload of the publishes these tests send, "hi".
    /// </summary>
    private static readonly byte[] s_hi = "hi"u8.ToArray();

    /// <summary>
    /// Whether the client's assembly was built with optimizations, as it ships; a Debug build allocates the state
    /// machine of every async call.
    /// </summary>
    private static readonly bool s_clientIsOptimized =
        typeof(MqttWireClient).Assembly.GetCustomAttribute<DebuggableAttribute>()?.IsJITOptimizerDisabled != true;

    /// <summary>
    /// Proves that when nothing was sent for half the keep-alive interval, the tick sends PINGREQ, and not before.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Tick_NothingSentForHalfTheInterval_SendsPingReq()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(TimeSpan.FromSeconds(10));
        harness.Time.Advance(TimeSpan.FromSeconds(5) - TimeSpan.FromTicks(1));
        byte[] beforeTheTick = harness.Link.TakeWritten();

        // Act
        harness.Time.Advance(TimeSpan.FromTicks(1));

        // Assert
        beforeTheTick.ShouldBeEmpty();
        harness.NextPacket().ShouldBe(ClientPackets.PingReq);
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a packet sent less than half an interval before a tick spares that tick's PINGREQ, and the next tick
    /// sends one.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Tick_PublishSentRecently_SendsNoPingReq()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(TimeSpan.FromSeconds(10));
        harness.Time.Advance(TimeSpan.FromSeconds(4));
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        byte[] publish = harness.NextPacket();

        // Act
        harness.Time.Advance(TimeSpan.FromSeconds(1));
        byte[] atFiveSeconds = harness.Link.TakeWritten();
        harness.Time.Advance(TimeSpan.FromSeconds(5));
        byte[] atTenSeconds = harness.NextPacket();

        // Assert
        publish.ShouldBe(ClientPackets.PublishQoS0);
        atFiveSeconds.ShouldBeEmpty();
        atTenSeconds.ShouldBe(ClientPackets.PingReq);
    }

    /// <summary>
    /// Proves a connection whose PINGREQs are each answered with PINGRESP stays connected for as long as the
    /// answers come.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Tick_PingRespArrives_KeepsTheConnection()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(TimeSpan.FromSeconds(10));
        List<byte[]> pings = [];

        // Act
        for (int tick = 0; tick < 12; tick++)
        {
            harness.Time.Advance(TimeSpan.FromSeconds(5));
            pings.Add(harness.NextPacket());
            harness.Link.Deliver(ClientPackets.PingResp);
        }

        // Assert
        pings.ShouldAllBe(ping => ping.SequenceEqual(ClientPackets.PingReq));
        harness.Client.IsConnected.ShouldBeTrue();
        harness.Link.IsAborted.ShouldBeFalse();
        harness.Closed.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves a tick that sends PINGREQ allocates nothing once warmed up, in an optimized build of the client: the
    /// packet goes out in a pooled buffer through the send path, on the thread that advances the clock. A Debug
    /// build compiles every async method's state machine as a class, which the send path then allocates, so there
    /// the test only checks the PINGREQ.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Tick_SteadyState_AllocatesNothing()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(TimeSpan.FromSeconds(10));
        for (int tick = 0; tick < 3; tick++)
        {
            harness.Time.Advance(TimeSpan.FromSeconds(5));
            harness.Link.Deliver(ClientPackets.PingResp);
        }

        harness.Link.TakeWritten();

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        harness.Time.Advance(TimeSpan.FromSeconds(5));
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        if (s_clientIsOptimized)
        {
            (after - before).ShouldBe(0);
        }

        harness.NextPacket().ShouldBe(ClientPackets.PingReq);
    }

    /// <summary>
    /// Proves a PINGREQ left unanswered for a full interval ends the connection with
    /// <see cref="MqttClientCloseReason.KeepAliveTimedOut"/> at that tick and not before, aborting the link without
    /// a DISCONNECT, and that the keep-alive timer is then disposed.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Tick_NoPingRespWithinTheInterval_EndsWithKeepAliveTimedOut()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(TimeSpan.FromSeconds(10), pool: pool);
        harness.Time.Advance(TimeSpan.FromSeconds(5));
        byte[] firstPing = harness.NextPacket();
        harness.Time.Advance(TimeSpan.FromSeconds(10) - TimeSpan.FromTicks(1));
        bool connectedJustBefore = harness.Client.IsConnected;
        harness.Link.TakeWritten();

        // Act
        harness.Time.Advance(TimeSpan.FromTicks(1));

        // Assert
        firstPing.ShouldBe(ClientPackets.PingReq);
        connectedJustBefore.ShouldBeTrue();
        harness.Client.IsConnected.ShouldBeFalse();
        harness.Link.IsAborted.ShouldBeTrue();
        harness.Link.ClosedWith.ShouldBeNull();
        harness.Link.TakeWritten().ShouldBeEmpty();
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.KeepAliveTimedOut, MqttReasonCode.Success, null, null)]);
        harness.Timers.ActiveTimers.ShouldBe(0);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves the Server Keep Alive of the CONNACK replaces the client's own interval: with 15 s asked for and 5 s
    /// given, the first PINGREQ goes at 2.5 s.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Tick_ServerKeepAlive_UsesTheServersInterval()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(TimeSpan.FromSeconds(15), ClientPackets.ConnAckServerKeepAlive5);
        harness.Time.Advance(TimeSpan.FromSeconds(2.5) - TimeSpan.FromTicks(1));
        byte[] beforeTheTick = harness.Link.TakeWritten();

        // Act
        harness.Time.Advance(TimeSpan.FromTicks(1));

        // Assert
        beforeTheTick.ShouldBeEmpty();
        harness.NextPacket().ShouldBe(ClientPackets.PingReq);
    }

    /// <summary>
    /// Proves a Server Keep Alive of 0 turns keep-alive off, as MQTT 5 (3.1.2.10) says: no PINGREQ is sent and an
    /// unacknowledged request does not end the connection, however long it waits.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Tick_ServerKeepAliveZero_SendsNothingAndKeepsTheConnection()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(TimeSpan.FromSeconds(10), ClientPackets.ConnAckServerKeepAlive0);
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        harness.NextPacket();

        // Act
        harness.Time.Advance(TimeSpan.FromMinutes(10));

        // Assert
        harness.Link.TakeWritten().ShouldBeEmpty();
        harness.Client.IsConnected.ShouldBeTrue();
        harness.Timers.PeriodicTimers().ShouldBeEmpty();
    }

    /// <summary>
    /// Proves a request left unacknowledged for twice the interval ends the connection with
    /// <see cref="MqttClientCloseReason.AcknowledgementTimedOut"/>, even while every PINGREQ is answered, aborting
    /// the link without a DISCONNECT and completing the request with <see cref="MqttRequestStatus.ConnectionLost"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Tick_OldestRecordOlderThanTwiceTheInterval_EndsWithAcknowledgementTimedOut()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(TimeSpan.FromSeconds(10), pool: pool);
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        harness.NextPacket();
        List<byte[]> pings = [];
        for (int tick = 0; tick < 3; tick++)
        {
            harness.Time.Advance(TimeSpan.FromSeconds(5));
            pings.Add(harness.NextPacket());
            harness.Link.Deliver(ClientPackets.PingResp);
        }

        bool connectedAtFifteenSeconds = harness.Client.IsConnected;

        // Act
        harness.Time.Advance(TimeSpan.FromSeconds(5));
        MqttRequestResult result = await publish;

        // Assert
        pings.ShouldAllBe(ping => ping.SequenceEqual(ClientPackets.PingReq));
        connectedAtFifteenSeconds.ShouldBeTrue();
        harness.Client.IsConnected.ShouldBeFalse();
        harness.Link.IsAborted.ShouldBeTrue();
        harness.Link.ClosedWith.ShouldBeNull();
        harness.Link.TakeWritten().ShouldBeEmpty();
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.AcknowledgementTimedOut, MqttReasonCode.Success, null, null)]);
        result.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves the acknowledgement deadline counts from the record's latest phase: a QoS 2 publish whose PUBREC
    /// came at 15 s is not ended at 20 s, but is at 35 s when its PUBCOMP never came.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Tick_RecordAdvancedByPubRec_CountsFromThePubRec()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(TimeSpan.FromSeconds(10));
        _ = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.ExactlyOnce);
        harness.NextPacket();
        for (int tick = 0; tick < 3; tick++)
        {
            harness.Time.Advance(TimeSpan.FromSeconds(5));
            harness.Link.Deliver(ClientPackets.PingResp);
        }

        harness.Link.Deliver(ClientPackets.PubRec1);

        // Act
        for (int tick = 0; tick < 3; tick++)
        {
            harness.Time.Advance(TimeSpan.FromSeconds(5));
            harness.Link.Deliver(ClientPackets.PingResp);
        }

        bool connectedAtThirtySeconds = harness.Client.IsConnected;
        harness.Time.Advance(TimeSpan.FromSeconds(5));

        // Assert
        connectedAtThirtySeconds.ShouldBeTrue();
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.AcknowledgementTimedOut, MqttReasonCode.Success, null, null)]);
    }

    /// <summary>
    /// Proves the keep-alive timer is disposed when the connection ends: afterwards every timer the client created
    /// is disposed, advancing the clock by many intervals writes nothing and throws nothing, and calling the
    /// timer's callback by hand, as a late tick would, does nothing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Tick_AfterTheEnd_TimerIsDisposed()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = await ConnectAsync(TimeSpan.FromSeconds(10), pool: pool);
        List<CountingTimeProvider.CountedTimer> keepAlive = harness.Timers.PeriodicTimers();
        harness.Link.EndFromPeer();

        // Act
        Exception? exception = Record.Exception(() =>
        {
            harness.Time.Advance(TimeSpan.FromSeconds(50));
            keepAlive[0].Fire();
        });

        // Assert
        keepAlive.Count.ShouldBe(1);
        keepAlive[0].IsDisposed.ShouldBeTrue();
        exception.ShouldBeNull();
        harness.Timers.ActiveTimers.ShouldBe(0);
        harness.Link.TakeWritten().ShouldBeEmpty();
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ConnectionLost, MqttReasonCode.Success, null, null)]);
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves disposing the client disposes the keep-alive timer, and no tick fires into the finished client.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Tick_AfterDisposeAsync_TimerIsDisposed()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(TimeSpan.FromSeconds(10));
        List<CountingTimeProvider.CountedTimer> keepAlive = harness.Timers.PeriodicTimers();
        ValueTask dispose = harness.Client.DisposeAsync();
        harness.NextPacket();
        harness.Link.EndFromPeer();
        await dispose;

        // Act
        harness.Time.Advance(TimeSpan.FromSeconds(50));
        keepAlive[0].Fire();

        // Assert
        keepAlive[0].IsDisposed.ShouldBeTrue();
        harness.Timers.ActiveTimers.ShouldBe(0);
        harness.Link.TakeWritten().ShouldBeEmpty();
    }

    /// <summary>
    /// Creates a client with a keep-alive and connects it, taking the CONNECT it wrote.
    /// </summary>
    /// <param name="keepAlive">
    /// The keep-alive the client asks for.
    /// </param>
    /// <param name="connAck">
    /// The server's CONNACK, or <see langword="null"/> for a plain accepting one.
    /// </param>
    /// <param name="pool">
    /// The pool the client rents from, or <see langword="null"/> for the shared pool.
    /// </param>
    /// <returns>
    /// The harness around the connected client.
    /// </returns>
    private static async Task<ClientHarness> ConnectAsync(TimeSpan keepAlive, byte[]? connAck = null, CountingPool? pool = null)
    {
        ClientHarness harness = ClientHarness.Create(keepAlive: keepAlive, pool: pool);
        await harness.ConnectAsync(connAck ?? ClientPackets.ConnAckAccepted);
        harness.NextPacket();
        return harness;
    }
}
