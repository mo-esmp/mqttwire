using System.Net.WebSockets;
using System.Text;
using Microsoft.Extensions.Time.Testing;
using MqttWire.Codec;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests the reconnecting client with its clock replaced: the attempts and their delays, fresh options and a new
/// client for each, subscriptions made again, the refusals that stop it, requests while disconnected, the order of
/// its callbacks, and disposing it in every state. Nothing waits in real time for a delay.
/// </summary>
public sealed class ReconnectingClientTests
{
    /// <summary>
    /// The payload of the publishes these tests send, "hi".
    /// </summary>
    private static readonly byte[] s_hi = "hi"u8.ToArray();

    /// <summary>
    /// The first delay, 500 ms.
    /// </summary>
    private static readonly TimeSpan s_initial = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// The longest delay, 15 seconds, which is also how long a connection must stay up to reset the back-off.
    /// </summary>
    private static readonly TimeSpan s_maxDelay = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Builds options with a keep-alive of 60 seconds, so a connection held for <see cref="s_maxDelay"/> sends no
    /// PINGREQ and needs no PINGRESP.
    /// </summary>
    private static readonly Func<int, CancellationToken, ValueTask<MqttWireClientOptions>> s_noPingWithinMaxDelay =
        static (_, _) => ValueTask.FromResult(new MqttWireClientOptions
        {
            Uri = new Uri("ws://test/mqtt"),
            ClientId = "c1",
            UserName = "u",
            Password = "p",
            KeepAlive = TimeSpan.FromSeconds(60),
        });

    /// <summary>
    /// Topics a caller cannot publish to: empty, a single-level wildcard, a multi-level wildcard, U+0000, an
    /// unpaired surrogate, 65,536 UTF-8 bytes, and a wildcard in a topic too long to encode on the stack.
    /// </summary>
    private static readonly string[] s_topicsThatCannotBeSent =
    [
        string.Empty,
        "a/+",
        "a/#",
        "a\0b",
        "a\uD800",
        new('a', 65_536),
        new string('a', 300) + "/+",
    ];

    /// <summary>
    /// Topic filters a caller cannot subscribe to: empty, a misplaced multi-level wildcard, a misplaced
    /// single-level wildcard, U+0000, an unpaired surrogate, 65,536 UTF-8 bytes, and a misplaced wildcard in a
    /// filter too long to encode on the stack.
    /// </summary>
    private static readonly string[] s_filtersThatCannotBeSent =
    [
        string.Empty,
        "a/#/b",
        "a+",
        "a\0",
        "a\uD800",
        new('a', 65_536),
        new string('a', 300) + "+",
    ];

    /// <summary>
    /// Proves the first attempt is made at once, and that <see cref="MqttWireReconnectingClient.StartAsync"/>
    /// completes with the server's CONNACK once <c>OnConnected</c> has been called for the first connection.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StartAsync_FirstAttemptConnects_CompletesWithTheAck()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        Task<MqttConnectAck> start = harness.Client.StartAsync();
        MemoryLink link = await harness.LinkAsync(1);

        // Act
        link.Deliver(ClientPackets.ConnAckWithReason);
        MqttConnectAck ack = await start.WaitAsync(ReconnectHarness.Timeout);

        // Assert
        ack.IsConnected.ShouldBeTrue();
        ack.ReasonString.ShouldBe("x");
        harness.AttemptTimes().ShouldBe([TimeSpan.Zero]);
        harness.Log.Events.ShouldBe(["connected first"]);
        link.TakeWritten().ShouldBe(ClientPackets.Connect);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a server that cannot be reached is tried again after 500 ms, then after another second, until the
    /// connection is made, and that the start completes with that connection.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StartAsync_ServerUnreachable_RetriesUntilConnected()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        harness.Links.FailNext(new WebSocketException("unreachable"));
        harness.Links.FailNext(new WebSocketException("unreachable"));
        Task<MqttConnectAck> start = harness.Client.StartAsync();
        await harness.Connector.WhenAttemptsAsync(1);

        // Act
        await harness.AdvanceAsync(s_initial);
        await harness.Connector.WhenAttemptsAsync(2);
        await harness.AdvanceAsync(TimeSpan.FromSeconds(1));
        MemoryLink link = await harness.LinkAsync(3);
        link.Deliver(ClientPackets.ConnAckAccepted);
        MqttConnectAck ack = await start.WaitAsync(ReconnectHarness.Timeout);

        // Assert
        ack.IsConnected.ShouldBeTrue();
        harness.AttemptTimes().ShouldBe([TimeSpan.Zero, s_initial, TimeSpan.FromMilliseconds(1_500)]);
        harness.Log.Events.ShouldBe(["connected first"]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a lost connection is reported, then tried again after 500 ms; a failed attempt doubles the delay to
    /// one second; and a connection that stays up for <c>MaxDelay</c>, 15 seconds, puts it back to 500 ms.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectionLost_ReconnectsAtTheDocumentedDelays()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create(clientOptions: s_noPingWithinMaxDelay);
        MemoryLink first = await harness.StartConnectedAsync();
        harness.Links.FailNext(new WebSocketException("unreachable"));

        // Act
        first.EndFromPeer();
        await harness.Log.WhenAsync("disconnected");
        await harness.AdvanceAsync(s_initial);
        await harness.Connector.WhenAttemptsAsync(2);
        await harness.AdvanceAsync(TimeSpan.FromSeconds(1));
        MemoryLink third = await harness.LinkAsync(3);
        third.Deliver(ClientPackets.ConnAckAccepted);
        await harness.Log.WhenAsync("connected again");
        harness.Time.Advance(s_maxDelay);
        third.EndFromPeer();
        await harness.Log.WhenAsync("disconnected", 2);
        await harness.AdvanceAsync(s_initial);
        await harness.Connector.WhenAttemptsAsync(4);

        // Assert
        harness.AttemptTimes().ShouldBe(
        [
            TimeSpan.Zero,
            s_initial,
            TimeSpan.FromMilliseconds(1_500),
            TimeSpan.FromMilliseconds(17_000),
        ]);
        harness.Log.Events.ShouldBe(
        [
            "connected first",
            "disconnected ConnectionLost",
            "connected again",
            "disconnected ConnectionLost",
        ]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a connection that ends before it has stayed up for <c>MaxDelay</c> does not put the delay back to
    /// 500 ms: a connection that ends at once is retried after 500 ms, and the next, held for one millisecond less
    /// than <c>MaxDelay</c>, after one second.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectionEndsAtOnce_BackoffKeepsGrowing()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create(clientOptions: s_noPingWithinMaxDelay);
        MemoryLink first = await harness.StartConnectedAsync();

        // Act
        first.EndFromPeer();
        await harness.Log.WhenAsync("disconnected");
        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);
        second.Deliver(ClientPackets.ConnAckAccepted);
        await harness.Log.WhenAsync("connected again");
        harness.Time.Advance(s_maxDelay - TimeSpan.FromMilliseconds(1));
        second.EndFromPeer();
        await harness.Log.WhenAsync("disconnected", 2);
        await harness.AdvanceAsync(TimeSpan.FromSeconds(1));
        await harness.Connector.WhenAttemptsAsync(3);

        // Assert
        harness.AttemptTimes().ShouldBe(
        [
            TimeSpan.Zero,
            s_initial,
            TimeSpan.FromMilliseconds(16_499),
        ]);
        harness.Log.Events.ShouldBe(
        [
            "connected first",
            "disconnected ConnectionLost",
            "connected again",
            "disconnected ConnectionLost",
        ]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves every attempt asks for fresh options and connects with a new client over a new link: the CONNECT on
    /// each link carries the client identifier of that attempt's options.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task EveryAttempt_CallsClientOptionsAndUsesANewClient()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create(
            clientOptions: static (call, _) => ValueTask.FromResult(ReconnectHarness.DefaultOptions($"c{call}")));
        MemoryLink first = await harness.StartConnectedAsync();

        // Act
        first.EndFromPeer();
        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);
        second.Deliver(ClientPackets.ConnAckAccepted);
        await harness.Log.WhenAsync("connected again");
        second.EndFromPeer();
        // The second connection ended at once, so the back-off grew to one second.
        await harness.AdvanceAsync(TimeSpan.FromSeconds(1));
        MemoryLink third = await harness.LinkAsync(3);
        third.Deliver(ClientPackets.ConnAckAccepted);
        await harness.Log.WhenAsync("connected again", 2);

        // Assert
        harness.OptionsCalls.ShouldBe(3);
        harness.Links.Links.Count.ShouldBe(3);
        ClientIdOf(first).ShouldBe("c1");
        ClientIdOf(second).ShouldBe("c2");
        ClientIdOf(third).ShouldBe("c3");
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a new connection subscribes again to every remembered filter, in the order they were first
    /// subscribed and with the QoS asked for then, before <c>OnConnected</c> reports the reconnect.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Reconnect_ResubscribesBeforeOnConnected()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        MemoryLink first = await harness.StartConnectedAsync();
        await harness.SubscribeAsync(first, "a/+", MqttQoS.AtLeastOnce, 1);
        await harness.SubscribeAsync(first, "b/+", MqttQoS.AtMostOnce, 2);
        first.EndFromPeer();
        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);

        // Act
        second.Deliver(ClientPackets.ConnAckAccepted);
        await second.WhenWrittenAsync(2);
        bool connectedAfterFirst = harness.Log.Events.Contains("connected again");
        second.Deliver(ReconnectHarness.SubAck(1, 0x01));
        await second.WhenWrittenAsync(3);
        bool connectedAfterSecond = harness.Log.Events.Contains("connected again");
        second.Deliver(ReconnectHarness.SubAck(2, 0x00));
        await harness.Log.WhenAsync("connected again");

        // Assert
        connectedAfterFirst.ShouldBeFalse();
        connectedAfterSecond.ShouldBeFalse();
        ReconnectHarness.Packets(second.TakeWritten()).ShouldBe(
        [
            ClientPackets.Connect,
            ReconnectHarness.Subscribe(1, "a/+", MqttQoS.AtLeastOnce),
            ReconnectHarness.Subscribe(2, "b/+", MqttQoS.AtMostOnce),
        ]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a remembered subscription the server refuses on a new connection is reported to
    /// <c>OnSubscriptionRefused</c> before <c>OnConnected</c>, and is not made again on the next connection.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Reconnect_SubscriptionRefused_ReportedAndForgotten()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        MemoryLink first = await harness.StartConnectedAsync();
        await harness.SubscribeAsync(first, "a/+", MqttQoS.AtLeastOnce, 1);
        await harness.SubscribeAsync(first, "b/+", MqttQoS.AtMostOnce, 2);
        first.EndFromPeer();
        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);

        // Act
        second.Deliver(ClientPackets.ConnAckAccepted);
        await second.WhenWrittenAsync(2);
        second.Deliver(ReconnectHarness.SubAck(1, 0x87));
        await second.WhenWrittenAsync(3);
        second.Deliver(ReconnectHarness.SubAck(2, 0x00));
        await harness.Log.WhenAsync("connected again");
        second.EndFromPeer();
        // The second connection ended at once, so the back-off grew to one second.
        await harness.AdvanceAsync(TimeSpan.FromSeconds(1));
        MemoryLink third = await harness.LinkAsync(3);
        third.Deliver(ClientPackets.ConnAckAccepted);
        await third.WhenWrittenAsync(2);
        third.Deliver(ReconnectHarness.SubAck(1, 0x00));
        await harness.Log.WhenAsync("connected again", 2);

        // Assert
        harness.Log.Events.ShouldBe(
        [
            "connected first",
            "disconnected ConnectionLost",
            "refused a/+ NotAuthorized",
            "connected again",
            "disconnected ConnectionLost",
            "connected again",
        ]);
        ReconnectHarness.Packets(third.TakeWritten()).ShouldBe(
        [
            ClientPackets.Connect,
            ReconnectHarness.Subscribe(1, "b/+", MqttQoS.AtMostOnce),
        ]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves an unsubscribed filter is forgotten: the next connection does not subscribe to it again.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Unsubscribe_ForgetsTheFilter()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        MemoryLink first = await harness.StartConnectedAsync();
        await harness.SubscribeAsync(first, "a/+", MqttQoS.AtLeastOnce, 1);
        ValueTask<MqttRequestResult> unsubscribe = harness.Client.UnsubscribeAsync("a/+");
        await first.WhenWrittenAsync(3);
        first.Deliver(ReconnectHarness.UnsubAck(2));
        MqttRequestResult unsubscribed = await unsubscribe.AsTask().WaitAsync(ReconnectHarness.Timeout);

        // Act
        first.EndFromPeer();
        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);
        second.Deliver(ClientPackets.ConnAckAccepted);
        await harness.Log.WhenAsync("connected again");

        // Assert
        unsubscribed.IsSuccess.ShouldBeTrue();
        ReconnectHarness.Packets(first.TakeWritten())[2].ShouldBe(ReconnectHarness.Unsubscribe(2, "a/+"));
        ReconnectHarness.Packets(second.TakeWritten()).ShouldBe([ClientPackets.Connect]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a filter unsubscribed while the new connection waits for its re-subscribe's SUBACK is unsubscribed on
    /// that connection before <c>OnConnected</c>, so the server does not keep it for the whole connection.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Reconnect_UnsubscribedDuringResubscribe_UnsubscribesBeforeOnConnected()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        MemoryLink first = await harness.StartConnectedAsync();
        await harness.SubscribeAsync(first, "a/+", MqttQoS.AtLeastOnce, 1);
        first.EndFromPeer();
        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);
        second.Deliver(ClientPackets.ConnAckAccepted);
        await second.WhenWrittenAsync(2);

        // Act
        MqttRequestResult unsubscribed = await harness.Client.UnsubscribeAsync("a/+");
        second.Deliver(ReconnectHarness.SubAck(1, 0x01));
        await harness.Log.WhenAsync("connected again");
        await second.WhenWrittenAsync(3).WaitAsync(ReconnectHarness.Timeout);

        // Assert
        unsubscribed.Status.ShouldBe(MqttRequestStatus.NotConnected);
        ReconnectHarness.Packets(second.TakeWritten()).ShouldBe(
        [
            ClientPackets.Connect,
            ReconnectHarness.Subscribe(1, "a/+", MqttQoS.AtLeastOnce),
            ReconnectHarness.Unsubscribe(2, "a/+"),
        ]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a subscribe whose SUBACK arrives after an unsubscribe of the same filter is not remembered, so the
    /// next connection does not subscribe to it again.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task SubscribeAsync_UnsubscribedBeforeTheSubAck_IsNotRemembered()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        MemoryLink first = await harness.StartConnectedAsync();
        ValueTask<MqttRequestResult> subscribe = harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce);
        await first.WhenWrittenAsync(2);
        ValueTask<MqttRequestResult> unsubscribe = harness.Client.UnsubscribeAsync("a/+");
        await first.WhenWrittenAsync(3);
        first.Deliver(ReconnectHarness.SubAck(1, 0x01));
        first.Deliver(ReconnectHarness.UnsubAck(2));
        MqttRequestResult subscribed = await subscribe.AsTask().WaitAsync(ReconnectHarness.Timeout);
        MqttRequestResult unsubscribed = await unsubscribe.AsTask().WaitAsync(ReconnectHarness.Timeout);

        // Act
        first.EndFromPeer();
        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);
        second.Deliver(ClientPackets.ConnAckAccepted);
        await harness.Log.WhenAsync("connected again");

        // Assert
        subscribed.IsSuccess.ShouldBeTrue();
        unsubscribed.IsSuccess.ShouldBeTrue();
        ReconnectHarness.Packets(second.TakeWritten()).ShouldBe([ClientPackets.Connect]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a subscribe made again after an unsubscribe of the same filter, both while an earlier subscribe still
    /// waits for its SUBACK, is remembered: only the subscribes that began before the unsubscribe are ignored.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task SubscribeAsync_SubscribedAgainAfterTheUnsubscribe_IsRemembered()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        MemoryLink first = await harness.StartConnectedAsync();
        ValueTask<MqttRequestResult> early = harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce);
        await first.WhenWrittenAsync(2);
        ValueTask<MqttRequestResult> unsubscribe = harness.Client.UnsubscribeAsync("a/+");
        await first.WhenWrittenAsync(3);
        ValueTask<MqttRequestResult> late = harness.Client.SubscribeAsync("a/+", MqttQoS.AtMostOnce);
        await first.WhenWrittenAsync(4);
        first.Deliver(ReconnectHarness.SubAck(1, 0x01));
        first.Deliver(ReconnectHarness.UnsubAck(2));
        first.Deliver(ReconnectHarness.SubAck(3, 0x00));
        await early.AsTask().WaitAsync(ReconnectHarness.Timeout);
        await unsubscribe.AsTask().WaitAsync(ReconnectHarness.Timeout);
        await late.AsTask().WaitAsync(ReconnectHarness.Timeout);

        // Act
        first.EndFromPeer();
        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);
        second.Deliver(ClientPackets.ConnAckAccepted);
        await second.WhenWrittenAsync(2).WaitAsync(ReconnectHarness.Timeout);
        second.Deliver(ReconnectHarness.SubAck(1, 0x00));
        await harness.Log.WhenAsync("connected again");

        // Assert
        ReconnectHarness.Packets(second.TakeWritten()).ShouldBe(
        [
            ClientPackets.Connect,
            ReconnectHarness.Subscribe(1, "a/+", MqttQoS.AtMostOnce),
        ]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a CONNACK refusal that the same CONNECT would meet again stops the reconnecting client for good:
    /// <c>OnStopped</c> is called once with it, the connection cycle ends, and no attempt follows however long the
    /// clock runs.
    /// </summary>
    /// <param name="reasonCode">
    /// The refusal: unsupported protocol version, client identifier not valid, bad user name or password, not
    /// authorized, or banned.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(0x84)]
    [InlineData(0x85)]
    [InlineData(0x86)]
    [InlineData(0x87)]
    [InlineData(0x8A)]
    public async Task ConnAckRefusal_ThatRetryingCannotFix_Stops(byte reasonCode)
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        MemoryLink first = await harness.StartConnectedAsync();
        first.EndFromPeer();
        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);

        // Act
        second.Deliver(ReconnectHarness.ConnAck(reasonCode));
        second.EndFromPeer();
        await harness.Client.Running.WaitAsync(ReconnectHarness.Timeout);
        harness.Time.Advance(TimeSpan.FromHours(1));

        // Assert
        harness.Log.Events.ShouldBe(
        [
            "connected first",
            "disconnected ConnectionLost",
            $"stopped {(MqttReasonCode)reasonCode}",
        ]);
        harness.Connector.Attempts.Count.ShouldBe(2);
        harness.OptionsCalls.ShouldBe(2);
        (await harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce)).Status.ShouldBe(MqttRequestStatus.NotConnected);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a bad user name or password on the very first attempt stops the reconnecting client and is what
    /// <see cref="MqttWireReconnectingClient.StartAsync"/> returns.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StartAsync_FirstAttemptBadUserNameOrPassword_ReturnsTheRefusal()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        Task<MqttConnectAck> start = harness.Client.StartAsync();
        MemoryLink link = await harness.LinkAsync(1);

        // Act
        link.Deliver(ClientPackets.ConnAckBadUserNameOrPassword);
        link.EndFromPeer();
        MqttConnectAck ack = await start.WaitAsync(ReconnectHarness.Timeout);
        await harness.Client.Running.WaitAsync(ReconnectHarness.Timeout);
        harness.Time.Advance(TimeSpan.FromHours(1));

        // Assert
        ack.Failure.ShouldBe(MqttClientCloseReason.ConnectRefused);
        ack.ReasonCode.ShouldBe(MqttReasonCode.BadUserNameOrPassword);
        harness.Log.Events.ShouldBe(["stopped BadUserNameOrPassword"]);
        harness.Connector.Attempts.Count.ShouldBe(1);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a refusal that may pass, server unavailable (0x88), and a CONNACK that never arrives within the
    /// connect timeout, are both tried again after the back-off.
    /// </summary>
    /// <param name="failure">
    /// <c>ServerUnavailable</c> or <c>ConnectTimeout</c>.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData("ServerUnavailable")]
    [InlineData("ConnectTimeout")]
    public async Task ConnAckRefusal_ServerUnavailableOrTimeout_Retries(string failure)
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        Task<MqttConnectAck> start = harness.Client.StartAsync();
        MemoryLink first = await harness.LinkAsync(1);
        TimeSpan failedAt = TimeSpan.Zero;

        // Act
        if (string.Equals(failure, "ServerUnavailable", StringComparison.Ordinal))
        {
            first.Deliver(ReconnectHarness.ConnAck(0x88));
            first.EndFromPeer();
        }
        else
        {
            failedAt = TimeSpan.FromSeconds(10);
            await harness.AdvanceAsync(failedAt);
        }

        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);
        second.Deliver(ClientPackets.ConnAckAccepted);
        MqttConnectAck ack = await start.WaitAsync(ReconnectHarness.Timeout);

        // Assert
        ack.IsConnected.ShouldBeTrue();
        harness.AttemptTimes().ShouldBe([TimeSpan.Zero, failedAt + s_initial]);
        harness.Log.Events.ShouldBe(["connected first"]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a publish or subscribe while there is no connection, before the start or during a back-off, fails at
    /// once with <see cref="MqttRequestStatus.NotConnected"/> and is not queued.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_WhileDisconnected_FailsAtOnceWithNotConnected()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        ValueTask<MqttRequestResult> beforeStart = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        MemoryLink first = await harness.StartConnectedAsync();
        first.EndFromPeer();
        await harness.Log.WhenAsync("disconnected");

        // Act
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        ValueTask<MqttRequestResult> subscribe = harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce);

        // Assert
        beforeStart.IsCompleted.ShouldBeTrue();
        publish.IsCompleted.ShouldBeTrue();
        subscribe.IsCompleted.ShouldBeTrue();
        (await beforeStart).Status.ShouldBe(MqttRequestStatus.NotConnected);
        (await publish).Status.ShouldBe(MqttRequestStatus.NotConnected);
        (await subscribe).Status.ShouldBe(MqttRequestStatus.NotConnected);
        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);
        second.Deliver(ClientPackets.ConnAckAccepted);
        await harness.Log.WhenAsync("connected again");
        ReconnectHarness.Packets(second.TakeWritten()).ShouldBe([ClientPackets.Connect]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a topic that cannot be published to throws <see cref="ArgumentException"/> for the parameter
    /// <c>topic</c> whatever the network does: before the start, during a back-off and while connected.
    /// </summary>
    /// <param name="index">
    /// The index of the topic in <see cref="s_topicsThatCannotBeSent"/>.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    public async Task PublishAsync_TopicCannotBeSent_ThrowsConnectedOrNot(int index)
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        string topic = s_topicsThatCannotBeSent[index];
        harness.Links.FailNext(new WebSocketException("unreachable"));

        // Act
        Exception? beforeStart = Record.Exception(() => { _ = harness.Client.PublishAsync(topic, s_hi, MqttQoS.AtLeastOnce); });
        Task<MqttConnectAck> start = harness.Client.StartAsync();
        await harness.Clock.WhenTimerAsync(s_initial);
        Exception? duringBackoff = Record.Exception(() => { _ = harness.Client.PublishAsync(topic, s_hi, MqttQoS.AtLeastOnce); });
        harness.Time.Advance(s_initial);
        MemoryLink link = await harness.LinkAsync(2);
        link.Deliver(ClientPackets.ConnAckAccepted);
        await start.WaitAsync(ReconnectHarness.Timeout);
        Exception? connected = Record.Exception(() => { _ = harness.Client.PublishAsync(topic, s_hi, MqttQoS.AtLeastOnce); });

        // Assert
        beforeStart.ShouldBeOfType<ArgumentException>().ParamName.ShouldBe("topic");
        duringBackoff.ShouldBeOfType<ArgumentException>().ParamName.ShouldBe("topic");
        connected.ShouldBeOfType<ArgumentException>().ParamName.ShouldBe("topic");
        ReconnectHarness.Packets(link.TakeWritten()).ShouldBe([ClientPackets.Connect]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a topic filter that cannot be sent throws <see cref="ArgumentException"/> for the parameter
    /// <c>topicFilter</c> from both subscribe and unsubscribe whatever the network does: before the start, during a
    /// back-off and while connected.
    /// </summary>
    /// <param name="index">
    /// The index of the filter in <see cref="s_filtersThatCannotBeSent"/>.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    public async Task SubscribeAsync_FilterCannotBeSent_ThrowsConnectedOrNot(int index)
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        string filter = s_filtersThatCannotBeSent[index];
        harness.Links.FailNext(new WebSocketException("unreachable"));
        List<Exception?> exceptions = [];

        // Act
        exceptions.Add(Record.Exception(() => { _ = harness.Client.SubscribeAsync(filter, MqttQoS.AtLeastOnce); }));
        exceptions.Add(Record.Exception(() => { _ = harness.Client.UnsubscribeAsync(filter); }));
        Task<MqttConnectAck> start = harness.Client.StartAsync();
        await harness.Clock.WhenTimerAsync(s_initial);
        exceptions.Add(Record.Exception(() => { _ = harness.Client.SubscribeAsync(filter, MqttQoS.AtLeastOnce); }));
        exceptions.Add(Record.Exception(() => { _ = harness.Client.UnsubscribeAsync(filter); }));
        harness.Time.Advance(s_initial);
        MemoryLink link = await harness.LinkAsync(2);
        link.Deliver(ClientPackets.ConnAckAccepted);
        await start.WaitAsync(ReconnectHarness.Timeout);
        exceptions.Add(Record.Exception(() => { _ = harness.Client.SubscribeAsync(filter, MqttQoS.AtLeastOnce); }));
        exceptions.Add(Record.Exception(() => { _ = harness.Client.UnsubscribeAsync(filter); }));

        // Assert
        exceptions.Count.ShouldBe(6);
        foreach (Exception? exception in exceptions)
        {
            exception.ShouldBeOfType<ArgumentException>().ParamName.ShouldBe("topicFilter");
        }

        ReconnectHarness.Packets(link.TakeWritten()).ShouldBe([ClientPackets.Connect]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves a QoS 1 publish in flight when the connection is lost completes with
    /// <see cref="MqttRequestStatus.ConnectionLost"/> and is not sent again on the next connection.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishAsync_InFlightWhenLost_CompletesWithConnectionLost()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        MemoryLink first = await harness.StartConnectedAsync();
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtLeastOnce);
        await first.WhenWrittenAsync(2);

        // Act
        first.EndFromPeer();
        MqttRequestResult result = await publish.AsTask().WaitAsync(ReconnectHarness.Timeout);
        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);
        second.Deliver(ClientPackets.ConnAckAccepted);
        await harness.Log.WhenAsync("connected again");

        // Assert
        result.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
        ReconnectHarness.Packets(first.TakeWritten())[1].ShouldBe(ClientPackets.PublishQoS1);
        ReconnectHarness.Packets(second.TakeWritten()).ShouldBe([ClientPackets.Connect]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves the callbacks come one at a time and in the documented order: each <c>OnConnected</c> followed by one
    /// <c>OnDisconnected</c>, and a message that arrives while <c>OnConnected</c> runs waits until it has returned.
    /// The message is delivered from another thread while <c>OnConnected</c> gives it 100 ms of real time to break
    /// in; the test passes as well when the thread starts late, so it never fails through slowness.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Callbacks_ComeOneAtATimeInOrder()
    {
        // Arrange
        MemoryLink? deliverOnConnected = null;
        ReconnectHarness harness = ReconnectHarness.Create(onCallback: entry =>
        {
            MemoryLink? link = Interlocked.Exchange(ref deliverOnConnected, null);
            if (link is not null && string.Equals(entry, "connected again", StringComparison.Ordinal))
            {
                Task deliver = Task.Run(() => link.Deliver(ClientPackets.PublishQoS0));
                SpinWait.SpinUntil(() => deliver.IsCompleted, TimeSpan.FromMilliseconds(100));
            }
        });
        MemoryLink first = await harness.StartConnectedAsync();
        first.EndFromPeer();
        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);
        Volatile.Write(ref deliverOnConnected, second);

        // Act
        second.Deliver(ClientPackets.ConnAckAccepted);
        await harness.Log.WhenAsync("message");
        second.EndFromPeer();
        await harness.Log.WhenAsync("disconnected", 2);
        // The second connection ended at once, so the back-off grew to one second.
        await harness.AdvanceAsync(TimeSpan.FromSeconds(1));
        MemoryLink third = await harness.LinkAsync(3);
        third.Deliver(ClientPackets.ConnAckAccepted);
        await harness.Log.WhenAsync("connected again", 2);
        await harness.DisposeConnectedAsync(third);

        // Assert
        harness.Log.Overlapped.ShouldBeFalse();
        harness.Log.Events.ShouldBe(
        [
            "connected first",
            "disconnected ConnectionLost",
            "connected again",
            "message a/b",
            "disconnected ConnectionLost",
            "connected again",
            "disconnected Disposed",
        ]);
    }

    /// <summary>
    /// Proves a callback that throws is reported to <c>OnCallbackError</c> and changes nothing else: the start
    /// completes, the connection works, and a lost connection is still followed by a reconnect.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Callback_Throws_ReportedToOnCallbackErrorAndCarriesOn()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create(onCallback: static entry =>
        {
            if (string.Equals(entry, "connected first", StringComparison.Ordinal))
            {
                throw new InvalidOperationException("The application's callback failed.");
            }
        });

        // Act
        MemoryLink first = await harness.StartConnectedAsync();
        MqttRequestResult published = await harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);
        first.EndFromPeer();
        await harness.AdvanceAsync(s_initial);
        MemoryLink second = await harness.LinkAsync(2);
        second.Deliver(ClientPackets.ConnAckAccepted);
        await harness.Log.WhenAsync("connected again");

        // Assert
        published.IsSuccess.ShouldBeTrue();
        harness.Log.Events.ShouldBe(
        [
            "connected first",
            "error InvalidOperationException",
            "disconnected ConnectionLost",
            "connected again",
        ]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves an exception from the <c>ClientOptions</c> callback is reported and counts as a failed attempt: no
    /// link is opened, and the next attempt follows after the back-off.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ClientOptions_Throws_CountsAsAFailedAttempt()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create(clientOptions: static (call, _) => call == 1
            ? throw new InvalidOperationException("No token.")
            : ValueTask.FromResult(ReconnectHarness.DefaultOptions()));
        Task<MqttConnectAck> start = harness.Client.StartAsync();

        // Act
        await harness.Log.WhenAsync("error");
        await harness.AdvanceAsync(s_initial);
        MemoryLink link = await harness.LinkAsync(1);
        link.Deliver(ClientPackets.ConnAckAccepted);
        MqttConnectAck ack = await start.WaitAsync(ReconnectHarness.Timeout);

        // Assert
        ack.IsConnected.ShouldBeTrue();
        harness.OptionsCalls.ShouldBe(2);
        harness.AttemptTimes().ShouldBe([s_initial]);
        harness.Log.Events.ShouldBe(["error InvalidOperationException", "connected first"]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves options that set <c>OnMessage</c>, which the reconnecting client must own, are refused with
    /// <see cref="ArgumentException"/> reported to <c>OnCallbackError</c>, and count as a failed attempt.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ClientOptions_SetsOnMessage_CountsAsAFailedAttempt()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create(clientOptions: static (call, _) => ValueTask.FromResult(call == 1
            ? new MqttWireClientOptions
            {
                Uri = new Uri("ws://test/mqtt"),
                ClientId = "c1",
                OnMessage = static (in MqttPublish _) => { },
            }
            : ReconnectHarness.DefaultOptions()));
        Task<MqttConnectAck> start = harness.Client.StartAsync();

        // Act
        await harness.Log.WhenAsync("error");
        await harness.AdvanceAsync(s_initial);
        MemoryLink link = await harness.LinkAsync(1);
        link.Deliver(ClientPackets.ConnAckAccepted);
        MqttConnectAck ack = await start.WaitAsync(ReconnectHarness.Timeout);

        // Assert
        ack.IsConnected.ShouldBeTrue();
        harness.OptionsCalls.ShouldBe(2);
        harness.AttemptTimes().ShouldBe([s_initial]);
        harness.Log.Events.ShouldBe(["error ArgumentException", "connected first"]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves disposing during a back-off, during an attempt or while connected ends everything: every link is
    /// closed or aborted, a start still waiting throws <see cref="ObjectDisposedException"/>, a connection that was
    /// reported is followed by <c>OnDisconnected</c> before the dispose returns, and nothing is called and no
    /// attempt is made after it, however long the clock runs.
    /// </summary>
    /// <param name="state">
    /// <c>Backoff</c>, <c>Attempt</c> or <c>Connected</c>.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData("Backoff")]
    [InlineData("Attempt")]
    [InlineData("Connected")]
    public async Task DisposeAsync_DuringBackoffAttemptOrConnection_NoCallbackAfter(string state)
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        Task<MqttConnectAck> start;
        MemoryLink? link = null;
        if (string.Equals(state, "Backoff", StringComparison.Ordinal))
        {
            harness.Links.FailNext(new WebSocketException("unreachable"));
            start = harness.Client.StartAsync();
            await harness.Clock.WhenTimerAsync(s_initial);
        }
        else if (string.Equals(state, "Attempt", StringComparison.Ordinal))
        {
            start = harness.Client.StartAsync();
            link = await harness.LinkAsync(1);
        }
        else
        {
            start = harness.Client.StartAsync();
            link = await harness.LinkAsync(1);
            link.Deliver(ClientPackets.ConnAckAccepted);
            await start.WaitAsync(ReconnectHarness.Timeout);
        }

        int attempts = harness.Connector.Attempts.Count;

        // Act
        ValueTask dispose = harness.Client.DisposeAsync();
        if (string.Equals(state, "Connected", StringComparison.Ordinal))
        {
            await link!.WhenWrittenAsync(2);
            link.EndFromPeer();
        }

        await dispose.AsTask().WaitAsync(ReconnectHarness.Timeout);
        IReadOnlyList<string> eventsWhenDisposed = harness.Log.Events;
        harness.Time.Advance(TimeSpan.FromHours(1));
        link?.EndFromPeer();
        Exception? startException = await Record.ExceptionAsync(() => start);

        // Assert
        harness.Log.Events.ShouldBe(eventsWhenDisposed);
        harness.Connector.Attempts.Count.ShouldBe(attempts);
        harness.Client.Running.IsCompleted.ShouldBeTrue();
        (await harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce)).Status.ShouldBe(MqttRequestStatus.NotConnected);
        if (string.Equals(state, "Connected", StringComparison.Ordinal))
        {
            startException.ShouldBeNull();
            eventsWhenDisposed.ShouldBe(["connected first", "disconnected Disposed"]);
            ReconnectHarness.Packets(link!.TakeWritten()).ShouldBe([ClientPackets.Connect, ClientPackets.Disconnect]);
        }
        else
        {
            startException.ShouldBeOfType<ObjectDisposedException>();
            eventsWhenDisposed.ShouldBeEmpty();
            (link?.IsAborted ?? true).ShouldBeTrue();
        }
    }

    /// <summary>
    /// Proves cancelling the start's token before the first connection stops the reconnecting client: the start
    /// throws <see cref="OperationCanceledException"/>, the connection cycle ends, and no attempt follows.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StartAsync_Cancelled_StopsTheWrapper()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        harness.Links.FailNext(new WebSocketException("unreachable"));
        using CancellationTokenSource cancellation = new();
        Task<MqttConnectAck> start = harness.Client.StartAsync(cancellation.Token);
        await harness.Clock.WhenTimerAsync(s_initial);

        // Act
        await cancellation.CancelAsync();
        Exception? exception = await Record.ExceptionAsync(() => start.WaitAsync(ReconnectHarness.Timeout));
        await harness.Client.Running.WaitAsync(ReconnectHarness.Timeout);
        harness.Time.Advance(TimeSpan.FromHours(1));

        // Assert
        OperationCanceledException canceled = exception.ShouldBeAssignableTo<OperationCanceledException>()!;
        canceled.CancellationToken.ShouldBe(cancellation.Token);
        harness.Connector.Attempts.Count.ShouldBe(1);
        harness.OptionsCalls.ShouldBe(1);
        harness.Log.Events.ShouldBeEmpty();
        (await harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce)).Status.ShouldBe(MqttRequestStatus.NotConnected);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves cancelling the start's token after the first connection changes nothing: the token covers only the
    /// start.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StartAsync_CancelledAfterConnected_KeepsTheConnection()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        using CancellationTokenSource cancellation = new();
        Task<MqttConnectAck> start = harness.Client.StartAsync(cancellation.Token);
        MemoryLink link = await harness.LinkAsync(1);
        link.Deliver(ClientPackets.ConnAckAccepted);
        await start.WaitAsync(ReconnectHarness.Timeout);

        // Act
        await cancellation.CancelAsync();
        MqttRequestResult published = await harness.Client.PublishAsync("a/b", s_hi, MqttQoS.AtMostOnce);

        // Assert
        published.IsSuccess.ShouldBeTrue();
        harness.Client.Running.IsCompleted.ShouldBeFalse();
        harness.Log.Events.ShouldBe(["connected first"]);
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves the reconnecting client starts once: a second start throws <see cref="InvalidOperationException"/>,
    /// and a start after dispose throws <see cref="ObjectDisposedException"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StartAsync_CalledTwiceOrAfterDispose_Throws()
    {
        // Arrange
        ReconnectHarness harness = ReconnectHarness.Create();
        ReconnectHarness disposed = ReconnectHarness.Create();
        await disposed.Client.DisposeAsync();
        await harness.StartConnectedAsync();

        // Act
        Exception? second = Record.Exception(() => { _ = harness.Client.StartAsync(); });
        Exception? afterDispose = Record.Exception(() => { _ = disposed.Client.StartAsync(); });

        // Assert
        second.ShouldBeOfType<InvalidOperationException>();
        afterDispose.ShouldBeOfType<ObjectDisposedException>();
        disposed.Connector.Attempts.ShouldBeEmpty();
        await harness.StopAsync();
    }

    /// <summary>
    /// Proves options that cannot work are refused when the reconnecting client is created.
    /// </summary>
    /// <param name="initialMilliseconds">
    /// The initial delay.
    /// </param>
    /// <param name="maxMilliseconds">
    /// The longest delay.
    /// </param>
    [Theory]
    [InlineData(0L, 15_000L)]
    [InlineData(-1L, 15_000L)]
    [InlineData(500L, 499L)]
    [InlineData(500L, 4_294_967_295L)]
    public void Constructor_DelaysOutOfRange_ThrowsArgumentOutOfRange(long initialMilliseconds, long maxMilliseconds)
    {
        // Arrange
        MqttWireReconnectOptions options = new()
        {
            ClientOptions = static _ => ValueTask.FromResult(ReconnectHarness.DefaultOptions()),
            InitialDelay = TimeSpan.FromMilliseconds(initialMilliseconds),
            MaxDelay = TimeSpan.FromMilliseconds(maxMilliseconds),
        };

        // Act
        Exception? exception = Record.Exception(() => new MqttWireReconnectingClient(options));

        // Assert
        exception.ShouldBeOfType<ArgumentOutOfRangeException>();
    }

    /// <summary>
    /// Proves an initial delay under one millisecond is refused with <see cref="ArgumentOutOfRangeException"/>.
    /// </summary>
    [Fact]
    public void Constructor_InitialDelayUnderOneMillisecond_ThrowsArgumentOutOfRange()
    {
        // Arrange
        MqttWireReconnectOptions options = new()
        {
            ClientOptions = static _ => ValueTask.FromResult(ReconnectHarness.DefaultOptions()),
            InitialDelay = TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond - 1),
            MaxDelay = TimeSpan.FromMilliseconds(1),
        };

        // Act
        Exception? exception = Record.Exception(() => new MqttWireReconnectingClient(options));

        // Assert
        exception.ShouldBeOfType<ArgumentOutOfRangeException>();
    }

    /// <summary>
    /// Proves options that fail at once, at the smallest delay and the lowest random part, are not retried on the
    /// caller's thread: the start returns after one attempt, and the retry waits for a timer of one millisecond.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StartAsync_OptionsFailAtOnceAtTheSmallestDelay_RetriesOnATimer()
    {
        // Arrange
        TimerRecordingClock clock = new(new FakeTimeProvider());
        int calls = 0;
        MqttWireReconnectOptions options = new()
        {
            ClientOptions = _ =>
            {
                Interlocked.Increment(ref calls);
                throw new InvalidOperationException("No options.");
            },
            InitialDelay = TimeSpan.FromMilliseconds(1),
            MaxDelay = TimeSpan.FromMilliseconds(1),
            OnCallbackError = static _ => { },
        };
        await using MqttWireReconnectingClient client = new(options, new MemoryLinkConnector(), clock, static () => 0.0);

        // Act
        Task<MqttConnectAck> start = client.StartAsync();
        int callsWhenStartReturned = Volatile.Read(ref calls);
        await clock.WhenTimerAsync(TimeSpan.FromMilliseconds(1));

        // Assert
        callsWhenStartReturned.ShouldBe(1);
        start.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves missing options, or a missing <c>ClientOptions</c> callback, are refused with
    /// <see cref="ArgumentNullException"/>.
    /// </summary>
    [Fact]
    public void Constructor_NullOptionsOrCallback_ThrowsArgumentNull()
    {
        // Arrange
        MqttWireReconnectOptions noCallback = new() { ClientOptions = null! };

        // Act
        Exception? nullOptions = Record.Exception(() => new MqttWireReconnectingClient(null!));
        Exception? nullCallback = Record.Exception(() => new MqttWireReconnectingClient(noCallback));

        // Assert
        nullOptions.ShouldBeOfType<ArgumentNullException>();
        nullCallback.ShouldBeOfType<ArgumentNullException>();
    }

    /// <summary>
    /// Reads the client identifier from the CONNECT a link carried first.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <returns>
    /// The client identifier.
    /// </returns>
    private static string ClientIdOf(MemoryLink link)
    {
        byte[] connect = ReconnectHarness.Packets(link.TakeWritten())[0];
        ConnectCodec.TryDecode(connect, out ConnectPacket packet).ShouldBe(MqttReasonCode.Success);
        return Encoding.UTF8.GetString(packet.ClientId);
    }
}
