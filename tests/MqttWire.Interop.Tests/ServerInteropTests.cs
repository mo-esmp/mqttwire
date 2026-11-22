using System.Text;
using System.Threading.Channels;
using MQTTnet;
using MQTTnet.Protocol;

namespace MqttWire.Interop.Tests;

/// <summary>
/// Proves MqttWire's server, through the test host, against MQTTnet's client over a real WebSocket on loopback.
/// </summary>
public sealed class ServerInteropTests
{
    /// <summary>
    /// The fail-safe wait for something that is expected to happen; a passing test never waits this long.
    /// </summary>
    private static readonly TimeSpan s_failSafe = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Proves a message an MQTTnet client publishes reaches another MQTTnet client subscribed to its topic once, at
    /// the QoS of both.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the subscription and of the publish.
    /// </param>
    [Theory]
    [InlineData(MqttQualityOfServiceLevel.AtMostOnce)]
    [InlineData(MqttQualityOfServiceLevel.AtLeastOnce)]
    [InlineData(MqttQualityOfServiceLevel.ExactlyOnce)]
    public async Task Publish_FromMqttNetClient_ReachesASubscriberAtThatQoS(MqttQualityOfServiceLevel qos)
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        Inbox inbox = new();
        IMqttClient subscriber = await host.ConnectMqttNetClientAsync("net-sub", inbox.AddAsync);
        await subscriber.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic("t/1").WithQualityOfServiceLevel(qos).Build());
        IMqttClient publisher = await host.ConnectMqttNetClientAsync("net-pub");

        // Act
        MqttClientPublishResult published = await publisher.PublishAsync(Message("t/1", "hi", qos));
        Received first = await inbox.NextAsync();
        await publisher.PublishAsync(Message("t/1", "marker", qos));
        Received next = await inbox.NextAsync();

        // Assert
        published.IsSuccess.ShouldBeTrue();
        first.ShouldBe(new Received("t/1", "hi", qos));
        next.Payload.ShouldBe("marker");
    }

    /// <summary>
    /// Proves the server answers an MQTTnet client's subscription with the QoS it asked for, as the test host grants.
    /// </summary>
    /// <param name="qos">
    /// The QoS asked for.
    /// </param>
    [Theory]
    [InlineData(MqttQualityOfServiceLevel.AtMostOnce)]
    [InlineData(MqttQualityOfServiceLevel.AtLeastOnce)]
    [InlineData(MqttQualityOfServiceLevel.ExactlyOnce)]
    public async Task Subscribe_MqttNetClient_IsGrantedTheQoSItAskedFor(MqttQualityOfServiceLevel qos)
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        IMqttClient client = await host.ConnectMqttNetClientAsync("net-1");

        // Act
        MqttClientSubscribeResult result = await client.SubscribeAsync(
            new MqttTopicFilterBuilder().WithTopic("t/1").WithQualityOfServiceLevel(qos).Build());

        // Assert
        result.Items.Count.ShouldBe(1);
        ((int)result.Items.First().ResultCode).ShouldBe((int)qos);
    }

    /// <summary>
    /// Proves an MQTTnet client reads the CONNACK the design announces: receive maximum 256, no maximum QoS (so QoS 2),
    /// no subscription identifiers, and no server keep-alive for a client that asked for 15 seconds.
    /// </summary>
    [Fact]
    public async Task Connect_MqttNetClient_SeesTheConnAckProperties()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        IMqttClient client = host.CreateMqttNetClient();
        MqttClientOptions options = host.CreateMqttNetOptions("net-1").WithKeepAlivePeriod(TimeSpan.FromSeconds(15)).Build();

        // Act
        MqttClientConnectResult result = await client.ConnectAsync(options);

        // Assert
        result.ResultCode.ShouldBe(MqttClientConnectResultCode.Success);
        result.IsSessionPresent.ShouldBeFalse();
        result.ReceiveMaximum.ShouldBe((ushort)256);
        result.MaximumQoS.ShouldBe(MqttQualityOfServiceLevel.ExactlyOnce);
        result.SubscriptionIdentifiersAvailable.ShouldBeFalse();
        result.ServerKeepAlive.ShouldBe((ushort)0);
    }

    /// <summary>
    /// Proves the test host's refusal of the user name <c>refuse-me</c> reaches an MQTTnet client as a refused connect
    /// with "bad user name or password".
    /// </summary>
    [Fact]
    public async Task Connect_RefusedUserName_MqttNetClientSeesBadUserNameOrPassword()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        IMqttClient client = host.CreateMqttNetClient();
        MqttClientOptions options = host.CreateMqttNetOptions("net-1").WithCredentials("refuse-me", "secret").Build();

        // Act
        MqttClientConnectResult result = await client.ConnectAsync(options).WaitAsync(s_failSafe);

        // Assert
        result.ResultCode.ShouldBe(MqttClientConnectResultCode.BadUserNameOrPassword);
        client.IsConnected.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a second MQTTnet client with the client id of a connected one takes over: the first is disconnected with
    /// "session taken over" and the second stays connected.
    /// </summary>
    [Fact]
    public async Task Connect_SameClientIdTwice_TheFirstIsDisconnectedWithSessionTakenOver()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        IMqttClient first = await host.ConnectMqttNetClientAsync("same");
        TaskCompletionSource<MqttClientDisconnectedEventArgs> disconnected = new(TaskCreationOptions.RunContinuationsAsynchronously);
        first.DisconnectedAsync += args =>
        {
            disconnected.TrySetResult(args);
            return Task.CompletedTask;
        };

        // Act
        IMqttClient second = await host.ConnectMqttNetClientAsync("same");
        MqttClientDisconnectedEventArgs args = await disconnected.Task.WaitAsync(s_failSafe);

        // Assert
        args.Reason.ShouldBe(MqttClientDisconnectReason.SessionTakenOver);
        second.IsConnected.ShouldBeTrue();
        (await second.TryPingAsync()).ShouldBeTrue();
    }

    /// <summary>
    /// Proves a QoS 1 publish of 70,000 bytes, over the default limit of 65,536 and in the band the server skips, is
    /// answered with a PUBACK carrying "quota exceeded", and the connection stays open for the next publish.
    /// </summary>
    [Fact]
    public async Task Publish_OverMaxPublishPayloadBytes_IsRefusedWithQuotaExceeded()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        IMqttClient client = await host.ConnectMqttNetClientAsync("net-1");
        MqttApplicationMessage tooLarge = new MqttApplicationMessageBuilder()
            .WithTopic("t/1")
            .WithPayload(new byte[70_000])
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();

        // Act
        MqttClientPublishResult refused = await client.PublishAsync(tooLarge).WaitAsync(s_failSafe);
        MqttClientPublishResult next = await client.PublishAsync(Message("t/1", "small", MqttQualityOfServiceLevel.AtLeastOnce)).WaitAsync(s_failSafe);

        // Assert
        refused.ReasonCode.ShouldBe(MqttClientPublishReasonCode.QuotaExceeded);
        refused.ReasonString.ShouldBe("The message is too large.");
        next.ReasonCode.ShouldBe(MqttClientPublishReasonCode.Success);
        client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves 200 QoS 2 publishes started back to back, within the receive maximum, each complete and each reach a
    /// QoS 2 subscriber exactly once: the marker published after them is the next message after the 200.
    /// </summary>
    [Fact]
    public async Task Publish_TwoHundredQoS2InARow_AllArriveOnce()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        Inbox inbox = new();
        IMqttClient subscriber = await host.ConnectMqttNetClientAsync("net-sub", inbox.AddAsync);
        await subscriber.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic("t/1").WithExactlyOnceQoS().Build());
        IMqttClient publisher = await host.ConnectMqttNetClientAsync("net-pub");

        // Act
        Task<MqttClientPublishResult>[] publishes = new Task<MqttClientPublishResult>[200];
        for (int i = 0; i < publishes.Length; i++)
        {
            publishes[i] = publisher.PublishAsync(Message("t/1", i.ToString(System.Globalization.CultureInfo.InvariantCulture), MqttQualityOfServiceLevel.ExactlyOnce));
        }

        MqttClientPublishResult[] results = await Task.WhenAll(publishes).WaitAsync(s_failSafe);
        HashSet<string> payloads = [];
        for (int i = 0; i < publishes.Length; i++)
        {
            payloads.Add((await inbox.NextAsync()).Payload);
        }

        await publisher.PublishAsync(Message("t/1", "marker", MqttQualityOfServiceLevel.ExactlyOnce)).WaitAsync(s_failSafe);
        Received next = await inbox.NextAsync();

        // Assert
        results.ShouldAllBe(result => result.ReasonCode == MqttClientPublishReasonCode.Success);
        payloads.Count.ShouldBe(200);
        next.Payload.ShouldBe("marker");
    }

    /// <summary>
    /// Builds a message with a text payload.
    /// </summary>
    /// <param name="topic">
    /// The topic.
    /// </param>
    /// <param name="payload">
    /// The payload as text.
    /// </param>
    /// <param name="qos">
    /// The QoS.
    /// </param>
    /// <returns>
    /// The message.
    /// </returns>
    private static MqttApplicationMessage Message(string topic, string payload, MqttQualityOfServiceLevel qos) =>
        new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(Encoding.UTF8.GetBytes(payload))
            .WithQualityOfServiceLevel(qos)
            .Build();

    /// <summary>
    /// A message an MQTTnet client received.
    /// </summary>
    /// <param name="Topic">
    /// The topic it arrived on.
    /// </param>
    /// <param name="Payload">
    /// The payload as text.
    /// </param>
    /// <param name="QoS">
    /// The QoS it was delivered at.
    /// </param>
    private readonly record struct Received(string Topic, string Payload, MqttQualityOfServiceLevel QoS);

    /// <summary>
    /// The messages a client received, in order, for a test to wait on.
    /// </summary>
    private sealed class Inbox
    {
        /// <summary>
        /// The queue the client's event writes and the test reads.
        /// </summary>
        private readonly Channel<Received> _messages = Channel.CreateUnbounded<Received>();

        /// <summary>
        /// Adds a message; the client's message event.
        /// </summary>
        /// <param name="args">
        /// The event's arguments.
        /// </param>
        /// <returns>
        /// A completed task.
        /// </returns>
        public Task AddAsync(MqttApplicationMessageReceivedEventArgs args)
        {
            MqttApplicationMessage message = args.ApplicationMessage;
            _messages.Writer.TryWrite(new Received(message.Topic, Encoding.UTF8.GetString(message.Payload), message.QualityOfServiceLevel));
            return Task.CompletedTask;
        }

        /// <summary>
        /// Waits for the next message, failing the test after the fail-safe time.
        /// </summary>
        /// <returns>
        /// The next message in arrival order.
        /// </returns>
        public async Task<Received> NextAsync() => await _messages.Reader.ReadAsync().AsTask().WaitAsync(s_failSafe);
    }
}
