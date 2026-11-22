using System.Text;
using MQTTnet;
using MQTTnet.Protocol;
using MQTTnet.Server;

namespace MqttWire.Interop.Tests;

/// <summary>
/// Proves the public client against MQTTnet's broker over a real WebSocket on loopback.
/// </summary>
public sealed class ClientInteropTests
{
    /// <summary>
    /// The fail-safe wait for something that is expected to happen; a passing test never waits this long.
    /// </summary>
    private static readonly TimeSpan s_failSafe = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Proves a client reaches MQTTnet's broker and is told it is connected.
    /// </summary>
    [Fact]
    public async Task ConnectAsync_MqttNetBroker_Connects()
    {
        // Arrange
        await using MqttNetBroker broker = await MqttNetBroker.StartAsync();
        await using MqttWireClient client = CreateClient(broker, "wire-1", inbox: null);

        // Act
        MqttConnectAck ack = await client.ConnectAsync().WaitAsync(s_failSafe);

        // Assert
        ack.IsConnected.ShouldBeTrue();
        ack.ReasonCode.ShouldBe(MqttReasonCode.Success);
        client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a refusal by the broker's credential check reaches the caller as a refused connect with code 0x86.
    /// </summary>
    [Fact]
    public async Task ConnectAsync_BadPassword_ReturnsBadUserNameOrPassword()
    {
        // Arrange
        await using MqttNetBroker broker = await MqttNetBroker.StartAsync((user, password) => password == "right");
        await using MqttWireClient client = CreateClient(broker, "wire-1", inbox: null, userName: "me", password: "wrong");

        // Act
        MqttConnectAck ack = await client.ConnectAsync().WaitAsync(s_failSafe);

        // Assert
        ack.IsConnected.ShouldBeFalse();
        ack.Failure.ShouldBe(MqttClientCloseReason.ConnectRefused);
        ack.ReasonCode.ShouldBe(MqttReasonCode.BadUserNameOrPassword);
        ((byte)ack.ReasonCode).ShouldBe((byte)0x86);
    }

    /// <summary>
    /// Proves a message published by the client reaches an MQTTnet subscriber at the subscribed QoS.
    /// </summary>
    /// <param name="qos">
    /// The QoS of both the publish and the subscription.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce)]
    [InlineData(MqttQoS.AtLeastOnce)]
    [InlineData(MqttQoS.ExactlyOnce)]
    public async Task Publish_MqttWireToMqttNet_Arrives(MqttQoS qos)
    {
        // Arrange
        await using MqttNetBroker broker = await MqttNetBroker.StartAsync();
        TaskCompletionSource<MqttApplicationMessage> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
        IMqttClient subscriber = await broker.CreateClientAsync("net-1", args =>
        {
            received.TrySetResult(args.ApplicationMessage);
            return Task.CompletedTask;
        });
        await subscriber.SubscribeAsync(new MqttTopicFilterBuilder()
            .WithTopic("a/b")
            .WithQualityOfServiceLevel((MqttQualityOfServiceLevel)(byte)qos)
            .Build());
        await using MqttWireClient client = CreateClient(broker, "wire-1", inbox: null);
        (await client.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();

        // Act
        MqttRequestResult result = await client.PublishAsync("a/b", Encoding.UTF8.GetBytes("hi"), qos).AsTask().WaitAsync(s_failSafe);
        MqttApplicationMessage message = await received.Task.WaitAsync(s_failSafe);

        // Assert
        result.IsSuccess.ShouldBeTrue();
        message.Topic.ShouldBe("a/b");
        Encoding.UTF8.GetString(message.Payload).ShouldBe("hi");
        ((byte)message.QualityOfServiceLevel).ShouldBe((byte)qos);
    }

    /// <summary>
    /// Proves a message published by an MQTTnet client reaches the client at the QoS it subscribed with.
    /// </summary>
    /// <param name="qos">
    /// The QoS of both the publish and the subscription.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce)]
    [InlineData(MqttQoS.AtLeastOnce)]
    [InlineData(MqttQoS.ExactlyOnce)]
    public async Task Publish_MqttNetToMqttWire_Arrives(MqttQoS qos)
    {
        // Arrange
        await using MqttNetBroker broker = await MqttNetBroker.StartAsync();
        Inbox<MqttQoS> inbox = new();
        await using MqttWireClient client = CreateClient(broker, "wire-1", inbox);
        (await client.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();
        (await client.SubscribeAsync("a/b", qos).AsTask().WaitAsync(s_failSafe)).IsSuccess.ShouldBeTrue();
        IMqttClient publisher = await broker.CreateClientAsync("net-1");

        // Act
        await publisher.PublishAsync(new MqttApplicationMessageBuilder()
            .WithTopic("a/b")
            .WithPayload("hi"u8.ToArray())
            .WithQualityOfServiceLevel((MqttQualityOfServiceLevel)(byte)qos)
            .Build());
        Received<MqttQoS> message = await inbox.NextAsync();

        // Assert
        message.ShouldBe(new Received<MqttQoS>("a/b", "hi", qos));
    }

    /// <summary>
    /// Proves a client subscribed to a wildcard receives its own publish exactly once, because the next message
    /// after it is the following publish and not a duplicate.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the subscription and the publishes.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce)]
    [InlineData(MqttQoS.AtLeastOnce)]
    [InlineData(MqttQoS.ExactlyOnce)]
    public async Task Publish_MqttWireToItself_ArrivesOnce(MqttQoS qos)
    {
        // Arrange
        await using MqttNetBroker broker = await MqttNetBroker.StartAsync();
        Inbox<MqttQoS> inbox = new();
        await using MqttWireClient client = CreateClient(broker, "wire-1", inbox);
        (await client.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();
        (await client.SubscribeAsync("a/#", qos).AsTask().WaitAsync(s_failSafe)).IsSuccess.ShouldBeTrue();

        // Act
        MqttRequestResult first = await client.PublishAsync("a/b", "one"u8.ToArray(), qos).AsTask().WaitAsync(s_failSafe);
        MqttRequestResult second = await client.PublishAsync("a/c", "two"u8.ToArray(), qos).AsTask().WaitAsync(s_failSafe);
        Received<MqttQoS> firstMessage = await inbox.NextAsync();
        Received<MqttQoS> secondMessage = await inbox.NextAsync();

        // Assert
        first.IsSuccess.ShouldBeTrue();
        second.IsSuccess.ShouldBeTrue();
        firstMessage.ShouldBe(new Received<MqttQoS>("a/b", "one", qos));
        secondMessage.ShouldBe(new Received<MqttQoS>("a/c", "two", qos));
    }

    /// <summary>
    /// Proves that after an unsubscribe the broker stops delivering: a later publish to the topic is skipped and
    /// the next message received is the one for a topic that is still subscribed.
    /// </summary>
    [Fact]
    public async Task UnsubscribeAsync_MqttNetBroker_StopsDelivery()
    {
        // Arrange
        await using MqttNetBroker broker = await MqttNetBroker.StartAsync();
        Inbox<MqttQoS> inbox = new();
        await using MqttWireClient client = CreateClient(broker, "wire-1", inbox);
        (await client.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();
        (await client.SubscribeAsync("a/#", MqttQoS.AtLeastOnce).AsTask().WaitAsync(s_failSafe)).IsSuccess.ShouldBeTrue();
        (await client.SubscribeAsync("z/#", MqttQoS.AtLeastOnce).AsTask().WaitAsync(s_failSafe)).IsSuccess.ShouldBeTrue();
        (await client.PublishAsync("a/b", "before"u8.ToArray(), MqttQoS.AtLeastOnce).AsTask().WaitAsync(s_failSafe)).IsSuccess.ShouldBeTrue();
        (await inbox.NextAsync()).Payload.ShouldBe("before");

        // Act
        MqttRequestResult unsubscribed = await client.UnsubscribeAsync("a/#").AsTask().WaitAsync(s_failSafe);
        await client.PublishAsync("a/b", "after"u8.ToArray(), MqttQoS.AtLeastOnce).AsTask().WaitAsync(s_failSafe);
        await client.PublishAsync("z/y", "marker"u8.ToArray(), MqttQoS.AtLeastOnce).AsTask().WaitAsync(s_failSafe);
        Received<MqttQoS> next = await inbox.NextAsync();

        // Assert
        unsubscribed.IsSuccess.ShouldBeTrue();
        next.ShouldBe(new Received<MqttQoS>("z/y", "marker", MqttQoS.AtLeastOnce));
    }

    /// <summary>
    /// Proves disposing the client sends a DISCONNECT, so the broker sees a clean goodbye and not a dropped link.
    /// </summary>
    [Fact]
    public async Task DisposeAsync_MqttNetBroker_BrokerSeesNormalDisconnect()
    {
        // Arrange
        await using MqttNetBroker broker = await MqttNetBroker.StartAsync();
        MqttWireClient client = CreateClient(broker, "wire-1", inbox: null);
        (await client.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();

        // Act
        await client.DisposeAsync();
        ClientDisconnectedEventArgs disconnected = await broker.FirstDisconnect.WaitAsync(s_failSafe);

        // Assert
        disconnected.ClientId.ShouldBe("wire-1");
        disconnected.DisconnectType.ShouldBe(MqttClientDisconnectType.Clean);
    }

    /// <summary>
    /// Proves a client with a one-second keep-alive pings often enough that the broker, which drops a silent
    /// client after one and a half intervals, still has it after three seconds. This is the only test that waits
    /// in real time.
    /// </summary>
    [Fact]
    public async Task KeepAlive_OneSecond_ConnectionStaysUpForThreeSeconds()
    {
        // Arrange
        await using MqttNetBroker broker = await MqttNetBroker.StartAsync();
        Inbox<MqttQoS> inbox = new();
        await using MqttWireClient client = CreateClient(broker, "wire-1", inbox, keepAlive: TimeSpan.FromSeconds(1));
        (await client.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();
        (await client.SubscribeAsync("a/#", MqttQoS.AtLeastOnce).AsTask().WaitAsync(s_failSafe)).IsSuccess.ShouldBeTrue();

        // Act
        await Task.Delay(TimeSpan.FromSeconds(3));
        MqttRequestResult result = await client.PublishAsync("a/b", "alive"u8.ToArray(), MqttQoS.AtLeastOnce).AsTask().WaitAsync(s_failSafe);
        Received<MqttQoS> message = await inbox.NextAsync();

        // Assert
        client.IsConnected.ShouldBeTrue();
        broker.FirstDisconnect.IsCompleted.ShouldBeFalse();
        result.IsSuccess.ShouldBeTrue();
        message.Payload.ShouldBe("alive");
    }

    /// <summary>
    /// Creates a client for the broker with the options the tests vary.
    /// </summary>
    /// <param name="broker">
    /// The broker to connect to.
    /// </param>
    /// <param name="clientId">
    /// The client identifier.
    /// </param>
    /// <param name="inbox">
    /// Where received messages go; <see langword="null"/> drops them.
    /// </param>
    /// <param name="userName">
    /// The user name, or <see langword="null"/>.
    /// </param>
    /// <param name="password">
    /// The password, or <see langword="null"/>.
    /// </param>
    /// <param name="keepAlive">
    /// The keep-alive interval; <see langword="null"/> keeps the default.
    /// </param>
    /// <returns>
    /// A client that has not connected yet.
    /// </returns>
    private static MqttWireClient CreateClient(
        MqttNetBroker broker,
        string clientId,
        Inbox<MqttQoS>? inbox,
        string? userName = null,
        string? password = null,
        TimeSpan? keepAlive = null)
    {
        MqttWireClientOptions options = new()
        {
            Uri = broker.WebSocketUri,
            ClientId = clientId,
            UserName = userName,
            Password = password,
            KeepAlive = keepAlive ?? TimeSpan.FromSeconds(15),
            OnMessage = (in MqttPublish message) => inbox?.Add(new Received<MqttQoS>(message.GetTopic(), Encoding.UTF8.GetString(message.Payload), message.QoS)),
        };
        return new MqttWireClient(options);
    }
}
