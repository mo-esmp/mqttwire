using System.Text;
using System.Threading.Channels;
using MqttWire.Server;

namespace MqttWire.Interop.Tests;

/// <summary>
/// Proves MqttWire's client against MqttWire's server, through the test host, over a real WebSocket on loopback.
/// </summary>
public sealed class ClientServerInteropTests
{
    /// <summary>
    /// The fail-safe wait for something that is expected to happen; a passing test never waits this long.
    /// </summary>
    private static readonly TimeSpan s_failSafe = ServerHost.FailSafe;

    /// <summary>
    /// Proves a message one client publishes reaches another client subscribed to its topic once, at the QoS of both.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the subscription and of the publish.
    /// </param>
    [Theory]
    [InlineData(MqttQoS.AtMostOnce)]
    [InlineData(MqttQoS.AtLeastOnce)]
    [InlineData(MqttQoS.ExactlyOnce)]
    public async Task PublishAsync_ToASubscriber_ArrivesAtThatQoS(MqttQoS qos)
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        Inbox<MqttQoS> inbox = new();
        await using MqttWireClient subscriber = CreateClient(host, "wire-sub", inbox);
        (await subscriber.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();
        (await subscriber.SubscribeAsync("t/1", qos).AsTask().WaitAsync(s_failSafe)).GrantedQoS.ShouldBe(qos);
        await using MqttWireClient publisher = CreateClient(host, "wire-pub", inbox: null);
        (await publisher.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();

        // Act
        MqttRequestResult published = await publisher.PublishAsync("t/1", "hi"u8.ToArray(), qos).AsTask().WaitAsync(s_failSafe);
        Received<MqttQoS> first = await inbox.NextAsync();
        await publisher.PublishAsync("t/1", "marker"u8.ToArray(), qos).AsTask().WaitAsync(s_failSafe);
        Received<MqttQoS> next = await inbox.NextAsync();

        // Assert
        published.IsSuccess.ShouldBeTrue();
        first.ShouldBe(new Received<MqttQoS>("t/1", "hi", qos));
        next.Payload.ShouldBe("marker");
    }

    /// <summary>
    /// Proves that QoS 2 deliveries from the server to MqttWire's client complete on both sides: the host hands the
    /// server 1,100 QoS 2 messages for one subscriber, more than four times the 256 exchanges the server keeps open
    /// towards one client, so the later ones go out only as the client's PUBREC and PUBCOMP free the earlier ones'
    /// places; every message arrives once, in order, and a marker sent behind them arrives next.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySend_MoreQoS2MessagesThanTheWindow_AllArriveOnceInOrder()
    {
        // Arrange
        const int Messages = 1_100;
        await using ServerHost host = await ServerHost.StartAsync(static options => options.MaxOutgoingMessages = Messages);
        Inbox<MqttQoS> inbox = new();
        await using MqttWireClient subscriber = CreateClient(host, "wire-sub", inbox);
        (await subscriber.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();
        host.Server.TryGetConnection("wire-sub", out MqttWireConnection? connection).ShouldBeTrue();
        MqttTopic topic = MqttTopic.Create("t/1");

        // Act
        for (int i = 0; i < Messages; i++)
        {
            connection.TrySend(topic, Encoding.UTF8.GetBytes($"m{i}"), MqttQoS.ExactlyOnce).ShouldBe(MqttSendResult.Taken);
        }

        List<Received<MqttQoS>> received = [];
        for (int i = 0; i < Messages; i++)
        {
            received.Add(await inbox.NextAsync());
        }

        connection.TrySend(topic, "marker"u8.ToArray(), MqttQoS.ExactlyOnce).ShouldBe(MqttSendResult.Taken);
        Received<MqttQoS> next = await inbox.NextAsync();

        // Assert
        received.Select(static message => message.Payload)
            .ShouldBe(Enumerable.Range(0, Messages).Select(static i => $"m{i}"));
        received.ShouldAllBe(static message => message.Topic == "t/1" && message.QoS == MqttQoS.ExactlyOnce);
        next.Payload.ShouldBe("marker");
        subscriber.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves the test host's refusal reaches the client as a refused connect that carries the host's reason code.
    /// </summary>
    [Fact]
    public async Task ConnectAsync_Refused_ReportsTheReasonCode()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        await using MqttWireClient client = CreateClient(host, "wire-1", inbox: null, userName: "refuse-me");

        // Act
        MqttConnectAck ack = await client.ConnectAsync().WaitAsync(s_failSafe);

        // Assert
        ack.IsConnected.ShouldBeFalse();
        ack.Failure.ShouldBe(MqttClientCloseReason.ConnectRefused);
        ack.ReasonCode.ShouldBe(MqttReasonCode.BadUserNameOrPassword);
        client.IsConnected.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a payload of 200,000 bytes, which the server reads whole and writes as a header and then pieces of the
    /// host's memory (plan decision 10), arrives whole and unchanged over a real socket.
    /// </summary>
    [Fact]
    public async Task PublishAsync_PayloadOf200000Bytes_ArrivesWhole()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync(options => options.MaxPublishPayloadBytes = 262_144);
        byte[] payload = new byte[200_000];
        new Random(14).NextBytes(payload);
        TaskCompletionSource<byte[]> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
        MqttWireClientOptions subscriberOptions = new()
        {
            Uri = host.WebSocketUri,
            ClientId = "wire-sub",
            OnMessage = (in MqttPublish message) => received.TrySetResult(message.Payload.ToArray()),
        };
        await using MqttWireClient subscriber = new(subscriberOptions);
        (await subscriber.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();
        (await subscriber.SubscribeAsync("t/1", MqttQoS.AtLeastOnce).AsTask().WaitAsync(s_failSafe)).IsSuccess.ShouldBeTrue();
        await using MqttWireClient publisher = CreateClient(host, "wire-pub", inbox: null);
        (await publisher.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();

        // Act
        MqttRequestResult published = await publisher.PublishAsync("t/1", payload, MqttQoS.AtLeastOnce).AsTask().WaitAsync(s_failSafe);
        byte[] arrived = await received.Task.WaitAsync(s_failSafe);

        // Assert
        published.IsSuccess.ShouldBeTrue();
        arrived.SequenceEqual(payload).ShouldBeTrue();
    }

    /// <summary>
    /// Proves the reconnecting client, closed by the host with "administrative action", is told so, connects again,
    /// and subscribes again before it reports the new connection: a message published after that reaches it.
    /// </summary>
    [Fact]
    public async Task ReconnectingClient_ServerClosesTheConnection_ComesBackAndSubscribesAgain()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        Channel<MqttConnectedInfo> connected = Channel.CreateUnbounded<MqttConnectedInfo>();
        TaskCompletionSource<MqttClientCloseInfo> disconnected = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Inbox<MqttQoS> inbox = new();
        MqttWireReconnectOptions options = new()
        {
            ClientOptions = _ => ValueTask.FromResult(new MqttWireClientOptions { Uri = host.WebSocketUri, ClientId = "wire-rc" }),
            InitialDelay = TimeSpan.FromMilliseconds(50),
            OnConnected = info => connected.Writer.TryWrite(info),
            OnDisconnected = info => disconnected.TrySetResult(info),
            OnMessage = (in MqttPublish message) =>
                inbox.Add(new Received<MqttQoS>(message.GetTopic(), Encoding.UTF8.GetString(message.Payload), message.QoS)),
        };
        await using MqttWireReconnectingClient client = new(options);
        (await client.StartAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();
        (await connected.Reader.ReadAsync().AsTask().WaitAsync(s_failSafe)).IsReconnect.ShouldBeFalse();
        (await client.SubscribeAsync("t/1", MqttQoS.AtLeastOnce).AsTask().WaitAsync(s_failSafe)).IsSuccess.ShouldBeTrue();
        await using MqttWireClient publisher = CreateClient(host, "wire-pub", inbox: null);
        (await publisher.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();
        host.Server.TryGetConnection("wire-rc", out MqttWireConnection? first).ShouldBeTrue();

        // Act
        first.Close(MqttReasonCode.AdministrativeAction);
        MqttClientCloseInfo closed = await disconnected.Task.WaitAsync(s_failSafe);
        MqttConnectedInfo again = await connected.Reader.ReadAsync().AsTask().WaitAsync(s_failSafe);
        await publisher.PublishAsync("t/1", "after"u8.ToArray(), MqttQoS.AtLeastOnce).AsTask().WaitAsync(s_failSafe);
        Received<MqttQoS> message = await inbox.NextAsync();

        // Assert
        closed.Reason.ShouldBe(MqttClientCloseReason.ServerDisconnected);
        closed.ReasonCode.ShouldBe(MqttReasonCode.AdministrativeAction);
        again.IsReconnect.ShouldBeTrue();
        message.ShouldBe(new Received<MqttQoS>("t/1", "after", MqttQoS.AtLeastOnce));
        host.Server.TryGetConnection("wire-rc", out MqttWireConnection? second).ShouldBeTrue();
        second.ShouldNotBeSameAs(first);
    }

    /// <summary>
    /// Creates a client for the host.
    /// </summary>
    /// <param name="host">
    /// The host to connect to.
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
    /// <returns>
    /// A client that has not connected yet.
    /// </returns>
    private static MqttWireClient CreateClient(ServerHost host, string clientId, Inbox<MqttQoS>? inbox, string? userName = null)
    {
        MqttWireClientOptions options = new()
        {
            Uri = host.WebSocketUri,
            ClientId = clientId,
            UserName = userName,
            OnMessage = (in MqttPublish message) =>
                inbox?.Add(new Received<MqttQoS>(message.GetTopic(), Encoding.UTF8.GetString(message.Payload), message.QoS)),
        };
        return new MqttWireClient(options);
    }
}
