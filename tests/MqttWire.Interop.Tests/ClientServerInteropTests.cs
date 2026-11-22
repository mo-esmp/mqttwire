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
    private static readonly TimeSpan s_failSafe = TimeSpan.FromSeconds(10);

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
        Inbox inbox = new();
        await using MqttWireClient subscriber = CreateClient(host, "wire-sub", inbox);
        (await subscriber.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();
        (await subscriber.SubscribeAsync("t/1", qos).AsTask().WaitAsync(s_failSafe)).GrantedQoS.ShouldBe(qos);
        await using MqttWireClient publisher = CreateClient(host, "wire-pub", inbox: null);
        (await publisher.ConnectAsync().WaitAsync(s_failSafe)).IsConnected.ShouldBeTrue();

        // Act
        MqttRequestResult published = await publisher.PublishAsync("t/1", "hi"u8.ToArray(), qos).AsTask().WaitAsync(s_failSafe);
        Received first = await inbox.NextAsync();
        await publisher.PublishAsync("t/1", "marker"u8.ToArray(), qos).AsTask().WaitAsync(s_failSafe);
        Received next = await inbox.NextAsync();

        // Assert
        published.IsSuccess.ShouldBeTrue();
        first.ShouldBe(new Received("t/1", "hi", qos));
        next.Payload.ShouldBe("marker");
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
        Inbox inbox = new();
        MqttWireReconnectOptions options = new()
        {
            ClientOptions = _ => ValueTask.FromResult(new MqttWireClientOptions { Uri = host.WebSocketUri, ClientId = "wire-rc" }),
            InitialDelay = TimeSpan.FromMilliseconds(50),
            OnConnected = info => connected.Writer.TryWrite(info),
            OnDisconnected = info => disconnected.TrySetResult(info),
            OnMessage = (in MqttPublish message) =>
                inbox.Add(new Received(message.GetTopic(), Encoding.UTF8.GetString(message.Payload), message.QoS)),
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
        Received message = await inbox.NextAsync();

        // Assert
        closed.Reason.ShouldBe(MqttClientCloseReason.ServerDisconnected);
        closed.ReasonCode.ShouldBe(MqttReasonCode.AdministrativeAction);
        again.IsReconnect.ShouldBeTrue();
        message.ShouldBe(new Received("t/1", "after", MqttQoS.AtLeastOnce));
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
    private static MqttWireClient CreateClient(ServerHost host, string clientId, Inbox? inbox, string? userName = null)
    {
        MqttWireClientOptions options = new()
        {
            Uri = host.WebSocketUri,
            ClientId = clientId,
            UserName = userName,
            OnMessage = (in MqttPublish message) =>
                inbox?.Add(new Received(message.GetTopic(), Encoding.UTF8.GetString(message.Payload), message.QoS)),
        };
        return new MqttWireClient(options);
    }

    /// <summary>
    /// A message a client received, copied out of the read loop's buffer.
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
    private readonly record struct Received(string Topic, string Payload, MqttQoS QoS);

    /// <summary>
    /// The messages a client received, in order, for a test to wait on.
    /// </summary>
    private sealed class Inbox
    {
        /// <summary>
        /// The queue the read loop writes and the test reads.
        /// </summary>
        private readonly Channel<Received> _messages = Channel.CreateUnbounded<Received>();

        /// <summary>
        /// Adds a message; called from the client's callback.
        /// </summary>
        /// <param name="message">
        /// The message.
        /// </param>
        public void Add(Received message) => _messages.Writer.TryWrite(message);

        /// <summary>
        /// Waits for the next message, failing the test after the fail-safe time.
        /// </summary>
        /// <returns>
        /// The next message in arrival order.
        /// </returns>
        public async Task<Received> NextAsync() => await _messages.Reader.ReadAsync().AsTask().WaitAsync(s_failSafe);
    }
}
