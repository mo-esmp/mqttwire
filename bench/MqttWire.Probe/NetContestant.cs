using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;

namespace MqttWire.Probe;

/// <summary>
/// MQTTnet's client in the probe, named <c>mqttnet</c>. Its message is built once, as its cheapest use is.
/// </summary>
internal sealed class NetContestant : IContestant
{
    /// <summary>
    /// The topic the client subscribes to and publishes to.
    /// </summary>
    private const string EchoTopic = "p/echo/net";

    /// <summary>
    /// Released once for every message the client receives, and once more when its connection ends.
    /// </summary>
    private readonly SemaphoreSlim _echo = new(0);

    /// <summary>
    /// The pending publishes of a burst.
    /// </summary>
    private readonly Task<MqttClientPublishResult>[] _burst = new Task<MqttClientPublishResult>[Program.Burst];

    /// <summary>
    /// The client.
    /// </summary>
    private readonly IMqttClient _client;

    /// <summary>
    /// The QoS 1 message to <see cref="EchoTopic"/>, reused for every publish.
    /// </summary>
    private readonly MqttApplicationMessage _message;

    /// <summary>
    /// Whether the client's connection has ended; a waiter that wakes and sees it throws.
    /// </summary>
    private volatile bool _closed;

    /// <summary>
    /// Creates the client, not yet connected, and builds its message.
    /// </summary>
    /// <param name="payload">
    /// The payload of every publish.
    /// </param>
    private NetContestant(byte[] payload)
    {
        _client = new MqttClientFactory().CreateMqttClient();
        _client.ApplicationMessageReceivedAsync += _ =>
        {
            _echo.Release();
            return Task.CompletedTask;
        };
        _client.DisconnectedAsync += _ =>
        {
            OnClosed();
            return Task.CompletedTask;
        };
        _message = new MqttApplicationMessageBuilder()
            .WithTopic(EchoTopic)
            .WithPayload(payload)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();
    }

    /// <summary>
    /// Gets <c>mqttnet</c>.
    /// </summary>
    public string Name => "mqttnet";

    /// <summary>
    /// Creates the client, connects it with MQTT 5 and subscribes it at QoS 1 to its echo topic.
    /// </summary>
    /// <param name="brokerUri">
    /// The broker's WebSocket address.
    /// </param>
    /// <param name="payload">
    /// The payload of every publish.
    /// </param>
    /// <param name="keepAlive">
    /// The keep-alive.
    /// </param>
    /// <returns>
    /// The connected contestant.
    /// </returns>
    public static async Task<NetContestant> ConnectAsync(Uri brokerUri, byte[] payload, TimeSpan keepAlive)
    {
        NetContestant contestant = new(payload);
        MqttClientOptions options = new MqttClientOptionsBuilder()
            .WithClientId("p-net")
            .WithProtocolVersion(MqttProtocolVersion.V500)
            .WithKeepAlivePeriod(keepAlive)
            .WithWebSocketServer(builder => builder.WithUri(brokerUri.ToString()))
            .Build();
        await contestant._client.ConnectAsync(options).ConfigureAwait(false);
        await contestant._client.SubscribeAsync(new MqttTopicFilterBuilder()
            .WithTopic(EchoTopic)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build()).ConfigureAwait(false);
        return contestant;
    }

    /// <summary>
    /// Publishes 64 bytes at QoS 1 to the echo topic, awaits and checks the PUBACK, and then waits for the message
    /// back.
    /// </summary>
    /// <returns>
    /// A task that completes when the message is back.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The publish failed, or the connection ended before the message was back.
    /// </exception>
    public async Task RoundTripAsync()
    {
        Check(await _client.PublishAsync(_message).ConfigureAwait(false));
        await _echo.WaitAsync().ConfigureAwait(false);
        ThrowIfClosed();
    }

    /// <summary>
    /// Starts a burst of QoS 1 publishes to the echo topic, awaits and checks them all, and then waits for every
    /// message back.
    /// </summary>
    /// <returns>
    /// A task that completes when every message is back.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// A publish failed, or the connection ended before every message was back.
    /// </exception>
    public async Task BurstAsync()
    {
        for (int i = 0; i < Program.Burst; i++)
        {
            _burst[i] = _client.PublishAsync(_message);
        }

        for (int i = 0; i < Program.Burst; i++)
        {
            Check(await _burst[i].ConfigureAwait(false));
        }

        for (int i = 0; i < Program.Burst; i++)
        {
            await _echo.WaitAsync().ConfigureAwait(false);
            ThrowIfClosed();
        }
    }

    /// <summary>
    /// Disconnects and disposes the client.
    /// </summary>
    /// <returns>
    /// A task that completes when the client is disconnected.
    /// </returns>
    public async ValueTask DisposeAsync()
    {
        await _client.DisconnectAsync().ConfigureAwait(false);
        _client.Dispose();
        _echo.Dispose();
    }

    /// <summary>
    /// Checks the outcome of a publish.
    /// </summary>
    /// <param name="result">
    /// The outcome.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// The broker answered with a failure.
    /// </exception>
    private static void Check(MqttClientPublishResult result)
    {
        if (!result.IsSuccess)
        {
            throw new InvalidOperationException($"The mqttnet client's publish failed: {result.ReasonCode}.");
        }
    }

    /// <summary>
    /// Fails a wait that the end of the connection woke, rather than a message.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The connection has ended.
    /// </exception>
    private void ThrowIfClosed()
    {
        if (_closed)
        {
            throw new InvalidOperationException("The mqttnet client lost its connection during the probe.");
        }
    }

    /// <summary>
    /// Records that the connection ended and wakes a waiter; does nothing to the semaphore once it is disposed.
    /// </summary>
    private void OnClosed()
    {
        _closed = true;
        try
        {
            _echo.Release();
        }
        catch (ObjectDisposedException)
        {
            // Disposed: nobody waits any more.
        }
    }
}
