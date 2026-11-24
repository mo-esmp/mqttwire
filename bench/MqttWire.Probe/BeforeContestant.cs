#if BEFORE
extern alias before;

using MqttConnectAck = before::MqttWire.MqttConnectAck;
using MqttPublish = before::MqttWire.MqttPublish;
using MqttQoS = before::MqttWire.MqttQoS;
using MqttRequestResult = before::MqttWire.MqttRequestResult;
using MqttWireClient = before::MqttWire.MqttWireClient;
using MqttWireClientOptions = before::MqttWire.MqttWireClientOptions;

namespace MqttWire.Probe;

/// <summary>
/// The older MqttWire client in the probe, named <c>before</c>: the assembly given with <c>-p:BeforeAssembly</c>,
/// referenced under the extern alias <c>before</c>. Compiled only when that assembly is given (<c>BEFORE</c>).
/// </summary>
internal sealed class BeforeContestant : IContestant
{
    /// <summary>
    /// The topic the client subscribes to and publishes to.
    /// </summary>
    private const string EchoTopic = "p/echo/before";

    /// <summary>
    /// Released once for every message the client receives, and once more when its connection ends.
    /// </summary>
    private readonly SemaphoreSlim _echo = new(0);

    /// <summary>
    /// The pending publishes of a burst.
    /// </summary>
    private readonly ValueTask<MqttRequestResult>[] _burst = new ValueTask<MqttRequestResult>[Program.Burst];

    /// <summary>
    /// The payload of every publish.
    /// </summary>
    private readonly byte[] _payload;

    /// <summary>
    /// The client.
    /// </summary>
    private readonly MqttWireClient _client;

    /// <summary>
    /// Whether the client's connection has ended; a waiter that wakes and sees it throws.
    /// </summary>
    private volatile bool _closed;

    /// <summary>
    /// Creates the client, not yet connected.
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
    private BeforeContestant(Uri brokerUri, byte[] payload, TimeSpan keepAlive)
    {
        _payload = payload;
        _client = new MqttWireClient(new MqttWireClientOptions
        {
            Uri = brokerUri,
            ClientId = "p-before",
            KeepAlive = keepAlive,
            OnMessage = (in MqttPublish _) => _echo.Release(),
            OnClosed = _ => OnClosed(),
        });
    }

    /// <summary>
    /// Gets <c>before</c>.
    /// </summary>
    public string Name => "before";

    /// <summary>
    /// Creates the client, connects it and subscribes it at QoS 1 to its echo topic.
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
    /// <exception cref="InvalidOperationException">
    /// The client could not connect or subscribe.
    /// </exception>
    public static async Task<BeforeContestant> ConnectAsync(Uri brokerUri, byte[] payload, TimeSpan keepAlive)
    {
        BeforeContestant contestant = new(brokerUri, payload, keepAlive);
        MqttConnectAck ack = await contestant._client.ConnectAsync().ConfigureAwait(false);
        if (!ack.IsConnected)
        {
            throw new InvalidOperationException($"The before client could not connect: {ack.Failure}.");
        }

        MqttRequestResult subscribed = await contestant._client.SubscribeAsync(EchoTopic, MqttQoS.AtLeastOnce).ConfigureAwait(false);
        if (!subscribed.IsSuccess)
        {
            throw new InvalidOperationException($"The before client could not subscribe: {subscribed.Status}.");
        }

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
        Check(await _client.PublishAsync(EchoTopic, _payload, MqttQoS.AtLeastOnce).ConfigureAwait(false));
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
            _burst[i] = _client.PublishAsync(EchoTopic, _payload, MqttQoS.AtLeastOnce);
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
    /// Disposes the client, which closes its connection.
    /// </summary>
    /// <returns>
    /// A task that completes when the connection is closed.
    /// </returns>
    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync().ConfigureAwait(false);
        _echo.Dispose();
    }

    /// <summary>
    /// Checks the outcome of a publish.
    /// </summary>
    /// <param name="result">
    /// The outcome.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// The publish did not succeed.
    /// </exception>
    private static void Check(MqttRequestResult result)
    {
        if (!result.IsSuccess)
        {
            throw new InvalidOperationException($"The before client's publish failed: {result.Status}, {result.ReasonCode}.");
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
            throw new InvalidOperationException("The before client lost its connection during the probe.");
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
#endif
