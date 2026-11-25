using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet.AspNetCore;
using MQTTnet.Server;

namespace MqttWire.Microbench;

/// <summary>
/// MQTTnet's broker hosted in Kestrel on a free loopback port, reachable over a WebSocket at <c>/mqtt</c> with the
/// <c>mqtt</c> sub-protocol: the one broker both clients of <see cref="ClientComparisonBenchmarks"/> talk to, so
/// that only the clients differ. The interleaved probe, <c>bench/MqttWire.Probe</c>, links this file and uses the
/// same broker. It can count the publishes it receives on one topic, so that a benchmark can wait for the broker's
/// receipt of a batch (<see cref="ExpectPublishes"/>); the interceptor that counts them is registered by the first
/// <see cref="ExpectPublishes"/>, so a process that never counts does not pay for it on every publish. Disposing it
/// stops the host and frees the port.
/// </summary>
internal sealed class MqttNetBrokerHost : IAsyncDisposable
{
    /// <summary>
    /// The web application that hosts the broker.
    /// </summary>
    private readonly WebApplication _app;

    /// <summary>
    /// The broker, whose publish interceptor <see cref="ExpectPublishes"/> registers.
    /// </summary>
    private readonly MqttServer _server;

    /// <summary>
    /// Whether <see cref="CountPublishAsync"/> is registered as the broker's publish interceptor.
    /// </summary>
    private bool _counting;

    /// <summary>
    /// Released once when the publishes expected by <see cref="ExpectPublishes"/> have all been received, or by
    /// <see cref="WakePublishWaiter"/>.
    /// </summary>
    private readonly SemaphoreSlim _received = new(0);

    /// <summary>
    /// The topic whose publishes are counted, or <see langword="null"/> before the first
    /// <see cref="ExpectPublishes"/>.
    /// </summary>
    private volatile string? _countedTopic;

    /// <summary>
    /// The number of publishes on <see cref="_countedTopic"/> still expected; below zero once more arrived than
    /// were expected.
    /// </summary>
    private int _remaining;

    /// <summary>
    /// Creates the host around a started web application.
    /// </summary>
    /// <param name="app">
    /// The started web application.
    /// </param>
    /// <param name="server">
    /// The broker the application hosts.
    /// </param>
    /// <param name="webSocketUri">
    /// The address clients connect to.
    /// </param>
    private MqttNetBrokerHost(WebApplication app, MqttServer server, Uri webSocketUri)
    {
        _app = app;
        _server = server;
        WebSocketUri = webSocketUri;
    }

    /// <summary>
    /// Gets the WebSocket address of the broker, <c>ws://127.0.0.1:port/mqtt</c>.
    /// </summary>
    public Uri WebSocketUri { get; }

    /// <summary>
    /// Starts the broker on a free port of 127.0.0.1, with logging off so that it costs nothing per packet.
    /// </summary>
    /// <returns>
    /// The running broker.
    /// </returns>
    public static async Task<MqttNetBrokerHost> StartAsync()
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddHostedMqttServer(options => options.Build());
        builder.Services.AddMqttConnectionHandler();
        builder.Services.AddConnections();

        WebApplication app = builder.Build();
        app.UseWebSockets();
        app.MapMqtt("/mqtt");
        await app.StartAsync().ConfigureAwait(false);

        // Kestrel is listening once StartAsync returns, so the addresses feature is set and holds the bound port.
        string address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new MqttNetBrokerHost(app, app.Services.GetRequiredService<MqttServer>(),
            new Uri($"ws://{new Uri(address).Authority}/mqtt"));
    }

    /// <summary>
    /// Starts counting the publishes the broker receives on a topic: once <paramref name="count"/> of them have
    /// arrived, <see cref="WaitForExpectedPublishesAsync"/> completes. Call it before the first publish of the batch,
    /// and only when no earlier batch is still being counted. The first call registers the broker's publish
    /// interceptor, which then runs for every publish the broker receives; nothing is allocated by later calls.
    /// </summary>
    /// <param name="topic">
    /// The topic to count, compared by reference first and then by value; nothing is allocated for it.
    /// </param>
    /// <param name="count">
    /// The number of publishes in the batch.
    /// </param>
    public void ExpectPublishes(string topic, int count)
    {
        Volatile.Write(ref _remaining, count);
        _countedTopic = topic;
        if (!_counting)
        {
            _counting = true;
            _server.InterceptingPublishAsync += CountPublishAsync;
        }
    }

    /// <summary>
    /// Gets the number of publishes of the batch the broker has not received yet: 0 once the whole batch has
    /// arrived, more when a wait of <see cref="WaitForExpectedPublishesAsync"/> was woken without it, and below zero
    /// when more arrived than were expected.
    /// </summary>
    public int RemainingPublishes => Volatile.Read(ref _remaining);

    /// <summary>
    /// Waits until the broker has received every publish expected by <see cref="ExpectPublishes"/>, or until
    /// <see cref="WakePublishWaiter"/> wakes the wait; the caller tells the two apart with
    /// <see cref="RemainingPublishes"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the batch has arrived or the wait was woken.
    /// </returns>
    public Task WaitForExpectedPublishesAsync() => _received.WaitAsync();

    /// <summary>
    /// Wakes a wait of <see cref="WaitForExpectedPublishesAsync"/> that would otherwise never end, because the
    /// client that publishes the batch has lost its connection. Does nothing once the host is disposed.
    /// </summary>
    public void WakePublishWaiter()
    {
        try
        {
            _received.Release();
        }
        catch (ObjectDisposedException)
        {
            // The host is stopped: nobody waits any more.
        }
    }

    /// <summary>
    /// Counts a publish the broker received, when it is on the counted topic, and releases the waiter when it is the
    /// last one expected.
    /// </summary>
    /// <param name="args">
    /// The publish.
    /// </param>
    /// <returns>
    /// A completed task; the publish goes on as the broker handles it.
    /// </returns>
    private Task CountPublishAsync(InterceptingPublishEventArgs args)
    {
        if (string.Equals(args.ApplicationMessage.Topic, _countedTopic, StringComparison.Ordinal)
            && Interlocked.Decrement(ref _remaining) == 0)
        {
            _received.Release();
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Stops the host, which closes every connection and frees the port.
    /// </summary>
    /// <returns>
    /// A task that completes when the host has stopped.
    /// </returns>
    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync().ConfigureAwait(false);
        await _app.DisposeAsync().ConfigureAwait(false);
        _received.Dispose();
    }
}
