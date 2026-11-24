using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet.AspNetCore;

namespace MqttWire.Microbench;

/// <summary>
/// MQTTnet's broker hosted in Kestrel on a free loopback port, reachable over a WebSocket at <c>/mqtt</c> with the
/// <c>mqtt</c> sub-protocol: the one broker both clients of <see cref="ClientComparisonBenchmarks"/> talk to, so
/// that only the clients differ. Disposing it stops the host and frees the port.
/// </summary>
internal sealed class MqttNetBrokerHost : IAsyncDisposable
{
    /// <summary>
    /// The web application that hosts the broker.
    /// </summary>
    private readonly WebApplication _app;

    /// <summary>
    /// Creates the host around a started web application.
    /// </summary>
    /// <param name="app">
    /// The started web application.
    /// </param>
    /// <param name="webSocketUri">
    /// The address clients connect to.
    /// </param>
    private MqttNetBrokerHost(WebApplication app, Uri webSocketUri)
    {
        _app = app;
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
        return new MqttNetBrokerHost(app, new Uri($"ws://{new Uri(address).Authority}/mqtt"));
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
    }
}
