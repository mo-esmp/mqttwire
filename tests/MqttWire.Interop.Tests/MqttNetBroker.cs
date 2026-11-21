using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.AspNetCore;
using MQTTnet.Protocol;
using MQTTnet.Server;

namespace MqttWire.Interop.Tests;

/// <summary>
/// MQTTnet's broker hosted in Kestrel on a free loopback port, reachable over a WebSocket at <c>/mqtt</c> with the
/// <c>mqtt</c> sub-protocol. It also creates MQTTnet clients connected to the same endpoint, so both sides of an
/// interop test use the same path. Disposing it stops the host and frees the port.
/// </summary>
internal sealed class MqttNetBroker : IAsyncDisposable
{
    /// <summary>
    /// The web application that hosts the broker.
    /// </summary>
    private readonly WebApplication _app;

    /// <summary>
    /// The MQTTnet clients this broker created, disposed with it.
    /// </summary>
    private readonly List<IMqttClient> _clients = [];

    /// <summary>
    /// Completed with the first client disconnect the broker sees.
    /// </summary>
    private readonly TaskCompletionSource<ClientDisconnectedEventArgs> _firstDisconnect;

    /// <summary>
    /// Creates the broker around a started host.
    /// </summary>
    /// <param name="app">
    /// The started web application.
    /// </param>
    /// <param name="webSocketUri">
    /// The address clients connect to.
    /// </param>
    /// <param name="firstDisconnect">
    /// The source the server's disconnect event completes.
    /// </param>
    private MqttNetBroker(WebApplication app, Uri webSocketUri, TaskCompletionSource<ClientDisconnectedEventArgs> firstDisconnect)
    {
        _app = app;
        WebSocketUri = webSocketUri;
        _firstDisconnect = firstDisconnect;
    }

    /// <summary>
    /// Gets the WebSocket address of the broker, <c>ws://127.0.0.1:port/mqtt</c>.
    /// </summary>
    public Uri WebSocketUri { get; }

    /// <summary>
    /// Gets a task that completes with the first client disconnect the broker sees: its type says whether the
    /// client said goodbye with a DISCONNECT packet or just dropped.
    /// </summary>
    public Task<ClientDisconnectedEventArgs> FirstDisconnect => _firstDisconnect.Task;

    /// <summary>
    /// Starts the broker on a free port of 127.0.0.1.
    /// </summary>
    /// <param name="validate">
    /// Decides whether a user name and password may connect; <see langword="null"/> accepts everyone. A
    /// <see langword="false"/> answer refuses the connection with bad user name or password.
    /// </param>
    /// <returns>
    /// The running broker.
    /// </returns>
    public static async Task<MqttNetBroker> StartAsync(Func<string, string, bool>? validate = null)
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddHostedMqttServer(options => options.Build());
        builder.Services.AddMqttConnectionHandler();
        builder.Services.AddConnections();

        WebApplication app = builder.Build();
        TaskCompletionSource<ClientDisconnectedEventArgs> firstDisconnect = new(TaskCreationOptions.RunContinuationsAsynchronously);
        MqttServer server = app.Services.GetRequiredService<MqttServer>();
        server.ValidatingConnectionAsync += args =>
        {
            if (validate is not null && !validate(args.UserName ?? string.Empty, args.Password ?? string.Empty))
            {
                args.ReasonCode = MqttConnectReasonCode.BadUserNameOrPassword;
            }

            return Task.CompletedTask;
        };
        server.ClientDisconnectedAsync += args =>
        {
            firstDisconnect.TrySetResult(args);
            return Task.CompletedTask;
        };

        app.UseWebSockets();
        app.MapMqtt("/mqtt");
        await app.StartAsync().ConfigureAwait(false);

        // Kestrel is listening once StartAsync returns, so the addresses feature is set and holds the bound port.
        string address =app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        Uri uri = new($"ws://{new Uri(address).Authority}/mqtt");
        return new MqttNetBroker(app, uri, firstDisconnect);
    }

    /// <summary>
    /// Creates an MQTT 5 MQTTnet client, connects it to this broker over a WebSocket and returns it. The broker
    /// disposes it with itself.
    /// </summary>
    /// <param name="clientId">
    /// The client identifier.
    /// </param>
    /// <param name="onMessage">
    /// Called for every message the client receives; may be <see langword="null"/>.
    /// </param>
    /// <returns>
    /// The connected client.
    /// </returns>
    public async Task<IMqttClient> CreateClientAsync(string clientId, Func<MqttApplicationMessageReceivedEventArgs, Task>? onMessage = null)
    {
        IMqttClient client = new MqttClientFactory().CreateMqttClient();
        if (onMessage is not null)
        {
            client.ApplicationMessageReceivedAsync += onMessage;
        }

        MqttClientOptions options = new MqttClientOptionsBuilder()
            .WithClientId(clientId)
            .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
            .WithWebSocketServer(o => o.WithUri(WebSocketUri.ToString()))
            .Build();
        _clients.Add(client);
        await client.ConnectAsync(options).ConfigureAwait(false);
        return client;
    }

    /// <summary>
    /// Disconnects the clients this broker created and stops the host, which frees the port.
    /// </summary>
    /// <returns>
    /// A task that completes when the host has stopped.
    /// </returns>
    public async ValueTask DisposeAsync()
    {
        foreach (IMqttClient client in _clients)
        {
            client.Dispose();
        }

        await _app.StopAsync().ConfigureAwait(false);
        await _app.DisposeAsync().ConfigureAwait(false);
    }
}
