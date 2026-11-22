using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MqttWire.Server;
using MqttWire.TestHost;

namespace MqttWire.Interop.Tests;

/// <summary>
/// MqttWire's server hosted in Kestrel on a free port of 127.0.0.1, reachable over a WebSocket at <c>/mqtt</c>: the
/// test host's application (<see cref="Program"/>, with <see cref="ExactTopicHandler"/>), or the same with another
/// handler. Every line the server and Kestrel log at their levels is kept in <see cref="Logs"/>. It also creates
/// MQTTnet clients for the endpoint and disposes them with itself. Disposing it stops the host and frees the port.
/// </summary>
internal sealed class ServerHost : IAsyncDisposable
{
    /// <summary>
    /// The web application that hosts the server.
    /// </summary>
    private readonly WebApplication _app;

    /// <summary>
    /// The MQTTnet clients this host created, disposed with it.
    /// </summary>
    private readonly List<IMqttClient> _clients = [];

    /// <summary>
    /// Creates the host around a started application.
    /// </summary>
    /// <param name="app">
    /// The started application.
    /// </param>
    /// <param name="logs">
    /// The provider that records its log.
    /// </param>
    /// <param name="scheme">
    /// <c>http</c> or <c>https</c>.
    /// </param>
    private ServerHost(WebApplication app, LogRecorder logs, string scheme)
    {
        _app = app;
        Logs = logs;
        int port = Program.GetPort(app);
        BaseUri = new Uri($"{scheme}://127.0.0.1:{port}");
        WebSocketUri = new Uri($"{(scheme == "https" ? "wss" : "ws")}://127.0.0.1:{port}{Program.EndpointPath}");
        Server = app.Services.GetRequiredService<MqttWireServer>();
    }

    /// <summary>
    /// Gets the WebSocket address of the endpoint, <c>ws://127.0.0.1:port/mqtt</c>.
    /// </summary>
    public Uri WebSocketUri { get; }

    /// <summary>
    /// Gets the HTTP address of the application, <c>http://127.0.0.1:port</c>.
    /// </summary>
    public Uri BaseUri { get; }

    /// <summary>
    /// Gets the server, for the public calls a host makes on it.
    /// </summary>
    public MqttWireServer Server { get; }

    /// <summary>
    /// Gets the application's services.
    /// </summary>
    public IServiceProvider Services => _app.Services;

    /// <summary>
    /// Gets every line MqttWire logged at debug level and above, and the rest of the application at warning level and
    /// above.
    /// </summary>
    public LogRecorder Logs { get; }

    /// <summary>
    /// Starts the test host's application on a free port of 127.0.0.1.
    /// </summary>
    /// <param name="configure">
    /// Changes the server's options, or <see langword="null"/> to keep the test host's.
    /// </param>
    /// <returns>
    /// The running host.
    /// </returns>
    public static Task<ServerHost> StartAsync(Action<MqttWireServerOptions>? configure = null) =>
        StartAsync(new Setup { ConfigureServer = configure });

    /// <summary>
    /// Starts the test host's application, set up as <paramref name="setup"/> says.
    /// </summary>
    /// <param name="setup">
    /// How the application differs from the test host's own.
    /// </param>
    /// <returns>
    /// The running host.
    /// </returns>
    public static Task<ServerHost> StartAsync(Setup setup) => StartAsync(Program.CreateBuilder([]), setup);

    /// <summary>
    /// Starts an application like the test host's, with <typeparamref name="THandler"/> in place of
    /// <see cref="ExactTopicHandler"/>.
    /// </summary>
    /// <typeparam name="THandler">
    /// The handler; the test gets the one instance from <see cref="Services"/>.
    /// </typeparam>
    /// <param name="setup">
    /// How the application differs from the test host's own.
    /// </param>
    /// <returns>
    /// The running host.
    /// </returns>
    public static Task<ServerHost> StartAsync<THandler>(Setup setup)
        where THandler : class, IMqttWireHandler
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Services.AddMqttWireServer<THandler>();
        return StartAsync(builder, setup);
    }

    /// <summary>
    /// Creates an MQTTnet client that this host disposes with itself.
    /// </summary>
    /// <returns>
    /// A client that has not connected.
    /// </returns>
    public IMqttClient CreateMqttNetClient()
    {
        IMqttClient client = new MqttClientFactory().CreateMqttClient();
        _clients.Add(client);
        return client;
    }

    /// <summary>
    /// Creates the options of an MQTT 5 MQTTnet client for this host's endpoint.
    /// </summary>
    /// <param name="clientId">
    /// The client identifier.
    /// </param>
    /// <returns>
    /// The builder, for the test to add to before it builds.
    /// </returns>
    public MqttClientOptionsBuilder CreateMqttNetOptions(string clientId) =>
        new MqttClientOptionsBuilder()
            .WithClientId(clientId)
            .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
            .WithWebSocketServer(o => o.WithUri(WebSocketUri.ToString()));

    /// <summary>
    /// Creates an MQTTnet client, connects it to this host and returns it.
    /// </summary>
    /// <param name="clientId">
    /// The client identifier.
    /// </param>
    /// <param name="onMessage">
    /// Called for every message the client receives, or <see langword="null"/>.
    /// </param>
    /// <returns>
    /// The connected client.
    /// </returns>
    public async Task<IMqttClient> ConnectMqttNetClientAsync(string clientId,
        Func<MqttApplicationMessageReceivedEventArgs, Task>? onMessage = null)
    {
        IMqttClient client = CreateMqttNetClient();
        if (onMessage is not null)
        {
            client.ApplicationMessageReceivedAsync += onMessage;
        }

        await client.ConnectAsync(CreateMqttNetOptions(clientId).Build()).ConfigureAwait(false);
        return client;
    }

    /// <summary>
    /// Stops the application, which stops the server first: the stop of the host in the order a real host stops.
    /// </summary>
    /// <returns>
    /// A task that completes when the application has stopped.
    /// </returns>
    public Task StopAsync() => _app.StopAsync();

    /// <summary>
    /// Disposes the clients this host created and stops the application, which frees the port.
    /// </summary>
    /// <returns>
    /// A task that completes when the application has stopped.
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

    /// <summary>
    /// Sets the builder up, builds and starts the application.
    /// </summary>
    /// <param name="builder">
    /// The builder, with the server registered.
    /// </param>
    /// <param name="setup">
    /// How the application differs from the test host's own.
    /// </param>
    /// <returns>
    /// The running host.
    /// </returns>
    private static async Task<ServerHost> StartAsync(WebApplicationBuilder builder, Setup setup)
    {
        LogRecorder logs = new();
        builder.Logging.ClearProviders();
        builder.Logging.AddProvider(logs);
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.Logging.AddFilter("MqttWire", LogLevel.Debug);
        if (setup.ConfigureServer is not null)
        {
            builder.Services.Configure(setup.ConfigureServer);
        }

        string scheme = setup.Certificate is null ? "http" : "https";
        builder.WebHost.UseUrls($"{scheme}://127.0.0.1:0");
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.ConfigureEndpointDefaults(listen => listen.Protocols = setup.Protocols);
            if (setup.Certificate is not null)
            {
                kestrel.ConfigureHttpsDefaults(https => https.ServerCertificate = setup.Certificate);
            }
        });
        setup.ConfigureBuilder?.Invoke(builder);

        WebApplication app = builder.Build();
        if (setup.UseWebSockets)
        {
            Program.Configure(app);
        }
        else
        {
            app.MapMqttWire(Program.EndpointPath);
        }

        setup.ConfigureApp?.Invoke(app);
        await app.StartAsync().ConfigureAwait(false);
        return new ServerHost(app, logs, scheme);
    }

    /// <summary>
    /// How an application differs from the test host's own.
    /// </summary>
    public sealed class Setup
    {
        /// <summary>
        /// Gets the change to the server's options, or <see langword="null"/> for none.
        /// </summary>
        public Action<MqttWireServerOptions>? ConfigureServer { get; init; }

        /// <summary>
        /// Gets a value indicating whether the application calls <c>UseWebSockets()</c>; <see langword="true"/> by
        /// default. Without it only <c>MapMqttWire</c> is called.
        /// </summary>
        public bool UseWebSockets { get; init; } = true;

        /// <summary>
        /// Gets the HTTP versions Kestrel speaks; HTTP/1.1 by default, which is all Kestrel speaks without TLS unless
        /// told to speak only HTTP/2.
        /// </summary>
        public HttpProtocols Protocols { get; init; } = HttpProtocols.Http1;

        /// <summary>
        /// Gets the certificate to listen with over TLS, or <see langword="null"/> to listen without TLS.
        /// </summary>
        public X509Certificate2? Certificate { get; init; }

        /// <summary>
        /// Gets a further change to the builder, made before it builds, or <see langword="null"/>.
        /// </summary>
        public Action<WebApplicationBuilder>? ConfigureBuilder { get; init; }

        /// <summary>
        /// Gets a further change to the application, made after the endpoint is mapped, or <see langword="null"/>.
        /// </summary>
        public Action<WebApplication>? ConfigureApp { get; init; }
    }
}
