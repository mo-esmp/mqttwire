using System.Globalization;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using MqttWire.Server;

namespace MqttWire.TestHost;

/// <summary>
/// The test host: MqttWire's server with <see cref="ExactTopicHandler"/> at <c>/mqtt</c>. Run as a program, it
/// listens on the URLs given with <c>--urls</c>, takes <c>--max-publish-payload-bytes &lt;n&gt;</c>, prints
/// <c>LISTENING &lt;port&gt;</c> on standard output once Kestrel is listening, and stops when it reads the line
/// <c>STOP</c> on standard input. The tests that reference it as a library
/// build the same application with <see cref="CreateBuilder"/> and <see cref="Configure"/>.
/// </summary>
public static class Program
{
    /// <summary>
    /// The path of the MQTT endpoint.
    /// </summary>
    public const string EndpointPath = "/mqtt";

    /// <summary>
    /// The command-line key of <see cref="MqttWireServerOptions.MaxPublishPayloadBytes"/>, given as
    /// <c>--max-publish-payload-bytes &lt;n&gt;</c>.
    /// </summary>
    public const string MaxPublishPayloadBytesKey = "max-publish-payload-bytes";

    /// <summary>
    /// The line on standard input that stops the host when it runs as a program.
    /// </summary>
    public const string StopCommand = "STOP";

    /// <summary>
    /// Builds the application, starts it, prints <c>LISTENING &lt;port&gt;</c> with the port of the first address
    /// Kestrel listens on, and runs until the host is stopped.
    /// </summary>
    /// <param name="args">
    /// The command line: <c>--urls</c>, <c>--max-publish-payload-bytes</c>, and anything else ASP.NET Core reads.
    /// </param>
    /// <returns>
    /// A task that completes when the host has stopped.
    /// </returns>
    public static async Task Main(string[] args)
    {
        WebApplication app = CreateBuilder(args).Build();
        Configure(app);
        await app.StartAsync().ConfigureAwait(false);
        Console.WriteLine($"LISTENING {GetPort(app).ToString(CultureInfo.InvariantCulture)}");

        // Reading the console blocks its thread, so it gets one of its own; the process does not wait for it.
        _ = Task.Run(() => StopOnCommand(app.Lifetime, Console.In));
        await app.WaitForShutdownAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Reads lines until one is <see cref="StopCommand"/>, then asks the host to stop, as Ctrl+C does. A process
    /// that started the host can stop it this way on every platform; Windows has no signal for it.
    /// </summary>
    /// <param name="lifetime">
    /// The lifetime of the started application.
    /// </param>
    /// <param name="input">
    /// The host's standard input. When it ends without the command, nothing is stopped: a host started with no
    /// input keeps running.
    /// </param>
    private static void StopOnCommand(IHostApplicationLifetime lifetime, TextReader input)
    {
        while (input.ReadLine() is { } line)
        {
            if (string.Equals(line, StopCommand, StringComparison.Ordinal))
            {
                lifetime.StopApplication();
                return;
            }
        }
    }

    /// <summary>
    /// Creates the builder of the application with MqttWire's server and <see cref="ExactTopicHandler"/>
    /// registered, and <see cref="MqttWireServerOptions.MaxPublishPayloadBytes"/> taken from
    /// <c>--max-publish-payload-bytes</c> when it is given.
    /// </summary>
    /// <param name="args">
    /// The command line.
    /// </param>
    /// <returns>
    /// The builder, for the caller to change further before it builds.
    /// </returns>
    /// <exception cref="FormatException">
    /// <c>--max-publish-payload-bytes</c> is not a whole number.
    /// </exception>
    public static WebApplicationBuilder CreateBuilder(string[] args)
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
        string? maxPublishPayloadBytes = builder.Configuration[MaxPublishPayloadBytesKey];
        builder.Services.AddMqttWireServer<ExactTopicHandler>(options =>
        {
            if (maxPublishPayloadBytes is not null)
            {
                options.MaxPublishPayloadBytes = int.Parse(maxPublishPayloadBytes, CultureInfo.InvariantCulture);
            }
        });
        return builder;
    }

    /// <summary>
    /// Adds the WebSocket middleware and maps the MQTT endpoint at <see cref="EndpointPath"/>.
    /// </summary>
    /// <param name="app">
    /// The application built from <see cref="CreateBuilder"/>.
    /// </param>
    public static void Configure(WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);
        app.UseWebSockets();
        app.MapMqttWire(EndpointPath);
    }

    /// <summary>
    /// Gets the port of the first address a started application listens on.
    /// </summary>
    /// <param name="app">
    /// The started application.
    /// </param>
    /// <returns>
    /// The port Kestrel bound, also when the URL asked for port 0.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The application is not listening on any address.
    /// </exception>
    public static int GetPort(WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);
        IServerAddressesFeature? addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        string address = addresses?.Addresses.FirstOrDefault()
            ?? throw new InvalidOperationException("The application is not listening on any address.");
        return new Uri(address).Port;
    }
}
