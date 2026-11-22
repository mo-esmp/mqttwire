using System.Globalization;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using MqttWire.Server;

namespace MqttWire.TestHost;

/// <summary>
/// The test host: MqttWire's server with <see cref="ExactTopicHandler"/> at <c>/mqtt</c>. Run as a program, it
/// listens on the URLs given with <c>--urls</c>, takes <c>--max-publish-payload-bytes &lt;n&gt;</c>, and prints
/// <c>LISTENING &lt;port&gt;</c> on standard output once Kestrel is listening. The tests that reference it as a library
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
        await app.WaitForShutdownAsync().ConfigureAwait(false);
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
