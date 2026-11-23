using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using MqttWire.Server;

namespace MqttWire.Bench;

/// <summary>
/// The <c>server</c> role: Kestrel on 127.0.0.1 with the floor endpoint, MqttWire's endpoint, and the endpoints
/// the <c>run</c> role measures and drives it through. It prints <c>LISTENING &lt;port&gt;</c> once it listens,
/// and runs until its process is ended.
/// </summary>
internal static class ServerRole
{
    /// <summary>
    /// The path of the floor: a bare WebSocket endpoint (design 6.14 item 1).
    /// </summary>
    public const string FloorPath = "/floor";

    /// <summary>
    /// The path of the echo floor: a bare WebSocket endpoint like the floor that sends every message it receives
    /// back, so that it can be measured after the same traffic as MqttWire.
    /// </summary>
    public const string EchoPath = "/echo";

    /// <summary>
    /// The path of MqttWire's endpoint, with <see cref="NothingHandler"/> behind it.
    /// </summary>
    public const string MqttPath = "/mqtt";

    /// <summary>
    /// How many messages a burst sends to every connection.
    /// </summary>
    public const int BurstMessages = 10;

    /// <summary>
    /// The payload size of a burst's messages, in bytes.
    /// </summary>
    public const int BurstPayloadBytes = 256;

    /// <summary>
    /// The size of the buffer the floor reads a message into, when a client sends one; the benchmark's clients
    /// send nothing to the floor.
    /// </summary>
    private const int FloorBufferBytes = 256;

    /// <summary>
    /// The size of the buffer the echo floor reads a message into, which holds every message of the benchmark
    /// whole.
    /// </summary>
    private const int EchoBufferBytes = 512;

    /// <summary>
    /// The number of WebSockets open on the floor and echo endpoints.
    /// </summary>
    private static int s_floorConnections;

    /// <summary>
    /// Runs the server until its process is ended.
    /// </summary>
    /// <param name="port">
    /// The port to listen on, or 0 for a free one.
    /// </param>
    /// <param name="tls">
    /// Whether Kestrel serves TLS, with a self-signed certificate made in memory.
    /// </param>
    /// <returns>
    /// A task that completes when the host has stopped.
    /// </returns>
    public static async Task RunAsync(int port, bool tls)
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();

        // No log line per connection: neither endpoint is measured with a console logger at work.
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel => kestrel.Listen(IPAddress.Loopback, port, listen =>
        {
            if (tls)
            {
                listen.UseHttps(CreateCertificate());
            }
        }));
        builder.Services.AddMqttWireServer<NothingHandler>();

        WebApplication app = builder.Build();
        app.UseWebSockets();
        app.Map(FloorPath, HoldFloorConnectionAsync);
        app.Map(EchoPath, HoldEchoConnectionAsync);
        app.MapMqttWire(MqttPath);
        app.MapGet("/connections", (MqttWireServer server) => CountConnections(server));
        app.MapGet("/sweep", (MqttWireServer server) => GetLastSweepMicroseconds(server));
        app.MapGet("/measure", (MqttWireServer server) => Measure(server));
        app.MapPost("/burst", (int count, MqttWireServer server) => Burst(count, server));

        await app.StartAsync();
        IServerAddressesFeature? addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        string address = addresses?.Addresses.FirstOrDefault()
            ?? throw new InvalidOperationException("The server is not listening on any address.");
        Console.WriteLine($"LISTENING {new Uri(address).Port.ToString(CultureInfo.InvariantCulture)}");
        await app.WaitForShutdownAsync();
    }

    /// <summary>
    /// The floor: accepts a WebSocket with the sub-protocol <c>mqtt</c> and no keep-alive, so no ping timer, and
    /// waits in a zero-byte read until the socket ends. It holds no buffer while it waits.
    /// </summary>
    /// <param name="context">
    /// The request.
    /// </param>
    /// <returns>
    /// A task that completes when the WebSocket has ended; a request that is not a WebSocket asking for
    /// <c>mqtt</c> is answered with 400 at once.
    /// </returns>
    private static async Task HoldFloorConnectionAsync(HttpContext context)
    {
        if (!context.WebSockets.IsWebSocketRequest || !context.WebSockets.WebSocketRequestedProtocols.Contains("mqtt"))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        using WebSocket socket = await context.WebSockets.AcceptWebSocketAsync(new WebSocketAcceptContext
        {
            SubProtocol = "mqtt",
            KeepAliveInterval = TimeSpan.Zero,
        });
        Interlocked.Increment(ref s_floorConnections);
        try
        {
            while (true)
            {
                ValueWebSocketReceiveResult waited = await socket.ReceiveAsync(Memory<byte>.Empty, context.RequestAborted);
                if (waited.MessageType == WebSocketMessageType.Close)
                {
                    await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, statusDescription: null, context.RequestAborted);
                    return;
                }

                // Something arrived: it is read into a rented buffer, which goes back before the next wait.
                byte[] buffer = ArrayPool<byte>.Shared.Rent(FloorBufferBytes);
                try
                {
                    ValueWebSocketReceiveResult received = await socket.ReceiveAsync(buffer.AsMemory(0, FloorBufferBytes), context.RequestAborted);
                    if (received.MessageType == WebSocketMessageType.Close)
                    {
                        await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, statusDescription: null, context.RequestAborted);
                        return;
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
        }
        catch (Exception exception) when (exception is WebSocketException or OperationCanceledException)
        {
            // The client went away.
        }
        finally
        {
            Interlocked.Decrement(ref s_floorConnections);
        }
    }

    /// <summary>
    /// The echo floor: a bare WebSocket endpoint, accepted and parked as the floor is, that sends every message
    /// it receives back. A message is read into a rented buffer, which goes back before the next wait, so an
    /// idle connection holds no buffer here either. What this endpoint keeps after traffic is what ASP.NET Core
    /// and .NET's WebSocket keep, with no MQTT code involved.
    /// </summary>
    /// <param name="context">
    /// The request.
    /// </param>
    /// <returns>
    /// A task that completes when the WebSocket has ended; a request that is not a WebSocket asking for
    /// <c>mqtt</c> is answered with 400 at once.
    /// </returns>
    private static async Task HoldEchoConnectionAsync(HttpContext context)
    {
        if (!context.WebSockets.IsWebSocketRequest || !context.WebSockets.WebSocketRequestedProtocols.Contains("mqtt"))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        using WebSocket socket = await context.WebSockets.AcceptWebSocketAsync(new WebSocketAcceptContext
        {
            SubProtocol = "mqtt",
            KeepAliveInterval = TimeSpan.Zero,
        });
        Interlocked.Increment(ref s_floorConnections);
        try
        {
            while (true)
            {
                ValueWebSocketReceiveResult waited = await socket.ReceiveAsync(Memory<byte>.Empty, context.RequestAborted);
                if (waited.MessageType == WebSocketMessageType.Close)
                {
                    await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, statusDescription: null, context.RequestAborted);
                    return;
                }

                byte[] buffer = ArrayPool<byte>.Shared.Rent(EchoBufferBytes);
                try
                {
                    ValueWebSocketReceiveResult received;
                    do
                    {
                        received = await socket.ReceiveAsync(buffer.AsMemory(0, EchoBufferBytes), context.RequestAborted);
                        if (received.MessageType == WebSocketMessageType.Close)
                        {
                            await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, statusDescription: null, context.RequestAborted);
                            return;
                        }

                        await socket.SendAsync(buffer.AsMemory(0, received.Count), WebSocketMessageType.Binary,
                            received.EndOfMessage, context.RequestAborted);
                    }
                    while (!received.EndOfMessage);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
        }
        catch (Exception exception) when (exception is WebSocketException or OperationCanceledException)
        {
            // The client went away.
        }
        finally
        {
            Interlocked.Decrement(ref s_floorConnections);
        }
    }

    /// <summary>
    /// Counts the open connections on every endpoint.
    /// </summary>
    /// <param name="server">
    /// MqttWire's server.
    /// </param>
    /// <returns>
    /// The WebSockets open on the floor and the echo floor plus the connections MqttWire counts.
    /// </returns>
    private static int CountConnections(MqttWireServer server) =>
        Volatile.Read(ref s_floorConnections) + server.ConnectionCount;

    /// <summary>
    /// Measures the process: forces two full blocking collections with the finalizers run between them, then
    /// reads the figures.
    /// </summary>
    /// <param name="server">
    /// MqttWire's server.
    /// </param>
    /// <returns>
    /// The live managed bytes after the collections, the working set, the open connections, and how long
    /// MqttWire's last sweep took in microseconds. A sweep that ran while a collection of an earlier call had the
    /// process stopped took longer than it does: <c>/sweep</c>, read before the first measurement, is the figure to
    /// report.
    /// </returns>
    private static Measurement Measure(MqttWireServer server)
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        long liveManagedBytes = GC.GetTotalMemory(forceFullCollection: true);
        using Process process = Process.GetCurrentProcess();
        return new Measurement(liveManagedBytes, process.WorkingSet64, CountConnections(server),
            GetLastSweepMicroseconds(server));
    }

    /// <summary>
    /// Reads how long MqttWire's last sweep took, without collecting anything.
    /// </summary>
    /// <param name="server">
    /// MqttWire's server.
    /// </param>
    /// <returns>
    /// The time in microseconds; 0 before the first sweep.
    /// </returns>
    private static double GetLastSweepMicroseconds(MqttWireServer server) =>
        server.LastSweepTicks * 1_000_000.0 / TimeProvider.System.TimestampFrequency;

    /// <summary>
    /// Sends every connection <c>b0</c> to <c>b&lt;count − 1&gt;</c> a burst: <see cref="BurstMessages"/> messages
    /// of <see cref="BurstPayloadBytes"/> bytes at QoS 1. The connections are found by client id, so the handler
    /// needs no table that would be measured.
    /// </summary>
    /// <param name="count">
    /// How many connections there are.
    /// </param>
    /// <param name="server">
    /// MqttWire's server.
    /// </param>
    /// <returns>
    /// How many messages the server took; <c>count × 10</c> when every connection was there and took all of its
    /// burst.
    /// </returns>
    private static int Burst(int count, MqttWireServer server)
    {
        // One topic and one payload, shared by every message: TrySend copies neither.
        MqttTopic topic = MqttTopic.Create("burst");
        byte[] payload = new byte[BurstPayloadBytes];
        int taken = 0;
        for (int i = 0; i < count; i++)
        {
            if (!server.TryGetConnection(IdleClients.ClientIdOf(i), out MqttWireConnection? connection))
            {
                continue;
            }

            for (int message = 0; message < BurstMessages; message++)
            {
                if (connection.TrySend(topic, payload, MqttQoS.AtLeastOnce) == MqttSendResult.Taken)
                {
                    taken++;
                }
            }
        }

        return taken;
    }

    /// <summary>
    /// Makes a self-signed certificate for <c>localhost</c> and 127.0.0.1 in memory, valid for a week.
    /// </summary>
    /// <returns>
    /// The certificate with its private key, in a form Kestrel can serve on every platform.
    /// </returns>
    private static X509Certificate2 CreateCertificate()
    {
        using RSA key = RSA.Create(keySizeInBits: 2_048);
        CertificateRequest request = new("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        SubjectAlternativeNameBuilder names = new();
        names.AddDnsName("localhost");
        names.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(names.Build());
        DateTimeOffset now = DateTimeOffset.UtcNow;
        using X509Certificate2 created = request.CreateSelfSigned(now.AddDays(-1), now.AddDays(7));

        // Exported and loaded again: on Windows a certificate made this way cannot be served as it is.
        return X509CertificateLoader.LoadPkcs12(created.Export(X509ContentType.Pfx), password: null);
    }
}
