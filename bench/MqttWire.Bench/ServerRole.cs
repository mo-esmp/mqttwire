using System.Buffers;
using System.Collections.Concurrent;
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
    /// The path of the traffic floor: a bare WebSocket endpoint like the floor that plays MqttWire's side of the
    /// benchmark's activity from fixed bytes, so that it can be measured after the same traffic in the same
    /// directions.
    /// </summary>
    public const string TrafficPath = "/traffic";

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
    /// The size of the buffer the traffic floor reads a message into, which holds every message the benchmark's
    /// clients send whole.
    /// </summary>
    private const int TrafficBufferBytes = 256;

    /// <summary>
    /// The four bytes of a PUBREC for the packet identifier 1, which the traffic floor answers a publish at QoS 2
    /// with, as MqttWire does.
    /// </summary>
    private static readonly byte[] s_pubRec = [0x50, 0x02, 0x00, 0x01];

    /// <summary>
    /// The four bytes of a PUBCOMP for the packet identifier 1, which the traffic floor answers a PUBREL with.
    /// </summary>
    private static readonly byte[] s_pubComp = [0x70, 0x02, 0x00, 0x01];

    /// <summary>
    /// The packets of a burst as MqttWire writes them, byte for byte: ten publishes at QoS 1 to the topic
    /// <c>burst</c>, with the packet identifiers 1 to 10 and <see cref="BurstPayloadBytes"/> bytes of payload, 269
    /// bytes each. Shared by every connection of the traffic floor.
    /// </summary>
    private static readonly byte[][] s_burstPackets = CreateBurstPackets();

    /// <summary>
    /// The WebSockets open on the traffic floor, so that a burst can be sent to each. The floor keeps no such
    /// table; what it costs shows in the traffic floor's idle figure.
    /// </summary>
    private static readonly ConcurrentDictionary<WebSocket, bool> s_trafficSockets = new();

    /// <summary>
    /// The number of WebSockets open on the floor and the traffic floor.
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
        app.Map(TrafficPath, HoldTrafficConnectionAsync);
        app.MapMqttWire(MqttPath);
        app.MapGet("/connections", (MqttWireServer server) => CountConnections(server));
        app.MapGet("/sweep", (MqttWireServer server) => GetLastSweepMicroseconds(server));
        app.MapGet("/measure", (MqttWireServer server) => Measure(server));
        app.MapPost("/burst", (int count, MqttWireServer server) => Burst(count, server));
        app.MapPost("/traffic-burst", TrafficBurstAsync);

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
    /// The traffic floor: a bare WebSocket endpoint, accepted and parked as the floor is, that plays MqttWire's
    /// side of the benchmark's activity without any MQTT code. It answers a message that starts with the first
    /// byte of a publish at QoS 2 with the four bytes of a PUBREC, and one that starts with the first byte of a
    /// PUBREL with the four bytes of a PUBCOMP; everything else it reads and drops, as MqttWire reads a publish
    /// at QoS 0 and a PUBACK. A message is read into a rented buffer, which goes back before the next wait, so an
    /// idle connection holds no buffer here either. What this endpoint keeps after traffic is what ASP.NET Core
    /// and .NET's WebSocket keep.
    /// </summary>
    /// <remarks>
    /// The answers are written from here and the burst from <see cref="TrafficBurstAsync"/>, and a WebSocket
    /// takes one write at a time. The benchmark never has both at once: its clients have their answers before the
    /// burst is asked for, and a PUBACK gets no answer.
    /// </remarks>
    /// <param name="context">
    /// The request.
    /// </param>
    /// <returns>
    /// A task that completes when the WebSocket has ended; a request that is not a WebSocket asking for
    /// <c>mqtt</c> is answered with 400 at once.
    /// </returns>
    private static async Task HoldTrafficConnectionAsync(HttpContext context)
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
        s_trafficSockets.TryAdd(socket, value: true);
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

                byte[] buffer = ArrayPool<byte>.Shared.Rent(TrafficBufferBytes);
                try
                {
                    ValueWebSocketReceiveResult received = await socket.ReceiveAsync(buffer.AsMemory(0, TrafficBufferBytes), context.RequestAborted);
                    if (received.MessageType == WebSocketMessageType.Close)
                    {
                        await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, statusDescription: null, context.RequestAborted);
                        return;
                    }

                    // The first byte is all that is looked at: 0x34 is a publish at QoS 2, 0x62 a PUBREL.
                    byte[]? answer = received.Count == 0 ? null : buffer[0] switch
                    {
                        0x34 => s_pubRec,
                        0x62 => s_pubComp,
                        _ => null,
                    };
                    if (answer is not null)
                    {
                        await socket.SendAsync(answer, WebSocketMessageType.Binary, endOfMessage: true, context.RequestAborted);
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
            s_trafficSockets.TryRemove(socket, out _);
            Interlocked.Decrement(ref s_floorConnections);
        }
    }

    /// <summary>
    /// Sends every connection of the traffic floor a burst, as MqttWire's <c>/burst</c> does for its connections:
    /// the same ten packets, started for every connection before any is awaited, one write at a time on each
    /// connection.
    /// </summary>
    /// <returns>
    /// How many messages were sent: ten for every connection.
    /// </returns>
    private static async Task<int> TrafficBurstAsync()
    {
        WebSocket[] sockets = [.. s_trafficSockets.Keys];
        Task[] sending = new Task[sockets.Length];
        for (int i = 0; i < sockets.Length; i++)
        {
            sending[i] = SendBurstAsync(sockets[i]);
        }

        await Task.WhenAll(sending);
        return sockets.Length * BurstMessages;
    }

    /// <summary>
    /// Sends one connection of the traffic floor the packets of a burst, one after the other.
    /// </summary>
    /// <param name="socket">
    /// The connection's WebSocket.
    /// </param>
    /// <returns>
    /// A task that completes when the last packet is written.
    /// </returns>
    private static async Task SendBurstAsync(WebSocket socket)
    {
        foreach (byte[] packet in s_burstPackets)
        {
            await socket.SendAsync(packet, WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None);
        }
    }

    /// <summary>
    /// Builds the packets of a burst: for each identifier from 1 to <see cref="BurstMessages"/>, a PUBLISH at QoS 1
    /// to the topic <c>burst</c> with no properties and <see cref="BurstPayloadBytes"/> bytes of payload.
    /// </summary>
    /// <returns>
    /// The packets, 269 bytes each: the first byte, a remaining length of 266 in two bytes, the topic with its
    /// length (7), the identifier (2), the properties' length (1) and the payload (256).
    /// </returns>
    private static byte[][] CreateBurstPackets()
    {
        byte[][] packets = new byte[BurstMessages][];
        for (int i = 0; i < BurstMessages; i++)
        {
            int identifier = i + 1;
            packets[i] =
            [
                0x32, 0x8A, 0x02,
                0x00, 0x05, (byte)'b', (byte)'u', (byte)'r', (byte)'s', (byte)'t',
                (byte)(identifier >> 8), (byte)identifier,
                0x00,
                .. new byte[BurstPayloadBytes],
            ];
        }

        return packets;
    }

    /// <summary>
    /// Counts the open connections on every endpoint.
    /// </summary>
    /// <param name="server">
    /// MqttWire's server.
    /// </param>
    /// <returns>
    /// The WebSockets open on the floor and the traffic floor plus the connections MqttWire counts.
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
