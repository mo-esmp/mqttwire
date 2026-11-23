using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Json;
using System.Net.NetworkInformation;

namespace MqttWire.Bench;

/// <summary>
/// The <c>run</c> role: for the floor, the traffic floor and MqttWire, at a small and a large connection count, it starts a
/// fresh <c>server</c> process, opens the connections from this process, and reads the server's own measurement
/// in each state. Then it prints the report of <see cref="Figures"/>.
/// </summary>
internal static class RunRole
{
    /// <summary>
    /// How long a server is left quiet before it is measured: two sweeps and a little more.
    /// </summary>
    private static readonly TimeSpan s_quiet = TimeSpan.FromSeconds(4.5);

    /// <summary>
    /// The time between two measurements that are compared to see whether the heap has settled.
    /// </summary>
    private static readonly TimeSpan s_settleInterval = TimeSpan.FromSeconds(2);

    /// <summary>
    /// How long the heap is given to settle before the last measurement is taken as it is, with a note.
    /// </summary>
    private static readonly TimeSpan s_settleLimit = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long a server may take to start, or to count no connection after all were closed.
    /// </summary>
    private static readonly TimeSpan s_serverLimit = TimeSpan.FromSeconds(60);

    /// <summary>
    /// How long the run waits for free outbound ports before it goes on with a note.
    /// </summary>
    private static readonly TimeSpan s_portLimit = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Runs the benchmark and prints its report.
    /// </summary>
    /// <param name="smallCount">
    /// The small connection count.
    /// </param>
    /// <param name="largeCount">
    /// The large connection count, at least four times the small one (design 6.14); a run with less is refused.
    /// </param>
    /// <param name="tls">
    /// Whether the servers serve TLS.
    /// </param>
    /// <param name="dumps">
    /// A folder for heap dumps of MqttWire's server, idle and after activity, at both counts, taken with
    /// <c>dotnet-gcdump</c>, which must be installed; or <see langword="null"/> for none. Whether taking a dump
    /// changes what the server keeps afterwards is not known, so a dump taken between two states may change the
    /// figures of the later ones.
    /// </param>
    /// <returns>
    /// 0 when the run ended with a verdict, met or not; 1 when the counts are not ones a verdict can rest on, or
    /// the run is inconclusive because the floor or MqttWire's idle state was measured while its heap was still
    /// moving.
    /// </returns>
    public static async Task<int> RunAsync(int smallCount, int largeCount, bool tls, string? dumps)
    {
        // 64-bit, so that four times a large small count cannot wrap around.
        if (smallCount <= 0 || largeCount < 4L * smallCount)
        {
            Console.Error.WriteLine(
                "The small count must be above 0 and the large count at least four times the small one (design 6.14): "
                + "a slope between two counts that are close divides the difference of two separate heaps by too little.");
            return 1;
        }

        Stopwatch watch = Stopwatch.StartNew();
        Log($"Idle-memory benchmark: {smallCount:N0} and {largeCount:N0} connections{(tls ? ", TLS" : string.Empty)}.");
        if (dumps is not null)
        {
            Directory.CreateDirectory(dumps);
            Log($"Heap dumps go to {Path.GetFullPath(dumps)}; a dump taken between two states may change the later ones.");
        }

        Measurement floorSmall = (await MeasureTargetAsync(ServerRole.FloorPath, smallCount, tls, dumps: null))[0];
        Measurement floorLarge = (await MeasureTargetAsync(ServerRole.FloorPath, largeCount, tls, dumps: null))[0];
        Measurement[] trafficSmall = await MeasureTargetAsync(ServerRole.TrafficPath, smallCount, tls, dumps: null);
        Measurement[] trafficLarge = await MeasureTargetAsync(ServerRole.TrafficPath, largeCount, tls, dumps: null);
        Measurement[] small = await MeasureTargetAsync(ServerRole.MqttPath, smallCount, tls, dumps);
        Measurement[] large = await MeasureTargetAsync(ServerRole.MqttPath, largeCount, tls, dumps);

        string report = Figures.Report(smallCount, largeCount,
            new State("Floor, idle", floorSmall, floorLarge),
            new State("Traffic floor, idle", trafficSmall[0], trafficLarge[0]),
            new State("Traffic floor, after activity", trafficSmall[1], trafficLarge[1]),
            new State("1. MqttWire, idle", small[0], large[0]),
            new State("2. MqttWire, after activity", small[1], large[1]),
            new State("3. MqttWire, after churn", small[2], large[2]),
            out Verdict verdict);
        Console.WriteLine();
        Console.WriteLine(report);
        string outcome = verdict switch
        {
            Verdict.Met => "the target is met",
            Verdict.NotMet => "the target is NOT met",
            _ => "this run is INCONCLUSIVE: a heap was still moving when it was measured",
        };
        Log($"Done in {watch.Elapsed.TotalSeconds:N0} s. For an idle connection {outcome}.");
        return verdict == Verdict.Inconclusive ? 1 : 0;
    }

    /// <summary>
    /// Measures one endpoint at one connection count, against a server process of its own.
    /// </summary>
    /// <param name="path">
    /// The endpoint's path: the floor's, the traffic floor's or MqttWire's.
    /// </param>
    /// <param name="count">
    /// How many connections to hold.
    /// </param>
    /// <param name="tls">
    /// Whether the server serves TLS.
    /// </param>
    /// <param name="dumps">
    /// A folder for heap dumps of the server after its idle and its after-activity measurement, or
    /// <see langword="null"/> for none.
    /// </param>
    /// <returns>
    /// For the floor, one measurement: idle. For the traffic floor, two: idle and after activity. For MqttWire,
    /// three: idle, after activity, and after churn.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The server did not start, did not hold exactly <paramref name="count"/> connections when it was measured,
    /// or something else happened than the benchmark expects.
    /// </exception>
    private static async Task<Measurement[]> MeasureTargetAsync(string path, int count, bool tls, string? dumps)
    {
        EndpointKind kind = path switch
        {
            ServerRole.MqttPath => EndpointKind.Mqtt,
            ServerRole.TrafficPath => EndpointKind.Traffic,
            _ => EndpointKind.Floor,
        };
        string target = kind switch
        {
            EndpointKind.Mqtt => "MqttWire",
            EndpointKind.Traffic => "the traffic floor",
            _ => "the floor",
        };
        await WaitForPortsAsync(count);
        using Process server = StartServer(tls, out Task<int> listening);
        IdleClients? clients = null;
        try
        {
            int port = await listening.WaitAsync(s_serverLimit);
            string authority = string.Create(CultureInfo.InvariantCulture, $"127.0.0.1:{port}");
            using HttpClient http = CreateHttpClient(new Uri($"{(tls ? "https" : "http")}://{authority}"));
            clients = new IdleClients(new Uri($"{(tls ? "wss" : "ws")}://{authority}{path}"), kind);

            await clients.OpenAsync(count);
            Log($"{target}, {count:N0}: connected.");
            List<Measurement> measurements = [await MeasureAsync(http, clients, count, $"{target}, {count:N0}, idle")];
            await DumpAsync(server, dumps, string.Create(CultureInfo.InvariantCulture, $"idle-{count}"));
            if (kind == EndpointKind.Traffic)
            {
                await clients.ActivityAsync(async _ =>
                {
                    using HttpResponseMessage response = await http.PostAsync("/traffic-burst", content: null);
                    response.EnsureSuccessStatusCode();
                    return await response.Content.ReadFromJsonAsync<int>();
                });
                measurements.Add(await MeasureAsync(http, clients, count, $"{target}, {count:N0}, after activity"));
            }

            if (kind == EndpointKind.Mqtt)
            {
                await clients.ActivityAsync(async connections =>
                {
                    using HttpResponseMessage response = await http.PostAsync(
                        string.Create(CultureInfo.InvariantCulture, $"/burst?count={connections}"), content: null);
                    response.EnsureSuccessStatusCode();
                    return await response.Content.ReadFromJsonAsync<int>();
                });
                measurements.Add(await MeasureAsync(http, clients, count, $"{target}, {count:N0}, after activity"));
                await DumpAsync(server, dumps, string.Create(CultureInfo.InvariantCulture, $"after-activity-{count}"));

                await clients.CloseAsync();
                await WaitForNoConnectionAsync(http);
                await WaitForPortsAsync(count);
                await clients.OpenAsync(count);
                measurements.Add(await MeasureAsync(http, clients, count, $"{target}, {count:N0}, after churn"));
            }

            return [.. measurements];
        }
        finally
        {
            // The server goes first, so that it is the one that closes the connections: the side that closes
            // first keeps its port taken for minutes, and this process needs its ports for the next server.
            server.Kill(entireProcessTree: true);
            await server.WaitForExitAsync();
            await Task.Delay(TimeSpan.FromSeconds(1));
            if (clients is not null)
            {
                await clients.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// Measures the server in its present state: pings the connections so that none is dropped as silent, leaves
    /// the server quiet for two sweeps, reads how long the last of them took, then reads the server's measurement
    /// until two in a row agree.
    /// </summary>
    /// <param name="http">
    /// The client for the server's endpoints.
    /// </param>
    /// <param name="clients">
    /// The connections.
    /// </param>
    /// <param name="count">
    /// How many connections the server must hold.
    /// </param>
    /// <param name="what">
    /// What is measured, for the log.
    /// </param>
    /// <returns>
    /// The last measurement.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The server does not hold exactly <paramref name="count"/> connections.
    /// </exception>
    private static async Task<Measurement> MeasureAsync(HttpClient http, IdleClients clients, int count, string what)
    {
        await clients.PingAsync();
        await Task.Delay(s_quiet);

        // Read before the first forced collection: the last sweep ran with nothing else at work.
        double sweepMicroseconds = await http.GetFromJsonAsync<double>("/sweep");
        Stopwatch watch = Stopwatch.StartNew();
        Measurement previous = await ReadAsync(http);
        bool settled = false;
        while (watch.Elapsed < s_settleLimit)
        {
            await Task.Delay(s_settleInterval);
            Measurement current = await ReadAsync(http);
            long difference = Math.Abs(current.LiveManagedBytes - previous.LiveManagedBytes);
            previous = current;

            // Settled: within 8 KB or 0.02 %, whichever is larger. At 3,000 connections 8 KB is under 3 bytes each.
            if (difference <= Math.Max(8_192, current.LiveManagedBytes / 5_000))
            {
                settled = true;
                break;
            }
        }

        previous = previous with { LastSweepMicroseconds = sweepMicroseconds, Settled = settled };
        if (previous.Connections != count)
        {
            throw new InvalidOperationException($"{what}: the server holds {previous.Connections} connections, not {count}.");
        }

        Log($"{what}: {previous.LiveManagedBytes:N0} live managed bytes, working set {previous.WorkingSetBytes:N0}, "
            + $"last sweep {previous.LastSweepMicroseconds:N0} µs{(settled ? string.Empty : " (the heap was still moving)")}.");
        return previous;
    }

    /// <summary>
    /// Takes a heap dump of a server with <c>dotnet-gcdump collect</c>.
    /// </summary>
    /// <param name="server">
    /// The server's process.
    /// </param>
    /// <param name="folder">
    /// The folder the dump goes to, or <see langword="null"/> to take none.
    /// </param>
    /// <param name="name">
    /// The dump's file name, without its extension.
    /// </param>
    /// <returns>
    /// A task that completes when the dump is written; at once when none is taken.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// <c>dotnet-gcdump</c> could not be started, or ended with an error.
    /// </exception>
    private static async Task DumpAsync(Process server, string? folder, string name)
    {
        if (folder is null)
        {
            return;
        }

        string file = Path.Combine(folder, name + ".gcdump");
        File.Delete(file);
        ProcessStartInfo start = new("dotnet-gcdump") { RedirectStandardOutput = true, UseShellExecute = false };
        start.ArgumentList.Add("collect");
        start.ArgumentList.Add("-p");
        start.ArgumentList.Add(server.Id.ToString(CultureInfo.InvariantCulture));
        start.ArgumentList.Add("-o");
        start.ArgumentList.Add(file);
        using Process tool = Process.Start(start)
            ?? throw new InvalidOperationException("dotnet-gcdump could not be started.");
        string output = await tool.StandardOutput.ReadToEndAsync();
        await tool.WaitForExitAsync();
        if (tool.ExitCode != 0 || !File.Exists(file))
        {
            throw new InvalidOperationException($"dotnet-gcdump collect failed: {output}");
        }

        Log($"Heap dump written: {file}.");
    }

    /// <summary>
    /// Reads the server's measurement of itself.
    /// </summary>
    /// <param name="http">
    /// The client for the server's endpoints.
    /// </param>
    /// <returns>
    /// The measurement.
    /// </returns>
    private static async Task<Measurement> ReadAsync(HttpClient http) =>
        await http.GetFromJsonAsync<Measurement>("/measure");

    /// <summary>
    /// Waits until the server counts no connection, after all were closed.
    /// </summary>
    /// <param name="http">
    /// The client for the server's endpoints.
    /// </param>
    /// <returns>
    /// A task that completes when the count is 0.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// Connections were still counted after <see cref="s_serverLimit"/>.
    /// </exception>
    private static async Task WaitForNoConnectionAsync(HttpClient http)
    {
        Stopwatch watch = Stopwatch.StartNew();
        while (true)
        {
            int connections = await http.GetFromJsonAsync<int>("/connections");
            if (connections == 0)
            {
                return;
            }

            if (watch.Elapsed > s_serverLimit)
            {
                throw new InvalidOperationException($"The server still counts {connections} connections after all were closed.");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250));
        }
    }

    /// <summary>
    /// Starts this program again in the <c>server</c> role, on a free port.
    /// </summary>
    /// <param name="tls">
    /// Whether the server serves TLS.
    /// </param>
    /// <param name="listening">
    /// Completes with the port once the server has printed its <c>LISTENING</c> line; fails when the process
    /// ends without it.
    /// </param>
    /// <returns>
    /// The server's process, for the caller to end.
    /// </returns>
    private static Process StartServer(bool tls, out Task<int> listening)
    {
        string program = Environment.ProcessPath
            ?? throw new InvalidOperationException("The path of this process is not known.");
        ProcessStartInfo start = new(program) { RedirectStandardOutput = true, UseShellExecute = false };

        // Started through `dotnet MqttWire.Bench.dll`, the process is dotnet itself, and the program is its first argument.
        if (string.Equals(Path.GetFileNameWithoutExtension(program), "dotnet", StringComparison.OrdinalIgnoreCase))
        {
            start.ArgumentList.Add(typeof(RunRole).Assembly.Location);
        }

        start.ArgumentList.Add("server");
        start.ArgumentList.Add("--port");
        start.ArgumentList.Add("0");
        if (tls)
        {
            start.ArgumentList.Add("--tls");
        }

        Process server = Process.Start(start)
            ?? throw new InvalidOperationException("The server process could not be started.");
        listening = ReadPortAsync(server);
        return server;
    }

    /// <summary>
    /// Reads a server's standard output until its <c>LISTENING &lt;port&gt;</c> line.
    /// </summary>
    /// <param name="server">
    /// The server's process.
    /// </param>
    /// <returns>
    /// The port.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The output ended without the line.
    /// </exception>
    private static async Task<int> ReadPortAsync(Process server)
    {
        const string prefix = "LISTENING ";
        while (await server.StandardOutput.ReadLineAsync() is { } line)
        {
            if (line.StartsWith(prefix, StringComparison.Ordinal))
            {
                return int.Parse(line.AsSpan(prefix.Length), CultureInfo.InvariantCulture);
            }
        }

        throw new InvalidOperationException("The server ended before it listened.");
    }

    /// <summary>
    /// Creates the client for a server's own endpoints.
    /// </summary>
    /// <param name="baseAddress">
    /// The server's address.
    /// </param>
    /// <returns>
    /// The client. It accepts any certificate: with TLS the server's is self-signed, made in memory for one run
    /// on the loopback interface.
    /// </returns>
    private static HttpClient CreateHttpClient(Uri baseAddress) =>
        new(new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator,
        })
        {
            BaseAddress = baseAddress,
            Timeout = TimeSpan.FromSeconds(120),
        };

    /// <summary>
    /// Waits until enough outbound ports are free for the connections about to be opened: ports of connections
    /// that this process closed first stay taken for minutes. After <see cref="s_portLimit"/> it goes on with a
    /// note.
    /// </summary>
    /// <param name="needed">
    /// How many connections are about to be opened.
    /// </param>
    /// <returns>
    /// A task that completes when the ports are free, or the limit has passed.
    /// </returns>
    private static async Task WaitForPortsAsync(int needed)
    {
        const int margin = 512;
        (int first, int size) = GetDynamicPortRange();
        Stopwatch watch = Stopwatch.StartNew();
        while (true)
        {
            int free = size - CountPortsInUse(first, size);
            if (free >= needed + margin)
            {
                if (watch.Elapsed > TimeSpan.FromSeconds(1))
                {
                    Log($"Waited {watch.Elapsed.TotalSeconds:N0} s for {needed:N0} free outbound ports.");
                }

                return;
            }

            if (watch.Elapsed > s_portLimit)
            {
                Log($"Note: only {free:N0} outbound ports are free for {needed:N0} connections.");
                return;
            }

            await Task.Delay(TimeSpan.FromSeconds(5));
        }
    }

    /// <summary>
    /// Counts the ports of the dynamic range that a TCP connection of this machine uses.
    /// </summary>
    /// <param name="first">
    /// The first port of the range.
    /// </param>
    /// <param name="size">
    /// The number of ports in the range.
    /// </param>
    /// <returns>
    /// The number of distinct local ports in use.
    /// </returns>
    private static int CountPortsInUse(int first, int size)
    {
        HashSet<int> ports = [];
        foreach (TcpConnectionInformation connection in IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpConnections())
        {
            int port = connection.LocalEndPoint.Port;
            if (port >= first && port < first + size)
            {
                ports.Add(port);
            }
        }

        return ports.Count;
    }

    /// <summary>
    /// Gets the range outbound connections take their ports from.
    /// </summary>
    /// <returns>
    /// The first port and the number of ports: what Linux reports, or the default of Windows and macOS, 49152 to
    /// 65535.
    /// </returns>
    private static (int First, int Size) GetDynamicPortRange()
    {
        const string linux = "/proc/sys/net/ipv4/ip_local_port_range";
        if (File.Exists(linux))
        {
            string[] parts = File.ReadAllText(linux).Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
            int first = int.Parse(parts[0], CultureInfo.InvariantCulture);
            return (first, int.Parse(parts[1], CultureInfo.InvariantCulture) - first + 1);
        }

        return (49_152, 16_384);
    }

    /// <summary>
    /// Writes a line of progress with the time.
    /// </summary>
    /// <param name="message">
    /// The line.
    /// </param>
    private static void Log(string message) =>
        Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"[{DateTime.Now:HH:mm:ss}] {message}"));
}
