using System.Diagnostics;
using System.Globalization;
using MqttWire.Microbench;

namespace MqttWire.Probe;

/// <summary>
/// The interleaved probe: MqttWire's client of this branch (<c>after</c>), optionally an older build of it
/// (<c>before</c>), and MQTTnet's client (<c>mqttnet</c>), all connected to one MQTTnet broker on loopback, take turns
/// in an order that rotates every round, so that a drift of the machine during the run touches them all alike. Four
/// scenarios: the QoS 1 self-echo round trip and the burst of <see cref="Burst"/> QoS 1 publishes, each by a caller
/// that blocks on every operation and by one that awaits it. Each scenario runs <see cref="Ops"/> operations per
/// client per round, for <see cref="Rounds"/> rounds after two warm-up passes, and the table shows the median of the
/// rounds in microseconds per operation (per message for a burst) and the ratios of <c>after</c> to the others.
/// Every publish is checked, and a lost connection fails the run instead of hanging it. A second table shows the
/// median bytes allocated per operation, with the same rows, columns and ratios.
/// </summary>
/// <remarks>
/// <para>
/// Without an older build the probe compares the branch's client with MQTTnet only, and the solution builds it with
/// nothing extra. To add <c>before</c>, build the older client under another assembly name from a worktree, and build
/// the probe with the path to it, from the repository root (PowerShell or a POSIX shell):
/// </para>
/// <code>
/// git worktree add ../mqttwire-before &lt;commit&gt;
/// dotnet build -c Release ../mqttwire-before/src/MqttWire/MqttWire.csproj -p:AssemblyName=MqttWire.Before -o ../mqttwire-before-bin
/// dotnet build -c Release bench/MqttWire.Probe/MqttWire.Probe.csproj -p:BeforeAssembly=../../../mqttwire-before-bin/MqttWire.Before.dll
/// dotnet bench/MqttWire.Probe/bin/Release/net10.0/MqttWire.Probe.dll
/// git worktree remove ../mqttwire-before
/// </code>
/// <para>
/// <c>BeforeAssembly</c> is resolved from the probe's project folder, so an absolute path is the safer choice. The
/// probe built with it references the older assembly under the extern alias <c>before</c> and compiles the
/// <c>before</c> contestant (the <c>BEFORE</c> constant). Building the probe again
/// without the property drops <c>before</c> again. Commit 9aea873 is the client before the answer batching, the
/// "before" of <c>docs/bench/2026-11-qos1-latency.md</c>; commit de7af74 is the "before" of the measurements of the
/// send lock (issue 13).
/// </para>
/// <para>
/// The bytes per operation are counted with <see cref="GC.GetTotalAllocatedBytes(bool)"/> over the timed
/// operations, so the count is process-wide: the broker and every other thread are in it. The broker is the same for
/// every contestant, so the differences between the columns are the clients'.
/// </para>
/// </remarks>
internal static class Program
{
    /// <summary>
    /// The number of publishes in a burst.
    /// </summary>
    internal const int Burst = 50;

    /// <summary>
    /// The number of measured rounds.
    /// </summary>
    private const int Rounds = 30;

    /// <summary>
    /// The number of operations per client per scenario in one round; a burst counts its publishes.
    /// </summary>
    private const int Ops = 1000;

    /// <summary>
    /// The keep-alive of every client, MQTTnet's default; no round is long enough to see a PINGREQ.
    /// </summary>
    private static readonly TimeSpan s_keepAlive = TimeSpan.FromSeconds(15);

    /// <summary>
    /// The names of the scenarios, in the order <see cref="RunAsync"/> numbers them.
    /// </summary>
    private static readonly string[] s_scenarios = ["rt-blocking", "rt-awaiting", "burst-blocking", "burst-awaiting"];

    /// <summary>
    /// Starts the broker, connects the clients, runs the probe and prints the table.
    /// </summary>
    /// <returns>
    /// A task that completes when the clients and the broker are closed.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// A client could not connect or subscribe, a publish failed, or a connection ended during the run.
    /// </exception>
    public static async Task Main()
    {
        byte[] payload = new byte[64];
        new Random(1).NextBytes(payload);
        MqttNetBrokerHost broker = await MqttNetBrokerHost.StartAsync().ConfigureAwait(false);
        List<IContestant> contestants = [];
        try
        {
#if BEFORE
            contestants.Add(await BeforeContestant.ConnectAsync(broker.WebSocketUri, payload, s_keepAlive).ConfigureAwait(false));
#endif
            contestants.Add(await WireContestant.ConnectAsync(broker.WebSocketUri, payload, s_keepAlive).ConfigureAwait(false));
            contestants.Add(await NetContestant.ConnectAsync(broker.WebSocketUri, payload, s_keepAlive).ConfigureAwait(false));
            List<ProbeSample>[,] samples = await MeasureAsync(contestants).ConfigureAwait(false);
            Print(contestants, samples);
        }
        finally
        {
            foreach (IContestant contestant in contestants)
            {
                await contestant.DisposeAsync().ConfigureAwait(false);
            }

            await broker.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Warms every scenario up twice for every client, then runs <see cref="Rounds"/> rounds in which every client
    /// runs every scenario, in an order that rotates by one client each round.
    /// </summary>
    /// <param name="contestants">
    /// The clients.
    /// </param>
    /// <returns>
    /// For each scenario and client, the sample of each round.
    /// </returns>
    private static async Task<List<ProbeSample>[,]> MeasureAsync(List<IContestant> contestants)
    {
        int count = contestants.Count;
        for (int pass = 0; pass < 2; pass++)
        {
            for (int scenario = 0; scenario < s_scenarios.Length; scenario++)
            {
                foreach (IContestant contestant in contestants)
                {
                    await RunAsync(scenario, contestant).ConfigureAwait(false);
                }
            }
        }

        List<ProbeSample>[,] samples = new List<ProbeSample>[s_scenarios.Length, count];
        for (int scenario = 0; scenario < s_scenarios.Length; scenario++)
        {
            for (int client = 0; client < count; client++)
            {
                samples[scenario, client] = [];
            }
        }

        for (int round = 0; round < Rounds; round++)
        {
            for (int scenario = 0; scenario < s_scenarios.Length; scenario++)
            {
                for (int turn = 0; turn < count; turn++)
                {
                    int client = (round + turn) % count;
                    samples[scenario, client].Add(await RunAsync(scenario, contestants[client]).ConfigureAwait(false));
                }
            }
        }

        return samples;
    }

    /// <summary>
    /// Runs one scenario <see cref="Ops"/> operations long with one client and times it.
    /// </summary>
    /// <param name="scenario">
    /// The scenario: 0 blocking round trip, 1 awaiting round trip, 2 blocking burst, 3 awaiting burst.
    /// </param>
    /// <param name="contestant">
    /// The client.
    /// </param>
    /// <returns>
    /// The microseconds and the bytes allocated per operation; per message for a burst.
    /// </returns>
    private static async Task<ProbeSample> RunAsync(int scenario, IContestant contestant)
    {
        long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        long start = Stopwatch.GetTimestamp();
        switch (scenario)
        {
            case 0:
                for (int i = 0; i < Ops; i++)
                {
                    // Blocks on purpose: this scenario is a caller that blocks on each operation.
                    contestant.RoundTripAsync().GetAwaiter().GetResult();
                }

                break;
            case 1:
                for (int i = 0; i < Ops; i++)
                {
                    await contestant.RoundTripAsync().ConfigureAwait(false);
                }

                break;
            case 2:
                for (int i = 0; i < Ops / Burst; i++)
                {
                    // Blocks on purpose: this scenario is a caller that blocks on each burst.
                    contestant.BurstAsync().GetAwaiter().GetResult();
                }

                break;
            default:
                for (int i = 0; i < Ops / Burst; i++)
                {
                    await contestant.BurstAsync().ConfigureAwait(false);
                }

                break;
        }

        double microseconds = Stopwatch.GetElapsedTime(start).TotalMicroseconds / Ops;
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
        return new ProbeSample(microseconds, (double)allocated / Ops);
    }

    /// <summary>
    /// Prints the table of the microseconds per operation and the table of the bytes allocated per operation.
    /// </summary>
    /// <param name="contestants">
    /// The clients, in the order of the columns.
    /// </param>
    /// <param name="samples">
    /// For each scenario and client, the sample of each round.
    /// </param>
    private static void Print(List<IContestant> contestants, List<ProbeSample>[,] samples)
    {
        PrintTable("us per operation", contestants, samples, static sample => sample.Microseconds);
        Console.WriteLine();
        PrintTable("B/op", contestants, samples, static sample => sample.Bytes);
    }

    /// <summary>
    /// Prints, for each scenario, the median of every client and the ratios of <c>after</c> to each other client.
    /// </summary>
    /// <param name="unit">
    /// The unit named in the header.
    /// </param>
    /// <param name="contestants">
    /// The clients, in the order of the columns.
    /// </param>
    /// <param name="samples">
    /// For each scenario and client, the sample of each round.
    /// </param>
    /// <param name="selector">
    /// Picks the value of the table from a sample.
    /// </param>
    private static void PrintTable(string unit, List<IContestant> contestants, List<ProbeSample>[,] samples, Func<ProbeSample, double> selector)
    {
        int after = contestants.FindIndex(contestant => contestant.Name == "after");
        string header = $"scenario ({unit}, median of {Rounds} rounds)";
        foreach (IContestant contestant in contestants)
        {
            header += $" | {contestant.Name}";
        }

        foreach (IContestant contestant in contestants)
        {
            if (contestant.Name != "after")
            {
                header += $" | after/{contestant.Name}";
            }
        }

        Console.WriteLine(header);
        for (int scenario = 0; scenario < s_scenarios.Length; scenario++)
        {
            string line = s_scenarios[scenario].PadRight(15);
            double afterMedian = Median(samples[scenario, after], selector);
            for (int client = 0; client < contestants.Count; client++)
            {
                line += string.Create(CultureInfo.InvariantCulture, $" | {Median(samples[scenario, client], selector),8:F1}");
            }

            for (int client = 0; client < contestants.Count; client++)
            {
                if (client != after)
                {
                    line += string.Create(CultureInfo.InvariantCulture, $" | {afterMedian / Median(samples[scenario, client], selector),5:F2}");
                }
            }

            Console.WriteLine(line);
        }
    }

    /// <summary>
    /// Computes the median of one value of the samples, the mean of the middle two for an even count.
    /// </summary>
    /// <param name="samples">
    /// The samples, which are not changed.
    /// </param>
    /// <param name="selector">
    /// Picks the value from a sample.
    /// </param>
    /// <returns>
    /// The median.
    /// </returns>
    private static double Median(List<ProbeSample> samples, Func<ProbeSample, double> selector)
    {
        List<double> sorted = [];
        foreach (ProbeSample sample in samples)
        {
            sorted.Add(selector(sample));
        }

        sorted.Sort();
        int middle = sorted.Count / 2;
        return sorted.Count % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
    }
}
