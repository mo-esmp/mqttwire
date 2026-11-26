#pragma warning disable
using System.Diagnostics;
using System.Reflection;
using System.Text;
using MqttWire.Server;
using Xunit.Abstractions;

namespace MqttWire.Interop.Tests;

/// <summary>
/// Diagnostic only (not for merge): times the drain of the pending-write test per receive buffer size.
/// </summary>
public sealed class DrainDiagnosticTests(ITestOutputHelper output)
{
    public static IEnumerable<object[]> Cases()
    {
        int runs = int.TryParse(Environment.GetEnvironmentVariable("DIAG_RUNS"), out int r) ? r : 5;
        foreach (int size in new[] { 1024, 0 })
        {
            for (int i = 0; i < runs; i++)
            {
                yield return [size, i];
            }
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Drain(int receiveBufferSize, int run)
    {
        Dictionary<string, long> before = ReadTcpExt();
        await using ServerHost host = await ServerHost.StartAsync(options => options.MaxOutgoingBytes = 64 << 20);
        await using RawWebSocket client = await host.ConnectRawAsync(receiveBufferSize: receiveBufferSize);
        host.Server.TryGetConnection("c1", out MqttWireConnection? connection).ShouldBeTrue();
        byte[] payload = new byte[60_000];
        MqttTopic topic = MqttTopic.Create("a/b");
        for (int i = 0; i < 200; i++)
        {
            connection!.TrySend(topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        }

        await Task.Delay(500);
        FieldInfo writing = typeof(MqttWireConnection).GetField("_writing", BindingFlags.NonPublic | BindingFlags.Instance)!;
        FieldInfo outgoing = typeof(MqttWireConnection).GetField("_outgoingBytes", BindingFlags.NonPublic | BindingFlags.Instance)!;
        bool pending = (bool)writing.GetValue(connection)!;
        int left = (int)outgoing.GetValue(connection)!;

        await client.SendAsync(RawWebSocket.Text, "hello"u8.ToArray());
        Stopwatch watch = Stopwatch.StartNew();
        long lastAt = 0;
        long maxGap = 0;
        int maxGapAt = -1;
        int frames = 0;
        string end = "?";
        try
        {
            while (true)
            {
                RawWebSocket.Frame frame = await client.ReceiveAsync().WaitAsync(TimeSpan.FromSeconds(60));
                long now = watch.ElapsedMilliseconds;
                if (now - lastAt > maxGap)
                {
                    maxGap = now - lastAt;
                    maxGapAt = frames;
                }

                lastAt = now;
                frames++;
                if (frame.Opcode != RawWebSocket.Binary)
                {
                    end = $"opcode {frame.Opcode} status {frame.CloseStatus}";
                    break;
                }
            }
        }
        catch (Exception exception)
        {
            end = exception.GetType().Name + (exception.InnerException is null ? "" : "/" + exception.InnerException.GetType().Name + ":" + exception.InnerException.Message);
        }

        Dictionary<string, long> after = ReadTcpExt();
        StringBuilder deltas = new();
        foreach ((string key, long value) in after)
        {
            if (before.TryGetValue(key, out long old) && value != old && IsInteresting(key))
            {
                deltas.Append(key).Append('=').Append(value - old).Append(' ');
            }
        }

        string line = $"DIAG rcvbuf={receiveBufferSize} run={run} pendingBeforeText={pending} outgoingLeft={left} frames={frames} total={watch.ElapsedMilliseconds}ms maxGap={maxGap}ms@frame{maxGapAt} end={end} tcpext: {deltas}";
        output.WriteLine(line);
        Console.WriteLine(line);
    }

    private static bool IsInteresting(string key) =>
        key.Contains("Prune", StringComparison.Ordinal) || key.Contains("Drop", StringComparison.Ordinal)
        || key.Contains("Collapse", StringComparison.Ordinal) || key.Contains("Timeout", StringComparison.Ordinal)
        || key.Contains("Probe", StringComparison.Ordinal) || key.Contains("ZeroWindow", StringComparison.Ordinal)
        || key.Contains("Retrans", StringComparison.Ordinal) || key.Contains("Ofo", StringComparison.Ordinal)
        || key.Contains("Window", StringComparison.Ordinal) || key.Contains("Memory", StringComparison.Ordinal);

    private static Dictionary<string, long> ReadTcpExt()
    {
        Dictionary<string, long> values = [];
        if (!File.Exists("/proc/net/netstat"))
        {
            return values;
        }

        string[] lines = File.ReadAllLines("/proc/net/netstat");
        for (int i = 0; i + 1 < lines.Length; i += 2)
        {
            string[] names = lines[i].Split(' ', StringSplitOptions.RemoveEmptyEntries);
            string[] numbers = lines[i + 1].Split(' ', StringSplitOptions.RemoveEmptyEntries);
            for (int j = 1; j < names.Length && j < numbers.Length; j++)
            {
                if (long.TryParse(numbers[j], out long n))
                {
                    values[names[0] + names[j]] = n;
                }
            }
        }

        return values;
    }
}
