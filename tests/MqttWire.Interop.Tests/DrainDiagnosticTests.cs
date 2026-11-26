#pragma warning disable
using System.Diagnostics;
using System.Net.WebSockets;
using System.Reflection;
using System.Text;
using MqttWire.Server;
using Xunit.Abstractions;

namespace MqttWire.Interop.Tests;

/// <summary>
/// Diagnostic only (not for merge): the original shape of the pending-write test (1024-byte receive buffer, text sent
/// at once), run among the other tests; when a receive waits past 10 s it records the state of the server's
/// connection, the sockets and the server's log, then waits up to 60 s more to see how the drain ends.
/// </summary>
public sealed class DrainDiagnosticTests(ITestOutputHelper output)
{
    public static IEnumerable<object[]> Cases()
    {
        int runs = int.TryParse(Environment.GetEnvironmentVariable("DIAG_RUNS"), out int r) ? r : 3;
        for (int i = 0; i < runs; i++)
        {
            yield return [i];
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Capture(int run)
    {
        await using ServerHost host = await ServerHost.StartAsync(options => options.MaxOutgoingBytes = 64 << 20);
        await using RawWebSocket client = await host.ConnectRawAsync(receiveBufferSize: 1024);
        host.Server.TryGetConnection("c1", out MqttWireConnection? connection).ShouldBeTrue();
        Dictionary<string, long> netBefore = ReadTcpExt();
        byte[] payload = new byte[60_000];
        MqttTopic topic = MqttTopic.Create("a/b");
        for (int i = 0; i < 200; i++)
        {
            connection!.TrySend(topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        }

        string atText = State(connection!);
        await client.SendAsync(RawWebSocket.Text, "hello"u8.ToArray());
        Stopwatch watch = Stopwatch.StartNew();
        List<string> timeline = [];
        int frames = 0;
        long bytes = 0;
        string end = "?";
        bool stalled = false;
        Task<RawWebSocket.Frame>? receive = null;
        try
        {
            while (true)
            {
                receive = client.ReceiveAsync();
                int waited = 0;
                while (await Task.WhenAny(receive, Task.Delay(TimeSpan.FromSeconds(2))) != receive)
                {
                    waited += 2;
                    if (waited == 4)
                    {
                        stalled = true;
                        Emit($"DIAG STALL run={run} at={watch.ElapsedMilliseconds}ms frames={frames} bytes={bytes} available={Available(client)} stateAtText=[{atText}] stateNow=[{State(connection!)}]");
                        Emit("DIAG timeline: " + string.Join(" ", timeline.TakeLast(12)));
                    }

                    if (stalled)
                    {
                        Emit($"DIAG ss at={watch.ElapsedMilliseconds}ms available={Available(client)} state=[{State(connection!)}]\n" + Ss(host.WebSocketUri.Port));
                        Emit("DIAG tcpext since the sends: " + Delta(netBefore, ReadTcpExt()));
                    }

                    if (waited >= 70)
                    {
                        break;
                    }
                }

                if (!receive.IsCompleted)
                {
                    end = "still nothing after 70 s";
                    break;
                }

                if (stalled && end == "?")
                {
                    end = "";
                    Emit($"DIAG stall ended at={watch.ElapsedMilliseconds}ms");
                }

                RawWebSocket.Frame frame = await receive;
                frames++;
                bytes += frame.Payload.Length;
                timeline.Add($"{watch.ElapsedMilliseconds}:{frame.Opcode}/{frame.Payload.Length}");
                if (frame.Opcode != RawWebSocket.Binary)
                {
                    end = $"opcode {frame.Opcode} status {frame.CloseStatus}";
                    break;
                }
            }
        }
        catch (Exception exception)
        {
            end = exception.GetType().Name + (exception.InnerException is null ? "" : "/" + exception.InnerException.Message);
        }

        Emit($"DIAG {(stalled ? "STALLED" : "ok")} run={run} frames={frames} total={watch.ElapsedMilliseconds}ms end={end} stateEnd=[{State(connection!)}]");
        if (stalled)
        {
            Emit("DIAG timeline: " + string.Join(" ", timeline.TakeLast(12)));
            foreach (LogRecorder.LogEntry entry in host.Logs.Entries)
            {
                Emit($"DIAG log {entry.Level} {entry.EventId.Id} {entry.Message} {entry.Exception?.GetType().Name}: {entry.Exception?.Message}");
            }
        }
    }

    private void Emit(string text)
    {
        output.WriteLine(text);
        Console.WriteLine(text);
    }

    private static string State(MqttWireConnection connection)
    {
        StringBuilder text = new();
        foreach (string name in new[] { "_state", "_writing", "_aborting", "_closeStatus", "_outgoingBytes", "_writingBytes", "_writeStartedAt" })
        {
            FieldInfo? field = typeof(MqttWireConnection).GetField(name, BindingFlags.NonPublic | BindingFlags.Instance);
            text.Append(name).Append('=').Append(field?.GetValue(connection)).Append(' ');
        }

        object? link = typeof(MqttWireConnection).GetField("_link", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(connection);
        text.Append("link=").Append(link is null ? "null" : "set").Append(' ');
        if (link is not null)
        {
            foreach (FieldInfo field in link.GetType().GetFields(BindingFlags.NonPublic | BindingFlags.Instance))
            {
                object? value = field.GetValue(link);
                text.Append(field.Name).Append('=').Append(value is WebSocket socket ? "ws:" + socket.State : value).Append(' ');
            }
        }

        return text.ToString();
    }

    private static int Available(RawWebSocket client) =>
        ((System.Net.Sockets.Socket)typeof(RawWebSocket).GetField("_socket", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(client)!).Available;

    private static string Delta(Dictionary<string, long> before, Dictionary<string, long> after)
    {
        StringBuilder deltas = new();
        foreach ((string key, long value) in after)
        {
            if (before.TryGetValue(key, out long old) && value != old && key.StartsWith("TcpExt", StringComparison.Ordinal))
            {
                deltas.Append(key[6..]).Append('=').Append(value - old).Append(' ');
            }
        }

        return deltas.ToString();
    }

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
                    values[names[0][..^1] + names[j]] = n;
                }
            }
        }

        return values;
    }

    private static string Ss(int port)
    {
        try
        {
            ProcessStartInfo start = new("ss", $"-tinmoe state all ( sport = :{port} or dport = :{port} )")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            using Process process = Process.Start(start)!;
            string text = process.StandardOutput.ReadToEnd() + process.StandardError.ReadToEnd();
            process.WaitForExit(5000);
            return string.Join("\n", text.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(line => "DIAG   " + line.Trim()));
        }
        catch (Exception exception)
        {
            return "ss failed: " + exception.Message;
        }
    }
}
