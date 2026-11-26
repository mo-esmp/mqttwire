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
                Task done = await Task.WhenAny(receive, Task.Delay(TimeSpan.FromSeconds(10)));
                if (done != receive)
                {
                    stalled = true;
                    long at = watch.ElapsedMilliseconds;
                    StringBuilder report = new();
                    report.AppendLine($"DIAG STALL run={run} at={at}ms frames={frames} bytes={bytes} stateAtText=[{atText}] stateNow=[{State(connection!)}]");
                    report.AppendLine("DIAG timeline: " + string.Join(" ", timeline.TakeLast(12)));
                    report.AppendLine("DIAG ss:\n" + Ss(host.WebSocketUri.Port));
                    foreach (LogRecorder.LogEntry entry in host.Logs.Entries)
                    {
                        report.AppendLine($"DIAG log {entry.Level} {entry.EventId.Id} {entry.Message} {entry.Exception?.GetType().Name}: {entry.Exception?.Message}");
                    }

                    Emit(report.ToString());
                    done = await Task.WhenAny(receive, Task.Delay(TimeSpan.FromSeconds(60)));
                    if (done != receive)
                    {
                        end = $"still nothing after {watch.ElapsedMilliseconds}ms; ss:\n{Ss(host.WebSocketUri.Port)}\nstate=[{State(connection!)}]";
                        break;
                    }
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
            return text;
        }
        catch (Exception exception)
        {
            return "ss failed: " + exception.Message;
        }
    }
}
