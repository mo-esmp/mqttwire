using System.Globalization;
using System.Text;

namespace MqttWire.Bench;

/// <summary>
/// What the <c>server</c> role reports about itself at <c>/measure</c>.
/// </summary>
/// <param name="LiveManagedBytes">
/// The live managed bytes after two full collections.
/// </param>
/// <param name="WorkingSetBytes">
/// The working set of the process.
/// </param>
/// <param name="Connections">
/// The open connections, on the floor and on MqttWire together.
/// </param>
/// <param name="LastSweepMicroseconds">
/// How long MqttWire's last sweep took, in microseconds.
/// </param>
/// <param name="Settled">
/// Whether two readings in a row agreed before this one was taken. The server does not report it; the
/// <c>run</c> role sets it. A reading that is not settled was taken while the heap was still moving, and
/// decides nothing.
/// </param>
internal readonly record struct Measurement(
    long LiveManagedBytes,
    long WorkingSetBytes,
    int Connections,
    double LastSweepMicroseconds,
    bool Settled = false);

/// <summary>
/// A line through two measurements at two connection counts.
/// </summary>
/// <param name="PerConnection">
/// The slope: bytes per connection. It leaves out everything that does not grow with connections.
/// </param>
/// <param name="Fixed">
/// The line's value at no connections: the part that does not grow with them.
/// </param>
internal readonly record struct Line(double PerConnection, double Fixed);

/// <summary>
/// One state of the benchmark, measured at the small and at the large connection count.
/// </summary>
/// <param name="Name">
/// The state's name, as the report prints it.
/// </param>
/// <param name="Small">
/// The measurement at the small count.
/// </param>
/// <param name="Large">
/// The measurement at the large count.
/// </param>
internal readonly record struct State(string Name, Measurement Small, Measurement Large)
{
    /// <summary>
    /// Gets a value indicating whether both measurements were taken from a heap that had settled.
    /// </summary>
    public bool Settled => Small.Settled && Large.Settled;
}

/// <summary>
/// What a run says about the target.
/// </summary>
internal enum Verdict
{
    /// <summary>
    /// An idle connection costs MqttWire at most the target above the floor.
    /// </summary>
    Met,

    /// <summary>
    /// An idle connection costs MqttWire more than the target above the floor.
    /// </summary>
    NotMet,

    /// <summary>
    /// The floor or MqttWire's idle state was measured while its heap was still moving, so the run decides
    /// nothing.
    /// </summary>
    Inconclusive,
}

/// <summary>
/// The arithmetic of design 6.14 and the report: "per connection" is a slope between two counts, and only live
/// managed bytes decide against the target.
/// </summary>
internal static class Figures
{
    /// <summary>
    /// The target: MqttWire adds at most this many bytes to the floor for an idle connection. The benchmark
    /// decides it for the idle state. After activity and after churn the target is judged on MqttWire's own state
    /// (design 6.14), which a heap total cannot tell from what Kestrel's buffer pool keeps; those two states are
    /// reported against the idle floor for information.
    /// </summary>
    public const int TargetBytesAboveFloor = 1_024;

    /// <summary>
    /// How far the slope after activity may be above the idle slope, in bytes, and still count as equal: what the
    /// buffer pools keep after activity must not grow with connections. A slope below the idle one means that
    /// less was kept at the large count than at the small one, which is no growth.
    /// </summary>
    public const int EqualSlopeBytes = 16;

    /// <summary>
    /// Fits the line through two measurements.
    /// </summary>
    /// <param name="smallCount">
    /// The small connection count.
    /// </param>
    /// <param name="smallBytes">
    /// The bytes measured at it.
    /// </param>
    /// <param name="largeCount">
    /// The large connection count.
    /// </param>
    /// <param name="largeBytes">
    /// The bytes measured at it.
    /// </param>
    /// <returns>
    /// The slope, <c>(largeBytes − smallBytes) ÷ (largeCount − smallCount)</c>, and the line's value at no
    /// connections.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The two counts are the same, which gives no slope.
    /// </exception>
    public static Line Fit(int smallCount, long smallBytes, int largeCount, long largeBytes)
    {
        if (smallCount == largeCount)
        {
            throw new ArgumentException($"Two measurements at the same count ({smallCount}) give no slope.", nameof(largeCount));
        }

        double perConnection = (double)(largeBytes - smallBytes) / (largeCount - smallCount);
        return new Line(perConnection, smallBytes - (perConnection * smallCount));
    }

    /// <summary>
    /// Writes the report of one run as Markdown: a table of the states, pool retention, the traffic floor's figures
    /// for the same traffic, the sweep's time, and whether each state meets the target.
    /// </summary>
    /// <param name="smallCount">
    /// The small connection count.
    /// </param>
    /// <param name="largeCount">
    /// The large connection count.
    /// </param>
    /// <param name="floor">
    /// The floor, idle.
    /// </param>
    /// <param name="trafficIdle">
    /// The traffic floor, idle: a bare endpoint like the floor that can play MqttWire's side of the activity.
    /// </param>
    /// <param name="trafficAfterActivity">
    /// The traffic floor after the same traffic as MqttWire's activity, in the same directions, then quiet.
    /// </param>
    /// <param name="idle">
    /// MqttWire, connected and nothing else.
    /// </param>
    /// <param name="afterActivity">
    /// MqttWire after every connection published and received a burst, then quiet.
    /// </param>
    /// <param name="afterChurn">
    /// MqttWire after every connection was closed and opened again, then quiet.
    /// </param>
    /// <param name="verdict">
    /// Set to whether MqttWire's idle state meets the target, or to <see cref="Verdict.Inconclusive"/> when the
    /// floor or that state was measured while its heap was still moving. The states after activity and after
    /// churn do not decide it (design 6.14).
    /// </param>
    /// <returns>
    /// The report.
    /// </returns>
    public static string Report(int smallCount, int largeCount, State floor, State trafficIdle,
        State trafficAfterActivity, State idle, State afterActivity, State afterChurn, out Verdict verdict)
    {
        CultureInfo culture = CultureInfo.InvariantCulture;
        Line floorLine = FitLive(smallCount, largeCount, floor);
        StringBuilder report = new();
        report.AppendLine(culture, $"Per connection is the slope between {smallCount:N0} and {largeCount:N0} connections.");
        report.AppendLine();
        report.AppendLine("| State | Live managed bytes per connection | Above the floor | Fixed (MB) | Working set per connection |");
        report.AppendLine("|---|---|---|---|---|");
        foreach (State state in (ReadOnlySpan<State>)[floor, trafficIdle, trafficAfterActivity, idle, afterActivity, afterChurn])
        {
            Line live = FitLive(smallCount, largeCount, state);
            Line workingSet = Fit(smallCount, state.Small.WorkingSetBytes, largeCount, state.Large.WorkingSetBytes);
            bool isFloor = state == floor;
            double above = live.PerConnection - floorLine.PerConnection;
            string aboveText = isFloor ? "–" : above.ToString("N0", culture);
            string name = state.Settled ? state.Name : state.Name + " (NOT SETTLED)";
            report.AppendLine(culture,
                $"| {name} | {live.PerConnection:N0} | {aboveText} | {live.Fixed / 1_048_576:N1} | {workingSet.PerConnection:N0} |");
        }

        Line idleLine = FitLive(smallCount, largeCount, idle);
        Line activityLine = FitLive(smallCount, largeCount, afterActivity);
        double slopeDifference = activityLine.PerConnection - idleLine.PerConnection;
        string grows = slopeDifference <= EqualSlopeBytes ? "does not grow" : "GROWS";
        double keptSmall = (afterActivity.Small.LiveManagedBytes - idle.Small.LiveManagedBytes) / 1_048_576.0;
        double keptLarge = (afterActivity.Large.LiveManagedBytes - idle.Large.LiveManagedBytes) / 1_048_576.0;
        report.AppendLine();
        report.AppendLine(culture,
            $"- Kept after activity (live managed bytes after activity minus idle): {keptSmall:N1} MB at {smallCount:N0} connections, {keptLarge:N1} MB at {largeCount:N0}. Pool retention (the fixed part after activity minus the fixed part idle): {(activityLine.Fixed - idleLine.Fixed) / 1_024:N0} KB.");
        report.AppendLine(culture,
            $"- What is kept after activity {grows} with connections: the slope after activity is {slopeDifference:N0} bytes from the idle slope, and at most {EqualSlopeBytes} above it counts as equal.");
        Line trafficIdleLine = FitLive(smallCount, largeCount, trafficIdle);
        Line trafficActivityLine = FitLive(smallCount, largeCount, trafficAfterActivity);
        report.AppendLine(culture,
            $"- The same traffic on a bare endpoint: the traffic floor's slope after activity is {trafficActivityLine.PerConnection - trafficIdleLine.PerConnection:N0} bytes from its idle slope. Against the traffic floor in the same state, MqttWire is {idleLine.PerConnection - trafficIdleLine.PerConnection:N0} bytes per connection above it idle and {activityLine.PerConnection - trafficActivityLine.PerConnection:N0} after activity.");
        report.AppendLine(culture,
            $"- One sweep at {largeCount:N0} connections: {idle.Large.LastSweepMicroseconds:N0} µs idle, {afterActivity.Large.LastSweepMicroseconds:N0} µs after activity.");
        double idleAbove = idleLine.PerConnection - floorLine.PerConnection;
        if (!floor.Settled || !idle.Settled)
        {
            // A heap that was still moving when it was read gives a slope that means nothing.
            verdict = Verdict.Inconclusive;
            report.AppendLine(culture,
                $"- Target (floor + {TargetBytesAboveFloor:N0} bytes), {idle.Name}: INCONCLUSIVE. The heap of {(floor.Settled ? idle.Name : floor.Name)} was still moving when it was measured ({idleAbove:N0} bytes above the floor, which decides nothing). Run again.");
        }
        else
        {
            verdict = idleAbove <= TargetBytesAboveFloor ? Verdict.Met : Verdict.NotMet;
            report.AppendLine(culture,
                $"- Target (floor + {TargetBytesAboveFloor:N0} bytes), {idle.Name}: {(verdict == Verdict.Met ? "met" : "NOT met")} ({idleAbove:N0} bytes above the floor).");
        }

        foreach (State state in (ReadOnlySpan<State>)[afterActivity, afterChurn])
        {
            double above = FitLive(smallCount, largeCount, state).PerConnection - floorLine.PerConnection;
            string side = above <= TargetBytesAboveFloor ? "within" : "over";
            string moving = floor.Settled && state.Settled ? string.Empty : " The heap was still moving when one of the two was measured.";
            report.AppendLine(culture,
                $"- For information, against the idle floor: {state.Name} is {above:N0} bytes above it, {side} floor + {TargetBytesAboveFloor:N0}.{moving} This does not decide the target: there it is judged on MqttWire's own state (design 6.14), and the heap also holds what Kestrel's pool kept.");
        }

        return report.ToString();
    }

    /// <summary>
    /// Fits the line of a state's live managed bytes.
    /// </summary>
    /// <param name="smallCount">
    /// The small connection count.
    /// </param>
    /// <param name="largeCount">
    /// The large connection count.
    /// </param>
    /// <param name="state">
    /// The state.
    /// </param>
    /// <returns>
    /// The line.
    /// </returns>
    private static Line FitLive(int smallCount, int largeCount, State state) =>
        Fit(smallCount, state.Small.LiveManagedBytes, largeCount, state.Large.LiveManagedBytes);
}
