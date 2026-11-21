using Microsoft.Extensions.Time.Testing;

namespace MqttWire.Tests.Client;

/// <summary>
/// A <see cref="TimeProvider"/> over a <see cref="FakeTimeProvider"/> that records the due time of every timer it
/// creates, so a test can wait until a delay has started before it moves the clock. Time moves only when the fake
/// clock is advanced.
/// </summary>
/// <param name="inner">
/// The fake clock that tells the time and runs the timers.
/// </param>
internal sealed class TimerRecordingClock(FakeTimeProvider inner) : TimeProvider
{
    /// <summary>
    /// Guards the counts and the waiters.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// How many timers were created with each due time.
    /// </summary>
    private readonly Dictionary<TimeSpan, int> _created = [];

    /// <summary>
    /// How many timers of each due time a test has waited for so far.
    /// </summary>
    private readonly Dictionary<TimeSpan, int> _claimed = [];

    /// <summary>
    /// The tests waiting for a timer: its due time, how many of that due time must exist, and what completes.
    /// </summary>
    private readonly List<(TimeSpan DueTime, int Count, TaskCompletionSource Done)> _waiters = [];

    /// <summary>
    /// Gets the frequency of <see cref="GetTimestamp"/>, the fake clock's.
    /// </summary>
    public override long TimestampFrequency => inner.TimestampFrequency;

    /// <summary>
    /// Gets the local time zone, the fake clock's.
    /// </summary>
    public override TimeZoneInfo LocalTimeZone => inner.LocalTimeZone;

    /// <summary>
    /// Gets the current time from the fake clock.
    /// </summary>
    /// <returns>
    /// The fake clock's time.
    /// </returns>
    public override DateTimeOffset GetUtcNow() => inner.GetUtcNow();

    /// <summary>
    /// Gets the current timestamp from the fake clock.
    /// </summary>
    /// <returns>
    /// The fake clock's timestamp.
    /// </returns>
    public override long GetTimestamp() => inner.GetTimestamp();

    /// <summary>
    /// Creates a timer on the fake clock, then records its due time and releases a test waiting for it.
    /// </summary>
    /// <param name="callback">
    /// What the timer calls.
    /// </param>
    /// <param name="state">
    /// What it passes to the callback.
    /// </param>
    /// <param name="dueTime">
    /// When it first fires.
    /// </param>
    /// <param name="period">
    /// How often it fires after that.
    /// </param>
    /// <returns>
    /// The fake clock's timer, already in place when a waiting test is released.
    /// </returns>
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ITimer timer = inner.CreateTimer(callback, state, dueTime, period);
        lock (_gate)
        {
            int created = _created.GetValueOrDefault(dueTime) + 1;
            _created[dueTime] = created;
            for (int i = _waiters.Count - 1; i >= 0; i--)
            {
                if (_waiters[i].DueTime == dueTime && _waiters[i].Count <= created)
                {
                    _waiters[i].Done.TrySetResult();
                    _waiters.RemoveAt(i);
                }
            }
        }

        return timer;
    }

    /// <summary>
    /// Waits for the next timer with a due time that no earlier call has waited for: the first call for a due time
    /// waits for the first such timer, the second call for the second, and so on.
    /// </summary>
    /// <param name="dueTime">
    /// The timer's due time.
    /// </param>
    /// <returns>
    /// A task that completes, never inline, once that timer exists; it fails with <see cref="TimeoutException"/>
    /// after ten seconds of real time, so a test that is wrong fails instead of hanging.
    /// </returns>
    public Task WhenTimerAsync(TimeSpan dueTime)
    {
        lock (_gate)
        {
            int wanted = _claimed.GetValueOrDefault(dueTime) + 1;
            _claimed[dueTime] = wanted;
            if (_created.GetValueOrDefault(dueTime) >= wanted)
            {
                return Task.CompletedTask;
            }

            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add((dueTime, wanted, done));
            return done.Task.WaitAsync(TimeSpan.FromSeconds(10));
        }
    }
}
