using Microsoft.Extensions.Time.Testing;

namespace MqttWire.Tests.Client;

/// <summary>
/// A <see cref="TimeProvider"/> over a <see cref="FakeTimeProvider"/> that keeps every timer it creates, so a test
/// can prove that each timer is disposed and can call a timer's callback itself. Time moves only when the fake
/// clock is advanced.
/// </summary>
/// <param name="inner">
/// The fake clock that tells the time and runs the timers.
/// </param>
internal sealed class CountingTimeProvider(FakeTimeProvider inner) : TimeProvider
{
    /// <summary>
    /// Guards <see cref="_timers"/>.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// Every timer created, oldest first.
    /// </summary>
    private readonly List<CountedTimer> _timers = [];

    /// <summary>
    /// Gets the frequency of <see cref="GetTimestamp"/>, the fake clock's.
    /// </summary>
    public override long TimestampFrequency => inner.TimestampFrequency;

    /// <summary>
    /// Gets the local time zone, the fake clock's.
    /// </summary>
    public override TimeZoneInfo LocalTimeZone => inner.LocalTimeZone;

    /// <summary>
    /// Gets the number of timers created and not yet disposed.
    /// </summary>
    public int ActiveTimers
    {
        get
        {
            lock (_gate)
            {
                return _timers.Count(static timer => !timer.IsDisposed);
            }
        }
    }

    /// <summary>
    /// Lists the periodic timers created so far, oldest first: those with a period, such as the keep-alive timer.
    /// </summary>
    /// <returns>
    /// A copy of the list; later timers are not added to it.
    /// </returns>
    public List<CountedTimer> PeriodicTimers()
    {
        lock (_gate)
        {
            return [.. _timers.Where(static timer => timer.Period != Timeout.InfiniteTimeSpan && timer.Period != TimeSpan.Zero)];
        }
    }

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
    /// Creates a timer on the fake clock and keeps it.
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
    /// The timer, which counts its disposal.
    /// </returns>
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        CountedTimer timer = new(inner.CreateTimer(callback, state, dueTime, period), callback, state, period);
        lock (_gate)
        {
            _timers.Add(timer);
        }

        return timer;
    }

    /// <summary>
    /// A timer of the fake clock that records whether it was disposed, and lets a test call its callback.
    /// </summary>
    /// <param name="inner">
    /// The fake clock's timer.
    /// </param>
    /// <param name="callback">
    /// The timer's callback.
    /// </param>
    /// <param name="state">
    /// The state passed to the callback.
    /// </param>
    /// <param name="period">
    /// The period the timer was created with.
    /// </param>
    internal sealed class CountedTimer(ITimer inner, TimerCallback callback, object? state, TimeSpan period) : ITimer
    {
        /// <summary>
        /// 1 once the timer was disposed.
        /// </summary>
        private int _disposed;

        /// <summary>
        /// Gets the period the timer was created with.
        /// </summary>
        public TimeSpan Period { get; } = period;

        /// <summary>
        /// Gets a value indicating whether the timer was disposed.
        /// </summary>
        public bool IsDisposed => Volatile.Read(ref _disposed) == 1;

        /// <summary>
        /// Calls the timer's callback now, on this thread, as the timer would; also after the timer was disposed,
        /// as a real timer's callback may still run then.
        /// </summary>
        public void Fire() => callback(state);

        /// <summary>
        /// Changes when the timer fires.
        /// </summary>
        /// <param name="dueTime">
        /// When it next fires.
        /// </param>
        /// <param name="period">
        /// How often it fires after that.
        /// </param>
        /// <returns>
        /// <see langword="true"/> when the timer was changed; <see langword="false"/> after it was disposed.
        /// </returns>
        public bool Change(TimeSpan dueTime, TimeSpan period) => inner.Change(dueTime, period);

        /// <summary>
        /// Disposes the fake clock's timer and records the disposal.
        /// </summary>
        public void Dispose()
        {
            Volatile.Write(ref _disposed, 1);
            inner.Dispose();
        }

        /// <summary>
        /// Disposes the fake clock's timer and records the disposal.
        /// </summary>
        /// <returns>
        /// A task that completes when the timer is disposed.
        /// </returns>
        public ValueTask DisposeAsync()
        {
            Volatile.Write(ref _disposed, 1);
            return inner.DisposeAsync();
        }
    }
}
