namespace MqttWire.Tests.Client;

/// <summary>
/// What the reconnecting client's callbacks were called with, in order, and whether two of them ever ran at the
/// same time. Safe to use from several threads.
/// </summary>
internal sealed class CallbackLog
{
    /// <summary>
    /// Guards the events and the waiters.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// One line per callback, in the order they were entered.
    /// </summary>
    private readonly List<string> _events = [];

    /// <summary>
    /// The tests waiting for events: the prefix, how many events with it must exist, and what completes.
    /// </summary>
    private readonly List<(string Prefix, int Count, TaskCompletionSource Done)> _waiters = [];

    /// <summary>
    /// The number of callbacks running now.
    /// </summary>
    private int _running;

    /// <summary>
    /// 1 once a callback was entered while another was running.
    /// </summary>
    private int _overlapped;

    /// <summary>
    /// Gets a copy of the events so far, oldest first.
    /// </summary>
    public IReadOnlyList<string> Events
    {
        get
        {
            lock (_gate)
            {
                return [.. _events];
            }
        }
    }

    /// <summary>
    /// Gets a value indicating whether a callback was ever entered while another was running.
    /// </summary>
    public bool Overlapped => Volatile.Read(ref _overlapped) == 1;

    /// <summary>
    /// Records that a callback was entered, and releases the tests waiting for it. Every call is followed by one
    /// <see cref="Exit"/>.
    /// </summary>
    /// <param name="entry">
    /// What the callback was called with.
    /// </param>
    public void Enter(string entry)
    {
        if (Interlocked.Increment(ref _running) != 1)
        {
            Volatile.Write(ref _overlapped, 1);
        }

        lock (_gate)
        {
            _events.Add(entry);
            for (int i = _waiters.Count - 1; i >= 0; i--)
            {
                (string prefix, int count, TaskCompletionSource done) = _waiters[i];
                if (CountLocked(prefix) >= count)
                {
                    done.TrySetResult();
                    _waiters.RemoveAt(i);
                }
            }
        }
    }

    /// <summary>
    /// Records that the callback entered last on this path has returned or thrown.
    /// </summary>
    public void Exit() => Interlocked.Decrement(ref _running);

    /// <summary>
    /// Waits until a number of events start with a prefix.
    /// </summary>
    /// <param name="prefix">
    /// The start of the events, such as <c>connected</c>.
    /// </param>
    /// <param name="count">
    /// How many such events must exist.
    /// </param>
    /// <returns>
    /// A task that completes, never inline, once they exist; it fails with <see cref="TimeoutException"/> after
    /// ten seconds of real time.
    /// </returns>
    public Task WhenAsync(string prefix, int count = 1)
    {
        lock (_gate)
        {
            if (CountLocked(prefix) >= count)
            {
                return Task.CompletedTask;
            }

            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add((prefix, count, done));
            return done.Task.WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    /// <summary>
    /// Counts the events that start with a prefix; the caller holds <see cref="_gate"/>.
    /// </summary>
    /// <param name="prefix">
    /// The prefix.
    /// </param>
    /// <returns>
    /// The number of such events.
    /// </returns>
    private int CountLocked(string prefix)
    {
        int count = 0;
        foreach (string entry in _events)
        {
            if (entry.StartsWith(prefix, StringComparison.Ordinal))
            {
                count++;
            }
        }

        return count;
    }
}
