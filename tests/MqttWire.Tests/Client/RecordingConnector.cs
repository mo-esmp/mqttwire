using MqttWire.Link;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Client;

/// <summary>
/// An <see cref="IMqttLinkConnector"/> over a <see cref="MemoryLinkConnector"/> that records when each attempt to
/// open a link was made, by the test's clock, and lets a test wait for an attempt.
/// </summary>
/// <param name="inner">
/// The connector that hands out the links or the scripted failures.
/// </param>
/// <param name="time">
/// The clock the attempts are timed by.
/// </param>
internal sealed class RecordingConnector(MemoryLinkConnector inner, TimeProvider time) : IMqttLinkConnector
{
    /// <summary>
    /// Guards the attempts and the waiters.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// When each attempt was made, oldest first.
    /// </summary>
    private readonly List<DateTimeOffset> _attempts = [];

    /// <summary>
    /// The tests waiting for a number of attempts, and what completes.
    /// </summary>
    private readonly List<(int Count, TaskCompletionSource Done)> _waiters = [];

    /// <summary>
    /// Gets when each attempt was made, oldest first.
    /// </summary>
    public IReadOnlyList<DateTimeOffset> Attempts
    {
        get
        {
            lock (_gate)
            {
                return [.. _attempts];
            }
        }
    }

    /// <summary>
    /// Asks the inner connector for a link, then records the attempt and releases a test waiting for it.
    /// </summary>
    /// <param name="options">
    /// The options, passed on.
    /// </param>
    /// <param name="cancellationToken">
    /// The token, passed on; an already cancelled one throws and is not recorded.
    /// </param>
    /// <returns>
    /// The inner connector's result.
    /// </returns>
    public ValueTask<LinkConnectResult> ConnectAsync(MqttWireClientOptions options, CancellationToken cancellationToken)
    {
        ValueTask<LinkConnectResult> result = inner.ConnectAsync(options, cancellationToken);
        lock (_gate)
        {
            _attempts.Add(time.GetUtcNow());
            for (int i = _waiters.Count - 1; i >= 0; i--)
            {
                if (_waiters[i].Count <= _attempts.Count)
                {
                    _waiters[i].Done.TrySetResult();
                    _waiters.RemoveAt(i);
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Waits until a number of attempts were made in all.
    /// </summary>
    /// <param name="count">
    /// The number of attempts.
    /// </param>
    /// <returns>
    /// A task that completes, never inline, once that many attempts were made; it fails with
    /// <see cref="TimeoutException"/> after ten seconds of real time.
    /// </returns>
    public Task WhenAttemptsAsync(int count)
    {
        lock (_gate)
        {
            if (_attempts.Count >= count)
            {
                return Task.CompletedTask;
            }

            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add((count, done));
            return done.Task.WaitAsync(TimeSpan.FromSeconds(10));
        }
    }
}
