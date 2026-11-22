namespace MqttWire.Tests;

/// <summary>
/// Bounds the waits of the tests, so that a run that never ends fails its test with a timeout instead of hanging the
/// whole suite. The bound is generous and is reached only by a broken run: no outcome depends on it, and it never makes
/// a slow test pass.
/// </summary>
internal static class BoundedWaits
{
    /// <summary>
    /// How long a test waits for anything before it fails.
    /// </summary>
    public static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Waits for a task, at most <see cref="Limit"/>.
    /// </summary>
    /// <param name="task">
    /// The task.
    /// </param>
    /// <param name="what">
    /// What the task is, for the message of the timeout, for example "the connection's run".
    /// </param>
    /// <returns>
    /// A task that completes as <paramref name="task"/> does.
    /// </returns>
    /// <exception cref="TimeoutException">
    /// The task did not complete within <see cref="Limit"/>; the message names what was awaited.
    /// </exception>
    public static async Task Within(this Task task, string what)
    {
        try
        {
            await task.WaitAsync(Limit);
        }
        catch (TimeoutException exception) when (!task.IsCompleted)
        {
            throw new TimeoutException($"{what} did not complete within {Limit.TotalSeconds} seconds.", exception);
        }
    }

    /// <summary>
    /// Waits for a thread to end, at most <see cref="Limit"/>.
    /// </summary>
    /// <param name="thread">
    /// The thread, started.
    /// </param>
    /// <param name="what">
    /// What the thread does, for the message of the timeout.
    /// </param>
    /// <exception cref="TimeoutException">
    /// The thread did not end within <see cref="Limit"/>.
    /// </exception>
    public static void JoinWithin(this Thread thread, string what)
    {
        if (!thread.Join(Limit))
        {
            throw new TimeoutException($"{what} did not end within {Limit.TotalSeconds} seconds.");
        }
    }
}
