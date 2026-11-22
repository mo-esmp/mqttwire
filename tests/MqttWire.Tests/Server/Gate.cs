namespace MqttWire.Tests.Server;

/// <summary>
/// The gate the threads of a racing test wait at, so that they start as close together as the scheduler allows.
/// </summary>
internal static class Gate
{
    /// <summary>
    /// Counts a thread in at the gate and spins until every thread of the test is there.
    /// </summary>
    /// <param name="ready">
    /// The number of threads at the gate, shared by all of them.
    /// </param>
    /// <param name="threads">
    /// The number of threads the gate waits for.
    /// </param>
    public static void Wait(ref int ready, int threads)
    {
        Interlocked.Increment(ref ready);
        SpinWait spin = default;
        while (Volatile.Read(ref ready) < threads)
        {
            spin.SpinOnce(sleep1Threshold: -1);
        }
    }

    /// <summary>
    /// Waits at the gate, then spins for a delay of the thread's own and runs its action, so that the threads of a
    /// round meet in an order that changes from round to round.
    /// </summary>
    /// <param name="ready">
    /// The number of threads at the gate, shared by all of them.
    /// </param>
    /// <param name="threads">
    /// The number of threads the gate waits for.
    /// </param>
    /// <param name="delay">
    /// How many spins the thread waits once the gate has opened.
    /// </param>
    /// <param name="action">
    /// What the thread does.
    /// </param>
    public static void Run(ref int ready, int threads, int delay, Action action)
    {
        Wait(ref ready, threads);
        Thread.SpinWait(delay);
        action();
    }
}
