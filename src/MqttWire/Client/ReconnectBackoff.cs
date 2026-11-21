namespace MqttWire.Client;

/// <summary>
/// The reconnecting client's delays between attempts: doubling from the initial delay up to the maximum, with a
/// random part of ±20 % so that many clients do not come back in the same instant.
/// </summary>
internal static class ReconnectBackoff
{
    /// <summary>
    /// The smallest random factor, for a random number of 0.
    /// </summary>
    private const double MinFactor = 0.8;

    /// <summary>
    /// The width of the random factor's range: a random number of 1 gives <see cref="MinFactor"/> plus this, 1.2.
    /// </summary>
    private const double FactorRange = 0.4;

    /// <summary>
    /// Computes the delay before a retry: <c>min(max, initial × 2^(attempt − 1))</c> times a factor from 0.8 to 1.2,
    /// and never more than <paramref name="max"/>. Neither the doubling nor the factor can overflow, however late
    /// the attempt.
    /// </summary>
    /// <param name="attempt">
    /// The retry since the last successful connection, from 1: the first retry waits about
    /// <paramref name="initial"/>.
    /// </param>
    /// <param name="initial">
    /// The first delay; greater than zero.
    /// </param>
    /// <param name="max">
    /// The longest delay; at least <paramref name="initial"/>.
    /// </param>
    /// <param name="random01">
    /// A random number from 0 to 1, which picks the factor from 0.8 to 1.2; a number outside that range is taken
    /// as the nearest end.
    /// </param>
    /// <returns>
    /// The delay, rounded to whole ticks.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="attempt"/> is below 1.
    /// </exception>
    public static TimeSpan GetDelay(int attempt, TimeSpan initial, TimeSpan max, double random01)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);

        // initial × 2^doublings is over max exactly when initial is over max ÷ 2^doublings (rounded down), which
        // cannot overflow; a long holds at most 63 doublings of one tick.
        int doublings = attempt - 1;
        long baseTicks = doublings >= 63 || initial.Ticks > max.Ticks >> doublings
            ? max.Ticks
            : Math.Min(initial.Ticks << doublings, max.Ticks);

        double factor = MinFactor + (FactorRange * Math.Clamp(random01, 0.0, 1.0));
        double ticks = Math.Round(baseTicks * factor);
        return ticks >= max.Ticks ? max : TimeSpan.FromTicks((long)ticks);
    }
}
