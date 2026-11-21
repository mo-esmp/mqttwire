using MqttWire.Client;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests the reconnecting client's delays between attempts: doubling from the initial delay up to the maximum, with
/// a random part of ±20 % that never passes the maximum.
/// </summary>
public sealed class ReconnectBackoffTests
{
    /// <summary>
    /// The default first delay, 500 ms.
    /// </summary>
    private static readonly TimeSpan s_initial = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// The default longest delay, 15 s.
    /// </summary>
    private static readonly TimeSpan s_max = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Proves that with a random part of 0.5 (a factor of exactly 1) the delay doubles from the initial delay with
    /// each attempt and stays at the maximum once it reaches it, also for attempts so late that the doubling would
    /// overflow.
    /// </summary>
    /// <param name="attempt">
    /// The retry, from 1.
    /// </param>
    /// <param name="expectedMilliseconds">
    /// The delay expected.
    /// </param>
    [Theory]
    [InlineData(1, 500)]
    [InlineData(2, 1_000)]
    [InlineData(3, 2_000)]
    [InlineData(4, 4_000)]
    [InlineData(5, 8_000)]
    [InlineData(6, 15_000)]
    [InlineData(50, 15_000)]
    [InlineData(64, 15_000)]
    [InlineData(int.MaxValue, 15_000)]
    public void GetDelay_AttemptN_DoublesFromInitialUpToMax(int attempt, int expectedMilliseconds)
    {
        // Arrange
        TimeSpan expected = TimeSpan.FromMilliseconds(expectedMilliseconds);

        // Act
        TimeSpan delay = ReconnectBackoff.GetDelay(attempt, s_initial, s_max, 0.5);

        // Assert
        delay.ShouldBe(expected);
    }

    /// <summary>
    /// Proves the random part scales the delay from 0.8 (random 0) to 1.2 (random 1) times, and that the result is
    /// never more than the maximum.
    /// </summary>
    /// <param name="attempt">
    /// The retry, from 1.
    /// </param>
    /// <param name="random01">
    /// The random number.
    /// </param>
    /// <param name="expectedMilliseconds">
    /// The delay expected.
    /// </param>
    [Theory]
    [InlineData(1, 0.0, 400)]
    [InlineData(1, 1.0, 600)]
    [InlineData(4, 0.0, 3_200)]
    [InlineData(4, 1.0, 4_800)]
    [InlineData(5, 1.0, 9_600)]
    [InlineData(6, 0.0, 12_000)]
    [InlineData(6, 1.0, 15_000)]
    [InlineData(50, 0.0, 12_000)]
    [InlineData(50, 1.0, 15_000)]
    public void GetDelay_RandomBounds_StaysWithinTwentyPercentAndUnderMax(int attempt, double random01, int expectedMilliseconds)
    {
        // Arrange
        TimeSpan expected = TimeSpan.FromMilliseconds(expectedMilliseconds);

        // Act
        TimeSpan delay = ReconnectBackoff.GetDelay(attempt, s_initial, s_max, random01);

        // Assert
        delay.ShouldBe(expected);
    }

    /// <summary>
    /// Proves a maximum near the largest <see cref="TimeSpan"/> neither overflows the doubling nor the random part.
    /// </summary>
    [Fact]
    public void GetDelay_HugeMaximum_DoesNotOverflow()
    {
        // Arrange
        TimeSpan max = TimeSpan.MaxValue;

        // Act
        TimeSpan delay = ReconnectBackoff.GetDelay(int.MaxValue, TimeSpan.FromTicks(1), max, 1.0);

        // Assert
        delay.ShouldBe(max);
    }
}
