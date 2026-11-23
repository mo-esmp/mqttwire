using System.Buffers;
using MqttWire.Server;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the server's record of how long its last sweep took, which the idle-memory benchmark reports (design
/// 6.14).
/// </summary>
public sealed class SweepTimingTests
{
    /// <summary>
    /// Proves that a server that has not swept yet reports a last sweep of no length.
    /// </summary>
    [Fact]
    public void LastSweepTicks_NoSweepYet_IsZero()
    {
        // Arrange
        MqttWireServer server = new(new RecordingHandler(), new MqttWireServerOptions(), new RecordingLogger(),
            new SteppingClock(step: 7), ArrayPool<byte>.Shared);

        // Act
        long ticks = server.LastSweepTicks;

        // Assert
        ticks.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a sweep records how long it took in the timestamp units of the server's clock: the difference
    /// between the clock's reading at its start and at its end.
    /// </summary>
    [Fact]
    public void LastSweepTicks_AfterASweep_IsTheTimeBetweenItsStartAndItsEnd()
    {
        // Arrange: a clock that moves 7 units at every reading, so a sweep that reads it twice took 7.
        MqttWireServer server = new(new RecordingHandler(), new MqttWireServerOptions(), new RecordingLogger(),
            new SteppingClock(step: 7), ArrayPool<byte>.Shared);

        // Act
        server.Sweep();
        long first = server.LastSweepTicks;
        server.Sweep();
        long second = server.LastSweepTicks;

        // Assert: the last sweep's own length, not a running total.
        first.ShouldBe(7);
        second.ShouldBe(7);
    }

    /// <summary>
    /// A clock whose timestamp moves forward by a fixed step every time it is read.
    /// </summary>
    /// <param name="step">
    /// How far the timestamp moves at each reading.
    /// </param>
    private sealed class SteppingClock(long step) : TimeProvider
    {
        /// <summary>
        /// The last timestamp handed out.
        /// </summary>
        private long _timestamp;

        /// <summary>
        /// Gets the next timestamp: the last one plus the step.
        /// </summary>
        /// <returns>
        /// The timestamp.
        /// </returns>
        public override long GetTimestamp() => Interlocked.Add(ref _timestamp, step);
    }
}
