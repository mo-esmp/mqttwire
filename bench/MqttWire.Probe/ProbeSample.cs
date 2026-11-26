namespace MqttWire.Probe;

/// <summary>
/// The result of one scenario run by one client: the time and the allocation per operation.
/// </summary>
/// <param name="Microseconds">
/// The microseconds per operation; per message for a burst.
/// </param>
/// <param name="Bytes">
/// The bytes allocated per operation, process-wide, so the broker's share is in it.
/// </param>
internal readonly record struct ProbeSample(double Microseconds, double Bytes);
