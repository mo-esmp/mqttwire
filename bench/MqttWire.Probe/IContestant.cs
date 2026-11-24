namespace MqttWire.Probe;

/// <summary>
/// One client in the probe, connected to the broker and subscribed at QoS 1 to its own echo topic.
/// </summary>
internal interface IContestant : IAsyncDisposable
{
    /// <summary>
    /// Gets the name of the client in the probe's table: <c>before</c>, <c>after</c> or <c>mqttnet</c>.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Publishes 64 bytes at QoS 1 to the echo topic, awaits and checks the PUBACK, and then waits for the broker's
    /// delivery of the same message back to the client.
    /// </summary>
    /// <returns>
    /// A task that completes when the message is back.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The publish failed, or the client's connection ended before the message was back.
    /// </exception>
    Task RoundTripAsync();

    /// <summary>
    /// Starts <see cref="Program.Burst"/> QoS 1 publishes to the echo topic without awaiting, awaits and checks them
    /// all, and then waits for the same number of messages back.
    /// </summary>
    /// <returns>
    /// A task that completes when every message is back.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// A publish failed, or the client's connection ended before every message was back.
    /// </exception>
    Task BurstAsync();
}
