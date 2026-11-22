using System.Threading.Channels;

namespace MqttWire.Interop.Tests;

/// <summary>
/// A message a client received, copied out of whatever buffer it arrived in.
/// </summary>
/// <typeparam name="TQoS">
/// The client's own type for a QoS.
/// </typeparam>
/// <param name="Topic">
/// The topic it arrived on.
/// </param>
/// <param name="Payload">
/// The payload as text.
/// </param>
/// <param name="QoS">
/// The QoS it was delivered at.
/// </param>
internal readonly record struct Received<TQoS>(string Topic, string Payload, TQoS QoS);

/// <summary>
/// The messages a client received, in order, for a test to wait on.
/// </summary>
/// <typeparam name="TQoS">
/// The client's own type for a QoS.
/// </typeparam>
internal sealed class Inbox<TQoS>
{
    /// <summary>
    /// The queue the client's callback writes and the test reads.
    /// </summary>
    private readonly Channel<Received<TQoS>> _messages = Channel.CreateUnbounded<Received<TQoS>>();

    /// <summary>
    /// Adds a message; called from the client's callback.
    /// </summary>
    /// <param name="message">
    /// The message.
    /// </param>
    public void Add(Received<TQoS> message) => _messages.Writer.TryWrite(message);

    /// <summary>
    /// Waits for the next message, failing the test after the fail-safe time.
    /// </summary>
    /// <returns>
    /// The next message in arrival order.
    /// </returns>
    /// <exception cref="TimeoutException">
    /// No message arrived within <see cref="ServerHost.FailSafe"/>.
    /// </exception>
    public async Task<Received<TQoS>> NextAsync() =>
        await _messages.Reader.ReadAsync().AsTask().WaitAsync(ServerHost.FailSafe).ConfigureAwait(false);
}
