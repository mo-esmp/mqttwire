using Microsoft.Extensions.Options;

namespace MqttWire.Server.Internal;

/// <summary>
/// Checks <see cref="MqttWireServerOptions"/> and reports every invalid value in one result.
/// </summary>
internal sealed class MqttWireServerOptionsValidator : IValidateOptions<MqttWireServerOptions>
{
    /// <summary>
    /// The largest payload limit the options accept, in bytes.
    /// </summary>
    private const int MaxPayloadLimit = 16_777_216;

    /// <summary>
    /// The smallest byte limit of an outgoing queue the options accept.
    /// </summary>
    private const int MinOutgoingBytes = 65_536;

    /// <summary>
    /// Checks the options.
    /// </summary>
    /// <param name="name">The name of the options instance; not used.</param>
    /// <param name="options">The options to check.</param>
    /// <returns>
    /// A successful result when every value is valid; otherwise a failed result with one failure for each invalid
    /// value.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    public ValidateOptionsResult Validate(string? name, MqttWireServerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        List<string> failures = [];
        if (options.MaxQoS is not (MqttQoS.AtLeastOnce or MqttQoS.ExactlyOnce))
        {
            failures.Add("MaxQoS must be AtLeastOnce or ExactlyOnce.");
        }

        if (options.MaxPublishPayloadBytes is < 1 or > MaxPayloadLimit)
        {
            failures.Add("MaxPublishPayloadBytes must be from 1 to 16,777,216.");
        }

        if (options.MaxOutgoingMessages < 1)
        {
            failures.Add("MaxOutgoingMessages must be at least 1.");
        }

        if (options.MaxOutgoingBytes < MinOutgoingBytes)
        {
            failures.Add("MaxOutgoingBytes must be at least 65,536.");
        }

        return failures.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
    }
}
