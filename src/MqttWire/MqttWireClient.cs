using MqttWire.Client;
using MqttWire.Codec;

namespace MqttWire;

/// <summary>
/// An MQTT 5 client over a WebSocket. One instance serves one connection.
/// </summary>
public sealed class MqttWireClient : IAsyncDisposable
{
    /// <summary>
    /// Creates a client. Nothing is connected until <c>ConnectAsync</c> is called.
    /// </summary>
    /// <param name="options">
    /// The options.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// The options, or a required option, are <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The URI is not an absolute <c>ws</c> or <c>wss</c> URI, or the client identifier, user name or password is
    /// something MQTT cannot carry: an empty client identifier, more than 65,535 UTF-8 bytes, U+0000 or an
    /// unpaired surrogate.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The keep-alive or the maximum incoming packet size is outside its range.
    /// </exception>
    public MqttWireClient(MqttWireClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        // Required members can still be null when the caller suppresses the compiler's warning.
        if (options.Uri is null || options.ClientId is null)
        {
            throw new ArgumentNullException(nameof(options), "The URI and the client identifier are required.");
        }

        if (!options.Uri.IsAbsoluteUri || (!string.Equals(options.Uri.Scheme, Uri.UriSchemeWs, StringComparison.Ordinal)
                && !string.Equals(options.Uri.Scheme, Uri.UriSchemeWss, StringComparison.Ordinal)))
        {
            throw new ArgumentException("The URI must be an absolute ws or wss URI.", nameof(options));
        }

        if (options.ClientId.Length == 0)
        {
            throw new ArgumentException("The client identifier must not be empty.", nameof(options));
        }

        CheckText(options.ClientId, "client identifier");
        if (options.UserName is not null)
        {
            CheckText(options.UserName, "user name");
        }

        if (options.Password is not null)
        {
            CheckText(options.Password, "password");
        }

        if (options.KeepAlive < TimeSpan.FromSeconds(1) || options.KeepAlive > TimeSpan.FromSeconds(ushort.MaxValue))
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.KeepAlive, "The keep-alive must be from 1 to 65,535 seconds.");
        }

        if (options.MaxIncomingPacketBytes is < ClientLimits.MinIncomingPacketBytes or > ClientLimits.MaxIncomingPacketBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.MaxIncomingPacketBytes, "The maximum incoming packet size must be from 4,096 to 268,435,460 bytes.");
        }

        Options = options;
        KeepAlive = TimeSpan.FromSeconds(Math.Ceiling(options.KeepAlive.TotalSeconds));
    }

    /// <summary>
    /// Gets the options the client was created with.
    /// </summary>
    internal MqttWireClientOptions Options { get; }

    /// <summary>
    /// Gets the keep-alive interval rounded up to whole seconds.
    /// </summary>
    internal TimeSpan KeepAlive { get; }

    /// <summary>
    /// Releases the client. Nothing is held yet.
    /// </summary>
    /// <returns>
    /// A completed task.
    /// </returns>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <summary>
    /// Checks that text can be carried by MQTT as a UTF-8 string.
    /// </summary>
    /// <param name="value">
    /// The text.
    /// </param>
    /// <param name="what">
    /// What the text is, for the message.
    /// </param>
    /// <exception cref="ArgumentException">
    /// The text is over 65,535 UTF-8 bytes, contains U+0000 or has an unpaired surrogate.
    /// </exception>
    private static void CheckText(string value, string what)
    {
        try
        {
            FieldSize.Utf8String(value);
        }
        catch (ArgumentException ex)
        {
            throw new ArgumentException($"The {what} cannot be sent: {ex.Message}", nameof(value), ex);
        }
    }
}
