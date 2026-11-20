using System.Diagnostics.CodeAnalysis;

namespace MqttWire.Codec;

/// <summary>
/// Throw helpers shared by the packet codecs. They are kept out of line so the success paths stay small.
/// </summary>
internal static class CodecThrow
{
    /// <summary>
    /// Throws because a destination cannot hold the packet an encoder was asked to write.
    /// </summary>
    /// <param name="paramName">
    /// The name of the caller's destination argument.
    /// </param>
    /// <exception cref="ArgumentException">
    /// Always.
    /// </exception>
    [DoesNotReturn]
    public static void DestinationTooSmall(string paramName)
        => throw new ArgumentException("The destination is smaller than the packet.", paramName);

    /// <summary>
    /// Throws because an encoder was given a value MQTT cannot carry.
    /// </summary>
    /// <param name="message">
    /// What is wrong.
    /// </param>
    /// <param name="paramName">
    /// The argument or field at fault.
    /// </param>
    /// <exception cref="ArgumentException">
    /// Always.
    /// </exception>
    [DoesNotReturn]
    public static void Invalid(string message, string paramName) => throw new ArgumentException(message, paramName);

    /// <summary>
    /// Throws because an encoder was given a number outside the range MQTT allows for it.
    /// </summary>
    /// <param name="message">
    /// What is wrong.
    /// </param>
    /// <param name="paramName">
    /// The argument or field at fault.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Always.
    /// </exception>
    [DoesNotReturn]
    public static void OutOfRange(string message, string paramName) => throw new ArgumentOutOfRangeException(paramName, message);
}
