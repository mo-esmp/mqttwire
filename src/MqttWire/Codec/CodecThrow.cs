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
    /// <exception cref="ArgumentException">
    /// Always.
    /// </exception>
    [DoesNotReturn]
    public static void DestinationTooSmall()
        => throw new ArgumentException("The destination is smaller than the packet.", "destination");
}
