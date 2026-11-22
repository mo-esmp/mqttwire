using System.Text;

namespace MqttWire.Server.Internal;

/// <summary>
/// Shapes the reason strings a host hands to the server so that they fit in a packet.
/// </summary>
internal static class ReasonStrings
{
    /// <summary>
    /// Cuts a reason string to at most <see cref="ServerLimits.MaxReasonStringBytes"/> UTF-8 bytes, at a character
    /// boundary: a surrogate pair is kept whole or dropped whole. Allocates only when the string has to be cut.
    /// </summary>
    /// <param name="reasonString">
    /// The reason string; may be <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see langword="null"/> for <see langword="null"/>; the same instance when it already fits; otherwise the
    /// longest prefix that fits.
    /// </returns>
    public static string? Cut(string? reasonString)
    {
        if (reasonString is null || reasonString.Length * 3 <= ServerLimits.MaxReasonStringBytes)
        {
            return reasonString;
        }

        int bytes = 0;
        int index = 0;
        while (index < reasonString.Length)
        {
            int chars = char.IsHighSurrogate(c: reasonString[index]) && index + 1 < reasonString.Length
                && char.IsLowSurrogate(c: reasonString[index + 1]) ? 2 : 1;
            int size = Encoding.UTF8.GetByteCount(chars: reasonString.AsSpan(start: index, length: chars));
            if (bytes + size > ServerLimits.MaxReasonStringBytes)
            {
                return reasonString[..index];
            }

            bytes += size;
            index += chars;
        }

        return reasonString;
    }

    /// <summary>
    /// Checks, where the host hands a reason string over, that a packet can carry it: no U+0000 and no unpaired
    /// surrogate, the two things MQTT's UTF-8 strings forbid that a .NET string can hold. Its length is not checked:
    /// a string too long is cut, never refused. Allocates nothing.
    /// </summary>
    /// <param name="reasonString">
    /// The reason string; <see langword="null"/> passes.
    /// </param>
    /// <param name="paramName">
    /// The name of the host's argument, for the exception.
    /// </param>
    /// <exception cref="ArgumentException">
    /// The reason string holds U+0000 or an unpaired surrogate: the host's bug.
    /// </exception>
    public static void ThrowIfNotEncodable(string? reasonString, string paramName)
    {
        if (reasonString is null)
        {
            return;
        }

        if (reasonString.Contains('\0', StringComparison.Ordinal))
        {
            throw new ArgumentException(message: "The reason string contains U+0000, which MQTT does not allow.",
                paramName: paramName);
        }

        ReadOnlySpan<char> rest = reasonString;
        int index = rest.IndexOfAnyInRange(lowInclusive: '\uD800', highInclusive: '\uDFFF');
        while (index >= 0)
        {
            rest = rest[index..];
            if (rest.Length < 2 || !char.IsHighSurrogate(c: rest[0]) || !char.IsLowSurrogate(c: rest[1]))
            {
                throw new ArgumentException(
                    message: "The reason string has an unpaired surrogate, which UTF-8 cannot carry.",
                    paramName: paramName);
            }

            rest = rest[2..];
            index = rest.IndexOfAnyInRange(lowInclusive: '\uD800', highInclusive: '\uDFFF');
        }
    }
}
