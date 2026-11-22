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
}
