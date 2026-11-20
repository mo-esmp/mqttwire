namespace MqttWire.Codec;

/// <summary>
/// The rules for topic names and topic filters (MQTT 5, 4.7). Both take UTF-8 bytes. The characters the rules look
/// for are ASCII, which never occur inside a multi-byte UTF-8 sequence, so scanning bytes is correct.
/// </summary>
internal static class Topics
{
    /// <summary>
    /// The single-level wildcard.
    /// </summary>
    private const byte SingleLevel = (byte)'+';

    /// <summary>
    /// The multi-level wildcard.
    /// </summary>
    private const byte MultiLevel = (byte)'#';

    /// <summary>
    /// The level separator.
    /// </summary>
    private const byte Separator = (byte)'/';

    /// <summary>
    /// U+0000, which no MQTT string may hold.
    /// </summary>
    private const byte Null = 0;

    /// <summary>
    /// The start of a shared subscription filter.
    /// </summary>
    private static ReadOnlySpan<byte> SharePrefix => "$share/"u8;

    /// <summary>
    /// Tells whether bytes are a valid topic name, the topic of a PUBLISH.
    /// </summary>
    /// <param name="utf8">
    /// The topic name; its UTF-8 well-formedness is not checked here.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the name is empty or contains <c>+</c>, <c>#</c> or U+0000, all looked for in one
    /// scan.
    /// </returns>
    public static bool IsValidName(ReadOnlySpan<byte> utf8)
        => !utf8.IsEmpty && !utf8.ContainsAny(SingleLevel, MultiLevel, Null);

    /// <summary>
    /// Finds the first wildcard or U+0000 in a topic name, so a decoder tells both faults apart in one scan.
    /// </summary>
    /// <param name="utf8">
    /// The topic name.
    /// </param>
    /// <returns>
    /// The index of the first <c>+</c>, <c>#</c> or U+0000; -1 when there is none.
    /// </returns>
    public static int IndexOfWildcardOrNull(ReadOnlySpan<byte> utf8) => utf8.IndexOfAny(SingleLevel, MultiLevel, Null);

    /// <summary>
    /// Tells whether a topic filter is a shared subscription, one that starts with <c>$share/</c>.
    /// </summary>
    /// <param name="utf8">
    /// The topic filter; whether the rest of it is valid is not checked here.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the filter starts with <c>$share/</c>.
    /// </returns>
    public static bool IsShared(ReadOnlySpan<byte> utf8) => utf8.StartsWith(SharePrefix);

    /// <summary>
    /// Tells whether bytes are a valid topic filter, the filter of a SUBSCRIBE or UNSUBSCRIBE.
    /// </summary>
    /// <param name="utf8">
    /// The topic filter; its UTF-8 well-formedness is not checked here.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the filter is empty, when <c>#</c> is not a whole last level, when <c>+</c> is not
    /// a whole level, or, for <c>$share/{name}/{filter}</c>, when the share name is empty or holds <c>+</c>, <c>#</c>
    /// or <c>/</c>, or the filter after it is empty or invalid.
    /// </returns>
    public static bool IsValidFilter(ReadOnlySpan<byte> utf8)
    {
        if (!IsShared(utf8))
        {
            return IsValidPlainFilter(utf8);
        }

        ReadOnlySpan<byte> rest = utf8[SharePrefix.Length..];
        int end = rest.IndexOf(Separator);
        if (end <= 0 || rest[..end].ContainsAny(SingleLevel, MultiLevel))
        {
            return false;
        }

        return IsValidPlainFilter(rest[(end + 1)..]);
    }

    /// <summary>
    /// Checks a filter without the shared subscription prefix, jumping from one wildcard to the next.
    /// </summary>
    /// <param name="filter">
    /// The filter.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when it is empty or a wildcard is misplaced.
    /// </returns>
    private static bool IsValidPlainFilter(ReadOnlySpan<byte> filter)
    {
        if (filter.IsEmpty)
        {
            return false;
        }

        int offset = 0;
        while (true)
        {
            int found = filter[offset..].IndexOfAny(SingleLevel, MultiLevel);
            if (found < 0)
            {
                return true;
            }

            int position = offset + found;
            if (position > 0 && filter[position - 1] != Separator)
            {
                return false;
            }

            if (filter[position] == MultiLevel)
            {
                return position == filter.Length - 1;
            }

            if (position + 1 < filter.Length && filter[position + 1] != Separator)
            {
                return false;
            }

            offset = position + 1;
        }
    }
}
