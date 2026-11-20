using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// Walks the topic filters of a validated UNSUBSCRIBE in wire order. The packet was validated whole when it was
/// decoded; over bytes that were not, the walk stops at the first filter that cannot be read.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal ref struct TopicFilterEnumerator
{
    /// <summary>
    /// The reader over the validated filters; it only moves forward.
    /// </summary>
    private MqttReader _reader;

    /// <summary>
    /// Creates an enumerator over validated filters.
    /// </summary>
    /// <param name="filters">
    /// The filters of an UNSUBSCRIBE that <see cref="UnsubscribeCodec.TryDecode"/> accepted.
    /// </param>
    public TopicFilterEnumerator(ReadOnlySpan<byte> filters) => _reader = new MqttReader(filters);

    /// <summary>
    /// The filter found by the last successful <see cref="MoveNext"/>, as UTF-8 without its length prefix.
    /// </summary>
    public ReadOnlySpan<byte> Current { get; private set; }

    /// <summary>
    /// Returns this enumerator, so the struct works in <c>foreach</c>.
    /// </summary>
    /// <returns>
    /// A copy of this enumerator.
    /// </returns>
    public readonly TopicFilterEnumerator GetEnumerator() => this;

    /// <summary>
    /// Moves to the next filter.
    /// </summary>
    /// <returns>
    /// <see langword="false"/> when there are no more filters, or when the next one runs past the end (only possible
    /// over bytes the decoder did not validate).
    /// </returns>
    public bool MoveNext()
    {
        if (!_reader.TryReadBinary(out ReadOnlySpan<byte> filter))
        {
            return false;
        }

        Current = filter;
        return true;
    }
}
