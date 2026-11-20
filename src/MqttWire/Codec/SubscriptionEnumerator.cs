using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// Walks the subscriptions of a validated SUBSCRIBE in wire order. It has no error path: the packet was validated
/// whole when it was decoded.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal ref struct SubscriptionEnumerator
{
    /// <summary>
    /// The reader over the validated entries; it only moves forward.
    /// </summary>
    private MqttReader _reader;

    /// <summary>
    /// Creates an enumerator over validated entries.
    /// </summary>
    /// <param name="entries">
    /// The entries of a SUBSCRIBE that <see cref="SubscribeCodec.TryDecode"/> accepted.
    /// </param>
    public SubscriptionEnumerator(ReadOnlySpan<byte> entries) => _reader = new MqttReader(entries);

    /// <summary>
    /// The subscription found by the last successful <see cref="MoveNext"/>.
    /// </summary>
    public Subscription Current { get; private set; }

    /// <summary>
    /// Returns this enumerator, so the struct works in <c>foreach</c>.
    /// </summary>
    /// <returns>
    /// A copy of this enumerator.
    /// </returns>
    public readonly SubscriptionEnumerator GetEnumerator() => this;

    /// <summary>
    /// Moves to the next subscription.
    /// </summary>
    /// <returns>
    /// <see langword="false"/> when there are no more subscriptions.
    /// </returns>
    public bool MoveNext()
    {
        if (_reader.Remaining == 0)
        {
            return false;
        }

        _reader.TryReadBinary(out ReadOnlySpan<byte> filter);
        _reader.TryReadByte(out byte options);
        Current = new Subscription
        {
            TopicFilter = filter,
            MaximumQoS = (MqttQoS)(options & SubscribeCodec.QosMask),
            NoLocal = (options & SubscribeCodec.NoLocalFlag) != 0,
            RetainAsPublished = (options & SubscribeCodec.RetainAsPublishedFlag) != 0,
            RetainHandling = (byte)((options >> SubscribeCodec.RetainHandlingShift) & SubscribeCodec.RetainHandlingMask),
        };
        return true;
    }
}
