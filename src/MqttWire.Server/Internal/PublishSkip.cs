using System.Runtime.InteropServices;
using MqttWire.Codec;

namespace MqttWire.Server.Internal;

/// <summary>
/// The skip of a PUBLISH too large to be read whole (design 6.6, "Reading, with no buffer while idle"): of the
/// packet's variable header it keeps only what it needs to find the packet identifier, the topic's length and the
/// identifier itself, and it passes over everything else as it arrives, across as many reads as it takes, until
/// exactly the packet's length has been consumed. It is a mutable struct held in a field of the connection and in
/// use only while a packet is being skipped; only the code that reads uses it, so it is not thread-safe. The default
/// value is the inactive skip.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal struct PublishSkip
{
    /// <summary>
    /// The size of the topic's length prefix, and of the packet identifier.
    /// </summary>
    private const int FieldSize = sizeof(ushort);

    /// <summary>
    /// The position of the QoS bits in the flags of a PUBLISH.
    /// </summary>
    private const int QoSShift = 1;

    /// <summary>
    /// Selects the QoS bits once shifted down.
    /// </summary>
    private const int QoSMask = 0b11;

    /// <summary>
    /// The packet's Remaining Length: the bytes of the variable header and the payload, which are what is skipped;
    /// 0 when no packet is being skipped.
    /// </summary>
    private int _length;

    /// <summary>
    /// How many of the <see cref="_length"/> bytes have been consumed.
    /// </summary>
    private int _consumed;

    /// <summary>
    /// The topic's length, as far as its two bytes have been read.
    /// </summary>
    private ushort _topicLength;

    /// <summary>
    /// The packet identifier, as far as its two bytes have been read; 0 at QoS 0.
    /// </summary>
    private ushort _identifier;

    /// <summary>
    /// Gets a value indicating whether a packet is being skipped: from <see cref="Start"/> until the struct is reset
    /// to its default value.
    /// </summary>
    public readonly bool IsActive => _length != 0;

    /// <summary>
    /// Gets a value indicating whether exactly the packet's length has been consumed.
    /// </summary>
    public readonly bool IsComplete => _consumed == _length;

    /// <summary>
    /// Gets the QoS of the packet, from the flags of its fixed header.
    /// </summary>
    public MqttQoS QoS { readonly get; private set; }

    /// <summary>
    /// Gets the packet identifier; meaningful only once the packet is complete, and 0 at QoS 0.
    /// </summary>
    public readonly ushort PacketIdentifier => _identifier;

    /// <summary>
    /// Starts the skip of a packet whose fixed header has been read and consumed; the next byte fed is the first
    /// byte of its variable header. The header's flags were already judged valid by <see cref="PacketHeader.TryRead"/>.
    /// </summary>
    /// <param name="header">
    /// The PUBLISH's fixed header; its Remaining Length is above 0.
    /// </param>
    public void Start(in PacketHeader header)
    {
        _length = header.RemainingLength;
        _consumed = 0;
        _topicLength = 0;
        _identifier = 0;
        QoS = GetQoS(header);
    }

    /// <summary>
    /// Reads the QoS of a PUBLISH from the flags of its fixed header.
    /// </summary>
    /// <param name="header">
    /// The PUBLISH's fixed header; <see cref="PacketHeader.TryRead"/> has refused the flags that carry QoS 3.
    /// </param>
    /// <returns>
    /// The QoS.
    /// </returns>
    public static MqttQoS GetQoS(in PacketHeader header) => (MqttQoS)((header.Flags >> QoSShift) & QoSMask);

    /// <summary>
    /// Consumes bytes of the packet, up to what the packet still has: reads the topic's length and the packet
    /// identifier from them and passes over the rest. Nothing is copied or kept.
    /// </summary>
    /// <param name="bytes">
    /// The bytes read; those beyond the packet's end are not consumed and belong to the next packet.
    /// </param>
    /// <param name="error">
    /// <see cref="MqttReasonCode.Success"/>; <see cref="MqttReasonCode.MalformedPacket"/> when the topic's length
    /// runs past the packet, the packet ends before its identifier, or it ends where its property length should
    /// start (right after the topic at QoS 0, right after the identifier at QoS 1 and 2); <see cref="MqttReasonCode.ProtocolError"/>
    /// for a packet identifier of 0 at QoS 1 or 2, the code <see cref="PublishCodec"/> gives it. After an error the
    /// skip is in no meaningful state and the connection is to end.
    /// </param>
    /// <returns>
    /// The number of bytes consumed: all of <paramref name="bytes"/> up to the packet's end, or, after an error, as
    /// far as the byte that showed it.
    /// </returns>
    public int Feed(ReadOnlySpan<byte> bytes, out MqttReasonCode error)
    {
        error = MqttReasonCode.Success;
        int take = Math.Min(bytes.Length, _length - _consumed);
        int index = 0;
        while (index < take)
        {
            if (_consumed < FieldSize)
            {
                _topicLength = (ushort)((_topicLength << 8) | bytes[index]);
                index++;
                _consumed++;
                // At QoS 0 the property length must follow the topic; at QoS 1 and 2 the identifier must, and is
                // read before the property length is missed, since the codec judges an identifier of 0 first.
                if (_consumed == FieldSize
                    && (IdentifierEnd > _length || (QoS == MqttQoS.AtMostOnce && IdentifierEnd == _length)))
                {
                    error = MqttReasonCode.MalformedPacket;
                    return index;
                }

                continue;
            }

            int identifierStart = FieldSize + _topicLength;
            if (QoS != MqttQoS.AtMostOnce && _consumed >= identifierStart && _consumed < IdentifierEnd)
            {
                _identifier = (ushort)((_identifier << 8) | bytes[index]);
                index++;
                _consumed++;
                if (_consumed == IdentifierEnd && (_identifier == 0 || _consumed == _length))
                {
                    // An identifier of 0 is a protocol error; a packet that ends with its identifier has no property
                    // length, and is malformed, as PublishCodec judges both in the first band.
                    error = _identifier == 0 ? MqttReasonCode.ProtocolError : MqttReasonCode.MalformedPacket;
                    return index;
                }

                continue;
            }

            // Passed over up to the next field the skip reads, or to the packet's end.
            int boundary = QoS != MqttQoS.AtMostOnce && _consumed < identifierStart ? identifierStart : _length;
            int skipped = Math.Min(take - index, boundary - _consumed);
            index += skipped;
            _consumed += skipped;
        }

        return take;
    }

    /// <summary>
    /// Gets where the packet identifier ends in the variable header, or the topic at QoS 0; valid once the topic's
    /// length has been read.
    /// </summary>
    private readonly int IdentifierEnd =>
        FieldSize + _topicLength + (QoS == MqttQoS.AtMostOnce ? 0 : FieldSize);
}
