using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Unicode;

namespace MqttWire.Codec;

/// <summary>
/// Decodes and encodes the MQTT 5 PUBLISH packet (MQTT 5, 3.3).
/// </summary>
internal static class PublishCodec
{
    /// <summary>
    /// The DUP bit of the flags.
    /// </summary>
    private const int DupFlag = 0b1000;

    /// <summary>
    /// The position of the QoS bits in the flags.
    /// </summary>
    private const int QosShift = 1;

    /// <summary>
    /// Selects the QoS bits once shifted down.
    /// </summary>
    private const int QosMask = 0b11;

    /// <summary>
    /// The retain bit of the flags.
    /// </summary>
    private const int RetainFlag = 0b0001;

    /// <summary>
    /// The size of the packet identifier.
    /// </summary>
    private const int IdentifierSize = sizeof(ushort);

    /// <summary>
    /// The size of the property length written by the encoder, which writes no properties.
    /// </summary>
    private const int NoPropertiesSize = 1;

    /// <summary>
    /// Decodes one PUBLISH packet. Nothing allocates; the topic, properties and payload are views of the input.
    /// </summary>
    /// <param name="packet">
    /// Exactly one framed PUBLISH packet, fixed header included.
    /// </param>
    /// <param name="source">
    /// Who sent it; a Subscription Identifier is only allowed from a server.
    /// </param>
    /// <param name="publish">
    /// The decoded packet when the result is success; otherwise the default value.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>; <see cref="MqttReasonCode.ProtocolError"/> for a packet identifier of 0
    /// at QoS 1 or 2, a forbidden property value, a Subscription Identifier from a client, or an empty topic without
    /// a Topic Alias; <see cref="MqttReasonCode.TopicNameInvalid"/> for a topic with <c>+</c> or <c>#</c>;
    /// <see cref="MqttReasonCode.MalformedPacket"/> for any other fault in the body.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The buffer is not exactly one framed PUBLISH packet (a caller bug, see <see cref="PacketHeader.ReadExact(ReadOnlySpan{byte}, PacketType)"/>).
    /// </exception>
    public static MqttReasonCode TryDecode(ReadOnlySpan<byte> packet, PacketSource source, out PublishPacket publish)
    {
        publish = default;
        PacketHeader header = PacketHeader.ReadExact(packet, PacketType.Publish);
        MqttReader reader = new(packet[header.HeaderLength..]);
        MqttQoS qos = (MqttQoS)((header.Flags >> QosShift) & QosMask);

        MqttReasonCode result = TryReadTopicAndIdentifier(ref reader, qos, out ReadOnlySpan<byte> topic, out int found, out ushort identifier);
        if (result != MqttReasonCode.Success)
        {
            return result;
        }

        result = PropertyBlock.TryRead(ref reader, PropertyContext.Publish, out PropertyBlock properties);
        if (result != MqttReasonCode.Success)
        {
            return result;
        }

        if (source == PacketSource.Client && properties.Contains(PropertyId.SubscriptionIdentifier))
        {
            return MqttReasonCode.ProtocolError;
        }

        bool hasTopicAlias = properties.Contains(PropertyId.TopicAlias);
        if (topic.IsEmpty)
        {
            if (!hasTopicAlias)
            {
                return MqttReasonCode.ProtocolError;
            }
        }
        else if (found >= 0)
        {
            return MqttReasonCode.TopicNameInvalid;
        }

        publish = new PublishPacket
        {
            QoS = qos,
            Dup = (header.Flags & DupFlag) != 0,
            Retain = (header.Flags & RetainFlag) != 0,
            Topic = topic,
            PacketIdentifier = identifier,
            Properties = properties,
            Payload = reader.ReadToEnd(),
            HasTopicAlias = hasTopicAlias,
        };
        return MqttReasonCode.Success;
    }

    /// <summary>
    /// Reads the part of the variable header ahead of the properties: the topic name, checked to be well-formed
    /// UTF-8 without U+0000, and the packet identifier at QoS 1 and 2. A wildcard is not a fault here: its position
    /// is handed back so the caller reports it only after its later checks.
    /// </summary>
    /// <param name="reader">
    /// The reader, positioned at the topic; it moves past the topic and the identifier. After a failure its
    /// position is unspecified.
    /// </param>
    /// <param name="qos">
    /// The QoS from the fixed header, which decides whether a packet identifier follows the topic.
    /// </param>
    /// <param name="topic">
    /// The topic name as UTF-8, a view of the input; unspecified after a failure.
    /// </param>
    /// <param name="found">
    /// The index of the first wildcard in the topic, or -1 when it has none; unspecified after a failure.
    /// </param>
    /// <param name="identifier">
    /// The packet identifier; 0 at QoS 0 and after a failure.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>; <see cref="MqttReasonCode.MalformedPacket"/> for a topic that is cut
    /// short, not well-formed UTF-8 or holds U+0000, or a missing packet identifier;
    /// <see cref="MqttReasonCode.ProtocolError"/> for a packet identifier of 0 at QoS 1 or 2.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static MqttReasonCode TryReadTopicAndIdentifier(
        scoped ref MqttReader reader, MqttQoS qos, out ReadOnlySpan<byte> topic, out int found, out ushort identifier)
    {
        found = -1;
        identifier = 0;

        // The topic is read as binary and checked here, so it is scanned twice rather than three times: once for
        // well-formed UTF-8, once for U+0000 and the wildcards together.
        if (!reader.TryReadBinary(out topic) || !Utf8.IsValid(topic))
        {
            return MqttReasonCode.MalformedPacket;
        }

        // U+0000 anywhere makes the packet malformed at once; a wildcard is reported only after the later checks.
        found = Topics.IndexOfWildcardOrNull(topic);
        if (found >= 0 && topic[found..].Contains((byte)0))
        {
            return MqttReasonCode.MalformedPacket;
        }

        if (qos != MqttQoS.AtMostOnce)
        {
            if (!reader.TryReadUInt16(out identifier))
            {
                return MqttReasonCode.MalformedPacket;
            }

            if (identifier == 0)
            {
                return MqttReasonCode.ProtocolError;
            }
        }

        return MqttReasonCode.Success;
    }

    /// <summary>
    /// Gets the size of everything before the payload: the fixed header, the topic, the packet identifier at QoS 1
    /// and 2, and the property length. The topic must be well-formed UTF-8, which is not checked (see
    /// <see cref="PublishFields.Topic"/>).
    /// </summary>
    /// <param name="fields">
    /// The fields to encode.
    /// </param>
    /// <param name="payloadLength">
    /// The payload size, which sets the remaining length in the fixed header.
    /// </param>
    /// <returns>
    /// The size in bytes.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The fields or the payload length cannot be encoded: see <see cref="WriteHeader"/>.
    /// </exception>
    public static int GetHeaderSize(in PublishFields fields, int payloadLength)
    {
        int remainingLength = GetRemainingLength(fields, payloadLength, out int variableHeaderLength);
        return PacketHeader.GetLength(remainingLength) + variableHeaderLength;
    }

    /// <summary>
    /// Gets the size of the whole PUBLISH packet: <see cref="GetHeaderSize"/> plus the payload. The topic must be
    /// well-formed UTF-8, which is not checked (see <see cref="PublishFields.Topic"/>).
    /// </summary>
    /// <param name="fields">
    /// The fields to encode.
    /// </param>
    /// <param name="payloadLength">
    /// The payload size.
    /// </param>
    /// <returns>
    /// The size in bytes.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The fields or the payload length cannot be encoded: see <see cref="WriteHeader"/>.
    /// </exception>
    public static int GetSize(in PublishFields fields, int payloadLength)
    {
        int remainingLength = GetRemainingLength(fields, payloadLength, out _);
        return PacketHeader.GetLength(remainingLength) + remainingLength;
    }

    /// <summary>
    /// Writes everything before the payload, for a server that writes a large payload from the host memory
    /// itself. The remaining length counts <paramref name="payloadLength"/> bytes that this call does not write.
    /// The topic must be well-formed UTF-8, which is not checked (see <see cref="PublishFields.Topic"/>).
    /// </summary>
    /// <param name="destination">
    /// Where to write; it must hold at least <see cref="GetHeaderSize"/> bytes.
    /// </param>
    /// <param name="fields">
    /// The fields to encode.
    /// </param>
    /// <param name="payloadLength">
    /// The size of the payload the caller will write after the header.
    /// </param>
    /// <returns>
    /// The number of bytes written, equal to <see cref="GetHeaderSize"/>.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// A caller bug, checked before the first byte is written so nothing is written: the destination is too small,
    /// the QoS is not 0, 1 or 2, the identifier is not 0 at QoS 0 and from 1 to 65,535 at QoS 1 and 2, DUP is set at
    /// QoS 0, the topic is empty, holds <c>+</c>, <c>#</c> or U+0000 or is over 65,535 bytes, the payload length is
    /// negative, or the remaining length would pass <see cref="VariableByteInteger.MaxValue"/>.
    /// </exception>
    public static int WriteHeader(Span<byte> destination, in PublishFields fields, int payloadLength)
    {
        int remainingLength = GetRemainingLength(fields, payloadLength, out int variableHeaderLength);
        if (destination.Length < PacketHeader.GetLength(remainingLength) + variableHeaderLength)
        {
            CodecThrow.DestinationTooSmall(nameof(destination));
        }

        return WriteHeaderCore(destination, fields, remainingLength);
    }

    /// <summary>
    /// Writes a whole PUBLISH packet: the header and a copy of the payload. No properties are written. The topic
    /// must be well-formed UTF-8, which is not checked (see <see cref="PublishFields.Topic"/>).
    /// </summary>
    /// <param name="destination">
    /// Where to write; it must hold at least <see cref="GetSize"/> bytes.
    /// </param>
    /// <param name="fields">
    /// The fields to encode.
    /// </param>
    /// <param name="payload">
    /// The application message.
    /// </param>
    /// <returns>
    /// The number of bytes written, equal to <see cref="GetSize"/>.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// A caller bug, as for <see cref="WriteHeader"/>; nothing is written.
    /// </exception>
    public static int Write(Span<byte> destination, in PublishFields fields, ReadOnlySpan<byte> payload)
    {
        int remainingLength = GetRemainingLength(fields, payload.Length, out _);
        if (destination.Length < PacketHeader.GetLength(remainingLength) + remainingLength)
        {
            CodecThrow.DestinationTooSmall(nameof(destination));
        }

        int headerLength = WriteHeaderCore(destination, fields, remainingLength);
        payload.CopyTo(destination[headerLength..]);
        return headerLength + payload.Length;
    }

    /// <summary>
    /// Writes the header of already validated fields into a destination already known to be large enough.
    /// </summary>
    /// <param name="destination">
    /// Where to write.
    /// </param>
    /// <param name="fields">
    /// The validated fields.
    /// </param>
    /// <param name="remainingLength">
    /// The remaining length from <see cref="GetRemainingLength"/>.
    /// </param>
    /// <returns>
    /// The number of bytes written.
    /// </returns>
    private static int WriteHeaderCore(Span<byte> destination, in PublishFields fields, int remainingLength)
    {
        int flags = ((int)fields.QoS << QosShift) | (fields.Dup ? DupFlag : 0) | (fields.Retain ? RetainFlag : 0);
        int fixedHeaderLength = PacketHeader.Write(destination, PacketType.Publish, (byte)flags, remainingLength);
        MqttWriter writer = new(destination[fixedHeaderLength..]);

        writer.WriteUtf8String(fields.Topic);
        if (fields.QoS != MqttQoS.AtMostOnce)
        {
            writer.WriteUInt16(fields.PacketIdentifier);
        }

        writer.WriteVariableByteInteger(0);
        return fixedHeaderLength + writer.Written;
    }

    /// <summary>
    /// Checks the fields and measures the packet once, for the sizes and the writers to share.
    /// </summary>
    /// <param name="fields">
    /// The fields to check.
    /// </param>
    /// <param name="payloadLength">
    /// The payload size.
    /// </param>
    /// <param name="variableHeaderLength">
    /// The size of the variable header: topic, identifier and property length.
    /// </param>
    /// <returns>
    /// The remaining length: the variable header plus the payload.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// See <see cref="WriteHeader"/>.
    /// </exception>
    private static int GetRemainingLength(in PublishFields fields, int payloadLength, out int variableHeaderLength)
    {
        if (fields.QoS > MqttQoS.ExactlyOnce)
        {
            CodecThrow.Invalid("The QoS is not 0, 1 or 2.", "QoS");
        }

        bool hasIdentifier = fields.QoS != MqttQoS.AtMostOnce;
        if (hasIdentifier == (fields.PacketIdentifier == 0))
        {
            CodecThrow.Invalid("The packet identifier must be 0 at QoS 0 and from 1 to 65,535 at QoS 1 and 2.", "PacketIdentifier");
        }

        // A sender must clear DUP at QoS 0 (MQTT-3.3.1-2); the decoder accepts it on input, the encoder does not send it.
        if (fields.Dup && !hasIdentifier)
        {
            CodecThrow.Invalid("DUP cannot be set at QoS 0.", "Dup");
        }

        if (payloadLength < 0)
        {
            CodecThrow.Invalid("The payload length is negative.", nameof(payloadLength));
        }

        // Throws when the topic is over 65,535 bytes.
        int length = FieldSize.Utf8String(fields.Topic) + NoPropertiesSize;
        if (hasIdentifier)
        {
            length += IdentifierSize;
        }

        // One scan for the wildcards and U+0000; well-formed UTF-8 is the caller's duty, asserted in debug builds.
        if (!Topics.IsValidName(fields.Topic))
        {
            CodecThrow.Invalid("The topic is empty or holds a wildcard or U+0000.", "Topic");
        }

        Debug.Assert(Utf8.IsValid(fields.Topic), "The topic is not well-formed UTF-8.");

        // Summed in a long: the payload alone may be as large as int.MaxValue.
        if (length + (long)payloadLength > VariableByteInteger.MaxValue)
        {
            CodecThrow.Invalid("The packet is longer than the largest remaining length.", nameof(payloadLength));
        }

        variableHeaderLength = length;
        return length + payloadLength;
    }
}
