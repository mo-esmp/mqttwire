using System.Diagnostics.CodeAnalysis;

namespace MqttWire.Codec;

/// <summary>
/// A decoded PUBLISH packet: views over the bytes it was decoded from, so it is only valid while they are.
/// </summary>
internal readonly ref struct PublishPacket
{
    /// <summary>
    /// The quality of service of the delivery.
    /// </summary>
    public MqttQoS QoS { get; init; }

    /// <summary>
    /// Whether this is a re-delivery of an earlier attempt; the flag as the sender set it.
    /// </summary>
    public bool Dup { get; init; }

    /// <summary>
    /// Whether the message is to be retained.
    /// </summary>
    public bool Retain { get; init; }

    /// <summary>
    /// The topic name as UTF-8, checked to be well-formed and free of wildcards. It is empty only when
    /// <see cref="HasTopicAlias"/> is <see langword="true"/>.
    /// </summary>
    public ReadOnlySpan<byte> Topic { get; init; }

    /// <summary>
    /// The packet identifier; 0 at QoS 0, where the packet has none.
    /// </summary>
    public ushort PacketIdentifier { get; init; }

    /// <summary>
    /// The validated PUBLISH properties.
    /// </summary>
    public PropertyBlock Properties { get; init; }

    /// <summary>
    /// The application message, which may be empty.
    /// </summary>
    public ReadOnlySpan<byte> Payload { get; init; }

    /// <summary>
    /// Whether the properties hold a Topic Alias, found once while decoding. The decoder reports it and does not
    /// resolve it.
    /// </summary>
    public bool HasTopicAlias { get; init; }
}

/// <summary>
/// What is written into a PUBLISH. MqttWire never writes PUBLISH properties, so the property length is always 0.
/// </summary>
internal readonly ref struct PublishFields
{
    /// <summary>
    /// The topic name as UTF-8, encoded once by the caller. The encoder checks that it is not empty, holds no
    /// wildcard and fits 65,535 bytes; it does not check that the bytes are well-formed UTF-8 or free of U+0000.
    /// </summary>
    public ReadOnlySpan<byte> Topic { get; init; }

    /// <summary>
    /// The quality of service; the default is QoS 0.
    /// </summary>
    public MqttQoS QoS { get; init; }

    /// <summary>
    /// The retain flag.
    /// </summary>
    public bool Retain { get; init; }

    /// <summary>
    /// The DUP flag, written as given.
    /// </summary>
    public bool Dup { get; init; }

    /// <summary>
    /// The packet identifier: 0 at QoS 0, from 1 to 65,535 at QoS 1 and 2; the encoder refuses anything else.
    /// </summary>
    public ushort PacketIdentifier { get; init; }
}

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
    /// The buffer is not exactly one framed PUBLISH packet (a caller bug, see <see cref="PacketHeader.ReadExact"/>).
    /// </exception>
    public static MqttReasonCode TryDecode(ReadOnlySpan<byte> packet, PacketSource source, out PublishPacket publish)
    {
        publish = default;
        PacketHeader header = PacketHeader.ReadExact(packet, PacketType.Publish);
        MqttReader reader = new(packet[header.HeaderLength..]);
        MqttQoS qos = (MqttQoS)((header.Flags >> QosShift) & QosMask);

        if (!reader.TryReadUtf8String(out ReadOnlySpan<byte> topic))
        {
            return MqttReasonCode.MalformedPacket;
        }

        ushort identifier = 0;
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

        MqttReasonCode result = PropertyBlock.TryRead(ref reader, PropertyContext.Publish, out PropertyBlock properties);
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
        else if (!Topics.IsValidName(topic))
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
    /// Gets the size of everything before the payload: the fixed header, the topic, the packet identifier at QoS 1
    /// and 2, and the property length.
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
    /// Gets the size of the whole PUBLISH packet: <see cref="GetHeaderSize"/> plus the payload.
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
    /// the QoS is not 0, 1 or 2, the identifier is not 0 at QoS 0 and from 1 to 65,535 at QoS 1 and 2, the topic is
    /// empty, holds <c>+</c> or <c>#</c> or is over 65,535 bytes, the payload length is negative, or the remaining
    /// length would pass <see cref="VariableByteInteger.MaxValue"/>.
    /// </exception>
    public static int WriteHeader(Span<byte> destination, in PublishFields fields, int payloadLength)
    {
        int remainingLength = GetRemainingLength(fields, payloadLength, out int variableHeaderLength);
        if (destination.Length < PacketHeader.GetLength(remainingLength) + variableHeaderLength)
        {
            CodecThrow.DestinationTooSmall();
        }

        return WriteHeaderCore(destination, fields, remainingLength);
    }

    /// <summary>
    /// Writes a whole PUBLISH packet: the header and a copy of the payload. No properties are written.
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
            CodecThrow.DestinationTooSmall();
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
            ThrowInvalid("The QoS is not 0, 1 or 2.", "QoS");
        }

        bool hasIdentifier = fields.QoS != MqttQoS.AtMostOnce;
        if (hasIdentifier == (fields.PacketIdentifier == 0))
        {
            ThrowInvalid("The packet identifier must be 0 at QoS 0 and from 1 to 65,535 at QoS 1 and 2.", "PacketIdentifier");
        }

        if (payloadLength < 0)
        {
            ThrowInvalid("The payload length is negative.", nameof(payloadLength));
        }

        // Throws when the topic is over 65,535 bytes.
        int length = FieldSize.Utf8String(fields.Topic) + NoPropertiesSize;
        if (hasIdentifier)
        {
            length += IdentifierSize;
        }

        if (!Topics.IsValidName(fields.Topic))
        {
            ThrowInvalid("The topic is empty or holds a wildcard.", "Topic");
        }

        // Summed in a long: the payload alone may be as large as int.MaxValue.
        if (length + (long)payloadLength > VariableByteInteger.MaxValue)
        {
            ThrowInvalid("The packet is longer than the largest remaining length.", nameof(payloadLength));
        }

        variableHeaderLength = length;
        return length + payloadLength;
    }

    /// <summary>
    /// Throws for a caller bug; kept out of line so the success path stays small.
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
    private static void ThrowInvalid(string message, string paramName) => throw new ArgumentException(message, paramName);
}
