using System.Diagnostics;
using MqttWire.Client;
using MqttWire.Codec;

namespace MqttWire;

/// <content>
/// Publishing: <see cref="PublishAsync"/>, and the PUBACK, PUBREC and PUBCOMP that end its exchanges.
/// </content>
public sealed partial class MqttWireClient
{
    /// <summary>
    /// The longest topic, in UTF-8 bytes, that is encoded on the stack; a longer one is encoded into a rented
    /// buffer.
    /// </summary>
    private const int StackTopicBytes = 256;

    /// <summary>
    /// The size of a PUBLISH's packet identifier, present at QoS 1 and 2.
    /// </summary>
    private const int PacketIdentifierBytes = 2;

    /// <summary>
    /// The size of an empty property block: its one-byte length, 0.
    /// </summary>
    private const int NoPropertiesBytes = 1;

    /// <summary>
    /// The lowest reason code that reports a failure.
    /// </summary>
    private const byte FirstFailureCode = 0x80;

    /// <summary>
    /// Publishes a message. The topic and payload are checked and encoded before this returns, so the caller may
    /// reuse the payload at once, and the message goes out after every message of an earlier call.
    /// </summary>
    /// <param name="topic">
    /// The topic name.
    /// </param>
    /// <param name="payload">
    /// The payload; copied before this returns.
    /// </param>
    /// <param name="qos">
    /// The quality of service.
    /// </param>
    /// <returns>
    /// The outcome, which never throws. At QoS 0 it is <see cref="MqttRequestStatus.Completed"/> with success at
    /// once, when the message is handed to the send path. At QoS 1 it completes on the server's PUBACK, with its
    /// reason code; at QoS 2 on the PUBCOMP, or on a PUBREC with a reason code of 0x80 or more, with that code. A
    /// code of 0x80 or more means the server refused the message (<see cref="MqttRequestResult.IsSuccess"/> is
    /// <see langword="false"/>). It fails at once and nothing is sent with
    /// <see cref="MqttRequestStatus.NotConnected"/>, <see cref="MqttRequestStatus.QoSNotSupported"/> (above the
    /// server's Maximum QoS), <see cref="MqttRequestStatus.PacketTooLarge"/> (over the server's Maximum Packet
    /// Size), <see cref="MqttRequestStatus.ReceiveMaximumReached"/> (as many QoS 1 and 2 publishes in flight as the
    /// server's Receive Maximum allows, and never more than 1,024) or
    /// <see cref="MqttRequestStatus.NoFreePacketIdentifier"/>. A QoS 1 or 2 publish still in flight when the
    /// connection ends completes with <see cref="MqttRequestStatus.ConnectionLost"/>: the server may or may not
    /// have it, and it is not sent again. The task must be awaited at most once; a caller that does not care about
    /// the acknowledgement need not await it.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="topic"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topic"/> is empty, holds <c>+</c>, <c>#</c> or U+0000, has an unpaired surrogate, or is over
    /// 65,535 UTF-8 bytes. Nothing is sent and no packet identifier is used.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="qos"/> is above 2. Nothing is sent and no packet identifier is used.
    /// </exception>
    public ValueTask<MqttRequestResult> PublishAsync(string topic, ReadOnlyMemory<byte> payload, MqttQoS qos)
    {
        ArgumentNullException.ThrowIfNull(topic);
        if (qos > MqttQoS.ExactlyOnce)
        {
            // Not ThrowIfGreaterThan: an enum is not IComparable<T>, and a cast would name "(byte)qos".
            throw new ArgumentOutOfRangeException(nameof(qos), qos, "The QoS must be 0, 1 or 2.");
        }

        int topicLength = MeasureTopic(topic);
        byte[]? rentedTopic = topicLength <= StackTopicBytes ? null : _pool.Rent(topicLength);
        Span<byte> utf8Topic = rentedTopic is null ? stackalloc byte[StackTopicBytes] : rentedTopic;
        try
        {
            utf8Topic = utf8Topic[..FieldSize.StrictUtf8.GetBytes(topic, utf8Topic)];
            if (!Topics.IsValidName(utf8Topic))
            {
                throw new ArgumentException("The topic cannot be sent: it is empty or holds the wildcard + or #.", nameof(topic));
            }

            return Publish(utf8Topic, payload.Span, qos);
        }
        finally
        {
            if (rentedTopic is not null)
            {
                _pool.Return(rentedTopic);
            }
        }
    }

    /// <summary>
    /// Measures a topic as UTF-8 and checks that MQTT can carry it as a string.
    /// </summary>
    /// <param name="topic">
    /// The topic.
    /// </param>
    /// <returns>
    /// The number of UTF-8 bytes, without the length prefix.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The topic is over 65,535 UTF-8 bytes, contains U+0000 or has an unpaired surrogate.
    /// </exception>
    private static int MeasureTopic(string topic)
    {
        try
        {
            return FieldSize.Utf8String(topic) - FieldSize.LengthPrefix;
        }
        catch (ArgumentException ex)
        {
            throw new ArgumentException($"The topic cannot be sent: {ex.Message}", nameof(topic), ex);
        }
    }

    /// <summary>
    /// Measures a PUBLISH without properties.
    /// </summary>
    /// <param name="topicLength">
    /// The topic's UTF-8 bytes, without the length prefix.
    /// </param>
    /// <param name="payloadLength">
    /// The payload's length.
    /// </param>
    /// <param name="qos">
    /// The QoS, which decides whether a packet identifier is written.
    /// </param>
    /// <returns>
    /// The whole packet's size, or <see cref="long.MaxValue"/> when its remaining length is over the largest MQTT
    /// allows, so that no server can take it.
    /// </returns>
    private static long MeasurePublish(int topicLength, int payloadLength, MqttQoS qos)
    {
        long remaining = FieldSize.LengthPrefix + topicLength
            + (qos == MqttQoS.AtMostOnce ? 0 : PacketIdentifierBytes)
            + NoPropertiesBytes + (long)payloadLength;
        return remaining > VariableByteInteger.MaxValue
            ? long.MaxValue
            : PacketHeader.GetLength((int)remaining) + remaining;
    }

    /// <summary>
    /// Makes the result of a publish that fails at once, sending nothing.
    /// </summary>
    /// <param name="status">
    /// Why it failed.
    /// </param>
    /// <returns>
    /// A completed task with the failure; its reason code is meaningless.
    /// </returns>
    private static ValueTask<MqttRequestResult> FailPublish(MqttRequestStatus status)
        => new(new MqttRequestResult(status, MqttReasonCode.UnspecifiedError));

    /// <summary>
    /// Tells whether an acknowledgement is the one a record waits for.
    /// </summary>
    /// <param name="type">
    /// PUBACK, PUBREC, PUBCOMP, SUBACK or UNSUBACK.
    /// </param>
    /// <param name="kind">
    /// What the record waits for.
    /// </param>
    /// <returns>
    /// <see langword="false"/> for an acknowledgement of the wrong kind, a protocol error.
    /// </returns>
    private static bool IsAwaited(PacketType type, RecordKind kind) => (type, kind) switch
    {
        (PacketType.PubAck, RecordKind.PublishQoS1) => true,
        (PacketType.PubRec, RecordKind.PublishQoS2AwaitingPubRec) => true,
        (PacketType.PubComp, RecordKind.PublishQoS2AwaitingPubComp) => true,
        (PacketType.SubAck, RecordKind.Subscribe) => true,
        (PacketType.UnsubAck, RecordKind.Unsubscribe) => true,
        _ => false,
    };

    /// <summary>
    /// Checks a publish against the connection, puts its record in place, encodes it and hands it to the send
    /// lock, in one call so that its place in the send order is the order of the calls.
    /// </summary>
    /// <param name="topic">
    /// The topic as UTF-8, already checked.
    /// </param>
    /// <param name="payload">
    /// The payload.
    /// </param>
    /// <param name="qos">
    /// The quality of service.
    /// </param>
    /// <returns>
    /// The outcome, as <see cref="PublishAsync"/> reports it.
    /// </returns>
    private ValueTask<MqttRequestResult> Publish(ReadOnlySpan<byte> topic, ReadOnlySpan<byte> payload, MqttQoS qos)
    {
        long size = MeasurePublish(topic.Length, payload.Length, qos);
        RequestCompletion? completion = null;
        ushort packetIdentifier = 0;
        lock (_gate)
        {
            if (_phase != Phase.Connected || _records is not OutgoingRecords records)
            {
                return FailPublish(MqttRequestStatus.NotConnected);
            }

            if (qos > _serverMaximumQoS)
            {
                return FailPublish(MqttRequestStatus.QoSNotSupported);
            }

            if (size > _serverMaximumPacketSize)
            {
                return FailPublish(MqttRequestStatus.PacketTooLarge);
            }

            if (qos != MqttQoS.AtMostOnce)
            {
                // The record is in place, under the lock and while connected, before the packet reaches the send
                // lock (6.9): an acknowledgement finds it, and the end of the connection completes it.
                completion = records.RentCompletion();
                RecordKind kind = qos == MqttQoS.AtLeastOnce ? RecordKind.PublishQoS1 : RecordKind.PublishQoS2AwaitingPubRec;
                if (!records.TryAdd(kind, _time.GetTimestamp(), completion, out OutgoingRecord? record, out MqttRequestStatus failure))
                {
                    // Never handed out, so it goes straight back for the next publish.
                    records.ReturnCompletion(completion);
                    return FailPublish(failure);
                }

                packetIdentifier = record.PacketIdentifier;
            }
        }

        // Within the largest packet the server takes, which is never above the largest MQTT allows.
        byte[] packet = _pool.Rent((int)size);
        PublishFields fields = new() { Topic = topic, QoS = qos, PacketIdentifier = packetIdentifier };
        int length = PublishCodec.Write(packet, fields, payload);
        Debug.Assert(length == size, "The publish was measured differently from how it was written.");

        // Not awaited: the send lock queues the write now, in call order, and returns the buffer once the write
        // has ended. A failed write ends the connection, which completes the record with ConnectionLost.
        _ = SendAsync(packet, length, isDisconnect: false);
        if (completion is null)
        {
            // QoS 0 ends when the message is handed over.
            return new ValueTask<MqttRequestResult>(new MqttRequestResult(MqttRequestStatus.Completed, MqttReasonCode.Success));
        }

        return completion.Task;
    }

    /// <summary>
    /// Handles a PUBACK, PUBREC or PUBCOMP from the server. The acknowledgement a publish waits for completes it
    /// with the server's reason code; a PUBREC under 0x80 is answered with PUBREL and the publish waits for
    /// PUBCOMP. For an identifier not in use, a PUBACK or PUBCOMP is ignored and a PUBREC is answered with PUBREL
    /// 0x92.
    /// </summary>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code when the result is <see cref="EndStep.DisconnectAndClose"/>.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.None"/> to carry on; <see cref="EndStep.SendReply"/> for a PUBREL to send;
    /// <see cref="EndStep.DisconnectAndClose"/> for a malformed acknowledgement, or one of the wrong kind for its
    /// identifier (0x82), which leaves the record for the end of the connection to complete.
    /// </returns>
    private EndStep HandlePublishAck(ReadOnlySpan<byte> packet, out MqttReasonCode disconnect)
    {
        disconnect = AckCodec.TryDecode(packet, out AckPacket ack);
        if (disconnect != MqttReasonCode.Success)
        {
            return EndStep.DisconnectAndClose;
        }

        RequestCompletion completion;
        lock (_gate)
        {
            // There are no records before an accepting CONNACK, so no acknowledgement can be due.
            if (_records is not OutgoingRecords records)
            {
                disconnect = MqttReasonCode.ProtocolError;
                return EndStep.DisconnectAndClose;
            }

            if (!records.TryGet(ack.PacketIdentifier, out OutgoingRecord? record))
            {
                return ack.Type == PacketType.PubRec
                    ? RecordReply(PacketType.PubRel, ack.PacketIdentifier, MqttReasonCode.PacketIdentifierNotFound)
                    : EndStep.None;
            }

            if (!IsAwaited(ack.Type, record.Kind))
            {
                disconnect = MqttReasonCode.ProtocolError;
                return EndStep.DisconnectAndClose;
            }

            if (ack.Type == PacketType.PubRec && (byte)ack.ReasonCode < FirstFailureCode)
            {
                // Released with PUBREL; the identifier and the publish slot stay in use until PUBCOMP (MQTT 4.3.3).
                records.Advance(record, RecordKind.PublishQoS2AwaitingPubComp, _time.GetTimestamp());
                return RecordReply(PacketType.PubRel, ack.PacketIdentifier, MqttReasonCode.Success);
            }

            // Read before Remove recycles the record.
            completion = record.Completion;
            records.Remove(record);
        }

        // Its continuation runs asynchronously (decision 6), so no caller code runs on the read loop.
        completion.Complete(new MqttRequestResult(MqttRequestStatus.Completed, ack.ReasonCode));
        return EndStep.None;
    }
}
