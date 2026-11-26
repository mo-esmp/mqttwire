using System.Diagnostics;
using MqttWire.Client;
using MqttWire.Codec;

namespace MqttWire;

/// <content>
/// Subscribing: <see cref="SubscribeAsync"/> and <see cref="UnsubscribeAsync"/>, and the SUBACK and UNSUBACK that
/// end their exchanges.
/// </content>
public sealed partial class MqttWireClient
{
    /// <summary>
    /// Subscribes to one topic filter and waits for the server's SUBACK.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter, which may hold wildcards.
    /// </param>
    /// <param name="qos">
    /// The highest QoS the client asks for.
    /// </param>
    /// <param name="cancellationToken">
    /// Ends the wait for the SUBACK. It does not take the request back: the packet identifier stays in use until
    /// the SUBACK or the end of the connection, and the subscription may still be made.
    /// </param>
    /// <returns>
    /// The outcome, which never throws except for the cancellation of the wait. It completes on the SUBACK with its
    /// reason code: <see cref="MqttRequestResult.GrantedQoS"/> gives the QoS the server granted, which may be lower
    /// than asked, and a code of 0x80 or more means the server refused the subscription. It fails at once and
    /// nothing is sent with <see cref="MqttRequestStatus.NotConnected"/>,
    /// <see cref="MqttRequestStatus.PacketTooLarge"/> (over the server's Maximum Packet Size) or
    /// <see cref="MqttRequestStatus.NoFreePacketIdentifier"/>. A subscribe still waiting when the connection ends
    /// completes with <see cref="MqttRequestStatus.ConnectionLost"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="topicFilter"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topicFilter"/> is not a valid topic filter, or cannot be encoded: it is empty, misplaces a
    /// wildcard, holds U+0000, has an unpaired surrogate, or is over 65,535 UTF-8 bytes. Nothing is sent and no
    /// packet identifier is used.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="qos"/> is above 2. Nothing is sent and no packet identifier is used.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="cancellationToken"/> was cancelled before the call (nothing is sent and no identifier is
    /// used), or while the SUBACK was awaited.
    /// </exception>
    public ValueTask<MqttRequestResult> SubscribeAsync(string topicFilter, MqttQoS qos, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topicFilter);
        if (qos > MqttQoS.ExactlyOnce)
        {
            // Not ThrowIfGreaterThan: an enum is not IComparable<T>, and a cast would name "(byte)qos".
            throw new ArgumentOutOfRangeException(nameof(qos), qos, "The QoS must be 0, 1 or 2.");
        }

        return RequestAsync(topicFilter, qos, RecordKind.Subscribe, cancellationToken);
    }

    /// <summary>
    /// Unsubscribes from one topic filter and waits for the server's UNSUBACK.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter, exactly as it was subscribed.
    /// </param>
    /// <param name="cancellationToken">
    /// Ends the wait for the UNSUBACK. It does not take the request back: the packet identifier stays in use until
    /// the UNSUBACK or the end of the connection.
    /// </param>
    /// <returns>
    /// The outcome, as <see cref="SubscribeAsync"/> reports it; the reason code is the UNSUBACK's, and 0x80 or
    /// more means the server refused.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="topicFilter"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topicFilter"/> is not a valid topic filter, or cannot be encoded. Nothing is sent and no
    /// packet identifier is used.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="cancellationToken"/> was cancelled before the call (nothing is sent and no identifier is
    /// used), or while the UNSUBACK was awaited.
    /// </exception>
    public ValueTask<MqttRequestResult> UnsubscribeAsync(string topicFilter, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topicFilter);
        return RequestAsync(topicFilter, MqttQoS.AtMostOnce, RecordKind.Unsubscribe, cancellationToken);
    }

    /// <summary>
    /// Checks and encodes a topic filter, then sends it in a SUBSCRIBE or UNSUBSCRIBE and waits for the answer.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter.
    /// </param>
    /// <param name="qos">
    /// The QoS a SUBSCRIBE asks for; unused by an UNSUBSCRIBE.
    /// </param>
    /// <param name="kind">
    /// <see cref="RecordKind.Subscribe"/> or <see cref="RecordKind.Unsubscribe"/>.
    /// </param>
    /// <param name="cancellationToken">
    /// Ends the wait.
    /// </param>
    /// <returns>
    /// The outcome, as <see cref="SubscribeAsync"/> reports it.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The filter is invalid. Nothing is sent and no packet identifier is used.
    /// </exception>
    private ValueTask<MqttRequestResult> RequestAsync(string topicFilter, MqttQoS qos, RecordKind kind, CancellationToken cancellationToken)
    {
        int filterLength = TopicChecks.MeasureTopicFilter(topicFilter);
        byte[]? rentedFilter = filterLength <= TopicChecks.StackTopicBytes ? null : _pool.Rent(filterLength);
        Span<byte> utf8Filter = rentedFilter is null ? stackalloc byte[TopicChecks.StackTopicBytes] : rentedFilter;
        try
        {
            utf8Filter = utf8Filter[..FieldSize.StrictUtf8.GetBytes(topicFilter, utf8Filter)];
            TopicChecks.CheckTopicFilter(utf8Filter);

            // After the checks, before anything is written or any identifier is used.
            if (cancellationToken.IsCancellationRequested)
            {
                return ValueTask.FromCanceled<MqttRequestResult>(cancellationToken);
            }

            RequestCompletion? completion = Request(utf8Filter, qos, kind, out MqttRequestStatus failure);
            if (completion is null)
            {
                return FailPublish(failure);
            }

            ValueTask<MqttRequestResult> answer = completion.Task;
            return cancellationToken.CanBeCanceled
                ? new ValueTask<MqttRequestResult>(answer.AsTask().WaitAsync(cancellationToken))
                : answer;
        }
        finally
        {
            if (rentedFilter is not null)
            {
                _pool.Return(rentedFilter);
            }
        }
    }

    /// <summary>
    /// Checks a SUBSCRIBE or UNSUBSCRIBE against the connection, puts its record in place, encodes it and hands it
    /// to the send path, in one call so that its place in the send order is the order of the calls.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter as UTF-8, already checked.
    /// </param>
    /// <param name="qos">
    /// The QoS a SUBSCRIBE asks for.
    /// </param>
    /// <param name="kind">
    /// <see cref="RecordKind.Subscribe"/> or <see cref="RecordKind.Unsubscribe"/>.
    /// </param>
    /// <param name="failure">
    /// Why nothing was sent, when the result is <see langword="null"/>.
    /// </param>
    /// <returns>
    /// The completion the acknowledgement completes, or <see langword="null"/> when the request fails at once.
    /// </returns>
    private RequestCompletion? Request(ReadOnlySpan<byte> topicFilter, MqttQoS qos, RecordKind kind, out MqttRequestStatus failure)
    {
        int size = kind == RecordKind.Subscribe ? SubscribeCodec.GetSize(topicFilter) : UnsubscribeCodec.GetSize(topicFilter);
        RequestCompletion completion;
        ushort packetIdentifier;
        lock (_gate)
        {
            failure = MqttRequestStatus.NotConnected;
            if (_phase != Phase.Connected || _records is not OutgoingRecords records)
            {
                return null;
            }

            if ((uint)size > _serverMaximumPacketSize)
            {
                failure = MqttRequestStatus.PacketTooLarge;
                return null;
            }

            // The record is in place, under the lock and while connected, before the packet reaches the send path:
            // the acknowledgement finds it, and the end of the connection completes it.
            completion = records.RentCompletion();
            if (!records.TryAdd(kind, _time.GetTimestamp(), completion, out OutgoingRecord? record, out failure))
            {
                // Never handed out, so it goes straight back for the next request.
                records.ReturnCompletion(completion);
                return null;
            }

            packetIdentifier = record.PacketIdentifier;
        }

        byte[] packet = _pool.Rent(size);
        int length = kind == RecordKind.Subscribe
            ? SubscribeCodec.Write(packet, packetIdentifier, topicFilter, qos)
            : UnsubscribeCodec.Write(packet, packetIdentifier, topicFilter);
        Debug.Assert(length == size, "The request was measured differently from how it was written.");

        // The send path writes it now or queues it, in call order, and returns the buffer once the write has ended.
        // A failed write ends the connection, which completes the record with ConnectionLost.
        Send(packet, length, SendKind.Other);
        return completion;
    }

    /// <summary>
    /// Handles a SUBACK or UNSUBACK from the server: completes the request that waits for it with the one reason
    /// code it carries. An acknowledgement for an identifier not in use is ignored. A cancelled caller's request stays
    /// in place, so its late acknowledgement is handled like any other.
    /// </summary>
    /// <param name="type">
    /// SUBACK or UNSUBACK.
    /// </param>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code when the result is <see cref="EndStep.DisconnectAndClose"/>.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.None"/> to carry on; <see cref="EndStep.DisconnectAndClose"/> for a malformed
    /// acknowledgement, one with other than one reason code, or one of the wrong kind for its identifier (0x82),
    /// which leaves the record for the end of the connection to complete.
    /// </returns>
    private EndStep HandleSubscribeAck(PacketType type, ReadOnlySpan<byte> packet, out MqttReasonCode disconnect)
    {
        ushort packetIdentifier;
        ReadOnlySpan<byte> reasonCodes;
        if (type == PacketType.SubAck)
        {
            disconnect = SubAckCodec.TryDecode(packet, out SubAckPacket subAck);
            packetIdentifier = subAck.PacketIdentifier;
            reasonCodes = subAck.ReasonCodes;
        }
        else
        {
            disconnect = UnsubAckCodec.TryDecode(packet, out UnsubAckPacket unsubAck);
            packetIdentifier = unsubAck.PacketIdentifier;
            reasonCodes = unsubAck.ReasonCodes;
        }

        if (disconnect != MqttReasonCode.Success)
        {
            return EndStep.DisconnectAndClose;
        }

        // The client sends one filter per packet, so one reason code answers it.
        if (reasonCodes.Length != 1)
        {
            disconnect = MqttReasonCode.ProtocolError;
            return EndStep.DisconnectAndClose;
        }

        RequestCompletion completion;
        lock (_gate)
        {
            if (_records is not OutgoingRecords records)
            {
                disconnect = MqttReasonCode.ProtocolError;
                return EndStep.DisconnectAndClose;
            }

            if (!records.TryGet(packetIdentifier, out OutgoingRecord? record))
            {
                return EndStep.None;
            }

            if (!IsAwaited(type, record.Kind))
            {
                disconnect = MqttReasonCode.ProtocolError;
                return EndStep.DisconnectAndClose;
            }

            // Read before Remove recycles the record.
            completion = record.Completion;
            records.Remove(record);
        }

        completion.Complete(new MqttRequestResult(MqttRequestStatus.Completed, (MqttReasonCode)reasonCodes[0]));
        return EndStep.None;
    }
}
