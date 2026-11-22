using MqttWire.Codec;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// QoS 1 and 2 for what a client publishes (design 6.6, "QoS 1 and 2 inside a connection", plan decision 13): the
/// count against the receive maximum the server announced, the QoS 2 exchange PUBLISH, PUBREC, PUBREL, PUBCOMP with
/// its records, the validation of the acknowledgements a client sends, and the acknowledgement deadline.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// Takes a place against the receive maximum of 256 the server announced for a QoS 1 or 2 publish, before the
    /// host sees it. The count is the QoS 2 publishes remembered (waiting for PUBREL) plus the last answers to the
    /// client's publishes that wait in the control buffer (<see cref="ControlBuffer.PublishAnswerCount"/>); an answer
    /// handed to the link counts as written. A QoS 2 publish whose identifier is remembered takes no new place. Only
    /// the read path adds to the count, so it cannot grow between this check and the answer.
    /// </summary>
    /// <param name="qos">
    /// The publish's QoS, 1 or 2.
    /// </param>
    /// <param name="packetIdentifier">
    /// Its packet identifier.
    /// </param>
    /// <param name="remembered">
    /// Whether the publish is at QoS 2 and its identifier is remembered.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the publish has its place; <see langword="false"/> when it would make the count
    /// 257, and the close has begun with 0x93 and <see cref="MqttCloseReason.ReceiveMaximumPassed"/>.
    /// </returns>
    private bool TryTakeReceivePlace(MqttQoS qos, ushort packetIdentifier, out bool remembered)
    {
        bool passed;
        lock (_sendLock)
        {
            ExchangeRecords? records = _records;
            remembered = qos == MqttQoS.ExactlyOnce && records is not null && records.ContainsIncoming(packetIdentifier);
            passed = !remembered
                     && (records?.IncomingCount ?? 0) + _control.PublishAnswerCount >= ServerLimits.ReceiveMaximum;
        }

        if (!passed)
        {
            return true;
        }

        BeginClose(MqttCloseReason.ReceiveMaximumPassed, MqttReasonCode.ReceiveMaximumExceeded, reasonString: null);
        return false;
    }

    /// <summary>
    /// Answers a QoS 2 publish the host has decided (design 6.6, "Publishes from the client at QoS 2", item 1): an
    /// accepted one is remembered and answered with PUBREC and the host's code; a refused one is answered with a PUBREC
    /// that carries the refusal, and is not remembered. A connection that stopped being Active meanwhile remembers
    /// and answers nothing.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The publish's packet identifier, not remembered.
    /// </param>
    /// <param name="result">
    /// The host's answer.
    /// </param>
    /// <returns>
    /// <see langword="true"/> to go on with the next packet; <see langword="false"/> when the connection is ending.
    /// </returns>
    private bool AnswerPublishQoS2(ushort packetIdentifier, in MqttPublishResult result)
    {
        if (result.IsAccepted && !RememberIncoming(packetIdentifier))
        {
            return false;
        }

        if (_state == ConnectionState.Active)
        {
            SendAck(PacketType.PubRec, packetIdentifier, result.ReasonCode, result.ReasonString);
        }

        return _state == ConnectionState.Active;
    }

    /// <summary>
    /// Remembers an accepted QoS 2 publish, waiting for its PUBREL; the first record takes the records object from
    /// the server's pool. The record is in place before its PUBREC is handed to the link.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier, not remembered; <see cref="TryTakeReceivePlace"/> made sure there is room.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when it is remembered; <see langword="false"/> when the connection is no longer Active,
    /// and nothing changed: nothing is added once the end of the connection may have dropped the records.
    /// </returns>
    private bool RememberIncoming(ushort packetIdentifier)
    {
        long now = _server.Time.GetTimestamp();
        lock (_sendLock)
        {
            if (_state != ConnectionState.Active)
            {
                return false;
            }

            ExchangeRecords? records = _records;
            if (records is null)
            {
                records = _server.Pools.Records.Get();
                Volatile.Write(ref _records, records);
            }

            records.AddIncoming(packetIdentifier, now);
            return true;
        }
    }

    /// <summary>
    /// Forgets a remembered QoS 2 publish; the last record gives the records object back to the server's pool.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when it was remembered; <see langword="false"/> when it was not, and nothing changed.
    /// </returns>
    private bool ForgetIncoming(ushort packetIdentifier)
    {
        ExchangeRecords? emptied = null;
        bool removed = false;
        lock (_sendLock)
        {
            if (_records is { } records && records.RemoveIncoming(packetIdentifier))
            {
                removed = true;
                if (records.IsEmpty)
                {
                    emptied = records;
                    Volatile.Write(ref _records, value: null);
                }
            }
        }

        // The pool's policy clears the records, so the next connection starts with none.
        if (emptied is not null)
        {
            _server.Pools.Records.Return(emptied);
        }

        return removed;
    }

    /// <summary>
    /// Handles a PUBREL (design 6.6, "Publishes from the client at QoS 2", item 3): a malformed one ends the
    /// connection with the decoder's code; otherwise its identifier is forgotten and the answer is PUBCOMP, success
    /// for a remembered identifier and 0x92 (packet identifier not found) for another.
    /// </summary>
    /// <param name="packet">
    /// The whole packet, a view of the read buffer, valid only during the call.
    /// </param>
    /// <returns>
    /// <see langword="true"/> to go on with the next packet; <see langword="false"/> when the connection is ending.
    /// </returns>
    private bool HandlePubRel(ReadOnlySpan<byte> packet)
    {
        MqttReasonCode code = AckCodec.TryDecode(packet, out AckPacket pubRel);
        if (code != MqttReasonCode.Success)
        {
            CloseUndecodable(code);
            return false;
        }

        MqttReasonCode answer = ForgetIncoming(pubRel.PacketIdentifier)
            ? MqttReasonCode.Success
            : MqttReasonCode.PacketIdentifierNotFound;
        SendAck(PacketType.PubComp, pubRel.PacketIdentifier, answer, reasonString: null);
        return _state == ConnectionState.Active;
    }

    /// <summary>
    /// Handles a PUBACK, PUBREC or PUBCOMP from the client: a malformed one ends the connection with the decoder's
    /// code; a valid one belongs to a delivery of the server's.
    /// </summary>
    /// <param name="packet">
    /// The whole packet, a view of the read buffer, valid only during the call.
    /// </param>
    /// <returns>
    /// <see langword="true"/> to go on with the next packet; <see langword="false"/> when the connection is ending.
    /// </returns>
    private bool HandleDeliveryAck(ReadOnlySpan<byte> packet)
    {
        MqttReasonCode code = AckCodec.TryDecode(packet, out _);
        if (code != MqttReasonCode.Success)
        {
            CloseUndecodable(code);
            return false;
        }

        // Task 10: the client's acknowledgements of the server's QoS 1 and 2 deliveries; until then they are ignored.
        return true;
    }

    /// <summary>
    /// Acts on the acknowledgement deadline of an Active connection; called by the sweep. A connection that holds no
    /// records costs one volatile read and no lock. Otherwise the oldest stamp is read under the lock, and when it
    /// is <see cref="ServerLimits.AcknowledgementDeadline"/> old the close begins with 0x82 and
    /// <see cref="MqttCloseReason.AcknowledgementTimedOut"/>; a connection Closing already changes nothing.
    /// </summary>
    /// <param name="now">
    /// The sweep's timestamp, from the server's clock.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the deadline has passed and the close was begun; otherwise <see langword="false"/>.
    /// </returns>
    private bool CheckAcknowledgementDeadline(long now)
    {
        if (Volatile.Read(ref _records) is null)
        {
            return false;
        }

        long oldest;
        lock (_sendLock)
        {
            oldest = _records?.OldestEnteredAt ?? 0;
        }

        if (oldest == 0 || now - oldest < _server.AcknowledgementDeadlineLength)
        {
            return false;
        }

        BeginClose(MqttCloseReason.AcknowledgementTimedOut, MqttReasonCode.ProtocolError, reasonString: null);
        return true;
    }
}
