using System.Diagnostics;
using MqttWire.Codec;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// QoS 1 and 2 inside a connection (design 6.6, "QoS 1 and 2 inside a connection", plan decision 13): for what a
/// client publishes, the count against the receive maximum the server announced and the QoS 2 exchange PUBLISH,
/// PUBREC, PUBREL, PUBCOMP with its records; for what the server delivers, the records, the client's window and the
/// client's acknowledgements; and the acknowledgement deadline of both.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// What an acknowledgement of the client did to the exchange of a delivery, decided under the lock by
    /// <see cref="ApplyDeliveryAck"/> and acted on outside it.
    /// </summary>
    private enum DeliveryAckOutcome : byte
    {
        /// <summary>
        /// Nothing: the identifier has no record and nothing answers the acknowledgement.
        /// </summary>
        Ignored,

        /// <summary>
        /// The exchange has ended and its record is to be removed.
        /// </summary>
        Ended,

        /// <summary>
        /// A PUBREC that is answered with PUBREL (success).
        /// </summary>
        AnswerPubRel,

        /// <summary>
        /// A PUBREC for an identifier with no record, answered with PUBREL 0x92 (packet identifier not found).
        /// </summary>
        AnswerPubRelNotFound,

        /// <summary>
        /// An acknowledgement of the wrong kind for a known identifier: a protocol error that ends the connection.
        /// </summary>
        WrongKind,
    }

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
            remembered = qos == MqttQoS.ExactlyOnce && records?.ContainsIncoming(packetIdentifier) == true;
            passed = !remembered
                     && (records?.IncomingCount ?? 0) + _control.PublishAnswerCount >= ServerLimits.ReceiveMaximum;
        }

        if (!passed)
        {
            return true;
        }

        BeginClose(CloseCause.ReceiveMaximumPassed, MqttReasonCode.ReceiveMaximumExceeded, reasonString: null);
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
    /// Handles a PUBACK, PUBREC or PUBCOMP from the client, which belongs to a delivery of the server's (design 6.6,
    /// "Deliveries at QoS 1 or 2"): a malformed one ends the connection with the decoder's code. One that ends its
    /// exchange (a PUBACK, a PUBCOMP, a PUBREC with a failure code) removes the record, which frees a place in the
    /// client's window; a successful PUBREC moves the exchange to waiting for PUBCOMP and is answered with PUBREL, as
    /// is a PUBREC repeated for an exchange that waits for PUBCOMP already. An acknowledgement of another kind for a
    /// known identifier ends the connection with 0x82. One for an identifier with no record is ignored, except a
    /// successful PUBREC, which is answered with PUBREL 0x92 (packet identifier not found); a failing one ends its
    /// exchange, so no PUBREL may follow it, as the client does (design 6.9).
    /// </summary>
    /// <param name="packet">
    /// The whole packet, a view of the read buffer, valid only during the call.
    /// </param>
    /// <returns>
    /// <see langword="true"/> to go on with the next packet; <see langword="false"/> when the connection is ending.
    /// </returns>
    private bool HandleDeliveryAck(ReadOnlySpan<byte> packet)
    {
        MqttReasonCode code = AckCodec.TryDecode(packet, out AckPacket ack);
        if (code != MqttReasonCode.Success)
        {
            CloseUndecodable(code);
            return false;
        }

        switch (ApplyDeliveryAck(ack.Type, ack.PacketIdentifier, failed: (byte)ack.ReasonCode >= 0x80))
        {
            case DeliveryAckOutcome.AnswerPubRel:
                SendAck(PacketType.PubRel, ack.PacketIdentifier, MqttReasonCode.Success, reasonString: null);
                break;

            case DeliveryAckOutcome.AnswerPubRelNotFound:
                SendAck(PacketType.PubRel, ack.PacketIdentifier, MqttReasonCode.PacketIdentifierNotFound,
                    reasonString: null);
                break;

            case DeliveryAckOutcome.WrongKind:
                BeginClose(CloseCause.ProtocolError, MqttReasonCode.ProtocolError, reasonString: null);
                return false;
        }

        return _state == ConnectionState.Active;
    }

    /// <summary>
    /// Applies an acknowledgement of the client to the record of its delivery, in one hold of the lock. When it ends
    /// the exchange, the record is removed (<see cref="EndDeliveryAt"/>): the last record gives the records object
    /// back to the server's pool once the lock is released, and when deliveries wait for the window with nothing
    /// being written, this caller takes the send path in the same hold and drains it once the lock is released.
    /// </summary>
    /// <param name="type">
    /// PUBACK, PUBREC or PUBCOMP.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier it names.
    /// </param>
    /// <param name="failed">
    /// Whether its reason code is a failure, 0x80 or above.
    /// </param>
    /// <returns>
    /// What the caller answers.
    /// </returns>
    private DeliveryAckOutcome ApplyDeliveryAck(PacketType type, ushort packetIdentifier, bool failed)
    {
        long now = _server.Time.GetTimestamp();
        ExchangeRecords? emptied = null;
        bool drain = false;
        DeliveryAckOutcome outcome;
        lock (_sendLock)
        {
            if (_records is not { } records || !records.TryFindDelivery(packetIdentifier, out int index))
            {
                outcome = type == PacketType.PubRec && !failed
                    ? DeliveryAckOutcome.AnswerPubRelNotFound
                    : DeliveryAckOutcome.Ignored;
            }
            else
            {
                outcome = MoveDelivery(ref records.DeliveryAt(index), type, failed, now);
                if (outcome == DeliveryAckOutcome.Ended)
                {
                    drain = EndDeliveryAt(records, index, out emptied);
                }
            }
        }

        // The pool's policy clears the records, so the next connection starts with none.
        if (emptied is not null)
        {
            _server.Pools.Records.Return(emptied);
        }

        if (drain)
        {
            ContinueSending(returned: null, failed: false, InlineWrites);
        }

        return outcome;
    }

    /// <summary>
    /// Moves a delivery's exchange on by the acknowledgement the client sent: a PUBACK ends a QoS 1 exchange; a
    /// PUBREC with a failure code ends a QoS 2 exchange waiting for it, and a successful one moves it to waiting for
    /// PUBCOMP, whose deadline starts now; a PUBREC for an exchange waiting for PUBCOMP already is a repetition and
    /// changes nothing; a PUBCOMP ends a QoS 2 exchange waiting for it. Anything else is of the wrong kind and
    /// changes nothing. Called under the lock.
    /// </summary>
    /// <param name="record">
    /// The delivery's record, changed in place.
    /// </param>
    /// <param name="type">
    /// PUBACK, PUBREC or PUBCOMP.
    /// </param>
    /// <param name="failed">
    /// Whether its reason code is a failure, 0x80 or above.
    /// </param>
    /// <param name="now">
    /// The time, a timestamp of the server's clock.
    /// </param>
    /// <returns>
    /// <see cref="DeliveryAckOutcome.Ended"/> when the record is to be removed; otherwise what the caller answers.
    /// </returns>
    private static DeliveryAckOutcome MoveDelivery(ref ExchangeRecord record, PacketType type, bool failed, long now)
    {
        switch (record.Phase)
        {
            case ExchangePhase.DeliveryAwaitingPubAck when type == PacketType.PubAck:
            case ExchangePhase.DeliveryAwaitingPubComp when type == PacketType.PubComp:
            case ExchangePhase.DeliveryAwaitingPubRec when type == PacketType.PubRec && failed:
                return DeliveryAckOutcome.Ended;

            case ExchangePhase.DeliveryAwaitingPubRec when type == PacketType.PubRec:
                record.Phase = ExchangePhase.DeliveryAwaitingPubComp;
                record.EnteredAt = now;
                return DeliveryAckOutcome.AnswerPubRel;

            case ExchangePhase.DeliveryAwaitingPubComp when type == PacketType.PubRec:
                return DeliveryAckOutcome.AnswerPubRel;

            default:
                return DeliveryAckOutcome.WrongKind;
        }
    }

    /// <summary>
    /// Removes the record of a delivery whose exchange has ended, which frees its place in the client's window;
    /// called under the lock. When deliveries wait while nothing is being written, which happens only while the
    /// first of them has no room in the window, the caller takes the send path in this same hold: the drain that
    /// went idle for want of room and this removal are each one hold of the lock, so whichever comes second sees
    /// the other, and no wake-up is lost.
    /// </summary>
    /// <param name="records">
    /// The connection's records.
    /// </param>
    /// <param name="index">
    /// The index of the delivery's record.
    /// </param>
    /// <param name="emptied">
    /// The records object when its last record was removed, to go back to the pool once the lock is released;
    /// otherwise <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the caller now holds the send path and is to drain it once the lock is released;
    /// otherwise <see langword="false"/>.
    /// </returns>
    private bool EndDeliveryAt(ExchangeRecords records, int index, out ExchangeRecords? emptied)
    {
        Debug.Assert(_sendLock.IsHeldByCurrentThread, "The caller holds the send path's lock.");
        records.RemoveDeliveryAt(index);
        emptied = null;
        if (records.IsEmpty)
        {
            emptied = records;
            Volatile.Write(ref _records, value: null);
        }

        if (_waiting is null || _writing || _state != ConnectionState.Active)
        {
            return false;
        }

        _writing = true;
        return true;
    }

    /// <summary>
    /// Tells whether a delivery at a QoS has room in the client's window now; called under the lock. A delivery at
    /// QoS 0 always has.
    /// </summary>
    /// <param name="qos">
    /// The delivery's QoS.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when it may be written now; <see langword="false"/> when the window is full.
    /// </returns>
    private bool HasWindowRoom(MqttQoS qos) =>
        qos == MqttQoS.AtMostOnce || (_records?.DeliveryCount ?? 0) < _clientWindow;

    /// <summary>
    /// Takes the packet identifier and the exchange record of a delivery at QoS 1 or 2 that is to be written now, in
    /// the hold in which that was decided, so that the record is in place before the packet is handed to the link;
    /// the first record takes the records object from the server's pool. Called under the lock, with the connection
    /// Active and <see cref="HasWindowRoom"/> true. A delivery at QoS 0 takes nothing.
    /// </summary>
    /// <param name="qos">
    /// The delivery's QoS, already lowered to <see cref="MqttWireServerOptions.MaxQoS"/>.
    /// </param>
    /// <param name="now">
    /// The time, a timestamp of the server's clock.
    /// </param>
    /// <returns>
    /// The packet identifier at QoS 1 and 2; 0 at QoS 0.
    /// </returns>
    private ushort TakeDeliveryRecord(MqttQoS qos, long now)
    {
        Debug.Assert(_sendLock.IsHeldByCurrentThread, "The caller holds the send path's lock.");
        if (qos == MqttQoS.AtMostOnce)
        {
            return 0;
        }

        ExchangeRecords? records = _records;
        if (records is null)
        {
            records = _server.Pools.Records.Get();
            Volatile.Write(ref _records, records);
        }

        ExchangePhase phase = qos == MqttQoS.AtLeastOnce
            ? ExchangePhase.DeliveryAwaitingPubAck
            : ExchangePhase.DeliveryAwaitingPubRec;
        return records.AddDelivery(phase, now, ref _lastDeliveryIdentifier);
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

        BeginClose(CloseCause.AcknowledgementTimedOut, MqttReasonCode.ProtocolError, reasonString: null);
        return true;
    }
}
