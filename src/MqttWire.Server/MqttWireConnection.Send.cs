using System.Buffers;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using MqttWire.Codec;
using MqttWire.Link;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// Sending (design 6.6, "Sending: taken at once, written when the socket allows"): the one send path every write
/// to the link goes through. A link takes one write at a time, so one flag, <see cref="_writing"/>, says who may
/// write: whoever sets it holds the send path, starts the write, and, when the write has ended, either takes the
/// next thing to write or clears the flag, in one hold of <see cref="_sendLock"/>. Everyone else adds to what
/// waits and returns. The lock is never held across a write, a close, an abort or a handler call.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// How many writes that complete at once a caller makes on its own thread before it hands the send path to the
    /// thread pool, so that a host's <see cref="TrySend"/>, or the read loop, never writes for ever for the callers
    /// that keep adding behind it.
    /// </summary>
    private const int InlineWrites = 16;

    /// <summary>
    /// The largest payload whose PUBLISH can be encoded whatever its topic: the header of every PUBLISH fits one
    /// buffer of <see cref="ServerLimits.MaxEncodingBufferBytes"/> (plan decision 10), so the Remaining Length of a
    /// payload up to this stays within what MQTT can carry.
    /// </summary>
    private const int MaxDeliveryPayloadBytes = VariableByteInteger.MaxValue - ServerLimits.MaxEncodingBufferBytes;

    /// <summary>
    /// What the holder of the send path does next, decided in one hold of the lock by <see cref="TakeNextStep"/>.
    /// </summary>
    private enum SendStep : byte
    {
        /// <summary>
        /// Nothing is left to do: the send path was given up in the same hold.
        /// </summary>
        Idle,

        /// <summary>
        /// A piece is ready to be written: the waiting control packets, or the next piece of a large payload.
        /// </summary>
        Write,

        /// <summary>
        /// A waiting delivery was taken and is to be encoded and written.
        /// </summary>
        Deliver,

        /// <summary>
        /// The WebSocket close of a connection that is Closing is to be handed to the link.
        /// </summary>
        Close,
    }

    /// <summary>
    /// An answer the read path produces for the client: PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK or
    /// PINGRESP, with the fields its type uses. Its reason string has already been shaped.
    /// </summary>
    [StructLayout(LayoutKind.Auto)]
    private readonly ref struct Answer
    {
        /// <summary>
        /// Gets the packet's type.
        /// </summary>
        public PacketType Type { get; init; }

        /// <summary>
        /// Gets the packet identifier, from 1 to 65,535; unused for a PINGRESP.
        /// </summary>
        public ushort PacketIdentifier { get; init; }

        /// <summary>
        /// Gets the reason code of a PUBACK, PUBREC, PUBREL or PUBCOMP; unused for the others.
        /// </summary>
        public MqttReasonCode Code { get; init; }

        /// <summary>
        /// Gets the reason string to carry, or <see langword="null"/> for none; unused for UNSUBACK and PINGRESP.
        /// </summary>
        public string? ReasonString { get; init; }

        /// <summary>
        /// Gets the reason codes of a SUBACK, one per topic filter; unused for the others.
        /// </summary>
        public ReadOnlySpan<MqttReasonCode> Codes { get; init; }

        /// <summary>
        /// Gets the number of topic filters of an UNSUBACK, each answered with success; unused for the others.
        /// </summary>
        public int Count { get; init; }

        /// <summary>
        /// Gets a value indicating whether the answer is the last one to a QoS 1 or 2 publish of the client, which
        /// counts against the receive maximum the server announced while it waits (plan decision 13): a PUBACK, a
        /// PUBCOMP that ends a remembered exchange (success), or a failing PUBREC. A successful PUBREC is not: the
        /// exchange's record counts instead; nor is a PUBCOMP 0x92, which answers no publish.
        /// </summary>
        public bool EndsIncomingPublish => Type switch
        {
            PacketType.PubAck => true,
            PacketType.PubRec => (byte)Code >= 0x80,
            PacketType.PubComp => Code == MqttReasonCode.Success,
            _ => false,
        };

        /// <summary>
        /// Gets the size of the encoded packet.
        /// </summary>
        /// <returns>
        /// The size in bytes.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// A field cannot be carried (a bug of this library: every field was validated where it came from).
        /// </exception>
        public int GetSize() => Type switch
        {
            PacketType.PingResp => PingCodec.PingResp.Length,
            PacketType.SubAck => SubAckCodec.GetSize(Codes.Length, ReasonString),
            PacketType.UnsubAck => UnsubAckCodec.GetSize(Count, reasonString: null),
            _ => AckCodec.GetSize(Code, ReasonString),
        };

        /// <summary>
        /// Encodes the packet.
        /// </summary>
        /// <param name="destination">
        /// Where to write: exactly <see cref="GetSize"/> bytes, all of which are written.
        /// </param>
        /// <exception cref="ArgumentException">
        /// A field cannot be carried, as for <see cref="GetSize"/>; nothing is written.
        /// </exception>
        public void Write(Span<byte> destination)
        {
            switch (Type)
            {
                case PacketType.PingResp:
                    PingCodec.PingResp.CopyTo(destination);
                    break;

                case PacketType.SubAck:
                    SubAckCodec.Write(destination, PacketIdentifier, Codes, ReasonString);
                    break;

                case PacketType.UnsubAck:
                    // The reason codes, all success (0x00), take the last Count bytes of the packet: they are cleared
                    // there and written over themselves, so no other buffer holds them.
                    Span<byte> codes = destination[^Count..];
                    codes.Clear();
                    UnsubAckCodec.Write(destination, PacketIdentifier, MemoryMarshal.Cast<byte, MqttReasonCode>(codes),
                        reasonString: null);
                    break;

                default:
                    AckCodec.Write(destination, Type, PacketIdentifier, Code, ReasonString);
                    break;
            }
        }
    }

    /// <summary>
    /// Takes a message for the client or refuses it, and returns at once (design 6.6): a connection that is not
    /// Active refuses it; a message too large for the client or for <see cref="MqttWireServerOptions.MaxOutgoingBytes"/>
    /// is refused and closes the connection with 0x95; with nothing in progress and nothing waiting the message is
    /// written now, by this caller; otherwise it waits behind the write in progress, unless one more would pass
    /// <see cref="MqttWireServerOptions.MaxOutgoingMessages"/> or <see cref="MqttWireServerOptions.MaxOutgoingBytes"/>,
    /// which evicts the connection with 0x97. Allocates nothing unless a write does not complete at once.
    /// </summary>
    /// <param name="topic">
    /// The topic, not the default value.
    /// </param>
    /// <param name="payload">
    /// The payload; referenced until it has been written, never copied into a pool and never changed.
    /// </param>
    /// <param name="qos">
    /// The QoS to deliver with, already lowered to <see cref="MqttWireServerOptions.MaxQoS"/>.
    /// </param>
    /// <returns>
    /// <see cref="MqttSendResult.Taken"/> when the message was written or waits; otherwise why it was not taken. A
    /// message that was not taken is held nowhere.
    /// </returns>
    private MqttSendResult TakeDelivery(MqttTopic topic, ReadOnlyMemory<byte> payload, MqttQoS qos)
    {
        MqttWireServerOptions options = _server.Options;
        int payloadLength = payload.Length;
        bool tooLarge = payloadLength > options.MaxOutgoingBytes || payloadLength > MaxDeliveryPayloadBytes
            || GetDeliverySize(topic, payloadLength, qos) > _clientMaxPacketSize;
        MqttSendResult result;
        lock (_sendLock)
        {
            if (_state != ConnectionState.Active)
            {
                return MqttSendResult.NotOpen;
            }

            if (tooLarge)
            {
                result = MqttSendResult.TooLarge;
            }
            else if (!_writing && _waiting is null)
            {
                // Nothing in progress and nothing waiting: this caller takes the send path and writes the message.
                _writing = true;
                _writingBytes = payloadLength;
                _outgoingBytes += payloadLength;
                result = MqttSendResult.Taken;
            }
            else if ((_waiting?.Count ?? 0) >= options.MaxOutgoingMessages
                     || (long)_outgoingBytes + payloadLength > options.MaxOutgoingBytes)
            {
                result = MqttSendResult.Evicted;
            }
            else
            {
                _waiting ??= _server.Pools.Waiting.Get();
                _waiting.Enqueue(new PendingDelivery(topic, payload, qos));
                _outgoingBytes += payloadLength;
                return MqttSendResult.Taken;
            }
        }

        // Outside the lock: a close writes or aborts, and a write may take as long as the link does.
        if (result == MqttSendResult.TooLarge)
        {
            BeginClose(MqttCloseReason.MessageTooLargeToSend, MqttReasonCode.PacketTooLarge, reasonString: null);
        }
        else if (result == MqttSendResult.Evicted)
        {
            BeginClose(MqttCloseReason.OutgoingLimitPassed, MqttReasonCode.QuotaExceeded, reasonString: null);
        }
        else
        {
            ReadOnlyMemory<byte> piece = EncodeDelivery(new PendingDelivery(topic, payload, qos), out byte[] buffer);
            StartSending(piece, buffer, InlineWrites);
        }

        return result;
    }

    /// <summary>
    /// Gets the size of the PUBLISH that carries a message at its QoS, the packet identifier of QoS 1 and 2
    /// included. The caller has made sure the payload is not over <see cref="MaxDeliveryPayloadBytes"/>.
    /// </summary>
    /// <param name="topic">
    /// The topic, not the default value.
    /// </param>
    /// <param name="payloadLength">
    /// The payload's length.
    /// </param>
    /// <param name="qos">
    /// The QoS, 0, 1 or 2.
    /// </param>
    /// <returns>
    /// The size of the whole packet in bytes.
    /// </returns>
    private static int GetDeliverySize(MqttTopic topic, int payloadLength, MqttQoS qos)
    {
        // Any identifier stands for the one the delivery will get: it has the same size.
        PublishFields fields = new()
        {
            Topic = topic.Utf8, QoS = qos, PacketIdentifier = qos == MqttQoS.AtMostOnce ? (ushort)0 : (ushort)1,
        };
        return PublishCodec.GetSize(fields, payloadLength);
    }

    /// <summary>
    /// Encodes a delivery for the write that follows; called by whoever holds the send path. A message whose whole
    /// packet is at most <see cref="ServerLimits.CopyThresholdBytes"/> is copied behind its header into one rented
    /// buffer. A larger one has only its header encoded into a rented buffer, and its payload is left in
    /// <see cref="_remainingPayload"/>, to be written from the host's memory in pieces (plan decision 10).
    /// </summary>
    /// <param name="delivery">
    /// The delivery, which <see cref="TakeDelivery"/> found small enough to send.
    /// </param>
    /// <param name="buffer">
    /// The rented buffer that holds the piece; the completion of its write returns it.
    /// </param>
    /// <returns>
    /// The first piece to write: the whole packet, or its header.
    /// </returns>
    private ReadOnlyMemory<byte> EncodeDelivery(in PendingDelivery delivery, out byte[] buffer)
    {
        // Task 10: a delivery whose QoS is 1 or 2 branches here. It takes its packet identifier, with its exchange
        // record in place before the packet is handed to the link, and is encoded at its own QoS. Until then every
        // delivery is written at QoS 0, whatever its QoS says.
        PublishFields fields = new() { Topic = delivery.Topic.Utf8, QoS = MqttQoS.AtMostOnce };
        int payloadLength = delivery.Payload.Length;
        int size = PublishCodec.GetSize(fields, payloadLength);
        if (size <= ServerLimits.CopyThresholdBytes)
        {
            buffer = _server.Pool.Rent(size);
            _remainingPayload = default;
            return buffer.AsMemory(start: 0, PublishCodec.Write(buffer, fields, delivery.Payload.Span));
        }

        // An empty payload behind a long topic leaves nothing to write, and no reference to the host's memory.
        buffer = _server.Pool.Rent(size - payloadLength);
        _remainingPayload = payloadLength == 0 ? default : delivery.Payload;
        return buffer.AsMemory(start: 0, PublishCodec.WriteHeader(buffer, fields, payloadLength));
    }

    /// <summary>
    /// Encodes the success CONNACK and writes it as the first write of the send path, which the admission step
    /// reserved for it (so no MQTT packet can precede it); what a host sent meanwhile is written behind it. It never
    /// waits: a write that does not complete at once is awaited elsewhere. A failed write ends the connection with
    /// <see cref="MqttCloseReason.ConnectionLost"/>.
    /// </summary>
    /// <param name="fields">
    /// The fields to encode.
    /// </param>
    /// <exception cref="ArgumentException">
    /// A field cannot be carried (the host's bug); nothing is rented or written, the connection begins to close,
    /// and the send path is given up, so nothing that waited is written in the CONNACK's place.
    /// </exception>
    private void SendConnAck(in ConnAckFields fields)
    {
        int size;
        try
        {
            // The size checks every field before anything is rented.
            size = ConnAckCodec.GetSize(fields);
        }
        catch (ArgumentException)
        {
            // The close finds the send path taken and aborts; then the path is given up and what waited is dropped.
            BeginClose(MqttCloseReason.ConnectionLost, MqttReasonCode.Success, reasonString: null);
            ContinueSending(returned: null, failed: false, InlineWrites);
            throw;
        }

        byte[] buffer = _server.Pool.Rent(size);
        StartSending(buffer.AsMemory(start: 0, ConnAckCodec.Write(buffer, fields)), buffer, InlineWrites);
    }

    /// <summary>
    /// Sends a PUBACK, PUBREC, PUBREL or PUBCOMP through the send path, as <see cref="SendAnswer"/> does. The reason
    /// string follows the rule of <see cref="ShapeReasonString"/>.
    /// </summary>
    /// <param name="type">
    /// The packet's type, one of the four.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier, from 1 to 65,535.
    /// </param>
    /// <param name="code">
    /// The reason code.
    /// </param>
    /// <param name="reasonString">
    /// The reason string, or <see langword="null"/>; validated where the host handed it over.
    /// </param>
    private void SendAck(PacketType type, ushort packetIdentifier, MqttReasonCode code, string? reasonString) =>
        SendAnswer(new Answer
        {
            Type = type,
            PacketIdentifier = packetIdentifier,
            Code = code,
            ReasonString = ShapeReasonString(type, code, count: 0, reasonString),
        });

    /// <summary>
    /// Sends a SUBACK through the send path, as <see cref="SendAnswer"/> does. The reason string follows the rule of
    /// <see cref="ShapeReasonString"/>. The caller has made sure the SUBACK fits the client's Maximum Packet Size
    /// without its reason string.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier of the SUBSCRIBE, from 1 to 65,535.
    /// </param>
    /// <param name="codes">
    /// One reason code per topic filter, in the SUBSCRIBE's order; at least one.
    /// </param>
    /// <param name="reasonString">
    /// The reason string, or <see langword="null"/>; validated where the host handed it over.
    /// </param>
    private void SendSubAck(ushort packetIdentifier, ReadOnlySpan<MqttReasonCode> codes, string? reasonString) =>
        SendAnswer(new Answer
        {
            Type = PacketType.SubAck,
            PacketIdentifier = packetIdentifier,
            Codes = codes,
            ReasonString = ShapeReasonString(PacketType.SubAck, MqttReasonCode.Success, codes.Length, reasonString),
        });

    /// <summary>
    /// Sends an UNSUBACK with success for every topic filter through the send path, as <see cref="SendAnswer"/>
    /// does. The caller has made sure the UNSUBACK fits the client's Maximum Packet Size.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier of the UNSUBSCRIBE, from 1 to 65,535.
    /// </param>
    /// <param name="count">
    /// The number of topic filters, at least one.
    /// </param>
    private void SendUnsubAck(ushort packetIdentifier, int count) =>
        SendAnswer(new Answer { Type = PacketType.UnsubAck, PacketIdentifier = packetIdentifier, Count = count });

    /// <summary>
    /// Sends a PINGRESP through the send path, as <see cref="SendAnswer"/> does; when one already waits behind the
    /// write in progress, another is not added.
    /// </summary>
    private void SendPingResp() => SendAnswer(new Answer { Type = PacketType.PingResp });

    /// <summary>
    /// Sends an answer of the read path and returns at once (design 6.6). The answer is encoded into a rented buffer
    /// first, so that nothing can fail once the send path's state has changed. Then, in one hold of the lock: a
    /// connection that is no longer Active drops it, so that nothing follows a DISCONNECT; with no write in progress
    /// this caller takes the send path and writes the buffer; with one in progress the answer is copied behind the
    /// control packets that wait, which are written before any waiting delivery, except that a PINGRESP is not added
    /// to one that waits, and that an answer that would pass 1,024 waiting control packets, the 64 KiB one control
    /// buffer holds, or <see cref="MqttWireServerOptions.MaxOutgoingBytes"/> evicts the connection with 0x97.
    /// </summary>
    /// <param name="answer">
    /// The answer.
    /// </param>
    /// <exception cref="ArgumentException">
    /// A field cannot be carried (a bug of this library); the buffer is returned and nothing changes.
    /// </exception>
    private void SendAnswer(in Answer answer)
    {
        ArrayPool<byte> pool = _server.Pool;
        int size = answer.GetSize();
        byte[] buffer = pool.Rent(size);
        try
        {
            answer.Write(buffer.AsSpan(start: 0, size));
        }
        catch (ArgumentException)
        {
            pool.Return(buffer);
            throw;
        }

        bool isPingResp = answer.Type == PacketType.PingResp;
        bool write = false;
        bool evict = false;
        lock (_sendLock)
        {
            if (_state != ConnectionState.Active)
            {
                // Closing: a DISCONNECT, if one is written, is the last MQTT packet.
            }
            else if (!_writing)
            {
                _writing = true;
                write = true;
            }
            else if (isPingResp && _control.HasPingResp)
            {
                // One PINGRESP waits already; it answers this PINGREQ too.
            }
            else if (_control.PacketCount >= ServerLimits.MaxWaitingControlPackets
                     || _control.ByteCount + size > ServerLimits.MaxEncodingBufferBytes
                     || (long)_outgoingBytes + size > _server.Options.MaxOutgoingBytes)
            {
                evict = true;
            }
            else
            {
                buffer.AsSpan(start: 0, size).CopyTo(_control.Reserve(pool, size));
                _control.Commit(size, isPingResp, answer.EndsIncomingPublish);
                _outgoingBytes += size;
            }
        }

        if (write)
        {
            StartSending(buffer.AsMemory(start: 0, size), buffer, InlineWrites);
            return;
        }

        pool.Return(buffer);
        if (evict)
        {
            BeginClose(MqttCloseReason.OutgoingLimitPassed, MqttReasonCode.QuotaExceeded, reasonString: null);
        }
    }

    /// <summary>
    /// The one rule for the reason string of every answer that can carry one (design 6.6, "A client's maximum packet
    /// size", and 6.7): it is dropped from PUBACK, PUBREC, PUBREL, PUBCOMP and SUBACK when the client sent Request
    /// Problem Information 0 (CONNACK and DISCONNECT keep theirs); it is cut to 256 bytes at a character boundary;
    /// and it is left out when the packet with it would pass the client's Maximum Packet Size. Allocates only when
    /// the string has to be cut.
    /// </summary>
    /// <param name="type">
    /// The answer's type: CONNACK, DISCONNECT, SUBACK, or one of the four acknowledgements of a PUBLISH.
    /// </param>
    /// <param name="code">
    /// The answer's reason code; unused for a SUBACK.
    /// </param>
    /// <param name="count">
    /// The number of reason codes of a SUBACK; unused for the others.
    /// </param>
    /// <param name="reasonString">
    /// The reason string, or <see langword="null"/>; validated where the host handed it over, so it can be encoded.
    /// </param>
    /// <returns>
    /// The reason string to carry, or <see langword="null"/> for none.
    /// </returns>
    private string? ShapeReasonString(PacketType type, MqttReasonCode code, int count, string? reasonString)
    {
        if (reasonString is null || (!_problemInformation && type is not (PacketType.ConnAck or PacketType.Disconnect)))
        {
            return null;
        }

        string? cut = ReasonStrings.Cut(reasonString);
        int size = type switch
        {
            PacketType.ConnAck => ConnAckCodec.GetSize(new ConnAckFields { ReasonCode = code, ReasonString = cut }),
            PacketType.Disconnect => DisconnectCodec.GetSize(code, cut),
            PacketType.SubAck => SubAckCodec.GetSize(count, cut),
            _ => AckCodec.GetSize(code, cut),
        };
        return size <= _clientMaxPacketSize ? cut : null;
    }

    /// <summary>
    /// Writes a piece and whatever is to be written after it, for as long as the writes complete at once; called by
    /// whoever holds the send path, outside the lock. It is a loop, not a recursion: each write that has ended
    /// returns its buffer and takes the next piece in one hold of the lock (<see cref="TryTakeNextPiece"/>). A write
    /// that does not complete at once is handed to <see cref="CompleteWriteAsync"/>, which carries on when it has
    /// ended, and this returns: nobody waits for the link.
    /// </summary>
    /// <param name="piece">
    /// The bytes to write as one WebSocket message.
    /// </param>
    /// <param name="buffer">
    /// The rented buffer the piece lies in, returned when the write has ended; <see langword="null"/> for a piece
    /// of the host's memory, which is never returned to a pool.
    /// </param>
    /// <param name="inlineWrites">
    /// How many writes that complete at once this thread still makes before it hands the send path to the thread
    /// pool.
    /// </param>
    private void StartSending(ReadOnlyMemory<byte> piece, byte[]? buffer, int inlineWrites)
    {
        IMqttLink link = RunningLink;
        while (true)
        {
            ValueTask write = StartWrite(link, piece);
            if (!write.IsCompleted)
            {
                // Stamped only for a write that stays with the link: one that completed at once needs no deadline.
                // The stamp is cleared by CompleteWriteAsync, which starts after this line, so it is never left set.
                Volatile.Write(ref _writeStartedAt, _server.Time.GetTimestamp());
                _ = CompleteWriteAsync(write, buffer, inlineWrites);
                return;
            }

            bool failed = !HasSucceeded(write);
            inlineWrites--;
            if (!TryTakeNextPiece(buffer, failed, inlineWrites, out piece, out buffer))
            {
                return;
            }
        }
    }

    /// <summary>
    /// Carries the send path on after a write, or nothing, has ended: returns the buffer, takes the next piece and
    /// writes it. Called by whoever holds the send path, outside the lock: the completion of a write or of a close,
    /// the thread pool after a hand-over, and a holder that has nothing of its own to write (a close without a
    /// packet, a CONNACK that is not to be written).
    /// </summary>
    /// <param name="returned">
    /// The rented buffer of the write that has ended, or <see langword="null"/>.
    /// </param>
    /// <param name="failed">
    /// Whether that write failed.
    /// </param>
    /// <param name="inlineWrites">
    /// How many writes that complete at once this thread makes before it hands the send path to the thread pool.
    /// </param>
    private void ContinueSending(byte[]? returned, bool failed, int inlineWrites)
    {
        if (TryTakeNextPiece(returned, failed, inlineWrites, out ReadOnlyMemory<byte> piece, out byte[]? buffer))
        {
            StartSending(piece, buffer, inlineWrites);
        }
    }

    /// <summary>
    /// Ends a write and finds the next piece; called by whoever holds the send path, outside the lock. In order: the
    /// buffer of the write that has ended goes back to the pool, here and nowhere else (design 6.8 rule 4); a failed
    /// write begins the close with <see cref="MqttCloseReason.ConnectionLost"/> and aborts the link; a caller that
    /// has used up its writes hands the send path to the thread pool; otherwise <see cref="TakeNextStep"/> decides,
    /// in one hold of the lock, what is next. A delivery is encoded here; the WebSocket close of a Closing
    /// connection is handed to the link here, and awaited elsewhere when it does not complete at once.
    /// </summary>
    /// <param name="returned">
    /// The rented buffer of the write that has ended, or <see langword="null"/>.
    /// </param>
    /// <param name="failed">
    /// Whether that write failed.
    /// </param>
    /// <param name="inlineWrites">
    /// How many writes that complete at once this thread still makes; at 0 or below, the send path is handed to the
    /// thread pool.
    /// </param>
    /// <param name="piece">
    /// The next piece to write when the result is <see langword="true"/>.
    /// </param>
    /// <param name="buffer">
    /// The rented buffer the piece lies in, or <see langword="null"/> for a piece of the host's memory.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the caller still holds the send path and is to write <paramref name="piece"/>;
    /// <see langword="false"/> when it no longer does: the path is idle, or its next step is another's (the thread
    /// pool's, or the completion of a pending close).
    /// </returns>
    private bool TryTakeNextPiece(byte[]? returned, bool failed, int inlineWrites, out ReadOnlyMemory<byte> piece,
        out byte[]? buffer)
    {
        IMqttLink link = RunningLink;
        if (returned is not null)
        {
            _server.Pool.Return(returned);
        }

        if (failed)
        {
            // The close finds the send path taken, by this caller, and aborts; when the connection was closing
            // already, nothing has aborted the link yet, so that a failed link ends now and not at the close deadline.
            BeginClose(MqttCloseReason.ConnectionLost, MqttReasonCode.Success, reasonString: null);
            link.Abort();
        }
        else if (inlineWrites <= 0)
        {
            // The send path stays taken across the hand-over, so nobody else writes and nothing is lost: whatever is
            // added meanwhile waits for the thread pool's hold.
            ThreadPool.UnsafeQueueUserWorkItem(
                static connection => connection.ContinueSending(returned: null, failed: false, int.MaxValue), this,
                preferLocal: false);
            piece = default;
            buffer = null;
            return false;
        }

        while (true)
        {
            SendStep step = TakeNextStep(failed, out PendingDelivery delivery, out piece, out buffer,
                out WebSocketCloseStatus closeStatus);
            switch (step)
            {
                case SendStep.Idle:
                    return false;

                case SendStep.Write:
                    return true;

                case SendStep.Deliver:
                    piece = EncodeDelivery(delivery, out byte[] encoded);
                    buffer = encoded;
                    return true;

                default:
                    if (!TryCloseLink(link, closeStatus))
                    {
                        return false;
                    }

                    // The next hold finds the close handed over and gives the send path up.
                    break;
            }
        }
    }

    /// <summary>
    /// Hands the WebSocket close of a Closing connection to the link; called by whoever holds the send path, outside
    /// the lock. A close that does not complete at once is awaited by <see cref="CompleteCloseAsync"/>, which then
    /// gives the send path up. One that fails aborts the link, so that the connection ends now and not at the close
    /// deadline.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <param name="status">
    /// The WebSocket status the peer sees.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the close has ended and the caller still holds the send path;
    /// <see langword="false"/> when the close is pending and its completion holds the send path now.
    /// </returns>
    private bool TryCloseLink(IMqttLink link, WebSocketCloseStatus status)
    {
        ValueTask close = StartClose(link, status);
        if (!close.IsCompleted)
        {
            _ = CompleteCloseAsync(close);
            return false;
        }

        if (!HasSucceeded(close))
        {
            link.Abort();
        }

        return true;
    }

    /// <summary>
    /// The one hold of the lock in which whoever holds the send path either takes the next thing to do or gives the
    /// path up (design 6.6): there is no moment at which it has found nothing and <see cref="_writing"/> is still
    /// set. A connection that is Closing drops what waits, is handed its WebSocket close when it has one to send and
    /// the last write did not fail, and otherwise goes idle (<see cref="TakeClosingStep"/>). One that is not takes,
    /// in this order: the next piece of the payload being written, the waiting control packets as one piece, the
    /// first waiting delivery; with none of them, it goes idle (<see cref="TakeOpenStep"/>). What was dropped goes
    /// back once the lock is released: the queue of deliveries to the server's pool, the control buffer to the buffer
    /// pool, the exchange records to theirs; and whoever waits for the send path to be idle is told then. This is the
    /// one place where the end of a connection drops what it holds on the way out, the exchange records included:
    /// nothing is added to a Closing connection, so what is taken here is taken once.
    /// </summary>
    /// <param name="failed">
    /// Whether the write that has just ended failed, so that no WebSocket close is attempted.
    /// </param>
    /// <param name="delivery">
    /// The delivery taken when the result is <see cref="SendStep.Deliver"/>.
    /// </param>
    /// <param name="piece">
    /// The piece to write when the result is <see cref="SendStep.Write"/>.
    /// </param>
    /// <param name="buffer">
    /// The rented buffer <paramref name="piece"/> lies in, which now belongs to its write; <see langword="null"/>
    /// for a piece of the host's memory.
    /// </param>
    /// <param name="closeStatus">
    /// The WebSocket status to close with when the result is <see cref="SendStep.Close"/>.
    /// </param>
    /// <returns>
    /// What the caller does next; <see cref="SendStep.Idle"/> when it no longer holds the send path.
    /// </returns>
    private SendStep TakeNextStep(bool failed, out PendingDelivery delivery, out ReadOnlyMemory<byte> piece,
        out byte[]? buffer, out WebSocketCloseStatus closeStatus)
    {
        delivery = default;
        piece = default;
        buffer = null;
        closeStatus = default;
        WaitingDeliveries? emptied = null;
        ControlBuffer dropped = default;
        ExchangeRecords? records = null;
        TaskCompletionSource? idle = null;
        SendStep step;
        lock (_sendLock)
        {
            if (_state >= ConnectionState.Closing)
            {
                // Read under the lock, under which every move to Closing is made: nothing is added after this.
                emptied = _waiting;
                _waiting = null;
                dropped = _control;
                _control = default;
                records = _records;
                Volatile.Write(ref _records, value: null);
                step = TakeClosingStep(failed, out closeStatus, out idle);
            }
            else
            {
                step = TakeOpenStep(out delivery, out piece, out buffer, out emptied);
            }
        }

        // The pool's policy clears the queue, so no payload or topic reference outlives the connection's use of it.
        if (emptied is not null)
        {
            _server.Pools.Waiting.Return(emptied);
        }

        // The pool's policy clears the records, so the next connection starts with none.
        if (records is not null)
        {
            _server.Pools.Records.Return(records);
        }

        dropped.Release(_server.Pool);
        idle?.TrySetResult();
        return step;
    }

    /// <summary>
    /// The step of a Closing connection, whose waiting deliveries and control packets the caller has taken away;
    /// called in the hold of <see cref="TakeNextStep"/>. Nothing is counted any more. The call that began the close
    /// is handed its WebSocket close once, unless the last write failed; in every other case the send path is given
    /// up, for good: nothing takes it again in Closing.
    /// </summary>
    /// <param name="failed">
    /// Whether the write that has just ended failed.
    /// </param>
    /// <param name="closeStatus">
    /// The WebSocket status to close with when the result is <see cref="SendStep.Close"/>.
    /// </param>
    /// <param name="idle">
    /// What the end of the run waits on, to be completed once the lock is released, when the result is
    /// <see cref="SendStep.Idle"/> and it waits; otherwise <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see cref="SendStep.Close"/> or <see cref="SendStep.Idle"/>.
    /// </returns>
    private SendStep TakeClosingStep(bool failed, out WebSocketCloseStatus closeStatus, out TaskCompletionSource? idle)
    {
        Debug.Assert(_sendLock.IsHeldByCurrentThread, "The caller holds the send path's lock.");
        closeStatus = (WebSocketCloseStatus)_closeStatus;
        idle = null;
        _remainingPayload = default;
        _writingBytes = 0;
        _outgoingBytes = 0;
        _closeStatus = 0;
        if (closeStatus != default && !failed)
        {
            return SendStep.Close;
        }

        _writing = false;
        idle = _sendIdle;
        _sendIdle = null;
        return SendStep.Idle;
    }

    /// <summary>
    /// The step of a connection that is not Closing; called in the hold of <see cref="TakeNextStep"/>. In this
    /// order: the next piece of the large payload being written, of at most
    /// <see cref="ServerLimits.MaxEncodingBufferBytes"/>, because the pieces of one PUBLISH follow each other; then,
    /// the delivery in progress being written whole and no longer counted, the waiting control packets, all of them
    /// as one piece, which so never wait behind a delivery; then the first waiting delivery, whose queue goes back to
    /// the pool when it was the last; and with none of them the send path is given up.
    /// </summary>
    /// <param name="delivery">
    /// The delivery taken when the result is <see cref="SendStep.Deliver"/>.
    /// </param>
    /// <param name="piece">
    /// The piece to write when the result is <see cref="SendStep.Write"/>.
    /// </param>
    /// <param name="buffer">
    /// The rented buffer <paramref name="piece"/> lies in; <see langword="null"/> for a piece of the host's memory.
    /// </param>
    /// <param name="emptied">
    /// The queue of waiting deliveries when its last delivery was taken, to go back to the pool once the lock is
    /// released; otherwise <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see cref="SendStep.Write"/>, <see cref="SendStep.Deliver"/> or <see cref="SendStep.Idle"/>.
    /// </returns>
    private SendStep TakeOpenStep(out PendingDelivery delivery, out ReadOnlyMemory<byte> piece, out byte[]? buffer,
        out WaitingDeliveries? emptied)
    {
        Debug.Assert(_sendLock.IsHeldByCurrentThread, "The caller holds the send path's lock.");
        delivery = default;
        piece = default;
        buffer = null;
        emptied = null;
        if (!_remainingPayload.IsEmpty)
        {
            // With the last piece taken, the reference to the host's memory goes too (design 6.8 rule 2).
            int length = Math.Min(_remainingPayload.Length, ServerLimits.MaxEncodingBufferBytes);
            piece = _remainingPayload[..length];
            _remainingPayload = length == _remainingPayload.Length ? default : _remainingPayload[length..];
            return SendStep.Write;
        }

        _outgoingBytes -= _writingBytes;
        _writingBytes = 0;
        if (_control.PacketCount > 0)
        {
            buffer = _control.Detach(out int length);
            _outgoingBytes -= length;
            piece = buffer.AsMemory(start: 0, length);
            return SendStep.Write;
        }

        if (_waiting is not { } waiting)
        {
            _writing = false;
            return SendStep.Idle;
        }

        // Task 10: the first waiting delivery is taken only when its QoS has room in the client's window. When it
        // has none, the send path is given up with the deliveries still waiting.
        delivery = waiting.Peek();
        waiting.Dequeue();
        _writingBytes = delivery.Payload.Length;
        if (waiting.Count == 0)
        {
            emptied = waiting;
            _waiting = null;
        }

        return SendStep.Deliver;
    }

    /// <summary>
    /// Awaits a write that did not complete at once, then carries the send path on: the buffer goes back, the next
    /// piece is written, and a write that failed begins the close with <see cref="MqttCloseReason.ConnectionLost"/>.
    /// Only this method ends such a write and returns its buffer, after the await has ended (design 6.8). It carries
    /// on on the thread that completes the write, for as long as the writes that follow complete at once. A write
    /// that has completed by the time this looks at it is not awaited at all, so this runs on the stack of the caller
    /// that started it: it then counts as one more of that caller's writes, so that such calls cannot nest deeper
    /// than <see cref="InlineWrites"/> before the send path is handed to the thread pool.
    /// </summary>
    /// <param name="write">
    /// The write in progress.
    /// </param>
    /// <param name="buffer">
    /// The rented buffer the write was given, or <see langword="null"/> for a piece of the host's memory.
    /// </param>
    /// <param name="inlineWrites">
    /// How many writes that complete at once the caller that started the write still makes on its thread.
    /// </param>
    /// <returns>
    /// A task that completes when the send path has been carried on; it never fails.
    /// </returns>
    private async Task CompleteWriteAsync(ValueTask write, byte[]? buffer, int inlineWrites)
    {
        int writesLeft = write.IsCompleted ? Math.Min(inlineWrites, InlineWrites) - 1 : int.MaxValue;
        bool failed = false;
        try
        {
            await write.ConfigureAwait(false);
        }
        catch (Exception)
        {
            failed = true;
        }

        Volatile.Write(ref _writeStartedAt, value: 0);
        ContinueSending(buffer, failed, writesLeft);
    }

    /// <summary>
    /// Awaits a WebSocket close that did not complete at once, then gives the send path up. A close that failed
    /// aborts the link, so that the connection ends now and not at the close deadline.
    /// </summary>
    /// <param name="close">
    /// The close in progress.
    /// </param>
    /// <returns>
    /// A task that completes when the send path is idle; it never fails.
    /// </returns>
    private async Task CompleteCloseAsync(ValueTask close)
    {
        try
        {
            await close.ConfigureAwait(false);
        }
        catch (Exception)
        {
            RunningLink.Abort();
        }

        ContinueSending(returned: null, failed: false, int.MaxValue);
    }

    /// <summary>
    /// Hands a piece to the link. A link that throws instead of returning a failed task is treated the same way.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <param name="piece">
    /// The bytes to write as one WebSocket message.
    /// </param>
    /// <returns>
    /// The write; it may have completed, with success or with an error.
    /// </returns>
    private static ValueTask StartWrite(IMqttLink link, ReadOnlyMemory<byte> piece)
    {
        try
        {
            return link.WriteAsync(piece, CancellationToken.None);
        }
        catch (Exception exception)
        {
            return ValueTask.FromException(exception);
        }
    }

    /// <summary>
    /// Hands the WebSocket close to the link. A link that throws instead of returning a failed task is treated the
    /// same way.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <param name="status">
    /// The WebSocket status the peer sees.
    /// </param>
    /// <returns>
    /// The close; it may have completed, with success or with an error.
    /// </returns>
    private static ValueTask StartClose(IMqttLink link, WebSocketCloseStatus status)
    {
        try
        {
            return link.CloseAsync(status, CancellationToken.None);
        }
        catch (Exception exception)
        {
            return ValueTask.FromException(exception);
        }
    }

    /// <summary>
    /// Takes the outcome of an operation of the link that has completed, which also lets the link reuse what backs
    /// the task.
    /// </summary>
    /// <param name="completed">
    /// The operation, which has completed.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when it succeeded; <see langword="false"/> when it failed or was cancelled.
    /// </returns>
    private static bool HasSucceeded(ValueTask completed)
    {
        if (completed.IsCompletedSuccessfully)
        {
            completed.GetAwaiter().GetResult();
            return true;
        }

        // Takes the failure out of the operation, so that it is observed here and reported nowhere else.
        _ = completed.AsTask().Exception;
        return false;
    }
}
