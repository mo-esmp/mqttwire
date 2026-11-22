using System.Buffers;
using System.Diagnostics;
using System.Net.WebSockets;
using MqttWire.Codec;
using MqttWire.Link;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// Sending (design 6.6, "Sending: taken at once, written when the socket allows"): the one send path every write
/// to the link goes through. A link takes one write at a time, so one flag, <see cref="_writing"/>, says who may
/// write: whoever sets it holds the send path, starts the write, and, when the write has ended, either takes the
/// next thing to write or clears the flag, in one hold of <see cref="_sendLock"/>. Everyone else adds to what
/// waits and returns. The lock is never held across a write, a close, an abort or a handler call. Whatever throws
/// while the send path is held (an encoding, the pool) is caught where the holder runs and ends the connection
/// (<see cref="FaultSendPath"/>), so the path is never left taken.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// How many packets a caller writes on its own thread, while the writes complete at once, before it hands the
    /// send path to the thread pool, so that a host's <see cref="TrySend"/>, or the read loop, never writes for ever
    /// for the callers that keep adding behind it. The pieces of one PUBLISH count as one packet.
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

        /// <summary>
        /// Something is left to write, but the caller has made its writes: the send path, still taken, is to be
        /// handed to the thread pool, which takes the next thing itself.
        /// </summary>
        HandOver,
    }

    /// <summary>
    /// Takes a message for the client or refuses it, and returns at once (design 6.6): a connection that is not
    /// Active refuses it; a message too large for the client or for <see cref="MqttWireServerOptions.MaxOutgoingBytes"/>
    /// is refused and closes the connection with 0x95; with nothing in progress, nothing waiting and, at QoS 1 or 2,
    /// room in the client's window, the message is written now, by this caller, a QoS 1 or 2 one with its packet
    /// identifier and exchange record taken in the same hold; otherwise it waits behind the write in progress or for
    /// the window, unless one more would pass <see cref="MqttWireServerOptions.MaxOutgoingMessages"/> or
    /// <see cref="MqttWireServerOptions.MaxOutgoingBytes"/>, which evicts the connection with 0x97. Allocates nothing
    /// unless a write does not complete at once.
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
    /// message that was not taken is held nowhere. A fault inside MqttWire while this caller encodes the message
    /// (<see cref="FaultSendPath"/>) is not thrown: it ends the connection, and the result is
    /// <see cref="MqttSendResult.NotOpen"/>.
    /// </returns>
    private MqttSendResult TakeDelivery(MqttTopic topic, ReadOnlyMemory<byte> payload, MqttQoS qos)
    {
        MqttWireServerOptions options = _server.Options;
        int payloadLength = payload.Length;
        bool tooLarge = payloadLength > options.MaxOutgoingBytes || payloadLength > MaxDeliveryPayloadBytes
            || GetDeliverySize(topic, payloadLength, qos) > _clientMaxPacketSize;
        long now = qos == MqttQoS.AtMostOnce ? 0 : _server.Time.GetTimestamp();
        ushort packetIdentifier = 0;
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
            else if (!_writing && _waiting is null && HasWindowRoom(qos))
            {
                // This caller writes the message; its record is in place before its packet is handed to the link.
                _writing = true;
                _writingBytes = payloadLength;
                _outgoingBytes += payloadLength;
                packetIdentifier = TakeDeliveryRecord(qos, now);
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
            result = WriteTakenDelivery(new PendingDelivery(topic, payload, qos), packetIdentifier);
        }

        return result;
    }

    /// <summary>
    /// Encodes and writes a delivery that <see cref="TakeDelivery"/> decided to write at once; called by the caller
    /// that took the send path for it, outside the lock.
    /// </summary>
    /// <param name="delivery">
    /// The delivery.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier its exchange record took, at QoS 1 and 2; 0 at QoS 0.
    /// </param>
    /// <returns>
    /// <see cref="MqttSendResult.Taken"/> when the message was handed to the link, whatever followed;
    /// <see cref="MqttSendResult.NotOpen"/> when its encoding threw inside MqttWire, which ended the connection.
    /// </returns>
    private MqttSendResult WriteTakenDelivery(in PendingDelivery delivery, ushort packetIdentifier)
    {
        ReadOnlyMemory<byte> piece;
        byte[] buffer;
        try
        {
            piece = EncodeDelivery(delivery, packetIdentifier, out buffer);
        }
        catch (Exception exception)
        {
            // A fault inside MqttWire, not the host's bug: the message was handed nowhere, and the encoding gave back
            // what it had rented. The host is told the connection is not open, which is now true.
            FaultSendPath(exception, unsent: null);
            return MqttSendResult.NotOpen;
        }

        // From here the message is with the link: a fault after it ends the connection, and the message was taken.
        StartSending(piece, buffer, InlineWrites);
        return MqttSendResult.Taken;
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
    /// Sends an answer of the read path and returns at once (design 6.6). The answer is encoded into a rented buffer
    /// first, so that nothing can fail once the send path's state has changed. Then, in one hold of the lock: a
    /// connection that is no longer Active drops it, so that nothing follows a DISCONNECT; with no write in progress
    /// this caller takes the send path and writes the buffer; with one in progress the answer is copied behind the
    /// control packets that wait, which are written before any waiting delivery, except that a PINGRESP is not added
    /// to one that waits, and that an answer that would pass 1,024 waiting control packets, the 64 KiB one control
    /// buffer holds, or <see cref="MqttWireServerOptions.MaxOutgoingBytes"/> evicts the connection with 0x97. Anything
    /// else that throws, the pool when the answer or the control buffer is rented, or the write path once the answer
    /// is with it, ends the connection as a fault (<see cref="FaultSendPath"/>); the caller then finds it Closing.
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
        bool isPingResp = answer.Type == PacketType.PingResp;
        byte[]? buffer = null;
        bool write = false;
        bool evict = false;
        try
        {
            buffer = pool.Rent(size);
            answer.Write(buffer.AsSpan(start: 0, size));
            write = TryTakeOrAddAnswer(buffer, size, isPingResp, answer.EndsIncomingPublish, out evict);
        }
        catch (Exception exception)
        {
            // Nothing of the send path changed: the hold that takes the path cannot throw once it has, and one that
            // adds throws, if at all, when the control buffer rents, before it counts anything.
            if (buffer is not null)
            {
                pool.Return(buffer);
            }

            if (exception is ArgumentException)
            {
                throw;
            }

            EndFaulted(exception);
            return;
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
    /// The one hold of the lock in which an encoded answer of the read path either takes the send path or is added
    /// behind the control packets that wait, as <see cref="SendAnswer"/> says.
    /// </summary>
    /// <param name="buffer">
    /// The rented buffer the answer is encoded in; it stays the caller's.
    /// </param>
    /// <param name="size">
    /// The answer's size in bytes.
    /// </param>
    /// <param name="isPingResp">
    /// Whether the answer is a PINGRESP.
    /// </param>
    /// <param name="endsIncomingPublish">
    /// Whether the answer is the last one to a QoS 1 or 2 publish of the client.
    /// </param>
    /// <param name="evict">
    /// <see langword="true"/> when the answer would pass a limit of what waits, so the connection is to be evicted.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the caller now holds the send path and is to write the buffer; otherwise
    /// <see langword="false"/>: the answer was copied behind what waits, dropped, or refused.
    /// </returns>
    /// <exception cref="Exception">
    /// The pool threw when the control buffer was rented or grown; nothing changed.
    /// </exception>
    private bool TryTakeOrAddAnswer(byte[] buffer, int size, bool isPingResp, bool endsIncomingPublish,
        out bool evict)
    {
        bool write = false;
        evict = false;
        ArrayPool<byte> pool = _server.Pool;
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
                _control.Commit(size, isPingResp, endsIncomingPublish);
                _outgoingBytes += size;
            }
        }

        return write;
    }

    /// <summary>
    /// Writes a piece and whatever is to be written after it, for as long as the writes complete at once; called by
    /// whoever holds the send path, outside the lock. It is a loop, not a recursion: each write that has ended
    /// returns its buffer and takes the next piece in one hold of the lock (<see cref="TryTakeNextPiece"/>). A write
    /// that does not complete at once is handed to <see cref="CompleteWriteAsync"/>, which carries on when it has
    /// ended, and this returns: nobody waits for the link. It never throws: a fault while it holds the send path ends
    /// the connection (<see cref="FaultSendPath"/>).
    /// </summary>
    /// <param name="piece">
    /// The bytes to write as one WebSocket message.
    /// </param>
    /// <param name="buffer">
    /// The rented buffer the piece lies in, returned when the write has ended; <see langword="null"/> for a piece
    /// of the host's memory, which is never returned to a pool.
    /// </param>
    /// <param name="inlineWrites">
    /// How many packets whose writes complete at once this thread still writes before it hands the send path to the
    /// thread pool; the pieces of one PUBLISH count once, when its last piece has been written.
    /// </param>
    private void StartSending(ReadOnlyMemory<byte> piece, byte[]? buffer, int inlineWrites)
    {
        try
        {
            IMqttLink link = RunningLink;
            while (true)
            {
                ValueTask write = StartWrite(link, piece);
                if (!write.IsCompleted)
                {
                    // Stamped only for a write that stays with the link: one that completed at once needs no
                    // deadline. The stamp is cleared by CompleteWriteAsync, which starts after this line, so it is
                    // never left set.
                    Volatile.Write(ref _writeStartedAt, _server.Time.GetTimestamp());
                    _ = CompleteWriteAsync(write, buffer, inlineWrites);
                    return;
                }

                bool failed = !HasSucceeded(write);
                if (_remainingPayload.IsEmpty)
                {
                    // A packet has been written whole: one PUBLISH counts once, whatever its pieces.
                    inlineWrites--;
                }

                if (!TryTakeNextPiece(buffer, failed, inlineWrites, out piece, out buffer))
                {
                    return;
                }
            }
        }
        catch (Exception exception)
        {
            // Every buffer of this loop is with the link or was taken back by the code that saw its write end, and
            // the encoding of a delivery gives back its own: the holder has nothing of its own left to return.
            FaultSendPath(exception, unsent: null);
        }
    }

    /// <summary>
    /// Carries the send path on after a write, or nothing, has ended: returns the buffer, takes the next piece and
    /// writes it. Called by whoever holds the send path, outside the lock: the completion of a write or of a close,
    /// the thread pool after a hand-over, a removal of a delivery record that wakes the deliveries waiting for the
    /// window, and a holder that has nothing of its own to write (a close without a packet, a CONNACK that is not to
    /// be written). It never throws: a fault while it holds the send path ends the connection
    /// (<see cref="FaultSendPath"/>), so that nothing is left in a task that nobody observes.
    /// </summary>
    /// <param name="returned">
    /// The rented buffer of the write that has ended, or <see langword="null"/>.
    /// </param>
    /// <param name="failed">
    /// Whether that write failed.
    /// </param>
    /// <param name="inlineWrites">
    /// How many packets whose writes complete at once this thread writes before it hands the send path to the thread
    /// pool.
    /// </param>
    private void ContinueSending(byte[]? returned, bool failed, int inlineWrites)
    {
        try
        {
            if (TryTakeNextPiece(returned, failed, inlineWrites, out ReadOnlyMemory<byte> piece, out byte[]? buffer))
            {
                StartSending(piece, buffer, inlineWrites);
            }
        }
        catch (Exception exception)
        {
            // As in StartSending: the buffer of the write that ended was taken back first, and nothing else is held.
            FaultSendPath(exception, unsent: null);
        }
    }

    /// <summary>
    /// Ends a write and finds the next piece; called by whoever holds the send path, outside the lock. In order: the
    /// buffer of the write that has ended goes back to the pool, here and nowhere else (design 6.8 rule 4); a failed
    /// write begins the close with <see cref="MqttCloseReason.ConnectionLost"/> and aborts the link; then
    /// <see cref="TakeNextStep"/> decides, in one hold of the lock, what is next, and a caller that has used up its
    /// writes and finds something left hands the send path to the thread pool. A delivery is encoded here; the
    /// WebSocket close of a Closing connection is handed to the link here, and awaited elsewhere when it does not
    /// complete at once.
    /// </summary>
    /// <param name="returned">
    /// The rented buffer of the write that has ended, or <see langword="null"/>.
    /// </param>
    /// <param name="failed">
    /// Whether that write failed.
    /// </param>
    /// <param name="inlineWrites">
    /// How many packets this thread still writes; at 0 or below, the send path is handed to the thread pool when
    /// something is left to write, and given up when nothing is.
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

        while (true)
        {
            SendStep step = TakeNextStep(failed, handOver: inlineWrites <= 0, out PendingDelivery delivery,
                out ushort packetIdentifier, out piece, out buffer, out WebSocketCloseStatus closeStatus);
            switch (step)
            {
                case SendStep.Idle:
                    return false;

                case SendStep.Write:
                    return true;

                case SendStep.Deliver:
                    piece = EncodeDelivery(delivery, packetIdentifier, out byte[] encoded);
                    buffer = encoded;
                    return true;

                case SendStep.HandOver:
                    // The send path stays taken across the hand-over, so nobody else writes and nothing is lost:
                    // whatever is added meanwhile waits for the thread pool's hold, which takes the next thing itself.
                    ThreadPool.UnsafeQueueUserWorkItem(
                        static connection => connection.ContinueSending(returned: null, failed: false, int.MaxValue),
                        this, preferLocal: false);
                    return false;

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
    /// first waiting delivery when it has room in the client's window; with none of them, it goes idle
    /// (<see cref="TakeOpenStep"/>), with deliveries still waiting when the first has no room. What was dropped goes
    /// back once the lock is released: the queue of deliveries to the server's pool, the control buffer to the buffer
    /// pool, the exchange records to theirs; and whoever waits for the send path to be idle is told then. This is the
    /// one place where the end of a connection drops what it holds on the way out, the exchange records included:
    /// nothing is added to a Closing connection, so what is taken here is taken once, and a call after the path was
    /// given up in Closing finds nothing and changes nothing. Should giving those back throw once the path has been
    /// given up, the fault ends the connection (<see cref="EndFaulted"/>) without touching the path again, which may
    /// be another's by then; whoever waits for it to be idle is told all the same.
    /// </summary>
    /// <param name="failed">
    /// Whether the write that has just ended failed, so that no WebSocket close is attempted.
    /// </param>
    /// <param name="handOver">
    /// Whether the caller has made its writes on its thread: a connection that is not Closing and has something left
    /// to write then takes nothing and answers <see cref="SendStep.HandOver"/>.
    /// </param>
    /// <param name="delivery">
    /// The delivery taken when the result is <see cref="SendStep.Deliver"/>.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier the delivery's exchange record took when the result is <see cref="SendStep.Deliver"/>
    /// and its QoS is 1 or 2; otherwise 0.
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
    private SendStep TakeNextStep(bool failed, bool handOver, out PendingDelivery delivery,
        out ushort packetIdentifier, out ReadOnlyMemory<byte> piece, out byte[]? buffer,
        out WebSocketCloseStatus closeStatus)
    {
        delivery = default;
        packetIdentifier = 0;
        piece = default;
        buffer = null;
        closeStatus = default;
        WaitingDeliveries? emptied = null;
        ControlBuffer dropped = default;
        ExchangeRecords? records = null;
        TaskCompletionSource? idle = null;
        SendStep step;

        // Read before the hold, for the record of a delivery taken in it: the clock is not called under the lock.
        long now = _server.Time.GetTimestamp();
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
                step = TakeOpenStep(now, handOver, out delivery, out packetIdentifier, out piece, out buffer,
                    out emptied);
            }
        }

        ReleaseDropped(step, emptied, records, dropped, idle);
        return step;
    }

    /// <summary>
    /// Gives back what the hold of <see cref="TakeNextStep"/> took away, once the lock is released, and tells whoever
    /// waits for the send path to be idle. Should the giving back throw after the hold gave the path up, the fault
    /// ends the connection without touching the path again, which may be another's by then; when the caller still
    /// holds the path, the exception goes on to the caller's own catch, which gives it up.
    /// </summary>
    /// <param name="step">
    /// The step the hold decided.
    /// </param>
    /// <param name="emptied">
    /// The queue of waiting deliveries to go back to the server's pool, or <see langword="null"/>.
    /// </param>
    /// <param name="records">
    /// The exchange records to go back to the server's pool, or <see langword="null"/>.
    /// </param>
    /// <param name="dropped">
    /// The control buffer whose array, never handed to the link, goes back to the buffer pool.
    /// </param>
    /// <param name="idle">
    /// What the end of the run waits on, or <see langword="null"/>; completed whatever happens.
    /// </param>
    private void ReleaseDropped(SendStep step, WaitingDeliveries? emptied, ExchangeRecords? records,
        ControlBuffer dropped, TaskCompletionSource? idle)
    {
        try
        {
            // The pool's policy clears the queue, so no payload or topic reference outlives the connection's use of
            // it.
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
        }
        catch (Exception exception) when (step == SendStep.Idle)
        {
            EndFaulted(exception);
        }
        finally
        {
            idle?.TrySetResult();
        }
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
    /// as one piece, which so never wait behind a delivery; then the first waiting delivery when it has room in the
    /// client's window, taking its packet identifier and record in this hold, its queue going back to the pool when it
    /// was the last; and with none of them the send path is given up. When the first waiting delivery has no room,
    /// the path is given up with the deliveries still waiting, every one of them behind it whatever its QoS: this is
    /// the one case in which deliveries wait while nothing is being written, and the removal of a delivery record by
    /// the read path, which frees a place, takes the send path again (<see cref="EndDeliveryAt"/>). A caller that has
    /// made its writes looks all the same, and takes nothing: with something to write it is told to hand the path
    /// over, still taken; with nothing, the path is given up here, so the thread pool is never woken for nothing.
    /// </summary>
    /// <param name="now">
    /// The time, a timestamp of the server's clock, for the record of a delivery taken.
    /// </param>
    /// <param name="handOver">
    /// Whether the caller has made its writes on its thread, so that it hands over instead of taking.
    /// </param>
    /// <param name="delivery">
    /// The delivery taken when the result is <see cref="SendStep.Deliver"/>.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier of the delivery taken, at QoS 1 and 2; otherwise 0.
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
    /// <see cref="SendStep.Write"/>, <see cref="SendStep.Deliver"/>, <see cref="SendStep.HandOver"/> or
    /// <see cref="SendStep.Idle"/>.
    /// </returns>
    private SendStep TakeOpenStep(long now, bool handOver, out PendingDelivery delivery, out ushort packetIdentifier,
        out ReadOnlyMemory<byte> piece, out byte[]? buffer, out WaitingDeliveries? emptied)
    {
        Debug.Assert(_sendLock.IsHeldByCurrentThread, "The caller holds the send path's lock.");
        delivery = default;
        packetIdentifier = 0;
        piece = default;
        buffer = null;
        emptied = null;
        if (!_remainingPayload.IsEmpty)
        {
            if (handOver)
            {
                return SendStep.HandOver;
            }

            // With the last piece taken, the reference to the host's memory goes too (design 6.8 rule 2).
            int length = Math.Min(_remainingPayload.Length, ServerLimits.MaxEncodingBufferBytes);
            piece = _remainingPayload[..length];
            _remainingPayload = length == _remainingPayload.Length ? default : _remainingPayload[length..];
            return SendStep.Write;
        }

        // Done in a hold that hands over too: the next hold then finds nothing more to take away.
        _outgoingBytes -= _writingBytes;
        _writingBytes = 0;
        if (_control.PacketCount > 0)
        {
            if (handOver)
            {
                return SendStep.HandOver;
            }

            buffer = _control.Detach(out int length);
            _outgoingBytes -= length;
            piece = buffer.AsMemory(start: 0, length);
            return SendStep.Write;
        }

        if (_waiting is not { } waiting || !HasWindowRoom(waiting.Peek().QoS))
        {
            _writing = false;
            return SendStep.Idle;
        }

        if (handOver)
        {
            return SendStep.HandOver;
        }

        delivery = waiting.Peek();
        waiting.Dequeue();
        packetIdentifier = TakeDeliveryRecord(delivery.QoS, now);
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
    /// than <see cref="InlineWrites"/> before the send path is handed to the thread pool. Here every write counts, a
    /// piece of a PUBLISH too, because what is bounded is the depth of the stack. A fault in what follows the write
    /// is caught by <see cref="ContinueSending"/>, so the task never fails and nothing is left unobserved.
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
    /// The last resort of a holder of the send path that something threw at (an encoding whose payload memory throws,
    /// the pool, anything inside MqttWire): the connection is beyond saving. It ends the connection as
    /// <see cref="EndFaulted"/> does, returns the one buffer the holder rented and has not handed to the link, then
    /// gives the path up through the Closing step, which drops what waits, the exchange records and the control
    /// buffer, clears the counts and tells whoever waits for the path to be idle; the last write is taken as failed,
    /// so no WebSocket close is attempted. Called only by a holder, from its catch; it costs nothing on the paths
    /// that do not throw.
    /// </summary>
    /// <param name="exception">
    /// What was thrown.
    /// </param>
    /// <param name="unsent">
    /// The rented buffer the holder still owns and has not handed to the link, or <see langword="null"/>. A buffer
    /// whose write was started is never passed: whoever awaits that write returns it.
    /// </param>
    private void FaultSendPath(Exception exception, byte[]? unsent)
    {
        try
        {
            EndFaulted(exception);
            if (unsent is not null)
            {
                _server.Pool.Return(unsent);
            }
        }
        finally
        {
            // Whatever became of the above, the path is given up, so that the end of the run never waits for ever.
            // The connection is Closing, so this is the Closing step, which never hands the path on.
            SendStep step = TakeNextStep(failed: true, handOver: false, out _, out _, out _, out _, out _);
            Debug.Assert(step == SendStep.Idle, "A Closing step after a failure always gives the send path up.");
        }
    }

    /// <summary>
    /// Ends a connection after a fault inside MqttWire's sending: logs it as an error (event 20, the client id and the
    /// exception; never a payload), begins the close with <see cref="MqttCloseReason.ConnectionLost"/>, unless an
    /// earlier close recorded its own reason, and aborts the link, which a close that had begun already has not
    /// necessarily done. It never touches the send path's own state: when the caller holds the path, the close finds
    /// it taken and aborts; when nobody does, the close takes it and writes its WebSocket close as any close does.
    /// </summary>
    /// <param name="exception">
    /// What was thrown.
    /// </param>
    private void EndFaulted(Exception exception)
    {
        ServerLog.SendPathFaulted(_server.Logger, ClientId, exception);
        BeginClose(MqttCloseReason.ConnectionLost, MqttReasonCode.Success, reasonString: null);
        RunningLink.Abort();
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
