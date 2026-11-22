using System.Buffers;
using System.Runtime.CompilerServices;
using MqttWire.Codec;
using MqttWire.Link;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// Reading: the read buffer, the framing of the first packet and of the packets after it.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// The reason string of the refusal of a publish too large.
    /// </summary>
    private const string MessageTooLarge = "The message is too large.";

    /// <summary>
    /// The position of the packet type in the first byte of a fixed header.
    /// </summary>
    private const int PacketTypeShift = 4;

    /// <summary>
    /// How a packet is read, judged by its fixed header.
    /// </summary>
    private enum PacketSize
    {
        /// <summary>
        /// The packet is read whole into the buffer.
        /// </summary>
        Whole,

        /// <summary>
        /// The packet is a PUBLISH that is skipped without being buffered, then refused.
        /// </summary>
        Skip,

        /// <summary>
        /// The packet is too large to be read at all: the connection ends with 0x95.
        /// </summary>
        TooLarge,
    }

    /// <summary>
    /// Where <see cref="FeedSkip"/> left the packet being skipped.
    /// </summary>
    private enum SkipStatus
    {
        /// <summary>
        /// The packet has bytes still to come; the buffer is empty.
        /// </summary>
        NeedMoreData,

        /// <summary>
        /// Exactly the packet's length has been consumed.
        /// </summary>
        Complete,

        /// <summary>
        /// The skipped part was broken and the close has begun.
        /// </summary>
        Ended,
    }

    /// <summary>
    /// What <see cref="FrameConnect"/> found in the read buffer.
    /// </summary>
    private enum ConnectFrame
    {
        /// <summary>
        /// The first packet is not complete yet; the buffer has room for the rest.
        /// </summary>
        NeedMoreData,

        /// <summary>
        /// A complete CONNECT is at the start of the buffer.
        /// </summary>
        Complete,

        /// <summary>
        /// The first packet was refused and the close has begun.
        /// </summary>
        Refused,
    }

    /// <summary>
    /// What <see cref="HandleFramedPackets"/> left in the read buffer.
    /// </summary>
    private enum FramedPackets
    {
        /// <summary>
        /// Every byte was handled; the buffer is empty.
        /// </summary>
        AllHandled,

        /// <summary>
        /// A packet is not complete yet; the buffer has room for the rest of it.
        /// </summary>
        NeedMoreData,

        /// <summary>
        /// The connection is ending; nothing more is to be read.
        /// </summary>
        Ended,
    }

    /// <summary>
    /// Reads until a complete CONNECT is in the read buffer. It holds no buffer while it waits for the first byte;
    /// then it rents <see cref="ServerLimits.FirstReadBufferBytes"/> and grows them to the CONNECT's length, at most
    /// <see cref="ServerLimits.MaxControlPacketBytes"/>. The caller returns the buffer on every way out. It uses the
    /// pooling builder, so its state goes back to a pool once the CONNECT is read.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <returns>
    /// The CONNECT's total length; 0 when the peer ended the link or the first packet was refused, and the close
    /// has begun.
    /// </returns>
    /// <exception cref="Exception">
    /// The wait or a read failed.
    /// </exception>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<int> ReadConnectAsync(IMqttLink link)
    {
        await link.WaitForDataAsync(HttpContext.RequestAborted).ConfigureAwait(false);
        _readBuffer.Rent(_server.Pool, ServerLimits.FirstReadBufferBytes);
        while (true)
        {
            int read = await link.ReadAsync(_readBuffer.GetReadMemory(), HttpContext.RequestAborted)
                .ConfigureAwait(false);
            if (read == 0)
            {
                BeginClose(CloseCause.ConnectionLost, MqttReasonCode.Success, reasonString: null);
                return 0;
            }

            _readBuffer.Advance(read);
            ConnectFrame frame = FrameConnect(out int length);
            if (frame != ConnectFrame.NeedMoreData)
            {
                return frame == ConnectFrame.Complete ? length : 0;
            }
        }
    }

    /// <summary>
    /// Looks at the first packet in the read buffer. A packet that is not a CONNECT, or a malformed fixed header,
    /// begins the close with WebSocket status 1002; a CONNECT longer than
    /// <see cref="ServerLimits.MaxControlPacketBytes"/> begins it with CONNACK packet too large. When the fixed header is
    /// whole but the packet is not, the buffer grows to the packet's length.
    /// </summary>
    /// <param name="length">
    /// The CONNECT's total length when the result is <see cref="ConnectFrame.Complete"/>; otherwise 0.
    /// </param>
    /// <returns>
    /// What was found.
    /// </returns>
    private ConnectFrame FrameConnect(out int length)
    {
        length = 0;
        OperationStatus status = PacketHeader.TryRead(_readBuffer.GetData(), out PacketHeader header);
        if (status == OperationStatus.NeedMoreData)
        {
            return ConnectFrame.NeedMoreData;
        }

        if (status != OperationStatus.Done)
        {
            RefuseFirstPacket(CloseCause.MalformedPacket, MqttReasonCode.MalformedPacket);
            return ConnectFrame.Refused;
        }

        if (header.Type != PacketType.Connect)
        {
            RefuseFirstPacket(CloseCause.ProtocolError, MqttReasonCode.ProtocolError);
            return ConnectFrame.Refused;
        }

        if (header.TotalLength > ServerLimits.MaxControlPacketBytes)
        {
            // Answered from the fixed header alone, before the buffer grows: CONNACK 0x95.
            BeginClose(CloseCause.ProtocolError, MqttReasonCode.PacketTooLarge, reasonString: null);
            return ConnectFrame.Refused;
        }

        if (_readBuffer.Count < header.TotalLength)
        {
            if (header.TotalLength > _readBuffer.Capacity)
            {
                _readBuffer.Grow(_server.Pool, header.TotalLength);
            }

            return ConnectFrame.NeedMoreData;
        }

        length = header.TotalLength;
        return ConnectFrame.Complete;
    }

    /// <summary>
    /// Handles what has arrived: rents the read buffer unless bytes from behind the CONNECT hold it already, reads,
    /// hands every complete packet to <see cref="HandlePacket"/>, and reads on while a packet is incomplete or being
    /// skipped. While the connection is Closing, what is read is thrown away. The buffer is back in the pool before
    /// it returns, on every way out. It uses the pooling builder, so no state of it outlives the call.
    /// </summary>
    /// <remarks>
    /// The partial-packet deadline starts when the buffer is rented, not when the first byte is read: the wait can
    /// end with nothing to read yet (a WebSocket frame whose payload the peer withholds), and the read would then
    /// hold the buffer with no deadline but the keep-alive. So a rented read buffer is never held for longer than the
    /// partial-packet deadline without a complete packet.
    /// </remarks>
    /// <returns>
    /// <see langword="true"/> when every byte read was handled, or thrown away because the connection is Closing,
    /// and the connection waits for more; <see langword="false"/> when the peer ended the link or the handling of a
    /// packet began the close.
    /// </returns>
    /// <exception cref="Exception">
    /// A read failed; the run records it. No buffer is held.
    /// </exception>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> HandleAvailableAsync()
    {
        IMqttLink link = RunningLink;
        try
        {
            // Bytes behind the CONNECT are framed before anything is read.
            bool mustRead = !_readBuffer.IsHeld;
            if (mustRead)
            {
                _readBuffer.Rent(_server.Pool, ServerLimits.FirstReadBufferBytes);
                if (Volatile.Read(ref _partialPacketAt) == 0)
                {
                    Volatile.Write(ref _partialPacketAt, _server.Time.GetTimestamp());
                }
            }

            while (true)
            {
                if (mustRead)
                {
                    int read = await link.ReadAsync(_readBuffer.GetReadMemory(), HttpContext.RequestAborted)
                        .ConfigureAwait(false);
                    if (read == 0)
                    {
                        BeginClose(CloseCause.ConnectionLost, MqttReasonCode.Success, reasonString: null);
                        return false;
                    }

                    _readBuffer.Advance(read);
                }

                mustRead = true;
                if (_state >= ConnectionState.Closing)
                {
                    // Closed from elsewhere (the host, the sweep, a takeover): read and thrown away until the link ends.
                    _readBuffer.Consume(_readBuffer.Count);
                    return true;
                }

                FramedPackets framed = HandleFramedPackets();
                if (framed != FramedPackets.NeedMoreData)
                {
                    return framed == FramedPackets.AllHandled;
                }
            }
        }
        finally
        {
            // Every read has ended here, so the buffer can go back.
            _readBuffer.Return(_server.Pool);
        }
    }

    /// <summary>
    /// Frames what is in the read buffer, in order: feeds a packet being skipped, hands each complete packet to
    /// <see cref="HandlePacket"/>, and starts the skip of a PUBLISH too large to be read whole. Then it stamps the
    /// deadlines of reading: the last packet when one was completed, which also clears the stamp of the partial
    /// packet, set when the buffer was rented (<see cref="HandleAvailableAsync"/>); and the partial packet anew when
    /// one is left incomplete behind a packet completed in this call.
    /// </summary>
    /// <returns>
    /// What is left in the buffer.
    /// </returns>
    private FramedPackets HandleFramedPackets()
    {
        FramedPackets framed = FramePackets(out bool completed);
        if (framed == FramedPackets.Ended)
        {
            return framed;
        }

        long now = _server.Time.GetTimestamp();
        if (completed)
        {
            Volatile.Write(ref _lastPacketAt, now);
            Volatile.Write(ref _partialPacketAt, value: 0);
        }

        if (framed == FramedPackets.NeedMoreData && Volatile.Read(ref _partialPacketAt) == 0)
        {
            Volatile.Write(ref _partialPacketAt, now);
        }

        return framed;
    }

    /// <summary>
    /// Frames the packets in the read buffer and handles each complete one, in order. A malformed fixed header ends
    /// the connection with 0x81, one of the reserved type 0 with 0x82, and a PUBLISH whose flags carry a QoS above
    /// <see cref="MqttWireServerOptions.MaxQoS"/> with 0x9B, all from the fixed header alone. A PUBLISH is judged by
    /// its Remaining Length (plan decision 6): up to the payload limit and
    /// <see cref="ServerLimits.PublishOverheadBytes"/> it is read whole; above that and up to
    /// <see cref="ServerLimits.SkipFactor"/> times the limit it is skipped; above both, the connection ends with
    /// 0x95. Any other packet over <see cref="ServerLimits.MaxControlPacketBytes"/> in total ends it with 0x95. When a
    /// packet is incomplete, the buffer is compacted or grown so the rest of it fits; while one is skipped, the
    /// buffer is emptied for the next read and never grows.
    /// </summary>
    /// <param name="completed">
    /// <see langword="true"/> when at least one packet was completed, a skipped one included.
    /// </param>
    /// <returns>
    /// What is left in the buffer.
    /// </returns>
    private FramedPackets FramePackets(out bool completed)
    {
        completed = false;
        while (true)
        {
            if (_publishSkip.IsActive)
            {
                SkipStatus skipped = FeedSkip();
                if (skipped != SkipStatus.Complete)
                {
                    return skipped == SkipStatus.Ended ? FramedPackets.Ended : FramedPackets.NeedMoreData;
                }

                completed = true;
                continue;
            }

            ReadOnlySpan<byte> data = _readBuffer.GetData();
            if (data.IsEmpty)
            {
                return FramedPackets.AllHandled;
            }

            OperationStatus status = PacketHeader.TryRead(data, out PacketHeader header);
            if (status == OperationStatus.NeedMoreData)
            {
                MakeRoom(data.Length + 1);
                return FramedPackets.NeedMoreData;
            }

            if (status != OperationStatus.Done || IsQoSAboveMax(header))
            {
                RefuseHeader(status, data[0]);
                return FramedPackets.Ended;
            }

            PacketSize size = StartPacket(header);
            if (size != PacketSize.Whole)
            {
                if (size == PacketSize.TooLarge)
                {
                    return FramedPackets.Ended;
                }

                continue;
            }

            if (data.Length < header.TotalLength)
            {
                MakeRoom(header.TotalLength);
                return FramedPackets.NeedMoreData;
            }

            completed = true;
            bool handled = HandlePacket(header, data[..header.TotalLength]);
            _readBuffer.Consume(header.TotalLength);
            if (!handled)
            {
                return FramedPackets.Ended;
            }
        }
    }

    /// <summary>
    /// Tells whether a packet is a PUBLISH whose QoS, read from the flags of its fixed header, is above
    /// <see cref="MqttWireServerOptions.MaxQoS"/> (design 6.6, plan decision 6).
    /// </summary>
    /// <param name="header">
    /// The packet's fixed header.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the connection is to end with 0x9B.
    /// </returns>
    private bool IsQoSAboveMax(in PacketHeader header) =>
        header.Type == PacketType.Publish && PublishSkip.GetQoS(header) > _server.Options.MaxQoS;

    /// <summary>
    /// Begins the close for a packet refused from its fixed header alone, before anything is buffered or skipped: a
    /// header that is not valid ends the connection with 0x81, or with 0x82 when its type is the reserved 0, a packet
    /// the server never reads; a valid one is a PUBLISH whose QoS is above
    /// <see cref="MqttWireServerOptions.MaxQoS"/>, and ends it with 0x9B.
    /// </summary>
    /// <param name="status">
    /// What <see cref="PacketHeader.TryRead"/> said of the header: <see cref="OperationStatus.Done"/> or a failure.
    /// </param>
    /// <param name="firstByte">
    /// The packet's first byte.
    /// </param>
    private void RefuseHeader(OperationStatus status, byte firstByte)
    {
        if (status == OperationStatus.Done)
        {
            BeginClose(CloseCause.ProtocolError, MqttReasonCode.QoSNotSupported, reasonString: null);
            return;
        }

        CloseUndecodable(firstByte >> PacketTypeShift == 0
            ? MqttReasonCode.ProtocolError
            : MqttReasonCode.MalformedPacket);
    }

    /// <summary>
    /// Judges a packet by its fixed header against the limit of its type, and acts on what is not read whole: a
    /// packet too large begins the close with 0x95, answered from the fixed header alone before the buffer grows; a
    /// PUBLISH to skip has its skip started and its fixed header consumed.
    /// </summary>
    /// <param name="header">
    /// The packet's fixed header, at the start of the read buffer.
    /// </param>
    /// <returns>
    /// Whether the packet is read whole, skipped, or too large to be read at all.
    /// </returns>
    private PacketSize StartPacket(in PacketHeader header)
    {
        PacketSize size = JudgeSize(header);
        if (size == PacketSize.TooLarge)
        {
            BeginClose(CloseCause.ProtocolError, MqttReasonCode.PacketTooLarge, reasonString: null);
        }
        else if (size == PacketSize.Skip)
        {
            _publishSkip.Start(header);
            _readBuffer.Consume(header.HeaderLength);
        }

        return size;
    }

    /// <summary>
    /// Judges a packet by its fixed header against the limit of its type.
    /// </summary>
    /// <param name="header">
    /// The packet's fixed header.
    /// </param>
    /// <returns>
    /// Whether the packet is read whole, skipped, or too large to be read at all.
    /// </returns>
    private PacketSize JudgeSize(in PacketHeader header)
    {
        if (header.Type != PacketType.Publish)
        {
            return header.TotalLength > ServerLimits.MaxControlPacketBytes ? PacketSize.TooLarge : PacketSize.Whole;
        }

        int limit = _server.Options.MaxPublishPayloadBytes;
        if (header.RemainingLength <= limit + ServerLimits.PublishOverheadBytes)
        {
            return PacketSize.Whole;
        }

        return header.RemainingLength <= (long)ServerLimits.SkipFactor * limit ? PacketSize.Skip : PacketSize.TooLarge;
    }

    /// <summary>
    /// Makes room in the read buffer for an incomplete packet: grows the buffer when the packet is longer than it,
    /// or moves the packet's bytes to the start when the free space after them is too small.
    /// </summary>
    /// <param name="size">
    /// The bytes the packet needs in all, those already read included.
    /// </param>
    private void MakeRoom(int size)
    {
        if (size > _readBuffer.Capacity)
        {
            _readBuffer.Grow(_server.Pool, size);
        }
        else if (_readBuffer.Count + _readBuffer.GetReadMemory().Length < size)
        {
            _readBuffer.Compact();
        }
    }

    /// <summary>
    /// Feeds the bytes in the read buffer to the packet being skipped and drops what it consumed. A broken skipped
    /// part begins the close with its code. When exactly the packet's length has been consumed, the skip ends and the
    /// packet is refused in the form of its QoS. When the packet still has bytes to come, the buffer is empty and is
    /// reset so the next read fills it from its start, at the size it has.
    /// </summary>
    /// <returns>
    /// Where the skip is: <see cref="SkipStatus.Complete"/> when the packet was refused and the connection goes on;
    /// <see cref="SkipStatus.Ended"/> when the connection is ending.
    /// </returns>
    private SkipStatus FeedSkip()
    {
        int consumed = _publishSkip.Feed(_readBuffer.GetData(), out MqttReasonCode error);
        _readBuffer.Consume(consumed);
        if (error != MqttReasonCode.Success)
        {
            CloseUndecodable(error);
            return SkipStatus.Ended;
        }

        if (_publishSkip.IsComplete)
        {
            MqttQoS qos = _publishSkip.QoS;
            ushort packetIdentifier = _publishSkip.PacketIdentifier;
            _publishSkip = default;
            return RefuseTooLarge(qos, packetIdentifier) ? SkipStatus.Complete : SkipStatus.Ended;
        }

        // Every byte read was consumed: the next read starts at the buffer's start.
        _readBuffer.Compact();
        return SkipStatus.NeedMoreData;
    }

    /// <summary>
    /// Refuses a publish too large with 0x97 and "The message is too large." in the form its QoS calls for: a
    /// PUBACK at QoS 1, a failing PUBREC at QoS 2, of which nothing is remembered, and nothing at QoS 0. A QoS 2
    /// publish whose identifier is remembered is a repeat, answered like any other with PUBREC success and nothing
    /// else. At QoS 1 and 2 the publish counts against the receive maximum like any other: one that would pass it
    /// ends the connection with 0x93 instead.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the publish.
    /// </param>
    /// <param name="packetIdentifier">
    /// Its packet identifier; 0 at QoS 0.
    /// </param>
    /// <returns>
    /// <see langword="true"/> to go on with the next packet; <see langword="false"/> when the connection is ending.
    /// </returns>
    private bool RefuseTooLarge(MqttQoS qos, ushort packetIdentifier)
    {
        bool remembered = false;
        if (qos != MqttQoS.AtMostOnce && !TryTakeReceivePlace(qos, packetIdentifier, out remembered))
        {
            return false;
        }

        if (remembered)
        {
            // A repeat of a remembered identifier, like any other (design 6.6, QoS 2 item 2): the host has had it.
            SendAck(PacketType.PubRec, packetIdentifier, MqttReasonCode.Success, reasonString: null);
        }
        else if (qos == MqttQoS.AtLeastOnce)
        {
            SendAck(PacketType.PubAck, packetIdentifier, MqttReasonCode.QuotaExceeded, MessageTooLarge);
        }
        else if (qos == MqttQoS.ExactlyOnce)
        {
            SendAck(PacketType.PubRec, packetIdentifier, MqttReasonCode.QuotaExceeded, MessageTooLarge);
        }

        return _state == ConnectionState.Active;
    }

    /// <summary>
    /// Begins the close for a packet the decoder, or the skip, refused: with <see cref="MqttCloseReason.MalformedPacket"/>
    /// for 0x81, and <see cref="MqttCloseReason.ProtocolError"/> for any other code; the DISCONNECT carries the code.
    /// </summary>
    /// <param name="code">
    /// The code the decoder gave.
    /// </param>
    private void CloseUndecodable(MqttReasonCode code)
    {
        CloseCause cause = code == MqttReasonCode.MalformedPacket
            ? CloseCause.MalformedPacket
            : CloseCause.ProtocolError;
        BeginClose(cause, code, reasonString: null);
    }
}
