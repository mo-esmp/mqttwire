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
                BeginClose(MqttCloseReason.ConnectionLost, MqttReasonCode.Success, reasonString: null);
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
            RefuseFirstPacket(MqttCloseReason.MalformedPacket);
            return ConnectFrame.Refused;
        }

        if (header.Type != PacketType.Connect)
        {
            RefuseFirstPacket(MqttCloseReason.ProtocolError);
            return ConnectFrame.Refused;
        }

        if (header.TotalLength > ServerLimits.MaxControlPacketBytes)
        {
            // Answered from the fixed header alone, before the buffer grows: CONNACK 0x95.
            BeginClose(MqttCloseReason.ProtocolError, MqttReasonCode.PacketTooLarge, reasonString: null);
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
    /// hands every complete packet to <see cref="HandlePacket"/>, and reads on while a packet is incomplete. The
    /// buffer is back in the pool before it returns, on every way out. It uses the pooling builder, so no state of
    /// it outlives the call.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when every byte read was handled and the connection waits for more;
    /// <see langword="false"/> when the peer ended the link or the connection is ending, and the close has begun.
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
            }

            while (true)
            {
                if (mustRead)
                {
                    int read = await link.ReadAsync(_readBuffer.GetReadMemory(), HttpContext.RequestAborted)
                        .ConfigureAwait(false);
                    if (read == 0)
                    {
                        BeginClose(MqttCloseReason.ConnectionLost, MqttReasonCode.Success, reasonString: null);
                        return false;
                    }

                    _readBuffer.Advance(read);
                }

                mustRead = true;
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
    /// Frames the packets in the read buffer and hands each complete one to <see cref="HandlePacket"/>, in order.
    /// A malformed fixed header ends the connection with 0x81, and a packet longer than any the server reads with
    /// 0x95. When a packet is incomplete, the buffer is compacted or grown so the rest of it fits.
    /// </summary>
    /// <returns>
    /// What is left in the buffer.
    /// </returns>
    private FramedPackets HandleFramedPackets()
    {
        while (true)
        {
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

            if (status != OperationStatus.Done)
            {
                BeginClose(MqttCloseReason.MalformedPacket, MqttReasonCode.MalformedPacket, reasonString: null);
                return FramedPackets.Ended;
            }

            // The bound on growth until each packet type has its own limit: the largest packet of any type.
            if (header.TotalLength > Math.Max(ServerLimits.MaxControlPacketBytes,
                    ServerLimits.PublishOverheadBytes + _server.Options.MaxPublishPayloadBytes))
            {
                BeginClose(MqttCloseReason.ProtocolError, MqttReasonCode.PacketTooLarge, reasonString: null);
                return FramedPackets.Ended;
            }

            if (data.Length < header.TotalLength)
            {
                MakeRoom(header.TotalLength);
                return FramedPackets.NeedMoreData;
            }

            bool handled = HandlePacket(header, data[..header.TotalLength]);
            _readBuffer.Consume(header.TotalLength);
            if (!handled)
            {
                return FramedPackets.Ended;
            }
        }
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
    /// Handles one complete packet received after the CONNACK. For now a DISCONNECT ends the connection with
    /// <see cref="MqttCloseReason.ClientDisconnected"/>, and every other packet is accepted and ignored: the limits,
    /// the deadlines and what each packet does come with later tasks.
    /// </summary>
    /// <param name="header">
    /// The packet's fixed header.
    /// </param>
    /// <param name="packet">
    /// The whole packet, fixed header included; a view of the read buffer, valid only during the call.
    /// </param>
    /// <returns>
    /// <see langword="true"/> to go on with the next packet; <see langword="false"/> when the connection is ending,
    /// and the close has begun.
    /// </returns>
#pragma warning disable S1172, IDE0060 // The packet's bytes are read once the packets are handled in earnest.
    private bool HandlePacket(in PacketHeader header, ReadOnlySpan<byte> packet)
#pragma warning restore S1172, IDE0060
    {
        if (header.Type == PacketType.Disconnect)
        {
            BeginClose(MqttCloseReason.ClientDisconnected, MqttReasonCode.Success, reasonString: null);
            return false;
        }

        return true;
    }
}
