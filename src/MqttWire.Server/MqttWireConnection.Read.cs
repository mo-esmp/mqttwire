using System.Buffers;
using System.Runtime.CompilerServices;
using MqttWire.Codec;
using MqttWire.Link;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// Reading: the read buffer and the framing of the first packet.
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
                BeginClose(MqttCloseReason.ConnectionLost, MqttReasonCode.Success, null);
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
    /// <see cref="ServerLimits.MaxControlPacketBytes"/> begins it with packet too large. When the fixed header is
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
            BeginClose(MqttCloseReason.MalformedPacket, MqttReasonCode.MalformedPacket, null);
            return ConnectFrame.Refused;
        }

        if (header.Type != PacketType.Connect)
        {
            BeginClose(MqttCloseReason.ProtocolError, MqttReasonCode.ProtocolError, null);
            return ConnectFrame.Refused;
        }

        if (header.TotalLength > ServerLimits.MaxControlPacketBytes)
        {
            BeginClose(MqttCloseReason.ProtocolError, MqttReasonCode.PacketTooLarge, null);
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
}
