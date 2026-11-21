using System.Runtime.InteropServices;
using MqttWire.Client;
using MqttWire.Codec;
using MqttWire.Link;

namespace MqttWire;

/// <content>
/// The read loop: reading, framing and handling the packets the server sends.
/// </content>
public sealed partial class MqttWireClient
{
    /// <summary>
    /// What the read loop does after handling the buffered packets.
    /// </summary>
    private enum EndStep
    {
        /// <summary>
        /// Carry on reading.
        /// </summary>
        None,

        /// <summary>
        /// Send the acknowledgement in <see cref="_reply"/>, then carry on handling the buffered packets.
        /// </summary>
        SendReply,

        /// <summary>
        /// This read loop recorded the end: close the link without a DISCONNECT.
        /// </summary>
        Close,

        /// <summary>
        /// This read loop recorded the end for a fault in the server's packets: send DISCONNECT with the fault's
        /// code, then close the link.
        /// </summary>
        DisconnectAndClose,
    }

    /// <summary>
    /// The read loop: waits for data holding no buffer, reads, handles every whole packet, until the link ends.
    /// Then it finishes the connection.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <returns>
    /// A task that completes when the connection is finished. It does not throw: should finishing fail, which only
    /// a bug in the client can cause, the connect and <see cref="DisposeAsync"/> fail with that exception instead
    /// of waiting for ever.
    /// </returns>
    private async Task ReadLoopAsync(IMqttLink link)
    {
        try
        {
            try
            {
                bool reading = true;
                while (reading)
                {
                    await link.WaitForDataAsync(CancellationToken.None).ConfigureAwait(false);
                    reading = await ReadAndHandleAsync(link).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                // A failed read, or the link aborted: the first reason recorded wins.
                TryBeginEnd(new MqttClientCloseInfo(MqttClientCloseReason.ConnectionLost, MqttReasonCode.Success, null, ex), EndMode.Abort);
            }

            FinishConnection(link);
        }
        catch (Exception ex)
        {
            // Only a bug in the client gets here. Release the link and fail whoever waits, rather than hang them.
            link.Abort();
            SetClosed();

            // Both are set with the link, before the read loop started.
            _connectResult!.TrySetException(ex);
            _ended!.TrySetException(ex);
        }
    }

    /// <summary>
    /// Reads once into the receive buffer and handles every whole packet in it, sending the acknowledgement a
    /// packet asks for before the next packet is handled. Once the end has begun, what is read is dropped
    /// unhandled, until the server ends the link or the close deadline aborts it.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the link has ended; <see langword="true"/> to read again.
    /// </returns>
    /// <exception cref="Exception">
    /// The read failed; the read loop records it.
    /// </exception>
    private async ValueTask<bool> ReadAndHandleAsync(IMqttLink link)
    {
        int read = await link.ReadAsync(_receive.GetReadMemory(), CancellationToken.None).ConfigureAwait(false);
        if (read == 0)
        {
            _receive.Clear();
            if (TryBeginEnd(new MqttClientCloseInfo(MqttClientCloseReason.ConnectionLost, MqttReasonCode.Success, null, null), EndMode.Close))
            {
                // Answer the server's close.
                await CloseLinkAsync(link, null).ConfigureAwait(false);
            }

            return false;
        }

        _receive.Advance(read);
        while (true)
        {
            if (_phase >= Phase.Closing)
            {
                _receive.Clear();
                return true;
            }

            EndStep step = HandleBuffered(out MqttReasonCode disconnect);
            if (step == EndStep.SendReply)
            {
                // Awaited before the next packet is handled, so the acknowledgements go out in the order the
                // packets came (MQTT 4.6). A failed write ends the connection, which the next turn sees.
                await SendReplyAsync().ConfigureAwait(false);
                continue;
            }

            if (step == EndStep.None)
            {
                _receive.Compact();
                return true;
            }

            _receive.Clear();
            await CloseLinkAsync(link, step == EndStep.DisconnectAndClose ? disconnect : null).ConfigureAwait(false);
            CompleteFailedConnect();
            return true;
        }
    }

    /// <summary>
    /// Frames and handles every whole packet in the receive buffer, until one asks for a reply or ends the
    /// connection.
    /// </summary>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code when the result is <see cref="EndStep.DisconnectAndClose"/>.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.None"/> when every whole packet is handled; <see cref="EndStep.SendReply"/> when the
    /// packet just handled, already consumed, asks for the reply in <see cref="_reply"/>; otherwise what this read
    /// loop, which recorded the end, does next.
    /// </returns>
    private EndStep HandleBuffered(out MqttReasonCode disconnect)
    {
        while (true)
        {
            FrameStatus status = _receive.TryFrame(out PacketHeader header);
            if (status == FrameStatus.NeedMoreData)
            {
                disconnect = MqttReasonCode.Success;
                return EndStep.None;
            }

            EndStep step;
            if (status == FrameStatus.Done)
            {
                ReadOnlySpan<byte> packet = _receive.GetPacket(header);
                step = _phase == Phase.Connecting
                    ? HandleConnAck(header, packet, out disconnect)
                    : HandlePacket(header, packet, out disconnect);
                _receive.Consume(header.TotalLength);
            }
            else
            {
                step = EndStep.DisconnectAndClose;
                disconnect = status == FrameStatus.PacketTooLarge ? MqttReasonCode.PacketTooLarge : MqttReasonCode.MalformedPacket;
            }

            if (step == EndStep.DisconnectAndClose
                && !TryBeginEnd(new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, disconnect, null, null), EndMode.Close))
            {
                // Another reason was recorded first; its recorder closes the link.
                return EndStep.None;
            }

            if (step != EndStep.None || _phase >= Phase.Closing)
            {
                return step;
            }
        }
    }

    /// <summary>
    /// Handles a packet after the CONNACK, by its type.
    /// </summary>
    /// <param name="header">
    /// The packet's fixed header.
    /// </param>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code when the result is <see cref="EndStep.DisconnectAndClose"/>.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.None"/> to carry on; <see cref="EndStep.SendReply"/> when the packet is answered with the
    /// reply in <see cref="_reply"/>; <see cref="EndStep.Close"/> for a DISCONNECT from the server this call
    /// recorded; <see cref="EndStep.DisconnectAndClose"/> for a malformed packet or one the server may not send.
    /// </returns>
    private EndStep HandlePacket(in PacketHeader header, ReadOnlySpan<byte> packet, out MqttReasonCode disconnect)
    {
        switch (header.Type)
        {
            case PacketType.Disconnect:
                return HandleDisconnect(packet, out disconnect);

            case PacketType.PingResp:
                disconnect = PingCodec.TryDecode(packet);
                if (disconnect != MqttReasonCode.Success)
                {
                    return EndStep.DisconnectAndClose;
                }

                // Keep-alive (Task 8) reads when the last answer came.
                Volatile.Write(ref _lastPingResponseTimestamp, _time.GetTimestamp());
                return EndStep.None;

            case PacketType.PubAck or PacketType.PubRec or PacketType.PubComp:
                return HandlePublishAck(packet, out disconnect);

            case PacketType.Publish or PacketType.PubRel or PacketType.SubAck or PacketType.UnsubAck:
                // Tasks 6 and 7 handle these. Until then the client subscribes to nothing, so a server that sends
                // one breaks the protocol.
                disconnect = MqttReasonCode.ProtocolError;
                return EndStep.DisconnectAndClose;

            default:
                // CONNECT, a second CONNACK, SUBSCRIBE, UNSUBSCRIBE, PINGREQ and AUTH: a server never sends these to
                // this client.
                disconnect = MqttReasonCode.ProtocolError;
                return EndStep.DisconnectAndClose;
        }
    }

    /// <summary>
    /// Handles a DISCONNECT from the server: records the end with its reason code and reason string. The client
    /// sends no DISCONNECT back; it closes the link.
    /// </summary>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code when the result is <see cref="EndStep.DisconnectAndClose"/>.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.Close"/> when this call recorded the end; <see cref="EndStep.None"/> when another
    /// reason was first; <see cref="EndStep.DisconnectAndClose"/> for a malformed DISCONNECT.
    /// </returns>
    private EndStep HandleDisconnect(ReadOnlySpan<byte> packet, out MqttReasonCode disconnect)
    {
        disconnect = DisconnectCodec.TryDecode(packet, out DisconnectPacket received);
        if (disconnect != MqttReasonCode.Success)
        {
            return EndStep.DisconnectAndClose;
        }

        MqttClientCloseInfo info = new(MqttClientCloseReason.ServerDisconnected, received.ReasonCode, ReadReasonString(received.Properties), null);
        return TryBeginEnd(info, EndMode.Close) ? EndStep.Close : EndStep.None;
    }

    /// <summary>
    /// Records the acknowledgement the read loop sends once the packet being handled is consumed, before it
    /// handles the next one.
    /// </summary>
    /// <param name="type">
    /// PUBACK, PUBREC, PUBREL or PUBCOMP.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier it carries.
    /// </param>
    /// <param name="reasonCode">
    /// Its reason code; a success is written without one.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.SendReply"/>, for the handler to return.
    /// </returns>
    private EndStep RecordReply(PacketType type, ushort packetIdentifier, MqttReasonCode reasonCode)
    {
        _reply = new Reply(type, packetIdentifier, reasonCode);
        return EndStep.SendReply;
    }

    /// <summary>
    /// Encodes the acknowledgement in <see cref="_reply"/> into a rented buffer and sends it through the send lock.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the link took it; <see langword="false"/> when the end had begun, or the write
    /// failed and ended the connection.
    /// </returns>
    private ValueTask<bool> SendReplyAsync()
    {
        Reply reply = _reply;
        byte[] packet = _pool.Rent(AckCodec.GetSize(reply.ReasonCode, null));
        int length = AckCodec.Write(packet, reply.Type, reply.PacketIdentifier, reply.ReasonCode, null);
        return SendAsync(packet, length, isDisconnect: false);
    }

    /// <summary>
    /// An acknowledgement the read loop owes the server: PUBACK, PUBREC, PUBREL or PUBCOMP, without properties.
    /// </summary>
    /// <param name="Type">
    /// The packet type.
    /// </param>
    /// <param name="PacketIdentifier">
    /// The packet identifier it carries.
    /// </param>
    /// <param name="ReasonCode">
    /// Its reason code.
    /// </param>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct Reply(PacketType Type, ushort PacketIdentifier, MqttReasonCode ReasonCode);
}
