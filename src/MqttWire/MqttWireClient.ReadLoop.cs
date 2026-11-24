using System.Runtime.CompilerServices;
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
        /// Returned by a packet's handler only: the packet is answered with the answer in <see cref="_reply"/>, which
        /// <see cref="HandleBuffered"/> records behind the answers waiting before it carries on with the next packet.
        /// </summary>
        RecordAnswer,

        /// <summary>
        /// Returned by <see cref="HandleBuffered"/> only: the answers waiting must be written before the next packet
        /// is handled (<see cref="PendingAnswers.MustWriteBeforeNextPacket"/>); write them, then carry on handling
        /// the buffered packets.
        /// </summary>
        WriteAnswers,

        /// <summary>
        /// This read loop recorded the end: close the link without a DISCONNECT.
        /// </summary>
        Close,

        /// <summary>
        /// This read loop recorded the end for a fault in the server's packets: send DISCONNECT with the fault's
        /// code, then close the link.
        /// </summary>
        DisconnectAndClose,

        /// <summary>
        /// The handler recorded the end itself, with the reason it wanted kept: send DISCONNECT with the handler's
        /// code, then close the link.
        /// </summary>
        DisconnectRecorded,
    }

    /// <summary>
    /// The read loop: waits for data holding no buffer, reads, handles every whole packet, until the link ends.
    /// Then it finishes the connection. When the wait does not complete at once, the answers waiting are written
    /// first, so that no answer waits while the loop does; the wait is awaited once, whatever that write did.
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
                    ValueTask waiting = link.WaitForDataAsync(CancellationToken.None);
                    if (!waiting.IsCompleted && _answers.Count > 0)
                    {
                        // The loop is about to wait: what it owes goes out first. The write does not throw; a failed
                        // one ends the connection, and the wait already started is still awaited, just below.
                        await WriteAnswersAsync().ConfigureAwait(false);
                    }

                    await waiting.ConfigureAwait(false);
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
            // Only a bug in the client gets here. Release the link, the answers waiting and the keep-alive timer,
            // and fail whoever waits, rather than hang them.
            link.Abort();
            _answers.Release();
            ITimer? keepAliveTimer;
            lock (_gate)
            {
                keepAliveTimer = TakeKeepAliveTimerLocked();
            }

            keepAliveTimer?.Dispose();
            SetClosed();

            // Both are set with the link, before the read loop started.
            _connectResult!.TrySetException(ex);
            _ended!.TrySetException(ex);
        }
    }

    /// <summary>
    /// Reads once into the receive buffer and handles every whole packet in it, recording the answer a packet asks
    /// for behind the ones waiting. The answers are written when the read does not complete at once, before the loop
    /// waits for it, and before the sixteenth packet after the oldest of them is handled. Once the end has begun,
    /// what is read is dropped unhandled, until the server ends the link or the close deadline aborts it. It uses
    /// the pooling builder, so a call that suspends allocates nothing in steady state.
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
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> ReadAndHandleAsync(IMqttLink link)
    {
        ValueTask<int> reading = link.ReadAsync(_receive.GetReadMemory(), CancellationToken.None);
        if (!reading.IsCompleted && _answers.Count > 0)
        {
            // A wait that completed does not mean the read will: the start of a message can come without the rest.
            // The write does not throw, and the read already started is still awaited, just below, so the memory it
            // was given stays the read's until it has ended.
            await WriteAnswersAsync().ConfigureAwait(false);
        }

        int read = await reading.ConfigureAwait(false);
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
            if (step == EndStep.WriteAnswers)
            {
                // Awaited before the next packet is handled, so the answers go out in the order the packets came
                // (MQTT 4.6). A failed write ends the connection, which the next turn sees.
                await WriteAnswersAsync().ConfigureAwait(false);
                continue;
            }

            if (step == EndStep.None)
            {
                _receive.Compact();
                return true;
            }

            _receive.Clear();
            await CloseLinkAsync(link, step is EndStep.DisconnectAndClose or EndStep.DisconnectRecorded ? disconnect : null).ConfigureAwait(false);
            CompleteFailedConnect();
            return true;
        }
    }

    /// <summary>
    /// Frames and handles every whole packet in the receive buffer, recording the answer a packet asks for behind
    /// the ones waiting, until the answers must be written or the end begins. An answer over the server's Maximum
    /// Packet Size is not recorded: it ends the connection.
    /// </summary>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code when the result is <see cref="EndStep.DisconnectAndClose"/>.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.None"/> when every whole packet is handled, or the end has begun;
    /// <see cref="EndStep.WriteAnswers"/> when the answers waiting must be written before the next packet is
    /// handled; otherwise what this read loop, which recorded the end, does next. Never
    /// <see cref="EndStep.RecordAnswer"/>.
    /// </returns>
    private EndStep HandleBuffered(out MqttReasonCode disconnect)
    {
        while (true)
        {
            if (_answers.MustWriteBeforeNextPacket)
            {
                disconnect = MqttReasonCode.Success;
                return EndStep.WriteAnswers;
            }

            FrameStatus status = _receive.TryFrame(out PacketHeader header);
            if (status == FrameStatus.NeedMoreData)
            {
                disconnect = MqttReasonCode.Success;
                return EndStep.None;
            }

            EndStep step;
            if (status == FrameStatus.Done)
            {
                _answers.CountPacket();
                ReadOnlySpan<byte> packet = _receive.GetPacket(header);
                step = _phase == Phase.Connecting
                    ? HandleConnAck(header, packet, out disconnect)
                    : HandlePacket(header, packet, out disconnect);
                _receive.Consume(header.TotalLength);
                if (step == EndStep.RecordAnswer)
                {
                    // Recorded once the handler has returned, so nothing is rented or encoded under the client's
                    // lock.
                    step = EndStep.None;
                    if (!TryAddAnswer())
                    {
                        // Over the server's Maximum Packet Size: the end has begun, and the read loop sees it.
                        return EndStep.None;
                    }
                }
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
    /// <see cref="EndStep.None"/> to carry on; <see cref="EndStep.RecordAnswer"/> when the packet is answered with
    /// the answer in <see cref="_reply"/>; <see cref="EndStep.Close"/> for a DISCONNECT from the server this call
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

                OnPingResponse();
                return EndStep.None;

            case PacketType.PubAck or PacketType.PubRec or PacketType.PubComp:
                return HandlePublishAck(packet, out disconnect);

            case PacketType.Publish:
                return HandlePublish(packet, out disconnect);

            case PacketType.PubRel:
                return HandlePubRel(packet, out disconnect);

            case PacketType.SubAck or PacketType.UnsubAck:
                return HandleSubscribeAck(header.Type, packet, out disconnect);

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
    /// Notes in <see cref="_reply"/> the answer the packet being handled asks for. It only notes it, so a handler may
    /// call it while holding the client's lock; <see cref="HandleBuffered"/> records it behind the answers waiting
    /// once the handler has returned and the packet is consumed.
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
    /// <see cref="EndStep.RecordAnswer"/>, for the handler to return.
    /// </returns>
    private EndStep RecordReply(PacketType type, ushort packetIdentifier, MqttReasonCode reasonCode)
    {
        _reply = new Reply(type, packetIdentifier, reasonCode);
        return EndStep.RecordAnswer;
    }

    /// <summary>
    /// Encodes the answer in <see cref="_reply"/> behind the answers waiting, renting their buffer for the first,
    /// unless the server's Maximum Packet Size does not allow it. Called by the read loop only, never under the
    /// client's lock.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the answer waits to be written. <see langword="false"/> when it is over the
    /// server's Maximum Packet Size: it is not recorded, and the connection ends with
    /// <see cref="MqttClientCloseReason.ProtocolError"/> and 0x95, aborting the link, unless another reason ended it
    /// first. The answers already waiting stay, for the end of the connection to return.
    /// </returns>
    private bool TryAddAnswer()
    {
        Reply reply = _reply;
        if (!FitsServerMaximumPacketSize(AckCodec.GetSize(reply.ReasonCode, null)))
        {
            // MQTT cannot go on without this answer, and the server may not be sent it (MQTT-3.2.2-15).
            EndOverServerMaximumPacketSize();
            return false;
        }

        _answers.Add(reply.Type, reply.PacketIdentifier, reply.ReasonCode);
        return true;
    }

    /// <summary>
    /// Writes every answer waiting as one WebSocket message through the send lock. The buffer is handed to
    /// <see cref="SendAsync"/>, which returns it to the pool whatever the outcome, so afterwards no answer waits.
    /// Called by the read loop only, and only while an answer waits.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the link took them. <see langword="false"/> when they were dropped because the
    /// end had begun, or the write failed and ended the connection with
    /// <see cref="MqttClientCloseReason.WriteFailed"/>, aborting the link. It does not throw.
    /// </returns>
    private ValueTask<bool> WriteAnswersAsync()
    {
        byte[] answers = _answers.Take(out int length);
        return SendAsync(answers, length, SendKind.Other);
    }

    /// <summary>
    /// An answer the read loop owes the server: PUBACK, PUBREC, PUBREL or PUBCOMP, without properties.
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
