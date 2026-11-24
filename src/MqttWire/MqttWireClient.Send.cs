using MqttWire.Client;
using MqttWire.Codec;
using MqttWire.Link;

namespace MqttWire;

/// <content>
/// The send path: one write at a time through the send lock, in the order the writers asked.
/// </content>
public sealed partial class MqttWireClient
{
    /// <summary>
    /// Writes one packet through the send lock: one write at a time, in the order the writers asked.
    /// </summary>
    /// <param name="packet">
    /// The encoded packet in a buffer rented from the client's pool. This call owns it from now on and returns it
    /// once the write has ended, whatever the outcome.
    /// </param>
    /// <param name="length">
    /// The packet's length.
    /// </param>
    /// <param name="kind">
    /// What the packet is: a DISCONNECT is the only packet written after the end began; a PINGREQ starts its
    /// PINGRESP deadline when its write begins; a PUBLISH gives back its reserved queued bytes, and a CONNECT has its
    /// credentials cleared, when the write ends, whatever the outcome.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the link took the packet. A link whose own close has started takes it without
    /// sending it (<see cref="IMqttLink.WriteAsync"/>), so <see langword="true"/> does not promise that the packet was
    /// sent. <see langword="false"/> when it was not written
    /// because the end had begun, or the write failed; a failed write ends the connection with
    /// <see cref="MqttClientCloseReason.WriteFailed"/> and aborts the link.
    /// </returns>
    /// <remarks>
    /// Allocation-free when the send lock is uncontended and the write completes synchronously; a write that
    /// completes asynchronously (always in the browser) allocates its state machine, as does a wait for a contended
    /// send lock. Most callers discard the returned task, so a pooling builder would not help here.
    /// </remarks>
    internal async ValueTask<bool> SendAsync(byte[] packet, int length, SendKind kind)
    {
        await _sendLock.WaitAsync().ConfigureAwait(false);
        try
        {
            IMqttLink? link = _link;
            if (link is null || (kind != SendKind.Disconnect && _phase >= Phase.Closing))
            {
                return false;
            }

            if (kind == SendKind.PingRequest)
            {
                // The deadline counts from here, not from the tick that queued it: a write stalled ahead of it in
                // the send lock is not the server failing to answer. Set before the write, so a PINGRESP that comes
                // at once finds it.
                Volatile.Write(ref _pingRequestTimestamp, _time.GetTimestamp());
            }

            await link.WriteAsync(packet.AsMemory(0, length), CancellationToken.None).ConfigureAwait(false);
            Volatile.Write(ref _lastSendTimestamp, _time.GetTimestamp());
            return true;
        }
        catch (Exception ex)
        {
            // Whatever the link throws, the write did not happen: the connection cannot go on.
            TryBeginEnd(new MqttClientCloseInfo(MqttClientCloseReason.WriteFailed, MqttReasonCode.Success, null, ex), EndMode.Abort);
            return false;
        }
        finally
        {
            switch (kind)
            {
                case SendKind.Connect:
                    // The shared pool hands this array to the next renter as it is, credentials included.
                    packet.AsSpan(0, length).Clear();
                    break;
                case SendKind.Publish:
                    Interlocked.Add(ref _pendingPublishBytes, -length);
                    break;
                case SendKind.PingRequest:
                    Volatile.Write(ref _pingQueued, 0);
                    break;
            }

            // The write has ended, so the buffer is no longer in use (6.8).
            _pool.Return(packet);
            _sendLock.Release();
        }
    }

    /// <summary>
    /// Whether a packet of the given size is within the server's Maximum Packet Size, which limits every packet the
    /// client sends after the CONNACK (MQTT 5, 3.2.2.3.6).
    /// </summary>
    /// <param name="size">
    /// The packet's encoded size, fixed header included.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the server accepts a packet that large; <see langword="false"/> when it may not
    /// be sent.
    /// </returns>
    private bool FitsServerMaximumPacketSize(int size) => (uint)size <= _serverMaximumPacketSize;

    /// <summary>
    /// Ends the connection because a packet it needs, an acknowledgement or a PINGREQ, is over the server's Maximum
    /// Packet Size: <see cref="MqttClientCloseReason.ProtocolError"/> with 0x95, packet too large, aborting the
    /// link without a DISCONNECT. Does nothing when another reason ended the connection first.
    /// </summary>
    private void EndOverServerMaximumPacketSize()
        => TryBeginEnd(
            new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.PacketTooLarge, null, null),
            EndMode.Abort);

    /// <summary>
    /// Encodes a DISCONNECT into a rented buffer.
    /// </summary>
    /// <param name="reasonCode">
    /// The DISCONNECT's reason code.
    /// </param>
    /// <param name="length">
    /// The packet's length.
    /// </param>
    /// <returns>
    /// The rented buffer, which the caller hands to <see cref="SendAsync"/>.
    /// </returns>
    private byte[] RentDisconnect(MqttReasonCode reasonCode, out int length)
    {
        byte[] buffer = _pool.Rent(DisconnectCodec.GetSize(reasonCode, null));
        length = DisconnectCodec.Write(buffer, reasonCode, null);
        return buffer;
    }
}
