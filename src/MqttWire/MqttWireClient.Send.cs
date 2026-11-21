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
    /// <param name="isDisconnect">
    /// <see langword="true"/> for the DISCONNECT that ends the connection, the only packet written after the end
    /// began.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the link took the packet. <see langword="false"/> when it was not written
    /// because the end had begun, or the write failed; a failed write ends the connection with
    /// <see cref="MqttClientCloseReason.WriteFailed"/> and aborts the link.
    /// </returns>
    internal async ValueTask<bool> SendAsync(byte[] packet, int length, bool isDisconnect)
    {
        await _sendLock.WaitAsync().ConfigureAwait(false);
        try
        {
            IMqttLink? link = _link;
            if (link is null || (!isDisconnect && _phase >= Phase.Closing))
            {
                return false;
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
            // The write has ended, so the buffer is no longer in use (6.8).
            _pool.Return(packet);
            _sendLock.Release();
        }
    }

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
