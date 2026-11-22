using MqttWire.Codec;

namespace MqttWire.Server;

/// <content>
/// Sending: the inline half of the send path, which writes a packet at once; the waiting half comes later.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// Encodes a CONNACK into a buffer rented from the server's pool and writes it; the buffer goes back when the
    /// write has ended (design 6.8). It never waits: a write that does not complete at once is awaited elsewhere. A
    /// failed write ends the connection with <see cref="MqttCloseReason.ConnectionLost"/>.
    /// </summary>
    /// <param name="fields">
    /// The fields to encode.
    /// </param>
    /// <exception cref="ArgumentException">
    /// A field cannot be carried (the host's bug); nothing is rented or written.
    /// </exception>
    private void SendConnAck(in ConnAckFields fields)
    {
        // The size checks every field before anything is rented.
        byte[] buffer = _server.Pool.Rent(ConnAckCodec.GetSize(fields));
        int length = ConnAckCodec.Write(buffer, fields);
        _ = WriteAndReturnAsync(buffer, length);
    }

    /// <summary>
    /// Encodes a DISCONNECT into a buffer rented from the server's pool and writes it, as
    /// <see cref="SendConnAck"/> does.
    /// </summary>
    /// <param name="code">
    /// The reason code.
    /// </param>
    /// <param name="reasonString">
    /// The reason string, at most 256 bytes, or <see langword="null"/>.
    /// </param>
    /// <exception cref="ArgumentException">
    /// The reason string cannot be carried (the host's bug); nothing is rented or written.
    /// </exception>
    private void SendDisconnect(MqttReasonCode code, string? reasonString)
    {
        // The size checks the reason string before anything is rented.
        byte[] buffer = _server.Pool.Rent(DisconnectCodec.GetSize(code, reasonString));
        int length = DisconnectCodec.Write(buffer, code, reasonString);
        _ = WriteAndReturnAsync(buffer, length);
    }

    /// <summary>
    /// Encodes a PUBACK, PUBREC, PUBREL or PUBCOMP into a buffer rented from the server's pool and writes it, as
    /// <see cref="SendConnAck"/> does. The reason string is dropped when the client sent Request Problem Information
    /// 0.
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
    /// The reason string, at most 256 bytes, or <see langword="null"/>.
    /// </param>
    /// <exception cref="ArgumentException">
    /// The reason string cannot be carried (the host's bug); nothing is rented or written.
    /// </exception>
    private void SendAck(PacketType type, ushort packetIdentifier, MqttReasonCode code, string? reasonString)
    {
        string? carried = _problemInformation ? reasonString : null;

        // The size checks the reason string before anything is rented.
        byte[] buffer = _server.Pool.Rent(AckCodec.GetSize(code, carried));
        int length = AckCodec.Write(buffer, type, packetIdentifier, code, carried);
        _ = WriteAndReturnAsync(buffer, length);
    }

    /// <summary>
    /// Writes a PINGRESP from a buffer rented from the server's pool, as <see cref="SendConnAck"/> does.
    /// </summary>
    private void SendPingResp()
    {
        ReadOnlySpan<byte> pingResp = PingCodec.PingResp;
        byte[] buffer = _server.Pool.Rent(pingResp.Length);
        pingResp.CopyTo(buffer);
        _ = WriteAndReturnAsync(buffer, pingResp.Length);
    }

    /// <summary>
    /// Writes an encoded packet and returns its buffer to the pool once the write has ended, whether it ended at
    /// once, later or with an exception; only this method returns it. A write that completes at once completes the
    /// method at once, without an allocation.
    /// </summary>
    /// <param name="buffer">
    /// The rented buffer that holds the packet.
    /// </param>
    /// <param name="length">
    /// The packet's length.
    /// </param>
    /// <returns>
    /// A task that completes when the buffer is back; it never fails.
    /// </returns>
    private async Task WriteAndReturnAsync(byte[] buffer, int length)
    {
        bool failed = false;
        try
        {
            await RunningLink.WriteAsync(buffer.AsMemory(0, length), CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            failed = true;
        }

        _server.Pool.Return(buffer);
        if (failed)
        {
            BeginClose(MqttCloseReason.ConnectionLost, MqttReasonCode.Success, reasonString: null);
        }
    }
}
