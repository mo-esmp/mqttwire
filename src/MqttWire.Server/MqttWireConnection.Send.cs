using System.Runtime.InteropServices;
using MqttWire.Codec;
using MqttWire.Server.Internal;

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
    /// Encodes a CONNACK that refuses the connection, with a reason code and a reason string and nothing else, and
    /// writes it as <see cref="SendConnAck"/> does. The reason string follows the rule of
    /// <see cref="ShapeReasonString"/>.
    /// </summary>
    /// <param name="code">
    /// The reason code, 0x80 or above.
    /// </param>
    /// <param name="reasonString">
    /// The reason string, or <see langword="null"/>; validated where the host handed it over.
    /// </param>
    private void SendConnAckRefusal(MqttReasonCode code, string? reasonString) =>
        SendConnAck(new ConnAckFields
        {
            ReasonCode = code, ReasonString = ShapeReasonString(PacketType.ConnAck, code, count: 0, reasonString),
        });

    /// <summary>
    /// Encodes a DISCONNECT into a buffer rented from the server's pool and writes it, as <see cref="SendConnAck"/>
    /// does. The reason string follows the rule of <see cref="ShapeReasonString"/>.
    /// </summary>
    /// <param name="code">
    /// The reason code.
    /// </param>
    /// <param name="reasonString">
    /// The reason string, or <see langword="null"/>; validated where the host handed it over.
    /// </param>
    private void SendDisconnect(MqttReasonCode code, string? reasonString)
    {
        string? carried = ShapeReasonString(PacketType.Disconnect, code, count: 0, reasonString);
        byte[] buffer = _server.Pool.Rent(DisconnectCodec.GetSize(code, carried));
        int length = DisconnectCodec.Write(buffer, code, carried);
        _ = WriteAndReturnAsync(buffer, length);
    }

    /// <summary>
    /// Encodes a PUBACK, PUBREC, PUBREL or PUBCOMP into a buffer rented from the server's pool and writes it, as
    /// <see cref="SendConnAck"/> does. The reason string follows the rule of <see cref="ShapeReasonString"/>.
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
    private void SendAck(PacketType type, ushort packetIdentifier, MqttReasonCode code, string? reasonString)
    {
        string? carried = ShapeReasonString(type, code, count: 0, reasonString);
        byte[] buffer = _server.Pool.Rent(AckCodec.GetSize(code, carried));
        int length = AckCodec.Write(buffer, type, packetIdentifier, code, carried);
        _ = WriteAndReturnAsync(buffer, length);
    }

    /// <summary>
    /// Encodes a SUBACK into a buffer rented from the server's pool and writes it, as <see cref="SendConnAck"/>
    /// does. The reason string follows the rule of <see cref="ShapeReasonString"/>. The caller has made sure the
    /// SUBACK fits the client's Maximum Packet Size without its reason string.
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
    private void SendSubAck(ushort packetIdentifier, ReadOnlySpan<MqttReasonCode> codes, string? reasonString)
    {
        string? carried = ShapeReasonString(PacketType.SubAck, MqttReasonCode.Success, codes.Length, reasonString);
        byte[] buffer = _server.Pool.Rent(SubAckCodec.GetSize(codes.Length, carried));
        int length = SubAckCodec.Write(buffer, packetIdentifier, codes, carried);
        _ = WriteAndReturnAsync(buffer, length);
    }

    /// <summary>
    /// Encodes an UNSUBACK with success for every topic filter into a buffer rented from the server's pool and writes
    /// it, as <see cref="SendConnAck"/> does. The caller has made sure the UNSUBACK fits the client's Maximum Packet
    /// Size.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier of the UNSUBSCRIBE, from 1 to 65,535.
    /// </param>
    /// <param name="count">
    /// The number of topic filters, at least one.
    /// </param>
    private void SendUnsubAck(ushort packetIdentifier, int count)
    {
        int size = UnsubAckCodec.GetSize(count, reasonString: null);
        byte[] buffer = _server.Pool.Rent(size);

        // The reason codes, all success (0x00), take the last count bytes of the packet: they are cleared there and
        // written over themselves, so no other buffer holds them.
        Span<byte> codes = buffer.AsSpan(size - count, count);
        codes.Clear();
        int length = UnsubAckCodec.Write(buffer, packetIdentifier, MemoryMarshal.Cast<byte, MqttReasonCode>(codes),
            reasonString: null);
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
