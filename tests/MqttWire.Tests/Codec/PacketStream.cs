using System.Buffers;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Which end of a connection reads a stream of packets; it decides which packet types are read.
/// </summary>
internal enum Reader
{
    /// <summary>
    /// The server reads what a client sends.
    /// </summary>
    Server,

    /// <summary>
    /// The client reads what a server sends.
    /// </summary>
    Client,
}

/// <summary>
/// What decoding one packet of a stream gave.
/// </summary>
/// <param name="Type">
/// The packet type, or <see langword="null"/> for a framing error, which is the last entry of a stream.
/// </param>
/// <param name="Status">
/// The decoder's answer; <see cref="MqttReasonCode.MalformedPacket"/> for a framing error, and
/// <see cref="MqttReasonCode.ProtocolError"/> for a type the reader never reads.
/// </param>
/// <param name="Hex">
/// The bytes of the packet in hex; empty for a framing error.
/// </param>
internal sealed record DecodedEntry(PacketType? Type, MqttReasonCode Status, string Hex);

/// <summary>
/// Runs a stream of packets through the framing and the codecs the way a read loop does, whole or in pieces.
/// </summary>
internal static class PacketStream
{
    /// <summary>
    /// Decodes a stream that is all there.
    /// </summary>
    /// <param name="stream">
    /// The bytes of the stream.
    /// </param>
    /// <param name="reader">
    /// Who reads the stream.
    /// </param>
    /// <returns>
    /// One entry per complete packet, in order. A framing error ends the list with one entry whose type is null;
    /// a packet whose decoder reports an error is recorded and decoding goes on. Bytes of an incomplete last packet
    /// give no entry.
    /// </returns>
    public static List<DecodedEntry> DecodeAll(ReadOnlySpan<byte> stream, Reader reader)
    {
        List<DecodedEntry> entries = [];
        int offset = 0;
        int calls = 0;
        Drain(stream, ref offset, ref calls, entries, reader);

        // One call per packet, each at least 2 bytes, and one that ends the only piece.
        calls.ShouldBeLessThanOrEqualTo((stream.Length / 2) + 1 + 1);
        return entries;
    }

    /// <summary>
    /// Decodes a stream that arrives in pieces, appending each piece to one growing buffer and decoding every
    /// complete packet after each piece, as a read loop does.
    /// </summary>
    /// <param name="stream">
    /// The bytes of the whole stream.
    /// </param>
    /// <param name="cuts">
    /// The positions in <paramref name="stream"/> where a piece ends, strictly increasing and from 1 to the length
    /// minus 1; the last piece ends at the end of the stream.
    /// </param>
    /// <param name="reader">
    /// Who reads the stream.
    /// </param>
    /// <returns>
    /// The entries as for <see cref="DecodeAll"/>; the same stream gives the same list however it is cut.
    /// </returns>
    public static List<DecodedEntry> DecodeInPieces(byte[] stream, IReadOnlyList<int> cuts, Reader reader)
    {
        List<DecodedEntry> entries = [];
        byte[] buffer = new byte[stream.Length];
        int filled = 0;
        int offset = 0;
        int calls = 0;
        int previous = 0;
        for (int piece = 0; piece <= cuts.Count; piece++)
        {
            int end = piece < cuts.Count ? cuts[piece] : stream.Length;
            end.ShouldBeGreaterThanOrEqualTo(previous);
            end.ShouldBeLessThanOrEqualTo(stream.Length);

            stream.AsSpan(previous, end - previous).CopyTo(buffer.AsSpan(filled));
            filled += end - previous;
            previous = end;

            if (Drain(buffer.AsSpan(0, filled), ref offset, ref calls, entries, reader))
            {
                break;
            }
        }

        // One call per packet, each at least 2 bytes, and at most one that ends each piece.
        calls.ShouldBeLessThanOrEqualTo((stream.Length / 2) + (cuts.Count + 1) + 1);
        return entries;
    }

    /// <summary>
    /// Frames and decodes every complete packet that starts at the offset, and moves the offset past them.
    /// </summary>
    /// <param name="buffer">
    /// Everything received so far.
    /// </param>
    /// <param name="offset">
    /// The start of the first packet not yet decoded; moved past each packet decoded.
    /// </param>
    /// <param name="calls">
    /// Counts the framing calls made.
    /// </param>
    /// <param name="entries">
    /// Where the entries are added.
    /// </param>
    /// <param name="reader">
    /// Who reads the stream.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when a framing error ended the stream; <see langword="false"/> when the rest of the
    /// buffer is an incomplete packet or nothing.
    /// </returns>
    private static bool Drain(ReadOnlySpan<byte> buffer, ref int offset, ref int calls, List<DecodedEntry> entries, Reader reader)
    {
        while (true)
        {
            calls++;
            OperationStatus status = PacketHeader.TryReadPacket(buffer[offset..], out PacketHeader header);
            if (status == OperationStatus.NeedMoreData)
            {
                return false;
            }

            if (status == OperationStatus.InvalidData)
            {
                entries.Add(new DecodedEntry(null, MqttReasonCode.MalformedPacket, string.Empty));
                return true;
            }

            status.ShouldBe(OperationStatus.Done);
            ReadOnlySpan<byte> packet = buffer.Slice(offset, header.TotalLength);
            entries.Add(new DecodedEntry(header.Type, Decode(packet, header.Type, reader), Convert.ToHexString(packet)));

            // Progress: a packet was consumed, so the offset moved.
            header.TotalLength.ShouldBeGreaterThanOrEqualTo(2);
            offset += header.TotalLength;
        }
    }

    /// <summary>
    /// Decodes one framed packet with the codec of its type, as the reader would.
    /// </summary>
    /// <param name="packet">
    /// Exactly one framed packet.
    /// </param>
    /// <param name="type">
    /// Its type.
    /// </param>
    /// <param name="reader">
    /// Who reads it.
    /// </param>
    /// <returns>
    /// The decoder's answer, or <see cref="MqttReasonCode.ProtocolError"/>, without calling a codec, for a type the
    /// reader never reads.
    /// </returns>
    private static MqttReasonCode Decode(ReadOnlySpan<byte> packet, PacketType type, Reader reader)
    {
        bool server = reader == Reader.Server;
        switch (type)
        {
            case PacketType.Connect when server:
                return ConnectCodec.TryDecode(packet, out ConnectPacket _);
            case PacketType.ConnAck when !server:
                return ConnAckCodec.TryDecode(packet, out ConnAckPacket _);
            case PacketType.Publish:
                return PublishCodec.TryDecode(packet, server ? PacketSource.Client : PacketSource.Server, out PublishPacket _);
            case PacketType.PubAck or PacketType.PubRec or PacketType.PubRel or PacketType.PubComp:
                return AckCodec.TryDecode(packet, out AckPacket _);
            case PacketType.Subscribe when server:
                return SubscribeCodec.TryDecode(packet, out SubscribePacket _);
            case PacketType.SubAck when !server:
                return SubAckCodec.TryDecode(packet, out SubAckPacket _);
            case PacketType.Unsubscribe when server:
                return UnsubscribeCodec.TryDecode(packet, out UnsubscribePacket _);
            case PacketType.UnsubAck when !server:
                return UnsubAckCodec.TryDecode(packet, out UnsubAckPacket _);
            case PacketType.PingReq when server:
            case PacketType.PingResp when !server:
                return PingCodec.TryDecode(packet);
            case PacketType.Disconnect:
                return DisconnectCodec.TryDecode(packet, out DisconnectPacket _);
            default:
                return MqttReasonCode.ProtocolError;
        }
    }
}
