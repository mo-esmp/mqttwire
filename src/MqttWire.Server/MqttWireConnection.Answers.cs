using System.Net.WebSockets;
using System.Runtime.InteropServices;
using MqttWire.Codec;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// What is sent (design 6.6): the encoding of every packet the server writes to a client other than the closing
/// one, the CONNACK of the admission, the answers of the read path (PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK,
/// UNSUBACK, PINGRESP), the PUBLISH of a delivery, and the one rule for their reason strings. How and when the bytes
/// reach the link is the send path's, in <c>MqttWireConnection.Send.cs</c>; the packet that carries a close's reason
/// code is the close's, in <c>MqttWireConnection.Close.cs</c>.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// An answer the read path produces for the client: PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK or
    /// PINGRESP, with the fields its type uses. Its reason string has already been shaped.
    /// </summary>
    [StructLayout(LayoutKind.Auto)]
    private readonly ref struct Answer
    {
        /// <summary>
        /// Gets the packet's type.
        /// </summary>
        public PacketType Type { get; init; }

        /// <summary>
        /// Gets the packet identifier, from 1 to 65,535; unused for a PINGRESP.
        /// </summary>
        public ushort PacketIdentifier { get; init; }

        /// <summary>
        /// Gets the reason code of a PUBACK, PUBREC, PUBREL or PUBCOMP; unused for the others.
        /// </summary>
        public MqttReasonCode Code { get; init; }

        /// <summary>
        /// Gets the reason string to carry, or <see langword="null"/> for none; unused for UNSUBACK and PINGRESP.
        /// </summary>
        public string? ReasonString { get; init; }

        /// <summary>
        /// Gets the reason codes of a SUBACK, one per topic filter; unused for the others.
        /// </summary>
        public ReadOnlySpan<MqttReasonCode> Codes { get; init; }

        /// <summary>
        /// Gets the number of topic filters of an UNSUBACK, each answered with success; unused for the others.
        /// </summary>
        public int Count { get; init; }

        /// <summary>
        /// Gets a value indicating whether the answer is the last one to a QoS 1 or 2 publish of the client, which
        /// counts against the receive maximum the server announced while it waits (plan decision 13): a PUBACK, a
        /// PUBCOMP that ends a remembered exchange (success), or a failing PUBREC. A successful PUBREC is not: the
        /// exchange's record counts instead; nor is a PUBCOMP 0x92, which answers no publish.
        /// </summary>
        public bool EndsIncomingPublish => Type switch
        {
            PacketType.PubAck => true,
            PacketType.PubRec => (byte)Code >= 0x80,
            PacketType.PubComp => Code == MqttReasonCode.Success,
            _ => false,
        };

        /// <summary>
        /// Gets the size of the encoded packet.
        /// </summary>
        /// <returns>
        /// The size in bytes.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// A field cannot be carried (a bug of this library: every field was validated where it came from).
        /// </exception>
        public int GetSize() => Type switch
        {
            PacketType.PingResp => PingCodec.PingResp.Length,
            PacketType.SubAck => SubAckCodec.GetSize(Codes.Length, ReasonString),
            PacketType.UnsubAck => UnsubAckCodec.GetSize(Count, reasonString: null),
            _ => AckCodec.GetSize(Code, ReasonString),
        };

        /// <summary>
        /// Encodes the packet.
        /// </summary>
        /// <param name="destination">
        /// Where to write: exactly <see cref="GetSize"/> bytes, all of which are written.
        /// </param>
        /// <exception cref="ArgumentException">
        /// A field cannot be carried, as for <see cref="GetSize"/>; nothing is written.
        /// </exception>
        public void Write(Span<byte> destination)
        {
            switch (Type)
            {
                case PacketType.PingResp:
                    PingCodec.PingResp.CopyTo(destination);
                    break;

                case PacketType.SubAck:
                    SubAckCodec.Write(destination, PacketIdentifier, Codes, ReasonString);
                    break;

                case PacketType.UnsubAck:
                    // The reason codes, all success (0x00), take the last Count bytes of the packet: they are cleared
                    // there and written over themselves, so no other buffer holds them.
                    Span<byte> codes = destination[^Count..];
                    codes.Clear();
                    UnsubAckCodec.Write(destination, PacketIdentifier, MemoryMarshal.Cast<byte, MqttReasonCode>(codes),
                        reasonString: null);
                    break;

                default:
                    AckCodec.Write(destination, Type, PacketIdentifier, Code, ReasonString);
                    break;
            }
        }
    }

    /// <summary>
    /// Encodes the success CONNACK and writes it as the first write of the send path, which the admission step
    /// reserved for it (so no MQTT packet can precede it); what a host sent meanwhile is written behind it. It never
    /// waits: a write that does not complete at once is awaited elsewhere. A failed write ends the connection with
    /// <see cref="MqttCloseReason.ConnectionLost"/>, and so does a fault inside MqttWire, such as a pool that throws
    /// (<see cref="FaultSendPath"/>). A host's user property that cannot be carried ends it as the host's bug
    /// (<see cref="RefuseUnencodableConnAck"/>). A connection that was closed since its admission gets no CONNACK
    /// (design 6.6, "Admission and takeover"): whether it is still Active is decided in a hold of the send path's
    /// lock, under which every move to Closing is made, immediately before the write is started, so a close either
    /// came first, and nothing is written, or comes after the CONNACK was decided on, finds a write in progress and
    /// aborts. It never throws; the caller then finds the connection Closing.
    /// </summary>
    /// <param name="fields">
    /// The fields to encode.
    /// </param>
    private void SendConnAck(in ConnAckFields fields)
    {
        int size;
        try
        {
            // The size checks every field before anything is rented.
            size = ConnAckCodec.GetSize(fields);
        }
        catch (ArgumentException exception)
        {
            RefuseUnencodableConnAck(exception);
            return;
        }

        byte[]? unsent = null;
        int length;
        try
        {
            unsent = _server.Pool.Rent(size);
            length = ConnAckCodec.Write(unsent, fields);
        }
        catch (Exception exception)
        {
            // The send path is reserved for this CONNACK: the fault gives it up, with the buffer if one was rented.
            FaultSendPath(exception, unsent);
            return;
        }

        bool active;
        lock (_sendLock)
        {
            active = _state == ConnectionState.Active;
        }

        if (!active)
        {
            // Closed while the CONNACK was encoded: that close found the path reserved and aborted the link. The
            // buffer was never handed to the link, so it goes back here, and the path is given up.
            ContinueSending(returned: unsent, failed: false, InlineWrites);
            return;
        }

        StartSending(unsent.AsMemory(start: 0, length), unsent, InlineWrites);
    }

    /// <summary>
    /// Ends an admitted connection whose success CONNACK cannot be encoded because a user property the host returned
    /// from <see cref="IMqttWireHandler.OnConnectAsync"/> cannot be carried. <see cref="MqttConnectResult.Accept"/>
    /// refuses such a value, so this happens only when the host changed the array after it was accepted, which its
    /// documentation forbids: it is the host's bug, not a lost link. This caller holds the send path, reserved for the
    /// CONNACK, so nothing has been written: in one hold of the lock the connection moves to Closing with
    /// <see cref="CloseCause.HandlerThrew"/>, which <see cref="IMqttWireHandler.OnClosed"/> then reports; event 15
    /// names <c>OnConnectAsync</c> and carries the exception; and CONNACK 0x83 is written in the success CONNACK's
    /// place, then the WebSocket close, with whatever waited dropped by the Closing step. When another close won the
    /// move first (it found the path taken and aborted the link), the exception is still logged and the path is given
    /// up. A fault while the refusal is encoded ends the connection as any fault of the send path does.
    /// </summary>
    /// <param name="exception">
    /// What the encoding of the success CONNACK threw.
    /// </param>
    private void RefuseUnencodableConnAck(ArgumentException exception)
    {
        bool won;
        lock (_sendLock)
        {
            won = TryEnterClosing(CloseCause.HandlerThrew, out _);
            if (won)
            {
                // The send path stays this caller's: the refusal, then the WebSocket close, are written through it.
                _closeStatus = (ushort)WebSocketCloseStatus.NormalClosure;
            }
        }

        LogEnd(CloseCause.HandlerThrew, MqttReasonCode.ImplementationSpecificError, payloadLength: 0,
            nameof(IMqttWireHandler.OnConnectAsync), exception);
        if (!won)
        {
            ContinueSending(returned: null, failed: false, InlineWrites);
            return;
        }

        byte[]? unsent = null;
        int length;
        try
        {
            ConnAckFields refusal = new() { ReasonCode = MqttReasonCode.ImplementationSpecificError };
            unsent = _server.Pool.Rent(ConnAckCodec.GetSize(refusal));
            length = ConnAckCodec.Write(unsent, refusal);
        }
        catch (Exception fault)
        {
            FaultSendPath(fault, unsent);
            return;
        }

        StartSending(unsent.AsMemory(start: 0, length), unsent, InlineWrites);
    }

    /// <summary>
    /// Sends a PUBACK, PUBREC, PUBREL or PUBCOMP through the send path, as <see cref="SendAnswer"/> does. The reason
    /// string follows the rule of <see cref="ShapeReasonString"/>.
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
    private void SendAck(PacketType type, ushort packetIdentifier, MqttReasonCode code, string? reasonString) =>
        SendAnswer(new Answer
        {
            Type = type,
            PacketIdentifier = packetIdentifier,
            Code = code,
            ReasonString = ShapeReasonString(type, code, count: 0, reasonString),
        });

    /// <summary>
    /// Sends a SUBACK through the send path, as <see cref="SendAnswer"/> does. The reason string follows the rule of
    /// <see cref="ShapeReasonString"/>. The caller has made sure the SUBACK fits the client's Maximum Packet Size
    /// without its reason string.
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
    private void SendSubAck(ushort packetIdentifier, ReadOnlySpan<MqttReasonCode> codes, string? reasonString) =>
        SendAnswer(new Answer
        {
            Type = PacketType.SubAck,
            PacketIdentifier = packetIdentifier,
            Codes = codes,
            ReasonString = ShapeReasonString(PacketType.SubAck, MqttReasonCode.Success, codes.Length, reasonString),
        });

    /// <summary>
    /// Sends an UNSUBACK with success for every topic filter through the send path, as <see cref="SendAnswer"/>
    /// does. The caller has made sure the UNSUBACK fits the client's Maximum Packet Size.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier of the UNSUBSCRIBE, from 1 to 65,535.
    /// </param>
    /// <param name="count">
    /// The number of topic filters, at least one.
    /// </param>
    private void SendUnsubAck(ushort packetIdentifier, int count) =>
        SendAnswer(new Answer { Type = PacketType.UnsubAck, PacketIdentifier = packetIdentifier, Count = count });

    /// <summary>
    /// Sends a PINGRESP through the send path, as <see cref="SendAnswer"/> does; when one already waits behind the
    /// write in progress, another is not added.
    /// </summary>
    private void SendPingResp() => SendAnswer(new Answer { Type = PacketType.PingResp });

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
    /// Encodes a delivery for the write that follows; called by whoever holds the send path. A message whose whole
    /// packet is at most <see cref="ServerLimits.CopyThresholdBytes"/> is copied behind its header into one rented
    /// buffer. A larger one has only its header encoded into a rented buffer, and its payload is left in
    /// <see cref="_remainingPayload"/>, to be written from the host's memory in pieces (plan decision 10).
    /// </summary>
    /// <param name="delivery">
    /// The delivery, which <see cref="TakeDelivery"/> found small enough to send.
    /// </param>
    /// <param name="packetIdentifier">
    /// The packet identifier its exchange record took, at QoS 1 and 2; 0 at QoS 0. DUP is never set: nothing is sent
    /// a second time inside a connection.
    /// </param>
    /// <param name="buffer">
    /// The rented buffer that holds the piece; the completion of its write returns it.
    /// </param>
    /// <returns>
    /// The first piece to write: the whole packet, or its header.
    /// </returns>
    /// <exception cref="Exception">
    /// The pool, or the payload's memory, threw (a fault inside MqttWire, or memory the host let go of): the buffer,
    /// if one was rented, is back in the pool, and <see cref="_remainingPayload"/> holds no reference to the payload.
    /// </exception>
    private ReadOnlyMemory<byte> EncodeDelivery(in PendingDelivery delivery, ushort packetIdentifier,
        out byte[] buffer)
    {
        PublishFields fields = new()
        {
            Topic = delivery.Topic.Utf8, QoS = delivery.QoS, PacketIdentifier = packetIdentifier,
        };
        int payloadLength = delivery.Payload.Length;
        int size = PublishCodec.GetSize(fields, payloadLength);
        bool whole = size <= ServerLimits.CopyThresholdBytes;
        buffer = _server.Pool.Rent(whole ? size : size - payloadLength);
        try
        {
            if (whole)
            {
                _remainingPayload = default;
                return buffer.AsMemory(start: 0, PublishCodec.Write(buffer, fields, delivery.Payload.Span));
            }

            // An empty payload behind a long topic leaves nothing to write, and no reference to the host's memory.
            int length = PublishCodec.WriteHeader(buffer, fields, payloadLength);
            _remainingPayload = payloadLength == 0 ? default : delivery.Payload;
            return buffer.AsMemory(start: 0, length);
        }
        catch (Exception)
        {
            // The buffer was never handed to the link: the code that rented it gives it back (design 6.8).
            _server.Pool.Return(buffer);
            throw;
        }
    }
}
