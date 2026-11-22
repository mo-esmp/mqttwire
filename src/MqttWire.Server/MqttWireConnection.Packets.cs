using System.Runtime.InteropServices;
using System.Text;
using MqttWire.Codec;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// What each packet from a client does once the connection is Active (design 6.6, "What each packet does", and
/// 6.7): the dispatch by type, and the handling of PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PINGREQ and DISCONNECT.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// The most SUBACK reason codes collected on the stack; a SUBSCRIBE with more filters rents a buffer for them.
    /// </summary>
    private const int MaxStackReasonCodes = 256;

    /// <summary>
    /// Handles one complete packet received after the CONNACK, by its type. PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PINGREQ
    /// and DISCONNECT do what design 6.6 says; a second CONNECT, AUTH and the packets only a server sends (CONNACK,
    /// SUBACK, UNSUBACK, PINGRESP) end the connection with 0x82. A packet that cannot be decoded ends it with the
    /// decoder's code.
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
    private bool HandlePacket(in PacketHeader header, ReadOnlySpan<byte> packet)
    {
        switch (header.Type)
        {
            case PacketType.Publish:
                return HandlePublish(packet);

            case PacketType.Subscribe:
                return HandleSubscribe(packet);

            case PacketType.Unsubscribe:
                return HandleUnsubscribe(packet);

            case PacketType.PingReq:
                return HandlePingReq(packet);

            case PacketType.Disconnect:
                return HandleDisconnect(packet);

            case PacketType.PubRel:
                // Task 9: PUBREL ends a QoS 2 exchange of the client's (PUBCOMP, or 0x92 for an unknown identifier).
                return true;

            case PacketType.PubAck:
            case PacketType.PubRec:
            case PacketType.PubComp:
                // Task 10: the client's acknowledgements of the server's QoS 1 and 2 deliveries.
                return true;

            default:
                // A second CONNECT, AUTH, or a packet only a server sends.
                BeginClose(MqttCloseReason.ProtocolError, MqttReasonCode.ProtocolError, reasonString: null);
                return false;
        }
    }

    /// <summary>
    /// Handles a PUBLISH read whole: decodes it, ends the connection with 0x94 for a topic alias, refuses a payload
    /// over the limit with 0x97, and hands the rest to the host's <see cref="IMqttWireHandler.OnPublish"/>, answering
    /// QoS 1 with PUBACK: success, or the host's refusal. A packet the decoder refuses begins the close with the
    /// decoder's code. A QoS above <see cref="MqttWireServerOptions.MaxQoS"/> was refused from the fixed header.
    /// </summary>
    /// <param name="packet">
    /// The whole packet, a view of the read buffer, valid only during the call.
    /// </param>
    /// <returns>
    /// <see langword="true"/> to go on with the next packet; <see langword="false"/> when the connection is ending.
    /// </returns>
    private bool HandlePublish(ReadOnlySpan<byte> packet)
    {
        MqttReasonCode code = PublishCodec.TryDecode(packet, PacketSource.Client, out PublishPacket publish);
        if (code != MqttReasonCode.Success)
        {
            CloseUndecodable(code);
            return false;
        }

        if (publish.HasTopicAlias)
        {
            // The server announces no Topic Alias Maximum, so any alias is invalid (design 6.7).
            BeginClose(MqttCloseReason.ProtocolError, MqttReasonCode.TopicAliasInvalid, reasonString: null);
            return false;
        }

        if (publish.Payload.Length > _server.Options.MaxPublishPayloadBytes)
        {
            return RefuseTooLarge(publish.QoS, publish.PacketIdentifier);
        }

        if (publish.QoS == MqttQoS.ExactlyOnce)
        {
            // Task 9: a QoS 2 publish that reaches the host (its record, OnPublish, PUBREC); unanswered until then.
            return true;
        }

        MqttPublish message = new(publish.Topic, publish.Payload, publish.QoS, publish.Retain);
        MqttPublishResult result;
        try
        {
            result = _server.Handler.OnPublish(this, message);
        }
        catch (Exception exception)
        {
            CloseHandlerThrew(nameof(IMqttWireHandler.OnPublish), exception);
            return false;
        }

        if (publish.QoS == MqttQoS.AtLeastOnce && _state == ConnectionState.Active)
        {
            SendAck(PacketType.PubAck, publish.PacketIdentifier, result.ReasonCode, result.ReasonString);
        }

        return _state == ConnectionState.Active;
    }

    /// <summary>
    /// Handles a SUBSCRIBE: a subscription identifier ends the connection with 0xA1, and a SUBACK that could not
    /// fit the client's Maximum Packet Size even without a reason string ends it with 0x95, both before the host is
    /// called. Otherwise each filter in order is either invalid (0x8F, without the host) or handed to the host's
    /// <see cref="IMqttWireHandler.OnSubscribe"/> with the QoS the client asked for; a granted QoS above
    /// <see cref="MqttWireServerOptions.MaxQoS"/> is lowered to it. One SUBACK answers, with one code per filter and
    /// the reason string of the host's first refusal. The codes are collected on the stack up to
    /// <see cref="MaxStackReasonCodes"/>, and in a rented buffer above that, which goes back on every way out.
    /// </summary>
    /// <param name="packet">
    /// The whole packet, a view of the read buffer, valid only during the call.
    /// </param>
    /// <returns>
    /// <see langword="true"/> to go on with the next packet; <see langword="false"/> when the connection is ending.
    /// </returns>
    private bool HandleSubscribe(ReadOnlySpan<byte> packet)
    {
        MqttReasonCode code = SubscribeCodec.TryDecode(packet, out SubscribePacket subscribe);
        if (code != MqttReasonCode.Success)
        {
            CloseUndecodable(code);
            return false;
        }

        if (subscribe.Properties.Contains(PropertyId.SubscriptionIdentifier))
        {
            // CONNACK announced Subscription Identifiers Available 0 (design 6.7).
            BeginClose(MqttCloseReason.ProtocolError, MqttReasonCode.SubscriptionIdentifiersNotSupported,
                reasonString: null);
            return false;
        }

        int count = subscribe.Count;
        if (!AnswerFits(SubAckCodec.GetSize(count, reasonString: null)))
        {
            return false;
        }

        byte[]? rented = count <= MaxStackReasonCodes ? null : _server.Pool.Rent(minimumLength: count);
        Span<MqttReasonCode> codes = rented is null
            ? stackalloc MqttReasonCode[MaxStackReasonCodes]
            : MemoryMarshal.Cast<byte, MqttReasonCode>(rented.AsSpan());
        try
        {
            codes = codes[..count];
            if (!DecideSubscriptions(subscribe, codes, out string? reasonString))
            {
                return false;
            }

            if (_state == ConnectionState.Active)
            {
                SendSubAck(subscribe.PacketIdentifier, codes, reasonString);
            }

            return _state == ConnectionState.Active;
        }
        finally
        {
            if (rented is not null)
            {
                _server.Pool.Return(rented);
            }
        }
    }

    /// <summary>
    /// Decides each subscription of a SUBSCRIBE, in order, and writes its SUBACK code: 0x8F for an invalid filter,
    /// without the host; otherwise the host's answer, with a granted QoS above
    /// <see cref="MqttWireServerOptions.MaxQoS"/> lowered to it. The filter is handed over as a string, which the
    /// interface asks for.
    /// </summary>
    /// <param name="subscribe">
    /// The decoded SUBSCRIBE, a view of the read buffer.
    /// </param>
    /// <param name="codes">
    /// Where the codes go, one per filter.
    /// </param>
    /// <param name="reasonString">
    /// The reason string of the host's first refusal, or <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when every filter was decided; <see langword="false"/> when the host threw, or the
    /// connection stopped being Active before every filter was decided, and the close has begun: the host is not
    /// called for the filters that are left.
    /// </returns>
    private bool DecideSubscriptions(in SubscribePacket subscribe, Span<MqttReasonCode> codes, out string? reasonString)
    {
        reasonString = null;
        bool refused = false;
        MqttQoS maxQoS = _server.Options.MaxQoS;
        int index = 0;
        foreach (Subscription subscription in subscribe.GetSubscriptions())
        {
            if (_state != ConnectionState.Active)
            {
                // Closed meanwhile, by the host from inside an earlier call or from elsewhere: the host is not asked
                // about the filters that are left, and no SUBACK follows.
                return false;
            }

            if (!Topics.IsValidFilter(subscription.TopicFilter))
            {
                codes[index++] = MqttReasonCode.TopicFilterInvalid;
                continue;
            }

            MqttSubscribeResult result;
            try
            {
                result = _server.Handler.OnSubscribe(this, Encoding.UTF8.GetString(subscription.TopicFilter),
                    subscription.MaximumQoS);
            }
            catch (Exception exception)
            {
                CloseHandlerThrew(nameof(IMqttWireHandler.OnSubscribe), exception);
                return false;
            }

            MqttReasonCode granted = result.ReasonCode;
            if (result.IsAccepted && (byte)granted > (byte)maxQoS)
            {
                granted = (MqttReasonCode)(byte)maxQoS;
            }
            else if (!result.IsAccepted && !refused)
            {
                refused = true;
                reasonString = result.ReasonString;
            }

            codes[index++] = granted;
        }

        return true;
    }

    /// <summary>
    /// Handles an UNSUBSCRIBE: an UNSUBACK that could not fit the client's Maximum Packet Size ends the connection
    /// with 0x95 before the host is called; otherwise each filter in order goes to the host's
    /// <see cref="IMqttWireHandler.OnUnsubscribe"/> as a string, and one UNSUBACK answers with success for each. The
    /// host is no longer called once the connection has stopped being Active.
    /// </summary>
    /// <param name="packet">
    /// The whole packet, a view of the read buffer, valid only during the call.
    /// </param>
    /// <returns>
    /// <see langword="true"/> to go on with the next packet; <see langword="false"/> when the connection is ending.
    /// </returns>
    private bool HandleUnsubscribe(ReadOnlySpan<byte> packet)
    {
        MqttReasonCode code = UnsubscribeCodec.TryDecode(packet, out UnsubscribePacket unsubscribe);
        if (code != MqttReasonCode.Success)
        {
            CloseUndecodable(code);
            return false;
        }

        if (!AnswerFits(UnsubAckCodec.GetSize(unsubscribe.Count, reasonString: null)))
        {
            return false;
        }

        foreach (ReadOnlySpan<byte> filter in unsubscribe.GetTopicFilters())
        {
            if (_state != ConnectionState.Active)
            {
                // Closed meanwhile: the host is not called for the filters that are left, and no UNSUBACK follows.
                return false;
            }

            try
            {
                _server.Handler.OnUnsubscribe(this, Encoding.UTF8.GetString(filter));
            }
            catch (Exception exception)
            {
                CloseHandlerThrew(nameof(IMqttWireHandler.OnUnsubscribe), exception);
                return false;
            }
        }

        if (_state == ConnectionState.Active)
        {
            SendUnsubAck(unsubscribe.PacketIdentifier, unsubscribe.Count);
        }

        return _state == ConnectionState.Active;
    }

    /// <summary>
    /// Handles a PINGREQ: one that is not exactly two bytes is malformed (0x81); otherwise it is answered with
    /// PINGRESP.
    /// </summary>
    /// <param name="packet">
    /// The whole packet, a view of the read buffer, valid only during the call.
    /// </param>
    /// <returns>
    /// <see langword="true"/> to go on with the next packet; <see langword="false"/> when the connection is ending.
    /// </returns>
    private bool HandlePingReq(ReadOnlySpan<byte> packet)
    {
        MqttReasonCode code = PingCodec.TryDecode(packet);
        if (code != MqttReasonCode.Success)
        {
            CloseUndecodable(code);
            return false;
        }

        SendPingResp();
        return _state == ConnectionState.Active;
    }

    /// <summary>
    /// Handles a DISCONNECT: a malformed one ends the connection with the decoder's code; otherwise it ends with
    /// <see cref="MqttCloseReason.ClientDisconnected"/> and nothing is written back. Its reason code and properties
    /// have no effect.
    /// </summary>
    /// <param name="packet">
    /// The whole packet, a view of the read buffer, valid only during the call.
    /// </param>
    /// <returns>
    /// <see langword="false"/>: the connection is ending.
    /// </returns>
    private bool HandleDisconnect(ReadOnlySpan<byte> packet)
    {
        MqttReasonCode code = DisconnectCodec.TryDecode(packet, out _);
        if (code != MqttReasonCode.Success)
        {
            CloseUndecodable(code);
            return false;
        }

        BeginClose(MqttCloseReason.ClientDisconnected, MqttReasonCode.Success, reasonString: null);
        return false;
    }

    /// <summary>
    /// Checks that an answer fits the client's Maximum Packet Size, and begins the close with 0x95 when it cannot:
    /// a SUBACK or UNSUBACK of thousands of codes against a client that announced a small size, the one control packet
    /// that may not fit (design 6.6, "A client's maximum packet size").
    /// </summary>
    /// <param name="size">
    /// The answer's size in bytes, without a reason string.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when it fits; <see langword="false"/> when the close has begun.
    /// </returns>
    private bool AnswerFits(int size)
    {
        if (size <= _clientMaxPacketSize)
        {
            return true;
        }

        BeginClose(MqttCloseReason.ProtocolError, MqttReasonCode.PacketTooLarge, reasonString: null);
        return false;
    }

    /// <summary>
    /// Ends the connection for a handler that threw out of a call on the read path (plan decision 17): logs the
    /// exception as an error that names the method, and begins the close with 0x83 and
    /// <see cref="MqttCloseReason.HandlerThrew"/>. The packet being handled gets no answer.
    /// </summary>
    /// <param name="method">
    /// The name of the handler method that threw.
    /// </param>
    /// <param name="exception">
    /// What it threw.
    /// </param>
    private void CloseHandlerThrew(string method, Exception exception)
    {
        ServerLog.HandlerThrew(_server.Logger, ClientId, method, exception);
        BeginClose(MqttCloseReason.HandlerThrew, MqttReasonCode.ImplementationSpecificError, reasonString: null);
    }
}
