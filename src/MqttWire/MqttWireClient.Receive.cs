using MqttWire.Client;
using MqttWire.Codec;

namespace MqttWire;

/// <content>
/// Receiving: the PUBLISH and PUBREL a server sends, at QoS 0, 1 and 2.
/// </content>
public sealed partial class MqttWireClient
{
    /// <summary>
    /// The QoS 2 identifiers handed on and not released; used by the read loop only.
    /// </summary>
    private readonly IncomingQoS2 _incomingQoS2 = new();

    /// <summary>
    /// Handles a PUBLISH from the server: hands it to <c>OnMessage</c>, then answers it by its QoS. A QoS 2
    /// PUBLISH whose identifier is remembered is answered with PUBREC again and not handed on again.
    /// </summary>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code when the result is <see cref="EndStep.DisconnectAndClose"/> or
    /// <see cref="EndStep.DisconnectRecorded"/>.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.None"/> at QoS 0; <see cref="EndStep.SendReply"/> for the PUBACK or PUBREC;
    /// <see cref="EndStep.DisconnectAndClose"/> for a PUBLISH the codec rejects (its code), one with a Topic Alias
    /// (0x94), or a QoS 2 one that passes the limit of remembered identifiers (0x93);
    /// <see cref="EndStep.DisconnectRecorded"/> when <c>OnMessage</c> threw, in which case no reply is sent.
    /// </returns>
    private EndStep HandlePublish(ReadOnlySpan<byte> packet, out MqttReasonCode disconnect)
    {
        disconnect = PublishCodec.TryDecode(packet, PacketSource.Server, out PublishPacket publish);
        if (disconnect != MqttReasonCode.Success)
        {
            return EndStep.DisconnectAndClose;
        }

        if (publish.HasTopicAlias)
        {
            // The client announces no Topic Alias Maximum, so the server may not use one.
            disconnect = MqttReasonCode.TopicAliasInvalid;
            return EndStep.DisconnectAndClose;
        }

        if (publish.QoS == MqttQoS.ExactlyOnce && _incomingQoS2.Contains(publish.PacketIdentifier))
        {
            // A repeat before PUBREL, with DUP or without: answered, not handed on again.
            return RecordReply(PacketType.PubRec, publish.PacketIdentifier, MqttReasonCode.Success);
        }

        // Every QoS 2 message waiting for its PUBREL holds a place in the Receive Maximum the client announced, and
        // a new QoS 1 or 2 message needs one more (MQTT-3.3.4-9). The table is exactly that size.
        if (publish.QoS != MqttQoS.AtMostOnce && _incomingQoS2.IsFull)
        {
            disconnect = MqttReasonCode.ReceiveMaximumExceeded;
            return EndStep.DisconnectAndClose;
        }

        if (!TryHandOn(in publish, out MqttClientCloseInfo failure))
        {
            // OnMessage threw: no acknowledgement for this message, and the connection ends with 0x83.
            disconnect = MqttReasonCode.ImplementationSpecificError;
            return TryBeginEnd(failure, EndMode.Close) ? EndStep.DisconnectRecorded : EndStep.None;
        }

        switch (publish.QoS)
        {
            case MqttQoS.AtLeastOnce:
                return RecordReply(PacketType.PubAck, publish.PacketIdentifier, MqttReasonCode.Success);

            case MqttQoS.ExactlyOnce:
                // Cannot fail: the identifier is absent and the table has room, and only this loop changes it.
                _incomingQoS2.TryAdd(publish.PacketIdentifier);
                return RecordReply(PacketType.PubRec, publish.PacketIdentifier, MqttReasonCode.Success);

            default:
                return EndStep.None;
        }
    }

    /// <summary>
    /// Calls <c>OnMessage</c> with views into the receive buffer, valid until it returns.
    /// </summary>
    /// <param name="publish">
    /// The decoded PUBLISH.
    /// </param>
    /// <param name="failure">
    /// When the handler threw, the close information naming the exception.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when there is no handler or it returned; <see langword="false"/> when it threw.
    /// </returns>
    private bool TryHandOn(in PublishPacket publish, out MqttClientCloseInfo failure)
    {
        failure = default;
        MqttMessageHandler? handler = MessageHandler;
        if (handler is null)
        {
            return true;
        }

        MqttPublish message = new(publish.Topic, publish.Payload, publish.QoS, publish.Retain);
        try
        {
            handler(in message);
            return true;
        }
        catch (Exception ex)
        {
            failure = new MqttClientCloseInfo(MqttClientCloseReason.MessageHandlerThrew, MqttReasonCode.ImplementationSpecificError, null, ex);
            return false;
        }
    }

    /// <summary>
    /// Handles a PUBREL from the server: forgets the identifier and answers with PUBCOMP, which is 0x92 when the
    /// identifier was not remembered.
    /// </summary>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code when the result is <see cref="EndStep.DisconnectAndClose"/>.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.SendReply"/> for the PUBCOMP; <see cref="EndStep.DisconnectAndClose"/> for a malformed
    /// PUBREL.
    /// </returns>
    private EndStep HandlePubRel(ReadOnlySpan<byte> packet, out MqttReasonCode disconnect)
    {
        disconnect = AckCodec.TryDecode(packet, out AckPacket ack);
        if (disconnect != MqttReasonCode.Success)
        {
            return EndStep.DisconnectAndClose;
        }

        return RecordReply(
            PacketType.PubComp,
            ack.PacketIdentifier,
            _incomingQoS2.Remove(ack.PacketIdentifier) ? MqttReasonCode.Success : MqttReasonCode.PacketIdentifierNotFound);
    }
}
