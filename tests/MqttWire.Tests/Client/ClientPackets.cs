using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Client;

/// <summary>
/// Byte sequences of whole packets that a client writes or a server sends it, for the client tests.
/// </summary>
internal static class ClientPackets
{
    /// <summary>
    /// The CONNECT the harness's client writes: client id "c1", user "u", password "p", keep-alive 15, receive
    /// maximum 1,024 and maximum packet size 1,048,576.
    /// </summary>
    public static readonly byte[] Connect = KnownPackets.ConnectWithMaximumPacketSize;

    /// <summary>
    /// CONNACK with success and no properties.
    /// </summary>
    public static readonly byte[] ConnAckAccepted = Hex.Parse("20 03 00 00 00");

    /// <summary>
    /// CONNACK refusing the connection with 0x86, bad user name or password.
    /// </summary>
    public static readonly byte[] ConnAckBadUserNameOrPassword = Hex.Parse("20 03 00 86 00");

    /// <summary>
    /// CONNACK with success and Session Present set, which a client that asked for a clean start cannot accept.
    /// </summary>
    public static readonly byte[] ConnAckSessionPresent = Hex.Parse("20 03 01 00 00");

    /// <summary>
    /// CONNACK with success, reason string "x" and the user property a=b.
    /// </summary>
    public static readonly byte[] ConnAckWithReason = Hex.Parse("20 0E 00 00 0B 1F 00 01 78 26 00 01 61 00 01 62");

    /// <summary>
    /// DISCONNECT with success, normal disconnection: what the client sends when it is disposed.
    /// </summary>
    public static readonly byte[] Disconnect = Hex.Parse("E0 00");

    /// <summary>
    /// DISCONNECT with 0x81, malformed packet.
    /// </summary>
    public static readonly byte[] DisconnectMalformedPacket = Hex.Parse("E0 01 81");

    /// <summary>
    /// DISCONNECT with 0x82, protocol error.
    /// </summary>
    public static readonly byte[] DisconnectProtocolError = Hex.Parse("E0 01 82");

    /// <summary>
    /// DISCONNECT with 0x8E, session taken over, as a server sends it.
    /// </summary>
    public static readonly byte[] DisconnectSessionTakenOver = Hex.Parse("E0 01 8E");

    /// <summary>
    /// DISCONNECT with 0x95, packet too large.
    /// </summary>
    public static readonly byte[] DisconnectPacketTooLarge = Hex.Parse("E0 01 95");

    /// <summary>
    /// PINGRESP.
    /// </summary>
    public static readonly byte[] PingResp = Hex.Parse("D0 00");

    /// <summary>
    /// CONNACK with success and Receive Maximum 2.
    /// </summary>
    public static readonly byte[] ConnAckReceiveMaximum2 = Hex.Parse("20 06 00 00 03 21 00 02");

    /// <summary>
    /// CONNACK with success and Receive Maximum 65,535, more than the client ever uses.
    /// </summary>
    public static readonly byte[] ConnAckReceiveMaximum65535 = Hex.Parse("20 06 00 00 03 21 FF FF");

    /// <summary>
    /// CONNACK with success and Maximum QoS 1.
    /// </summary>
    public static readonly byte[] ConnAckMaximumQoS1 = Hex.Parse("20 05 00 00 02 24 01");

    /// <summary>
    /// CONNACK with success and Maximum Packet Size 32.
    /// </summary>
    public static readonly byte[] ConnAckMaximumPacketSize32 = Hex.Parse("20 08 00 00 05 27 00 00 00 20");

    /// <summary>
    /// PUBLISH at QoS 0 to "a/b" with payload "hi".
    /// </summary>
    public static readonly byte[] PublishQoS0 = Hex.Parse("30 08 00 03 61 2F 62 00 68 69");

    /// <summary>
    /// PUBLISH at QoS 1 to "a/b" with payload "hi" and packet identifier 1.
    /// </summary>
    public static readonly byte[] PublishQoS1 = Hex.Parse("32 0A 00 03 61 2F 62 00 01 00 68 69");

    /// <summary>
    /// PUBLISH at QoS 2 to "a/b" with payload "hi" and packet identifier 1.
    /// </summary>
    public static readonly byte[] PublishQoS2 = Hex.Parse("34 0A 00 03 61 2F 62 00 01 00 68 69");

    /// <summary>
    /// PUBACK with success for packet identifier 1.
    /// </summary>
    public static readonly byte[] PubAck1 = Hex.Parse("40 02 00 01");

    /// <summary>
    /// PUBREC with success for packet identifier 1.
    /// </summary>
    public static readonly byte[] PubRec1 = Hex.Parse("50 02 00 01");

    /// <summary>
    /// PUBREL with success for packet identifier 1.
    /// </summary>
    public static readonly byte[] PubRel1 = Hex.Parse("62 02 00 01");

    /// <summary>
    /// PUBCOMP with success for packet identifier 1.
    /// </summary>
    public static readonly byte[] PubComp1 = Hex.Parse("70 02 00 01");

    /// <summary>
    /// PUBREL with 0x92, packet identifier not found, for packet identifier 7.
    /// </summary>
    public static readonly byte[] PubRelNotFound7 = Hex.Parse("62 03 00 07 92");
}
