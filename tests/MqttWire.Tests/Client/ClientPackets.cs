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
}
