namespace MqttWire.Tests.Codec;

/// <summary>
/// Byte sequences of whole packets that the tests decode and expect the encoders to write.
/// </summary>
internal static class KnownPackets
{
    /// <summary>
    /// CONNECT with client id "c1", user name "u", password 0x70, keep-alive 15, clean start and a receive maximum
    /// of 1,024.
    /// </summary>
    public static readonly byte[] Connect = Hex.Parse("10 18 00 04 4D 51 54 54 05 C2 00 0F 03 21 04 00 00 02 63 31 00 01 75 00 01 70");

    /// <summary>
    /// CONNECT with client id "c" and nothing else: no user name, no password, no Will, no properties.
    /// </summary>
    public static readonly byte[] ConnectMinimal = Hex.Parse("10 0E 00 04 4D 51 54 54 05 02 00 00 00 00 01 63");

    /// <summary>
    /// CONNECT with client id "c" and a Will with topic "w" and payload "x".
    /// </summary>
    public static readonly byte[] ConnectWithWill = Hex.Parse("10 15 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 00 00 01 77 00 01 78");

    /// <summary>
    /// CONNACK with success, a receive maximum of 256 and subscription identifiers not available.
    /// </summary>
    public static readonly byte[] ConnAck = Hex.Parse("20 08 00 00 05 21 01 00 29 00");

    /// <summary>
    /// PUBLISH at QoS 0 with topic "t" and an empty payload.
    /// </summary>
    public static readonly byte[] PublishQoS0 = Hex.Parse("30 04 00 01 74 00");

    /// <summary>
    /// PUBLISH at QoS 1 with topic "a/b", packet identifier 10 and payload "hi".
    /// </summary>
    public static readonly byte[] PublishQoS1 = Hex.Parse("32 0A 00 03 61 2F 62 00 0A 00 68 69");

    /// <summary>
    /// PUBLISH with DUP, QoS 2 and retain, topic "t", packet identifier 1 and payload "x".
    /// </summary>
    public static readonly byte[] PublishQoS2 = Hex.Parse("3D 07 00 01 74 00 01 00 78");

    /// <summary>
    /// PUBACK for packet identifier 10 in the short form: success, no reason code.
    /// </summary>
    public static readonly byte[] PubAck = Hex.Parse("40 02 00 0A");

    /// <summary>
    /// PUBREC for packet identifier 10 refused with quota exceeded and no reason string.
    /// </summary>
    public static readonly byte[] PubRecRefused = Hex.Parse("50 03 00 0A 97");

    /// <summary>
    /// PUBREL for packet identifier 10 in the short form; its flags are 0b0010.
    /// </summary>
    public static readonly byte[] PubRel = Hex.Parse("62 02 00 0A");

    /// <summary>
    /// PUBCOMP for packet identifier 10 with packet identifier not found.
    /// </summary>
    public static readonly byte[] PubCompNotFound = Hex.Parse("70 03 00 0A 92");

    /// <summary>
    /// PUBACK for packet identifier 10 with quota exceeded and the reason string "x".
    /// </summary>
    public static readonly byte[] PubAckWithReason = Hex.Parse("40 08 00 0A 97 04 1F 00 01 78");

    /// <summary>
    /// PINGREQ.
    /// </summary>
    public static readonly byte[] PingReq = Hex.Parse("C0 00");

    /// <summary>
    /// PINGRESP.
    /// </summary>
    public static readonly byte[] PingResp = Hex.Parse("D0 00");

    /// <summary>
    /// DISCONNECT in the short form: normal disconnection, no reason code.
    /// </summary>
    public static readonly byte[] Disconnect = Hex.Parse("E0 00");

    /// <summary>
    /// DISCONNECT with the session taken over and nothing else.
    /// </summary>
    public static readonly byte[] DisconnectTakenOver = Hex.Parse("E0 01 8E");

    /// <summary>
    /// DISCONNECT with the session taken over and the reason string "x".
    /// </summary>
    public static readonly byte[] DisconnectWithReason = Hex.Parse("E0 06 8E 04 1F 00 01 78");
}
