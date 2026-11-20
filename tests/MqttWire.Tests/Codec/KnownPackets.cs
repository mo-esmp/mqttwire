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
}
