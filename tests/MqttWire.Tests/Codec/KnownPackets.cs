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
}
