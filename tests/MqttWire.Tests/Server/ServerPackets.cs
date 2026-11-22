using System.Text;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Byte sequences of whole packets that a client sends the server or the server writes, for the server tests. The
/// PUBLISH and acknowledgement packets are those of <c>ClientPackets</c>.
/// </summary>
internal static class ServerPackets
{
    /// <summary>
    /// CONNECT with client id "c1", keep-alive 30 and no properties.
    /// </summary>
    public static readonly byte[] Connect = Hex.Parse("10 0F 00 04 4D 51 54 54 05 02 00 1E 00 00 02 63 31");

    /// <summary>
    /// CONNECT with client id "c1", keep-alive 0 and no properties.
    /// </summary>
    public static readonly byte[] ConnectKeepAlive0 = Hex.Parse("10 0F 00 04 4D 51 54 54 05 02 00 00 00 00 02 63 31");

    /// <summary>
    /// CONNECT with client id "c1", keep-alive 61 and no properties.
    /// </summary>
    public static readonly byte[] ConnectKeepAlive61 = Hex.Parse("10 0F 00 04 4D 51 54 54 05 02 00 3D 00 00 02 63 31");

    /// <summary>
    /// CONNECT of MQTT 3.1.1 (protocol level 4) with client id "c1" and keep-alive 30.
    /// </summary>
    public static readonly byte[] ConnectVersion4 = Hex.Parse("10 0E 00 04 4D 51 54 54 04 02 00 1E 00 02 63 31");

    /// <summary>
    /// CONNECT with an empty client id and keep-alive 0.
    /// </summary>
    public static readonly byte[] ConnectEmptyClientId = Hex.Parse("10 0D 00 04 4D 51 54 54 05 02 00 00 00 00 00");

    /// <summary>
    /// CONNECT with client id "c1", keep-alive 30 and a Will on topic "t" with an empty payload.
    /// </summary>
    public static readonly byte[] ConnectWithWill =
        Hex.Parse("10 15 00 04 4D 51 54 54 05 06 00 1E 00 00 02 63 31 00 00 01 74 00 00");

    /// <summary>
    /// CONNECT with client id "c1", keep-alive 30 and Authentication Method "x".
    /// </summary>
    public static readonly byte[] ConnectWithAuthMethod =
        Hex.Parse("10 13 00 04 4D 51 54 54 05 02 00 1E 04 15 00 01 78 00 02 63 31");

    /// <summary>
    /// CONNECT with client id "c1", keep-alive 30, user "u" and password "p".
    /// </summary>
    public static readonly byte[] ConnectUserAndPassword =
        Hex.Parse("10 15 00 04 4D 51 54 54 05 C2 00 1E 00 00 02 63 31 00 01 75 00 01 70");

    /// <summary>
    /// CONNECT with client id "c1", keep-alive 30 and Session Expiry Interval 60.
    /// </summary>
    public static readonly byte[] ConnectSessionExpiry60 =
        Hex.Parse("10 14 00 04 4D 51 54 54 05 02 00 1E 05 11 00 00 00 3C 00 02 63 31");

    /// <summary>
    /// CONNECT with client id "c1", keep-alive 30 and Receive Maximum 1.
    /// </summary>
    public static readonly byte[] ConnectReceiveMaximum1 =
        Hex.Parse("10 12 00 04 4D 51 54 54 05 02 00 1E 03 21 00 01 00 02 63 31");

    /// <summary>
    /// CONNECT with client id "c1", keep-alive 30 and Maximum Packet Size 4,096.
    /// </summary>
    public static readonly byte[] ConnectMaxPacketSize4096 =
        Hex.Parse("10 14 00 04 4D 51 54 54 05 02 00 1E 05 27 00 00 10 00 00 02 63 31");

    /// <summary>
    /// CONNECT with client id "c1", keep-alive 30 and Request Problem Information 0.
    /// </summary>
    public static readonly byte[] ConnectNoProblemInfo =
        Hex.Parse("10 11 00 04 4D 51 54 54 05 02 00 1E 02 17 00 00 02 63 31");

    /// <summary>
    /// CONNACK with success: Receive Maximum 256 and no subscription identifiers.
    /// </summary>
    public static readonly byte[] ConnAck = Hex.Parse("20 08 00 00 05 21 01 00 29 00");

    /// <summary>
    /// CONNACK with success and Server Keep Alive 60.
    /// </summary>
    public static readonly byte[] ConnAckKeepAlive60 = Hex.Parse("20 0B 00 00 08 21 01 00 29 00 13 00 3C");

    /// <summary>
    /// CONNACK with success and Maximum QoS 1.
    /// </summary>
    public static readonly byte[] ConnAckMaxQoS1 = Hex.Parse("20 0A 00 00 07 21 01 00 24 01 29 00");

    /// <summary>
    /// CONNACK with success and Session Expiry Interval 0.
    /// </summary>
    public static readonly byte[] ConnAckSessionExpiry0 = Hex.Parse("20 0D 00 00 0A 11 00 00 00 00 21 01 00 29 00");

    /// <summary>
    /// CONNACK refusing the Will with 0x87 and the reason string "Will messages are not supported.".
    /// </summary>
    public static readonly byte[] ConnAckWillRefused =
        [.. Hex.Parse("20 26 00 87 23 1F 00 20"), .. Encoding.UTF8.GetBytes("Will messages are not supported.")];

    /// <summary>
    /// SUBSCRIBE with packet identifier 1 for "a/b" at QoS 1.
    /// </summary>
    public static readonly byte[] Subscribe = Hex.Parse("82 09 00 01 00 00 03 61 2F 62 01");

    /// <summary>
    /// SUBACK for packet identifier 1 granting QoS 1.
    /// </summary>
    public static readonly byte[] SubAckGrantedQoS1 = Hex.Parse("90 04 00 01 00 01");

    /// <summary>
    /// SUBACK for packet identifier 1 refusing with 0x87, not authorized.
    /// </summary>
    public static readonly byte[] SubAckNotAuthorized = Hex.Parse("90 04 00 01 00 87");

    /// <summary>
    /// UNSUBSCRIBE with packet identifier 2 for "a/b".
    /// </summary>
    public static readonly byte[] Unsubscribe = Hex.Parse("A2 08 00 02 00 00 03 61 2F 62");

    /// <summary>
    /// UNSUBACK for packet identifier 2 with success.
    /// </summary>
    public static readonly byte[] UnsubAck = Hex.Parse("B0 04 00 02 00 00");

    /// <summary>
    /// AUTH with no reason code and no properties.
    /// </summary>
    public static readonly byte[] Auth = Hex.Parse("F0 00");

    /// <summary>
    /// PUBACK for packet identifier 1 refusing with 0x97, quota exceeded, and the reason string
    /// "The message is too large.".
    /// </summary>
    public static readonly byte[] TooLargePubAck1 =
        [.. Hex.Parse("40 20 00 01 97 1C 1F 00 19"), .. Encoding.UTF8.GetBytes("The message is too large.")];

    /// <summary>
    /// PUBREC for packet identifier 1 refusing with 0x97, quota exceeded, and the reason string
    /// "The message is too large.".
    /// </summary>
    public static readonly byte[] TooLargePubRec1 =
        [.. Hex.Parse("50 20 00 01 97 1C 1F 00 19"), .. Encoding.UTF8.GetBytes("The message is too large.")];

    /// <summary>
    /// Builds an MQTT 5 CONNECT with keep-alive 30 from its connect flags, its properties and its payload, for the
    /// CONNECT packets no constant above covers. The bytes are put together as given, so they may be ones the
    /// codec refuses.
    /// </summary>
    /// <param name="flags">
    /// The connect flags, for example <c>0x02</c> for a clean start alone.
    /// </param>
    /// <param name="properties">
    /// The properties, without their length; the length is written in front of them.
    /// </param>
    /// <param name="payload">
    /// The payload, each field already with its length prefix (see <see cref="Field"/>).
    /// </param>
    /// <returns>
    /// The packet's bytes, fixed header included.
    /// </returns>
    public static byte[] BuildConnect(byte flags, ReadOnlySpan<byte> properties, ReadOnlySpan<byte> payload)
    {
        byte[] variableHeader = [0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05, flags, 0x00, 0x1E];
        byte[] propertyLength = VariableByteInteger(properties.Length);
        int remaining = variableHeader.Length + propertyLength.Length + properties.Length + payload.Length;
        return [0x10, .. VariableByteInteger(remaining), .. variableHeader, .. propertyLength, .. properties, .. payload];
    }

    /// <summary>
    /// Builds a field with its two-byte length prefix, as a UTF-8 string or binary data is carried.
    /// </summary>
    /// <param name="value">
    /// The field's bytes, at most 65,535.
    /// </param>
    /// <returns>
    /// The length prefix followed by the bytes.
    /// </returns>
    public static byte[] Field(ReadOnlySpan<byte> value) => [(byte)(value.Length >> 8), (byte)value.Length, .. value];

    /// <summary>
    /// Builds a CONNACK refusing the connection with a reason code and no properties.
    /// </summary>
    /// <param name="code">
    /// The reason code.
    /// </param>
    /// <returns>
    /// The packet's bytes.
    /// </returns>
    public static byte[] ConnAckRefused(MqttReasonCode code) => [0x20, 0x03, 0x00, (byte)code, 0x00];

    /// <summary>
    /// Builds a DISCONNECT with a reason code and no properties.
    /// </summary>
    /// <param name="code">
    /// The reason code.
    /// </param>
    /// <returns>
    /// The packet's bytes.
    /// </returns>
    public static byte[] Disconnect(MqttReasonCode code) => [0xE0, 0x01, (byte)code];

    /// <summary>
    /// Encodes a variable byte integer (MQTT 5, 1.5.5).
    /// </summary>
    /// <param name="value">
    /// The value, at most 268,435,455.
    /// </param>
    /// <returns>
    /// The one to four bytes.
    /// </returns>
    private static byte[] VariableByteInteger(int value)
    {
        List<byte> bytes = [];
        do
        {
            byte digit = (byte)(value % 128);
            value /= 128;
            bytes.Add(value > 0 ? (byte)(digit | 0x80) : digit);
        }
        while (value > 0);

        return [.. bytes];
    }
}
