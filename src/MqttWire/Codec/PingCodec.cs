namespace MqttWire.Codec;

/// <summary>
/// The MQTT 5 keep-alive packets PINGREQ and PINGRESP (MQTT 5, 3.12 and 3.13); both are a fixed header and nothing
/// else.
/// </summary>
internal static class PingCodec
{
    /// <summary>
    /// The PINGREQ packet; constant data, so reading it allocates nothing.
    /// </summary>
    public static ReadOnlySpan<byte> PingReq => [0xC0, 0x00];

    /// <summary>
    /// The PINGRESP packet; constant data, so reading it allocates nothing.
    /// </summary>
    public static ReadOnlySpan<byte> PingResp => [0xD0, 0x00];

    /// <summary>
    /// Decodes one PINGREQ or PINGRESP packet.
    /// </summary>
    /// <param name="packet">
    /// Exactly one framed PINGREQ or PINGRESP packet.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/>, or <see cref="MqttReasonCode.MalformedPacket"/> when the remaining length
    /// is not 0.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The buffer is not exactly one framed PINGREQ or PINGRESP packet (a caller bug, see
    /// <see cref="PacketHeader.ReadExact(ReadOnlySpan{byte}, PacketType, PacketType)"/>).
    /// </exception>
    public static MqttReasonCode TryDecode(ReadOnlySpan<byte> packet)
        => PacketHeader.ReadExact(packet, PacketType.PingReq, PacketType.PingResp).RemainingLength == 0
            ? MqttReasonCode.Success
            : MqttReasonCode.MalformedPacket;
}
