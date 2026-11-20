using MQTTnet.Packets;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests the PINGREQ and PINGRESP packets.
/// </summary>
public sealed class PingCodecTests
{
    /// <summary>
    /// Proves the two constant packets are the bytes the standard gives.
    /// </summary>
    [Fact]
    public void PingReqAndPingResp_AreTheKnownBytes()
    {
        // Act & Assert
        PingCodec.PingReq.ToArray().ShouldBe(KnownPackets.PingReq);
        PingCodec.PingResp.ToArray().ShouldBe(KnownPackets.PingResp);
    }

    /// <summary>
    /// Proves both constant packets decode as success.
    /// </summary>
    [Fact]
    public void TryDecode_PingReqAndPingResp_AreSuccess()
    {
        // Act
        MqttReasonCode request = PingCodec.TryDecode(PingCodec.PingReq);
        MqttReasonCode response = PingCodec.TryDecode(PingCodec.PingResp);

        // Assert
        request.ShouldBe(MqttReasonCode.Success);
        response.ShouldBe(MqttReasonCode.Success);
    }

    /// <summary>
    /// Proves a PINGREQ or PINGRESP with a body is malformed.
    /// </summary>
    /// <param name="hex">
    /// The ping with a one-byte body.
    /// </param>
    [Theory]
    [InlineData("C0 01 00")]
    [InlineData("D0 01 00")]
    public void TryDecode_PingWithABody_IsMalformed(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act
        MqttReasonCode result = PingCodec.TryDecode(packet);

        // Assert
        result.ShouldBe(MqttReasonCode.MalformedPacket);
    }

    /// <summary>
    /// Proves a buffer that is not exactly one framed ping is a caller bug.
    /// </summary>
    /// <param name="hex">
    /// The unusable buffer.
    /// </param>
    [Theory]
    [InlineData("E0 00")]
    [InlineData("C0")]
    [InlineData("C0 00 00")]
    public void TryDecode_NotExactlyOnePing_Throws(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act & Assert
        Should.Throw<ArgumentException>(() => PingCodec.TryDecode(packet));
    }

    /// <summary>
    /// Proves the pings MQTTnet encodes are the constant packets and MQTTnet decodes the constant packets.
    /// </summary>
    [Fact]
    public void PingReqAndPingResp_MqttNetAgrees()
    {
        // Act
        byte[] request = MqttNetFormatter.Encode(new MqttPingReqPacket());
        byte[] response = MqttNetFormatter.Encode(new MqttPingRespPacket());
        MqttPacket decodedRequest = MqttNetFormatter.Decode(PingCodec.PingReq);
        MqttPacket decodedResponse = MqttNetFormatter.Decode(PingCodec.PingResp);

        // Assert
        request.ShouldBe(PingCodec.PingReq.ToArray());
        response.ShouldBe(PingCodec.PingResp.ToArray());
        decodedRequest.ShouldBeOfType<MqttPingReqPacket>();
        decodedResponse.ShouldBeOfType<MqttPingRespPacket>();
    }
}
