using System.Buffers;
using MQTTnet.Adapter;
using MQTTnet.Formatter;
using MQTTnet.Packets;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Encodes and decodes MQTT 5 packets with the MQTTnet library, as an independent cross-check of this codec.
/// It allocates freely; it is for tests only.
/// </summary>
internal static class MqttNetFormatter
{
    /// <summary>
    /// The initial size of the MQTTnet write buffer.
    /// </summary>
    private const int BufferSize = 4096;

    /// <summary>
    /// The size the MQTTnet write buffer may grow to.
    /// </summary>
    private const int MaxBufferSize = 65536;

    /// <summary>
    /// Encodes a packet as MQTTnet does for MQTT 5.
    /// </summary>
    /// <param name="packet">
    /// The MQTTnet packet to encode.
    /// </param>
    /// <returns>
    /// The whole framed packet, fixed header included.
    /// </returns>
    public static byte[] Encode(MqttPacket packet) => CreateAdapter().Encode(packet).ToArray();

    /// <summary>
    /// Decodes a whole framed packet with MQTTnet as MQTT 5.
    /// </summary>
    /// <param name="packet">
    /// Exactly one framed packet.
    /// </param>
    /// <returns>
    /// The MQTTnet packet.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The bytes are not exactly one framed packet.
    /// </exception>
    public static MqttPacket Decode(ReadOnlySpan<byte> packet)
    {
        // The packet type is not known here, so the header is read directly rather than through ReadExact.
        if (PacketHeader.TryReadPacket(packet, out PacketHeader header) != OperationStatus.Done || packet.Length != header.TotalLength)
        {
            throw new ArgumentException("The bytes are not exactly one framed packet.", nameof(packet));
        }

        byte[] body = packet[header.HeaderLength..].ToArray();
        return CreateAdapter().Decode(new ReceivedMqttPacket(packet[0], new ArraySegment<byte>(body), packet.Length));
    }

    /// <summary>
    /// Creates an MQTTnet adapter for MQTT 5 with its own buffer, so calls never share state.
    /// </summary>
    /// <returns>
    /// A new adapter.
    /// </returns>
    private static MqttPacketFormatterAdapter CreateAdapter()
        => new(MqttProtocolVersion.V500, new MqttBufferWriter(BufferSize, MaxBufferSize));
}
